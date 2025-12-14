# frozen_string_literal: true

require_relative "internal/commands"
require_relative "internal/topic_name"
require_relative "message_id"
require_relative "producer_message"
require_relative "errors"

module Pulsar
  # Producer sends messages to a Pulsar topic.
  #
  # A producer is created via Client#create_producer and is bound to a single
  # topic. Messages can be sent synchronously or asynchronously.
  #
  # Usage:
  #   producer = client.create_producer("my-topic")
  #
  #   # Synchronous send - blocks until ack received
  #   message_id = producer.send(ProducerMessage.new(payload: "Hello"))
  #
  #   producer.close
  class Producer
    # Default send timeout in seconds
    DEFAULT_SEND_TIMEOUT = 30

    # Default max pending messages
    DEFAULT_MAX_PENDING = 1000

    # @return [String] fully qualified topic name
    attr_reader :topic

    # @return [String] producer name (assigned by broker if not specified)
    attr_reader :name

    # @return [Integer] producer ID
    attr_reader :producer_id

    # @return [Boolean] whether the producer is ready
    attr_reader :ready

    alias ready? ready

    # Initialize a new producer
    #
    # @param connection_pool [Internal::ConnectionPool] connection pool
    # @param lookup_service [Internal::LookupService] lookup service
    # @param topic [String] topic name
    # @param options [Hash] producer options
    # @option options [String] :name producer name (auto-generated if nil)
    # @option options [Integer] :send_timeout send timeout in seconds
    # @option options [Integer] :max_pending_messages max pending messages
    def initialize(connection_pool:, lookup_service:, topic:, options: {})
      @connection_pool = connection_pool
      @lookup_service = lookup_service
      @topic_name = Internal::TopicName.new(topic)
      @topic = @topic_name.to_s
      @options = default_options.merge(options)

      @name = @options[:name]
      @producer_id = generate_producer_id
      @sequence_id = 0
      @connection = nil
      @ready = false
      @closed = false
      @mutex = Mutex.new

      # Pending messages waiting for receipts
      # Maps sequence_id => { callback:, sent_at: }
      @pending_messages = {}
      @pending_mutex = Mutex.new

      # Timeout checker thread
      @timeout_thread = nil

      # Connect and create producer on broker
      connect_to_broker

      # Start timeout checker after producer is ready
      start_timeout_checker
    end

    # Synchronous send - blocks until ack received
    #
    # @param message [ProducerMessage] message to send
    # @return [MessageId] the message ID assigned by broker
    # @raise [ProducerError] if send fails
    # @raise [TimeoutError] if send times out
    # @raise [ConnectionClosedError] if connection is closed
    def send(message)
      ensure_ready!
      validate_message!(message)

      queue = Queue.new
      internal_send(message) do |message_id, error|
        queue.push([message_id, error])
      end

      message_id, error = queue.pop
      puts "[Producer] Sent message to topic '#{@topic}': #{message_id}, #{message.payload}" unless error
      raise error if error

      message_id
    end

    # Close the producer
    #
    # Releases resources and closes the producer on the broker.
    # Waits for pending messages to complete (with timeout).
    #
    # @return [void]
    def close
      @mutex.synchronize do
        return if @closed

        @closed = true
        @ready = false
      end

      # Stop timeout checker
      stop_timeout_checker

      # Fail all pending messages
      fail_pending_messages(ProducerError.new("Producer closed"))

      # Send close command to broker if connected
      if @connection&.ready?
        begin
          close_cmd = Internal::Commands.close_producer(
            producer_id: @producer_id,
            request_id: @connection.next_request_id
          )
          @connection.send_request(close_cmd, timeout: 5)
        rescue
          # Ignore errors during close
        end

        @connection.unregister_producer(@producer_id)
      end
    end

    # Check if the producer is closed
    # @return [Boolean]
    def closed?
      @closed
    end

    # Handle send receipt from broker (called by connection)
    # @param receipt [Proto::CommandSendReceipt]
    # @api private
    def handle_send_receipt(receipt)
      sequence_id = receipt.sequence_id
      pending = remove_pending(sequence_id)

      return unless pending

      message_id = MessageId.from_proto(receipt.message_id)
      invoke_callback(pending[:callback], message_id, nil)
    end

    # Handle send error from broker (called by connection)
    # @param error [Proto::CommandSendError]
    # @api private
    def handle_send_error(error)
      sequence_id = error.sequence_id
      pending = remove_pending(sequence_id)

      return unless pending

      exception = ProducerError.new("Send failed: #{error.error} - #{error.message}")
      invoke_callback(pending[:callback], nil, exception)
    end

    # Handle close notification from broker
    # @api private
    def handle_close
      @mutex.synchronize do
        @ready = false
      end

      # Fail all pending messages
      fail_pending_messages(ConnectionClosedError.new("Broker requested close"))

      puts "[Producer] Broker requested close for producer '#{@name}'"
    end

    private

    # Default options for producer
    # @return [Hash]
    def default_options
      {
        name: nil,
        send_timeout: DEFAULT_SEND_TIMEOUT,
        max_pending_messages: DEFAULT_MAX_PENDING
      }
    end

    # Generate a unique producer ID
    # @return [Integer]
    def generate_producer_id
      # Use object_id as a simple unique ID
      # In a real implementation, this might be managed by the client
      object_id & 0x7FFFFFFF
    end

    # Connect to the broker serving this topic
    def connect_to_broker
      # Look up the broker for this topic
      @connection = @lookup_service.lookup_connection(@topic)

      # Register this producer with the connection for callbacks
      @connection.register_producer(@producer_id, self)

      # Create producer on the broker
      create_producer_on_broker
    end

    # Send PRODUCER command to broker and wait for success
    def create_producer_on_broker
      request_id = @connection.next_request_id

      producer_cmd = Internal::Commands.producer(
        producer_id: @producer_id,
        request_id: request_id,
        topic: @topic,
        producer_name: @options[:name]
      )

      response = @connection.send_request(producer_cmd, timeout: @options[:send_timeout])

      # Response is CommandProducerSuccess
      @name = response.producer_name
      @ready = true

      puts "[Producer] Created producer '#{@name}' for topic '#{@topic}'"
      puts "[Producer]   Producer ID: #{@producer_id}"
      puts "[Producer]   Last Sequence ID: #{response.last_sequence_id}"
      puts "[Producer]   Schema Version: #{response.schema_version.inspect}" if response.schema_version && !response.schema_version.empty?
    end

    # Internal send implementation
    # @param message [ProducerMessage]
    # @param callback [Proc] callback to invoke with result
    def internal_send(message, &callback)
      # Check pending message limit
      if pending_count >= @options[:max_pending_messages]
        error = ProducerError.new("Max pending messages (#{@options[:max_pending_messages]}) exceeded")
        invoke_callback(callback, nil, error)
        return
      end

      # Get next sequence ID
      sequence_id = next_sequence_id

      # Build message metadata
      metadata = Internal::Commands.message_metadata(
        producer_name: @name,
        sequence_id: sequence_id,
        publish_time: (Time.now.to_f * 1000).to_i,
        properties: message.properties,
        partition_key: message.key,
        event_time: message.event_time_millis
      )

      # Build CommandSend
      command_send = Internal::Commands.send_command(
        producer_id: @producer_id,
        sequence_id: sequence_id,
        num_messages: 1
      )

      # Register pending message before sending
      register_pending(sequence_id, callback)

      begin
        # Encode and send the message frame
        frame = Internal::Commands.encode_message(command_send, metadata, message.payload_bytes)
        @connection.send_command_raw(frame)
      rescue => e
        # Remove from pending and fail
        remove_pending(sequence_id)
        invoke_callback(callback, nil, e)
      end
    end

    # Get next sequence ID (thread-safe)
    # @return [Integer]
    def next_sequence_id
      @mutex.synchronize do
        seq = @sequence_id
        @sequence_id += 1
        seq
      end
    end

    # Register a pending message
    # @param sequence_id [Integer]
    # @param callback [Proc]
    def register_pending(sequence_id, callback)
      @pending_mutex.synchronize do
        @pending_messages[sequence_id] = {
          callback: callback,
          sent_at: Time.now
        }
      end
    end

    # Remove and return a pending message
    # @param sequence_id [Integer]
    # @return [Hash, nil]
    def remove_pending(sequence_id)
      @pending_mutex.synchronize do
        @pending_messages.delete(sequence_id)
      end
    end

    # Get count of pending messages
    # @return [Integer]
    def pending_count
      @pending_mutex.synchronize do
        @pending_messages.size
      end
    end

    # Fail all pending messages with an error
    # @param error [Exception]
    def fail_pending_messages(error)
      pending_copy = @pending_mutex.synchronize do
        copy = @pending_messages.dup
        @pending_messages.clear
        copy
      end

      pending_copy.each_value do |pending|
        invoke_callback(pending[:callback], nil, error)
      end
    end

    # Safely invoke a callback
    # @param callback [Proc, nil]
    # @param message_id [MessageId, nil]
    # @param error [Exception, nil]
    def invoke_callback(callback, message_id, error)
      return unless callback

      begin
        callback.call(message_id, error)
      rescue => e
        # Log callback errors but don't let them propagate
        puts "[Producer] Callback error: #{e.message}"
      end
    end

    # Ensure producer is ready for sending
    # @raise [ProducerError] if not ready
    def ensure_ready!
      raise ProducerError, "Producer is closed" if @closed
      raise ProducerError, "Producer is not ready" unless @ready
    end

    # Validate a message before sending
    # @param message [ProducerMessage]
    # @raise [ArgumentError] if message is invalid
    def validate_message!(message)
      raise ArgumentError, "Message cannot be nil" if message.nil?
      raise ArgumentError, "Message must be a ProducerMessage" unless message.is_a?(ProducerMessage)
      raise ArgumentError, "Message payload cannot be nil" if message.payload.nil?
    end

    # Start the timeout checker thread
    def start_timeout_checker
      return if @options[:send_timeout].nil? || @options[:send_timeout] <= 0

      @timeout_thread = Thread.new do
        Thread.current.name = "pulsar-producer-timeout-#{@producer_id}"
        timeout_check_loop
      end
    end

    # Stop the timeout checker thread
    def stop_timeout_checker
      return unless @timeout_thread

      @timeout_thread.kill
      @timeout_thread.join(1)
      @timeout_thread = nil
    end

    # Main loop for checking message timeouts
    def timeout_check_loop
      # Check every second
      check_interval = 1

      until @closed
        sleep(check_interval)
        check_pending_timeouts
      end
    end

    # Check for timed out pending messages
    def check_pending_timeouts
      timeout_seconds = @options[:send_timeout]
      return if timeout_seconds.nil? || timeout_seconds <= 0

      now = Time.now
      timed_out_entries = []

      @pending_mutex.synchronize do
        @pending_messages.each do |sequence_id, pending|
          if now - pending[:sent_at] >= timeout_seconds
            timed_out_entries << [sequence_id, pending]
          end
        end

        # Remove timed out entries
        timed_out_entries.each { |seq_id, _| @pending_messages.delete(seq_id) }
      end

      # Invoke callbacks outside the lock
      timed_out_entries.each do |sequence_id, pending|
        error = SendTimeoutError.new("Send timeout for message with sequence_id #{sequence_id}")
        invoke_callback(pending[:callback], nil, error)
      end
    end
  end
end
