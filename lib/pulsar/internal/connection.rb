# frozen_string_literal: true

require "socket"
require "monitor"
require "timeout"

require_relative "buffer"
require_relative "commands"
require_relative "../errors"

module Pulsar
  module Internal
    # Connection manages a single TCP connection to a Pulsar broker.
    #
    # It handles:
    # - Connection establishment and handshake
    # - Frame reading and writing
    # - Keep-alive ping/pong
    # - Connection state management
    #
    # Connection States:
    #   INIT -> CONNECTING -> READY -> CLOSED
    #
    # Thread Safety:
    #   - Writing is protected by a mutex
    #   - Reading happens in a dedicated background thread
    #   - State transitions are synchronized
    class Connection
      # Protocol version to use when connecting
      PROTOCOL_VERSION = Proto::ProtocolVersion::V20

      # Maximum message size (5 MB)
      MAX_MESSAGE_SIZE = 5 * 1024 * 1024

      # Default keep-alive interval in seconds
      DEFAULT_KEEP_ALIVE_INTERVAL = 30

      # Default connection timeout in seconds
      DEFAULT_CONNECTION_TIMEOUT = 10

      # Read buffer size
      READ_BUFFER_SIZE = 64 * 1024

      # Connection states
      module State
        INIT = :init
        CONNECTING = :connecting
        READY = :ready
        CLOSING = :closing
        CLOSED = :closed
      end

      # Client version string sent in CONNECT command
      CLIENT_VERSION = "Pulsar Ruby Client #{Pulsar::Client::VERSION}"

      # @return [String] broker host
      attr_reader :host

      # @return [Integer] broker port
      attr_reader :port

      # @return [Symbol] current connection state
      attr_reader :state

      # @return [Integer, nil] max message size from broker
      attr_reader :max_message_size

      # @return [String, nil] server version from broker
      attr_reader :server_version

      # Initialize a new connection
      # @param host [String] broker hostname
      # @param port [Integer] broker port (default: 6650)
      # @param connection_timeout [Integer] connection timeout in seconds
      # @param keep_alive_interval [Integer] keep-alive interval in seconds
      def initialize(host:, port: 6650, connection_timeout: DEFAULT_CONNECTION_TIMEOUT,
                     keep_alive_interval: DEFAULT_KEEP_ALIVE_INTERVAL)
        @host = host
        @port = port
        @connection_timeout = connection_timeout
        @keep_alive_interval = keep_alive_interval

        @state = State::INIT
        @socket = nil
        @read_buffer = Buffer.new(READ_BUFFER_SIZE)

        # Synchronization primitives
        @monitor = Monitor.new
        @write_mutex = Mutex.new
        @state_condition = @monitor.new_cond

        # Registered handlers for producers and consumers
        @producers = {}
        @consumers = {}

        # Pending requests waiting for response
        @pending_requests = {}
        @request_id_counter = 0

        # Read loop thread
        @read_thread = nil
        @keep_alive_thread = nil

        # Handshake completion tracking
        @handshake_completed = false
        @handshake_error = nil

        # Callbacks
        @on_close_callbacks = []
      end

      # Establish connection to the broker and perform handshake
      # @return [self]
      # @raise [ConnectionError] if connection fails
      # @raise [HandshakeError] if handshake fails
      # @raise [TimeoutError] if connection times out
      def connect
        @monitor.synchronize do
          raise ConnectionError, "Connection already established" if @state != State::INIT

          @state = State::CONNECTING
        end

        begin
          # Establish TCP connection with timeout
          @socket = connect_socket

          # Start reading frames
          start_read_loop

          # Perform handshake
          perform_handshake

          # Start keep-alive
          start_keep_alive

          @monitor.synchronize do
            @state = State::READY
            @state_condition.broadcast
          end

          self
        rescue => e
          close_internal(e)
          raise
        end
      end

      # Check if the connection is ready for use
      # @return [Boolean]
      def ready?
        @state == State::READY
      end

      # Check if the connection is closed
      # @return [Boolean]
      def closed?
        @state == State::CLOSED
      end

      # Close the connection gracefully
      # @return [void]
      def close
        close_internal(nil)
      end

      # Send a command without waiting for response
      # @param base_command [Proto::BaseCommand] command to send
      # @return [void]
      # @raise [ConnectionClosedError] if connection is closed
      def send_command(base_command)
        ensure_ready!
        send_frame(Commands.encode_command(base_command))
      end

      # Send a request and wait for response
      # @param base_command [Proto::BaseCommand] command to send
      # @param timeout [Integer] timeout in seconds
      # @yield [response] block called with response
      # @return [Object] response from broker
      # @raise [ConnectionClosedError] if connection is closed
      # @raise [TimeoutError] if request times out
      def send_request(base_command, timeout: 30, &callback)
        ensure_ready!

        request_id = extract_request_id(base_command)
        raise ArgumentError, "Command does not have a request_id" unless request_id

        response_queue = Queue.new

        @monitor.synchronize do
          @pending_requests[request_id] = response_queue
        end

        begin
          send_frame(Commands.encode_command(base_command))

          # Wait for response with timeout
          result = nil
          Timeout.timeout(timeout, Pulsar::TimeoutError) do
            result = response_queue.pop
          end

          if result.is_a?(Exception)
            raise result
          end

          callback&.call(result)
          result
        ensure
          @monitor.synchronize do
            @pending_requests.delete(request_id)
          end
        end
      end

      # Register a producer handler for receiving messages
      # @param producer_id [Integer] producer ID
      # @param handler [Object] handler object with #handle_response method
      def register_producer(producer_id, handler)
        @monitor.synchronize do
          @producers[producer_id] = handler
        end
      end

      # Unregister a producer handler
      # @param producer_id [Integer]
      def unregister_producer(producer_id)
        @monitor.synchronize do
          @producers.delete(producer_id)
        end
      end

      # Register a consumer handler for receiving messages
      # @param consumer_id [Integer] consumer ID
      # @param handler [Object] handler object with #handle_message method
      def register_consumer(consumer_id, handler)
        @monitor.synchronize do
          @consumers[consumer_id] = handler
        end
      end

      # Unregister a consumer handler
      # @param consumer_id [Integer]
      def unregister_consumer(consumer_id)
        @monitor.synchronize do
          @consumers.delete(consumer_id)
        end
      end

      # Add a callback to be called when connection closes
      # @yield [error] block called with optional error
      def on_close(&block)
        @on_close_callbacks << block
      end

      # Generate next request ID
      # @return [Integer]
      def next_request_id
        @monitor.synchronize do
          @request_id_counter += 1
        end
      end

      private

      # Connect the TCP socket with timeout
      # @return [TCPSocket]
      def connect_socket
        Timeout.timeout(@connection_timeout, Pulsar::TimeoutError) do
          TCPSocket.new(@host, @port)
        end
      rescue SocketError, Errno::ECONNREFUSED, Errno::EHOSTUNREACH => e
        raise Pulsar::ConnectionError, "Failed to connect to #{@host}:#{@port}: #{e.message}"
      end

      # Perform the CONNECT/CONNECTED handshake
      # @raise [HandshakeError] if handshake fails
      def perform_handshake
        connect_cmd = Commands.connect(
          client_version: CLIENT_VERSION,
          protocol_version: PROTOCOL_VERSION
        )

        send_frame(Commands.encode_command(connect_cmd))

        # Wait for CONNECTED response
        wait_for_handshake
      end

      # Wait for handshake completion
      # @raise [HandshakeError] if handshake fails
      # @raise [TimeoutError] if handshake times out
      def wait_for_handshake
        deadline = Time.now + @connection_timeout

        @monitor.synchronize do
          until @handshake_completed || @handshake_error || Time.now > deadline
            remaining = deadline - Time.now
            break if remaining <= 0
            @state_condition.wait(remaining)
          end

          raise Pulsar::TimeoutError, "Handshake timeout" unless @handshake_completed || @handshake_error
          raise @handshake_error if @handshake_error
        end
      end

      # Start the background read loop
      def start_read_loop
        @read_thread = Thread.new do
          Thread.current.name = "pulsar-reader-#{@host}:#{@port}"
          read_loop
        end
      end

      # Start the keep-alive thread
      def start_keep_alive
        return if @keep_alive_interval <= 0

        @keep_alive_thread = Thread.new do
          Thread.current.name = "pulsar-keepalive-#{@host}:#{@port}"
          keep_alive_loop
        end
      end

      # Main read loop - runs in background thread
      def read_loop
        while @state != State::CLOSED && @state != State::CLOSING
          begin
            # Read data from socket
            data = @socket.read_nonblock(READ_BUFFER_SIZE)
            @read_buffer.write(data)

            # Process complete frames
            process_frames
          rescue IO::WaitReadable
            IO.select([@socket], nil, nil, 1)
          rescue EOFError, IOError, Errno::ECONNRESET => e
            handle_connection_error(e)
            break
          rescue => e
            handle_connection_error(e)
            break
          end
        end
      end

      # Process all complete frames in the buffer
      def process_frames
        while @read_buffer.readable_bytes >= 4
          frame = Commands.decode_frame(@read_buffer)
          break unless frame # Not enough data for complete frame

          handle_frame(frame)
        end

        # Compact buffer to reclaim space
        @read_buffer.compact
      end

      # Handle a received frame
      # @param frame [Hash] decoded frame with :command and optional :metadata/:payload
      def handle_frame(frame)
        command = frame[:command]
        handle_command(command, frame[:metadata], frame[:payload])
      end

      # Handle a received command
      # @param command [Proto::BaseCommand] the command
      # @param metadata [Proto::MessageMetadata, nil] message metadata if present
      # @param payload [String, nil] message payload if present
      def handle_command(command, metadata, payload)
        case command.type
        when :CONNECTED
          handle_connected(command.connected)
        when :PING
          handle_ping
        when :PONG
          handle_pong
        when :SUCCESS
          handle_success(command.success)
        when :PRODUCER_SUCCESS
          handle_producer_success(command.producer_success)
        when :ERROR
          handle_error(command.error)
        when :SEND_RECEIPT
          handle_send_receipt(command.send_receipt)
        when :SEND_ERROR
          handle_send_error(command.send_error)
        when :MESSAGE
          handle_message(command.message, metadata, payload)
        when :CLOSE_PRODUCER
          handle_close_producer(command.close_producer)
        when :CLOSE_CONSUMER
          handle_close_consumer(command.close_consumer)
        when :LOOKUP_RESPONSE
          handle_lookup_response(command.lookupTopicResponse)
        when :PARTITIONED_METADATA_RESPONSE
          handle_partitioned_metadata_response(command.partitionMetadataResponse)
        else
          puts "Unknown command type: #{command.type}"
        end
      end

      # Handle CONNECTED response from broker
      # @param connected [Proto::CommandConnected]
      def handle_connected(connected)
        @monitor.synchronize do
          @server_version = connected.server_version
          @max_message_size = connected.max_message_size if connected.max_message_size > 0
          @handshake_completed = true
          @state_condition.broadcast
        end
      end

      # Handle PING from broker - respond with PONG
      def handle_ping
        pong_cmd = Commands.pong
        send_frame(Commands.encode_command(pong_cmd))
      rescue => e
        # Ignore errors when sending pong
      end

      # Handle PONG response - reset keep-alive timer
      def handle_pong
        # PONG received - connection is alive
        @last_pong_time = Time.now
      end

      # Handle SUCCESS response
      # @param success [Proto::CommandSuccess]
      def handle_success(success)
        dispatch_response(success.request_id, success)
      end

      # Handle PRODUCER_SUCCESS response
      # @param producer_success [Proto::CommandProducerSuccess]
      def handle_producer_success(producer_success)
        dispatch_response(producer_success.request_id, producer_success)
      end

      # Handle ERROR response
      # @param error [Proto::CommandError]
      def handle_error(error)
        exception = Pulsar::BrokerError.new(error.error, error.message)
        dispatch_response(error.request_id, exception)
      end

      # Handle SEND_RECEIPT
      # @param receipt [Proto::CommandSendReceipt]
      def handle_send_receipt(receipt)
        handler = @monitor.synchronize { @producers[receipt.producer_id] }
        handler&.handle_send_receipt(receipt)
      end

      # Handle SEND_ERROR
      # @param error [Proto::CommandSendError]
      def handle_send_error(error)
        handler = @monitor.synchronize { @producers[error.producer_id] }
        handler&.handle_send_error(error)
      end

      # Handle MESSAGE (incoming message for consumer)
      # @param message [Proto::CommandMessage]
      # @param metadata [Proto::MessageMetadata]
      # @param payload [String]
      def handle_message(message, metadata, payload)
        handler = @monitor.synchronize { @consumers[message.consumer_id] }
        handler&.handle_message(message, metadata, payload)
      end

      # Handle CLOSE_PRODUCER from broker
      # @param close_producer [Proto::CommandCloseProducer]
      def handle_close_producer(close_producer)
        handler = @monitor.synchronize { @producers[close_producer.producer_id] }
        handler&.handle_close
      end

      # Handle CLOSE_CONSUMER from broker
      # @param close_consumer [Proto::CommandCloseConsumer]
      def handle_close_consumer(close_consumer)
        handler = @monitor.synchronize { @consumers[close_consumer.consumer_id] }
        handler&.handle_close
      end

      # Handle LOOKUP_RESPONSE
      # @param response [Proto::CommandLookupTopicResponse]
      def handle_lookup_response(response)
        dispatch_response(response.request_id, response)
      end

      # Handle PARTITIONED_METADATA_RESPONSE
      # @param response [Proto::CommandPartitionedTopicMetadataResponse]
      def handle_partitioned_metadata_response(response)
        dispatch_response(response.request_id, response)
      end

      # Dispatch a response to the waiting request
      # @param request_id [Integer]
      # @param response [Object]
      def dispatch_response(request_id, response)
        queue = @monitor.synchronize { @pending_requests[request_id] }
        queue&.push(response)
      end

      # Keep-alive loop - runs in background thread
      def keep_alive_loop
        while @state == State::READY
          sleep(@keep_alive_interval)

          next unless @state == State::READY

          begin
            send_ping
          rescue => e
            # Ping failed - connection may be dead
            handle_connection_error(e)
            break
          end
        end
      end

      # Send a PING command
      def send_ping
        ping_cmd = Commands.ping
        send_frame(Commands.encode_command(ping_cmd))
      end

      # Send a frame over the socket
      # @param frame_bytes [String] encoded frame
      def send_frame(frame_bytes)
        @write_mutex.synchronize do
          raise Pulsar::ConnectionClosedError, "Connection is closed" if @socket.nil? || @socket.closed?
          @socket.write(frame_bytes)
          @socket.flush
        end
      end

      # Ensure the connection is ready for use
      # @raise [ConnectionClosedError] if not ready
      def ensure_ready!
        unless ready?
          raise Pulsar::ConnectionClosedError, "Connection is not ready (state: #{@state})"
        end
      end

      # Extract request_id from a command
      # @param base_command [Proto::BaseCommand]
      # @return [Integer, nil]
      def extract_request_id(base_command)
        case base_command.type
        when :PRODUCER
          base_command.producer.request_id
        when :SUBSCRIBE
          base_command.subscribe.request_id
        when :LOOKUP
          base_command.lookupTopic.request_id
        when :PARTITIONED_METADATA
          base_command.partitionMetadata.request_id
        when :CLOSE_PRODUCER
          base_command.close_producer.request_id
        when :CLOSE_CONSUMER
          base_command.close_consumer.request_id
        when :UNSUBSCRIBE
          base_command.unsubscribe.request_id
        when :SEEK
          base_command.seek.request_id
        when :GET_LAST_MESSAGE_ID
          base_command.getLastMessageId.request_id
        when :GET_SCHEMA
          base_command.getSchema.request_id
        when :GET_OR_CREATE_SCHEMA
          base_command.getOrCreateSchema.request_id
        else
          nil
        end
      end

      # Handle a connection error
      # @param error [Exception]
      def handle_connection_error(error)
        close_internal(error)
      end

      # Internal close implementation
      # @param error [Exception, nil] optional error that caused the close
      def close_internal(error)
        @monitor.synchronize do
          return if @state == State::CLOSED

          @state = State::CLOSING

          # Signal handshake waiters
          if error && !@handshake_completed
            @handshake_error = Pulsar::HandshakeError.new(error.message)
          end
          @state_condition.broadcast
        end

        # Close socket
        @write_mutex.synchronize do
          if @socket && !@socket.closed?
            @socket.close rescue nil
          end
        end

        # Wait for read thread to finish (with timeout)
        if @read_thread && @read_thread != Thread.current
          @read_thread.join(2)
        end

        # Stop keep-alive thread
        if @keep_alive_thread && @keep_alive_thread != Thread.current
          @keep_alive_thread.join(1)
        end

        # Fail all pending requests
        @monitor.synchronize do
          @pending_requests.each do |_, queue|
            queue.push(Pulsar::ConnectionClosedError.new("Connection closed"))
          end
          @pending_requests.clear

          @state = State::CLOSED
        end

        # Notify close callbacks
        @on_close_callbacks.each do |callback|
          callback.call(error) rescue nil
        end
      end
    end
  end
end
