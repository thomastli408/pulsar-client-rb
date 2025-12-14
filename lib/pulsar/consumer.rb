# frozen_string_literal: true

require "timeout"

require_relative "internal/commands"
require_relative "internal/topic_name"
require_relative "internal/dlq_router"
require_relative "message"
require_relative "message_id"
require_relative "errors"

module Pulsar
  # Consumer receives messages from a Pulsar topic.
  #
  # A consumer is created via Client#subscribe and is bound to a single
  # topic and subscription. Messages are received via the receive method.
  #
  # Usage:
  #   consumer = client.subscribe("my-topic", "my-subscription")
  #
  #   # Blocking receive
  #   message = consumer.receive
  #   puts message.data
  #   consumer.acknowledge(message)
  #
  #   # Receive with timeout
  #   message = consumer.receive(timeout: 10)
  #   if message
  #     process(message)
  #     consumer.ack(message)
  #   end
  #
  #   # Negative acknowledge (triggers redelivery)
  #   consumer.negative_acknowledge(message)
  #   # or
  #   consumer.nack(message)
  #
  #   consumer.close
  class Consumer
    # Default receiver queue size
    DEFAULT_RECEIVER_QUEUE_SIZE = 1000

    # @return [String] fully qualified topic name
    attr_reader :topic

    # @return [String] subscription name
    attr_reader :subscription

    # @return [Integer] consumer ID
    attr_reader :consumer_id

    # @return [String, nil] consumer name
    attr_reader :name

    # @return [Boolean] whether the consumer is ready
    attr_reader :ready

    alias ready? ready

    # Initialize a new consumer
    #
    # @param connection_pool [Internal::ConnectionPool] connection pool
    # @param lookup_service [Internal::LookupService] lookup service
    # @param topic [String] topic name
    # @param subscription [String] subscription name
    # @param client [Pulsar::Client, nil] client instance (required for DLQ support)
    # @param options [Hash] consumer options
    # @option options [Symbol] :subscription_type subscription type (:shared)
    # @option options [Symbol] :initial_position initial position (:latest, :earliest)
    # @option options [Integer] :receiver_queue_size max messages to prefetch
    # @option options [String] :name consumer name (auto-generated if nil)
    # @option options [DLQPolicy] :dlq_policy dead letter queue policy
    def initialize(connection_pool:, lookup_service:, topic:, subscription:, client: nil, options: {})
      @connection_pool = connection_pool
      @lookup_service = lookup_service
      @client = client
      @topic_name = Internal::TopicName.new(topic)
      @topic = @topic_name.to_s
      @subscription = subscription
      @options = default_options.merge(options)

      @name = @options[:name]
      @consumer_id = generate_consumer_id
      @connection = nil
      @ready = false
      @closed = false
      @mutex = Mutex.new

      # Message queue for received messages
      @message_queue = Queue.new

      # Flow control
      @receiver_queue_size = @options[:receiver_queue_size]
      @available_permits = 0
      @permits_mutex = Mutex.new

      # DLQ router (lazy initialized)
      @dlq_router = nil
      setup_dlq_router if @options[:dlq_policy]

      # Connect and subscribe
      connect_to_broker
    end

    # Receive next message (blocking with optional timeout)
    #
    # If DLQ policy is configured and a message has exceeded max redeliveries,
    # it will be automatically routed to the dead letter topic and the next
    # message will be returned instead.
    #
    # @param timeout [Integer, nil] timeout in seconds (nil = block forever)
    # @return [Message, nil] the received message, or nil if timeout
    # @raise [ConsumerError] if consumer is closed
    # @raise [TimeoutError] if timeout expires (only when timeout is specified)
    def receive(timeout: nil)
      ensure_ready!

      message = internal_receive(timeout)
      return nil if message.nil?

      # Check if message should go to DLT
      if @dlq_router&.should_route_to_dlq?(message)
        handle_dlq_message(message)
        return receive(timeout: timeout) # Get next message
      end

      message
    end

    # Acknowledge message (remove from subscription)
    #
    # @param message [Message] message to acknowledge
    # @return [void]
    # @raise [ConsumerError] if consumer is closed
    def acknowledge(message)
      ensure_ready!
      acknowledge_id(message.message_id)
    end

    # Alias for acknowledge
    alias ack acknowledge

    # Acknowledge by message ID
    #
    # @param message_id [MessageId] message ID to acknowledge
    # @return [void]
    # @raise [ConsumerError] if consumer is closed
    def acknowledge_id(message_id)
      ensure_ready!

      ack_cmd = Internal::Commands.ack(
        consumer_id: @consumer_id,
        message_id: message_id.to_proto,
        ack_type: :Individual
      )

      @connection.send_command(ack_cmd)
    end

    # Alias for acknowledge_id
    alias ack_id acknowledge_id

    # Negative acknowledge a message (trigger redelivery)
    #
    # The message will be redelivered to a consumer after a delay.
    # The broker increments the redelivery_count. If DLQ policy is configured
    # and redelivery_count reaches max_redeliveries, the message will be
    # automatically routed to the dead letter topic on next receive.
    #
    # @param message [Message] message to negative acknowledge
    # @return [void]
    # @raise [ConsumerError] if consumer is closed
    def negative_acknowledge(message)
      ensure_ready!
      negative_acknowledge_id(message.message_id)
    end

    # Alias for negative_acknowledge
    alias nack negative_acknowledge

    # Negative acknowledge by message ID
    #
    # @param message_id [MessageId] message ID to negative acknowledge
    # @return [void]
    # @raise [ConsumerError] if consumer is closed
    def negative_acknowledge_id(message_id)
      ensure_ready!

      redeliver_cmd = Internal::Commands.redeliver_unacknowledged(
        consumer_id: @consumer_id,
        message_ids: [message_id.to_proto]
      )

      @connection.send_command(redeliver_cmd)
    end

    # Alias for negative_acknowledge_id
    alias nack_id negative_acknowledge_id

    # Explicitly send a message to the dead letter topic and acknowledge it
    #
    # Use this method to immediately route a message to the DLT without
    # waiting for max redeliveries. Useful for poison messages that should
    # not be retried.
    #
    # @param message [Message] message to send to DLT
    # @return [MessageId] the message ID in the DLT
    # @raise [ConsumerError] if DLQ policy not configured or consumer is closed
    def send_to_dlq(message)
      ensure_ready!
      raise ConsumerError, "DLQ policy not configured" unless @dlq_router

      dlq_message_id = @dlq_router.route_to_dlq(message)
      acknowledge(message)
      dlq_message_id
    end

    # Close the consumer
    #
    # Releases resources and closes the consumer on the broker.
    #
    # @return [void]
    def close
      @mutex.synchronize do
        return if @closed

        @closed = true
        @ready = false
      end

      # Close DLQ router
      @dlq_router&.close

      # Clear message queue
      @message_queue.clear

      # Send close command to broker if connected
      if @connection&.ready?
        begin
          close_cmd = Internal::Commands.close_consumer(
            consumer_id: @consumer_id,
            request_id: @connection.next_request_id
          )
          @connection.send_request(close_cmd, timeout: 5)
        rescue
          # Ignore errors during close
        end

        @connection.unregister_consumer(@consumer_id)
      end
    end

    # Check if the consumer is closed
    # @return [Boolean]
    def closed?
      @closed
    end

    # Handle incoming message from broker (called by connection)
    # @param command_message [Proto::CommandMessage]
    # @param metadata [Proto::MessageMetadata]
    # @param payload [String]
    # @api private
    def handle_message(command_message, metadata, payload)
      message = Message.from_proto(command_message, metadata, payload, @topic)
      @message_queue.push(message)

      # Decrement available permits
      @permits_mutex.synchronize do
        @available_permits -= 1 if @available_permits > 0
      end
    end

    # Handle close notification from broker
    # @api private
    def handle_close
      @mutex.synchronize do
        @ready = false
      end
      puts "[Consumer] Broker requested close for consumer '#{@name}' on subscription '#{@subscription}'"
    end

    private

    # Default options for consumer
    # @return [Hash]
    def default_options
      {
        subscription_type: :shared,
        initial_position: :latest,
        receiver_queue_size: DEFAULT_RECEIVER_QUEUE_SIZE,
        name: nil,
        dlq_policy: nil
      }
    end

    # Set up the DLQ router if policy is configured
    def setup_dlq_router
      policy = @options[:dlq_policy]
      return unless policy

      raise ConsumerError, "Client is required for DLQ support" unless @client

      @dlq_router = Internal::DLQRouter.new(
        client: @client,
        policy: policy,
        source_topic: @topic,
        subscription: @subscription
      )
    end

    # Handle a message that should go to the DLT
    # @param message [Message] message to route to DLT
    def handle_dlq_message(message)
      puts "[Consumer] Routing message #{message.message_id} to DLQ (redelivery_count: #{message.redelivery_count})"
      @dlq_router.route_to_dlq(message)
      acknowledge(message) # Ack original to prevent further redelivery
    end

    # Internal receive implementation without DLQ handling
    # @param timeout [Integer, nil] timeout in seconds
    # @return [Message, nil]
    def internal_receive(timeout)
      if timeout.nil?
        # Block forever
        message = @message_queue.pop
        replenish_permits_if_needed
        message
      else
        # Block with timeout
        begin
          message = nil
          Timeout.timeout(timeout, Pulsar::TimeoutError) do
            message = @message_queue.pop
          end
          replenish_permits_if_needed
          message
        rescue Pulsar::TimeoutError
          nil
        end
      end
    end

    # Generate a unique consumer ID
    # @return [Integer]
    def generate_consumer_id
      object_id & 0x7FFFFFFF
    end

    # Connect to the broker serving this topic
    def connect_to_broker
      # Look up the broker for this topic
      @connection = @lookup_service.lookup_connection(@topic)

      # Register this consumer with the connection for callbacks
      @connection.register_consumer(@consumer_id, self)

      # Create subscription on the broker
      create_subscription_on_broker
    end

    # Send SUBSCRIBE command to broker and wait for success
    def create_subscription_on_broker
      request_id = @connection.next_request_id

      # Map subscription type to proto enum
      sub_type = case @options[:subscription_type]
                 when :shared then :Shared
                 when :failover then :Failover
                 when :exclusive then :Exclusive
                 else :Shared
                 end

      # Map initial position to proto enum
      initial_position = case @options[:initial_position]
                         when :earliest then :Earliest
                         when :latest then :Latest
                         else :Latest
                         end

      subscribe_cmd = Internal::Commands.subscribe(
        consumer_id: @consumer_id,
        request_id: request_id,
        topic: @topic,
        subscription: @subscription,
        sub_type: sub_type,
        consumer_name: @options[:name],
        initial_position: initial_position
      )

      @connection.send_request(subscribe_cmd, timeout: 30)

      @ready = true

      puts "[Consumer] Subscribed to '#{@topic}' with subscription '#{@subscription}'"
      puts "[Consumer]   Consumer ID: #{@consumer_id}"
      puts "[Consumer]   Type: #{@options[:subscription_type]}"
      puts "[Consumer]   Initial Position: #{@options[:initial_position]}"

      # Request initial permits
      request_permits(@receiver_queue_size)
    end

    # Request message permits from broker
    # @param count [Integer] number of permits to request
    def request_permits(count)
      return if count <= 0

      flow_cmd = Internal::Commands.flow(
        consumer_id: @consumer_id,
        message_permits: count
      )

      @connection.send_command(flow_cmd)

      @permits_mutex.synchronize do
        @available_permits += count
      end
    end

    # Replenish permits if we've consumed half of them
    def replenish_permits_if_needed
      should_replenish = false
      permits_to_request = 0

      @permits_mutex.synchronize do
        # Replenish when we've consumed at least half
        threshold = @receiver_queue_size / 2
        consumed = @receiver_queue_size - @available_permits

        if consumed >= threshold
          permits_to_request = consumed
          should_replenish = true
        end
      end

      request_permits(permits_to_request) if should_replenish
    end

    # Ensure consumer is ready for operations
    # @raise [ConsumerError] if not ready
    def ensure_ready!
      raise ConsumerError, "Consumer is closed" if @closed
      raise ConsumerError, "Consumer is not ready" unless @ready
    end
  end
end
