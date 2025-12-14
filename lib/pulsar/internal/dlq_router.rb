# frozen_string_literal: true

require_relative "../producer_message"

module Pulsar
  module Internal
    # DLQRouter handles routing failed messages to a dead letter topic.
    #
    # It lazily creates a producer for the DLT and adds metadata properties
    # to track the original message information.
    #
    # @api private
    class DLQRouter
      # DLQ metadata property keys
      PROP_ORIGINAL_TOPIC = "PULSAR_DLQ_ORIGINAL_TOPIC"
      PROP_ORIGINAL_SUBSCRIPTION = "PULSAR_DLQ_ORIGINAL_SUBSCRIPTION"
      PROP_ORIGINAL_MESSAGE_ID = "PULSAR_DLQ_ORIGINAL_MESSAGE_ID"
      PROP_REDELIVERY_COUNT = "PULSAR_DLQ_REDELIVERY_COUNT"

      # @param client [Pulsar::Client] the Pulsar client for creating DLQ producer
      # @param policy [Pulsar::DLQPolicy] the DLQ policy configuration
      # @param source_topic [String] the original topic name
      # @param subscription [String] the subscription name
      def initialize(client:, policy:, source_topic:, subscription:)
        @client = client
        @policy = policy
        @source_topic = source_topic
        @subscription = subscription
        @dlq_producer = nil
        @mutex = Mutex.new
        @dlq_topic = policy.generate_dlq_topic(source_topic, subscription)
      end

      # @return [String] the dead letter topic name
      attr_reader :dlq_topic

      # Check if a message should be routed to the DLT based on redelivery count
      #
      # @param message [Pulsar::Message] the received message
      # @return [Boolean] true if message has exceeded max redeliveries
      def should_route_to_dlq?(message)
        message.redelivery_count >= @policy.max_redeliveries
      end

      # Route a message to the dead letter topic
      #
      # Creates the DLQ producer lazily on first use. The message is sent
      # with additional properties tracking its origin.
      #
      # @param message [Pulsar::Message] the message to route to DLT
      # @return [Pulsar::MessageId] the message ID in the DLT
      # @raise [Pulsar::ProducerError] if sending to DLT fails
      def route_to_dlq(message)
        ensure_producer_created

        dlq_message = ProducerMessage.new(
          payload: message.payload,
          key: message.key,
          properties: build_dlq_properties(message)
        )

        @dlq_producer.send(dlq_message)
      end

      # Close the DLQ producer if it was created
      #
      # @return [void]
      def close
        @mutex.synchronize do
          if @dlq_producer
            @dlq_producer.close rescue nil
            @dlq_producer = nil
          end
        end
      end

      private

      # Lazily create the DLQ producer
      def ensure_producer_created
        @mutex.synchronize do
          return if @dlq_producer

          @dlq_producer = @client.create_producer(@dlq_topic)
        end
      end

      # Build DLQ message properties with original message metadata
      #
      # @param message [Pulsar::Message] the original message
      # @return [Hash<String, String>] properties for the DLQ message
      def build_dlq_properties(message)
        props = (message.properties || {}).dup
        props[PROP_ORIGINAL_TOPIC] = @source_topic
        props[PROP_ORIGINAL_SUBSCRIPTION] = @subscription
        props[PROP_ORIGINAL_MESSAGE_ID] = message.message_id.to_s
        props[PROP_REDELIVERY_COUNT] = message.redelivery_count.to_s
        props
      end
    end
  end
end
