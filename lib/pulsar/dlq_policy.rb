# frozen_string_literal: true

module Pulsar
  # DLQPolicy configures dead letter topic behavior for a consumer.
  #
  # When a message fails processing repeatedly (after max_redeliveries attempts),
  # it can be automatically routed to a dead letter topic (DLT) for later
  # investigation or manual processing.
  #
  # Usage:
  #   # Auto-generate DLT topic name
  #   policy = DLQPolicy.new(max_redeliveries: 3)
  #
  #   # Specify custom DLT topic
  #   policy = DLQPolicy.new(
  #     max_redeliveries: 5,
  #     dead_letter_topic: "my-app-dlq"
  #   )
  #
  #   consumer = client.subscribe("my-topic", "my-sub", dlq_policy: policy)
  class DLQPolicy
    # @return [Integer] maximum number of redelivery attempts before routing to DLT
    attr_reader :max_redeliveries

    # @return [String, nil] explicit dead letter topic name (nil = auto-generated)
    attr_reader :dead_letter_topic

    # @return [String, nil] initial subscription name to create on DLT
    attr_reader :initial_subscription_name

    # Initialize a new DLQ policy
    #
    # @param max_redeliveries [Integer] max redelivery attempts before DLT routing (required)
    # @param dead_letter_topic [String, nil] custom DLT topic name (auto-generated if nil)
    # @param initial_subscription_name [String, nil] subscription to create on DLT
    # @raise [ArgumentError] if max_redeliveries is not positive
    def initialize(max_redeliveries:, dead_letter_topic: nil, initial_subscription_name: nil)
      raise ArgumentError, "max_redeliveries must be positive" unless max_redeliveries.is_a?(Integer) && max_redeliveries > 0

      @max_redeliveries = max_redeliveries
      @dead_letter_topic = dead_letter_topic
      @initial_subscription_name = initial_subscription_name
    end

    # Generate the DLT topic name for a given source topic and subscription
    #
    # If dead_letter_topic was explicitly set, returns that value.
    # Otherwise, generates a name in the format: "{original_topic}-{subscription}-DLQ"
    #
    # @param original_topic [String] the source topic name
    # @param subscription [String] the subscription name
    # @return [String] the dead letter topic name
    def generate_dlq_topic(original_topic, subscription)
      @dead_letter_topic || "#{original_topic}-#{subscription}-DLQ"
    end
  end
end
