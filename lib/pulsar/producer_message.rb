# frozen_string_literal: true

module Pulsar
  # ProducerMessage represents a message to be sent to a Pulsar topic.
  #
  # Usage:
  #   message = ProducerMessage.new(payload: "Hello, Pulsar!")
  #   message = ProducerMessage.new(
  #     payload: "Hello",
  #     key: "my-key",
  #     properties: { "source" => "ruby" },
  #     event_time: Time.now
  #   )
  class ProducerMessage
    # @return [String] message payload
    attr_accessor :payload

    # @return [String, nil] partition key (optional)
    attr_accessor :key

    # @return [Hash<String, String>] message properties
    attr_accessor :properties

    # @return [Time, Integer, nil] event time (optional)
    attr_accessor :event_time

    # Initialize a new producer message
    #
    # @param payload [String] message payload (required)
    # @param key [String, nil] partition key (optional)
    # @param properties [Hash<String, String>] message properties (optional)
    # @param event_time [Time, Integer, nil] event time in milliseconds or Time object (optional)
    def initialize(payload:, key: nil, properties: {}, event_time: nil)
      @payload = payload
      @key = key
      @properties = properties || {}
      @event_time = event_time
    end

    # Convert event_time to milliseconds
    #
    # @return [Integer, nil] event time in milliseconds, or nil if not set
    def event_time_millis
      return nil if @event_time.nil?

      case @event_time
      when Time
        (@event_time.to_f * 1000).to_i
      when Integer
        @event_time
      else
        raise ArgumentError, "event_time must be a Time or Integer, got #{@event_time.class}"
      end
    end

    # Get the payload as binary string
    #
    # @return [String] payload as binary string
    def payload_bytes
      @payload.to_s.b
    end
  end
end
