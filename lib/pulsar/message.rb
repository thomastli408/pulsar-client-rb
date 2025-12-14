# frozen_string_literal: true

require_relative "message_id"

module Pulsar
  # Message represents a message received from a Pulsar topic.
  #
  # Messages are received via Consumer#receive and contain the payload,
  # metadata, and message ID for acknowledgment.
  #
  # Usage:
  #   message = consumer.receive(timeout: 10)
  #   puts message.data           # => "Hello, Pulsar!"
  #   puts message.message_id     # => "123:456:-1:-1"
  #   puts message.properties     # => { "key" => "value" }
  #   consumer.acknowledge(message)
  class Message
    # @return [MessageId] unique message identifier
    attr_reader :message_id

    # @return [String] message payload
    attr_reader :payload

    # @return [String, nil] partition key
    attr_reader :key

    # @return [Hash<String, String>] message properties
    attr_reader :properties

    # @return [Integer] publish time in milliseconds since epoch
    attr_reader :publish_time

    # @return [Integer, nil] event time in milliseconds since epoch
    attr_reader :event_time

    # @return [String] name of the producer that sent this message
    attr_reader :producer_name

    # @return [String] topic this message was received from
    attr_reader :topic

    # @return [Integer] number of times this message has been redelivered
    attr_reader :redelivery_count

    # @return [Integer] consumer ID that received this message
    attr_reader :consumer_id

    # Initialize a new received message
    #
    # @param message_id [MessageId] message identifier
    # @param payload [String] message payload
    # @param topic [String] topic name
    # @param consumer_id [Integer] consumer ID
    # @param producer_name [String] producer name
    # @param publish_time [Integer] publish time in milliseconds
    # @param redelivery_count [Integer] redelivery count
    # @param key [String, nil] partition key
    # @param properties [Hash] message properties
    # @param event_time [Integer, nil] event time in milliseconds
    def initialize(
      message_id:,
      payload:,
      topic:,
      consumer_id:,
      producer_name:,
      publish_time:,
      redelivery_count: 0,
      key: nil,
      properties: {},
      event_time: nil
    )
      @message_id = message_id
      @payload = payload
      @topic = topic
      @consumer_id = consumer_id
      @producer_name = producer_name
      @publish_time = publish_time
      @redelivery_count = redelivery_count
      @key = key
      @properties = properties || {}
      @event_time = event_time
    end

    # Alias for payload - common name in messaging systems
    # @return [String]
    def data
      @payload
    end

    # Get publish time as Time object
    # @return [Time]
    def publish_time_as_time
      Time.at(@publish_time / 1000.0)
    end

    # Get event time as Time object
    # @return [Time, nil]
    def event_time_as_time
      return nil if @event_time.nil? || @event_time == 0

      Time.at(@event_time / 1000.0)
    end

    # Check if this message has been redelivered
    # @return [Boolean]
    def redelivered?
      @redelivery_count > 0
    end

    # String representation
    # @return [String]
    def to_s
      "Message(id=#{@message_id}, topic=#{@topic}, redelivery=#{@redelivery_count})"
    end

    # Build a Message from protobuf components
    #
    # @param command_message [Proto::CommandMessage] the command message
    # @param metadata [Proto::MessageMetadata] message metadata
    # @param payload [String] message payload bytes
    # @param topic [String] topic name
    # @return [Message]
    def self.from_proto(command_message, metadata, payload, topic)
      message_id = MessageId.from_proto(command_message.message_id)

      # Extract properties from metadata
      properties = {}
      metadata.properties.each do |kv|
        properties[kv.key] = kv.value
      end

      new(
        message_id: message_id,
        payload: payload,
        topic: topic,
        consumer_id: command_message.consumer_id,
        producer_name: metadata.producer_name,
        publish_time: metadata.publish_time,
        redelivery_count: command_message.redelivery_count,
        key: metadata.partition_key.empty? ? nil : metadata.partition_key,
        properties: properties,
        event_time: metadata.event_time.zero? ? nil : metadata.event_time
      )
    end
  end
end
