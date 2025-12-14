# frozen_string_literal: true

require_relative "client/version"
require_relative "errors"
require_relative "internal/buffer"
require_relative "internal/checksum"
require_relative "internal/commands"
require_relative "internal/connection"
require_relative "internal/connection_pool"
require_relative "internal/service_url"
require_relative "internal/topic_name"
require_relative "internal/lookup_service"
require_relative "message_id"
require_relative "producer_message"
require_relative "producer"
require_relative "message"
require_relative "consumer"

module Pulsar
  # Client is the main entry point for interacting with a Pulsar cluster.
  #
  # A client manages connections to brokers, performs topic lookups, and
  # creates producers and consumers. A single client instance should be
  # shared across an application.
  #
  # Usage:
  #   client = Pulsar::Client.new("pulsar://localhost:6650")
  #
  #   # Create a producer
  #   producer = client.create_producer("my-topic")
  #   producer.send(ProducerMessage.new(payload: "Hello"))
  #   producer.close
  #
  #   # Create a consumer
  #   consumer = client.subscribe("my-topic", "my-subscription")
  #   message = consumer.receive
  #   consumer.acknowledge(message)
  #   consumer.close
  #
  #   # Clean up
  #   client.close
  class Client
    # @return [String] the service URL
    attr_reader :service_url

    # Initialize a new Pulsar client
    #
    # @param service_url [String] Pulsar service URL (e.g., "pulsar://localhost:6650")
    # @param options [Hash] client options
    # @option options [Integer] :connection_timeout connection timeout in seconds (default: 10)
    # @option options [Integer] :keep_alive_interval keep-alive interval in seconds (default: 30)
    # @option options [Integer] :operation_timeout default operation timeout in seconds (default: 30)
    # @raise [ArgumentError] if service_url is invalid
    def initialize(service_url, options = {})
      @service_url = service_url
      @options = default_options.merge(options)
      @closed = false
      @mutex = Mutex.new

      # Track created producers and consumers for cleanup
      @producers = []
      @consumers = []
      @resources_mutex = Mutex.new

      # Initialize internal components
      @parsed_service_url = Internal::ServiceUrl.new(service_url)
      @connection_pool = Internal::ConnectionPool.new(
        connection_timeout: @options[:connection_timeout],
        keep_alive_interval: @options[:keep_alive_interval]
      )
      @lookup_service = Internal::LookupService.new(
        @connection_pool,
        @parsed_service_url,
        timeout: @options[:operation_timeout]
      )
    end

    # Create a producer for a topic
    #
    # @param topic [String] topic name (can be short form like "my-topic" or
    #   fully qualified like "persistent://public/default/my-topic")
    # @param options [Hash] producer options
    # @option options [String] :name producer name (auto-generated if nil)
    # @option options [Integer] :send_timeout send timeout in seconds (default: 30)
    # @option options [Integer] :max_pending_messages max pending messages (default: 1000)
    # @return [Producer] the created producer
    # @raise [ClientError] if client is closed
    # @raise [ProducerError] if producer creation fails
    def create_producer(topic, options = {})
      ensure_open!

      producer = Producer.new(
        connection_pool: @connection_pool,
        lookup_service: @lookup_service,
        topic: topic,
        options: options
      )

      @resources_mutex.synchronize do
        @producers << producer
      end

      producer
    end

    # Subscribe to a topic
    #
    # Creates a consumer that subscribes to the specified topic with the
    # given subscription name.
    #
    # @param topic [String] topic name
    # @param subscription [String] subscription name
    # @param options [Hash] consumer options
    # @option options [Symbol] :subscription_type subscription type
    #   (:shared, :exclusive, :failover) (default: :shared)
    # @option options [Symbol] :initial_position where to start reading
    #   (:latest, :earliest) (default: :latest)
    # @option options [Integer] :receiver_queue_size max messages to prefetch (default: 1000)
    # @option options [String] :name consumer name (auto-generated if nil)
    # @return [Consumer] the created consumer
    # @raise [ClientError] if client is closed
    # @raise [ConsumerError] if subscription fails
    def subscribe(topic, subscription, options = {})
      ensure_open!

      consumer = Consumer.new(
        connection_pool: @connection_pool,
        lookup_service: @lookup_service,
        topic: topic,
        subscription: subscription,
        options: options
      )

      @resources_mutex.synchronize do
        @consumers << consumer
      end

      consumer
    end

    # Close the client and release all resources
    #
    # Closes all producers and consumers created by this client, then
    # closes all connections. After calling close, the client cannot be
    # used again.
    #
    # @return [void]
    def close
      @mutex.synchronize do
        return if @closed
        @closed = true
      end

      # Close all producers
      producers_to_close = @resources_mutex.synchronize { @producers.dup }
      producers_to_close.each do |producer|
        producer.close rescue nil
      end

      # Close all consumers
      consumers_to_close = @resources_mutex.synchronize { @consumers.dup }
      consumers_to_close.each do |consumer|
        consumer.close rescue nil
      end

      # Clear tracked resources
      @resources_mutex.synchronize do
        @producers.clear
        @consumers.clear
      end

      # Close all connections
      @connection_pool.close_all
    end

    # Check if the client is closed
    # @return [Boolean]
    def closed?
      @closed
    end

    private

    # Default options for the client
    # @return [Hash]
    def default_options
      {
        connection_timeout: Internal::Connection::DEFAULT_CONNECTION_TIMEOUT,
        keep_alive_interval: Internal::Connection::DEFAULT_KEEP_ALIVE_INTERVAL,
        operation_timeout: 30
      }
    end

    # Ensure the client is open
    # @raise [ClientError] if client is closed
    def ensure_open!
      raise ClientError, "Client is closed" if @closed
    end
  end
end
