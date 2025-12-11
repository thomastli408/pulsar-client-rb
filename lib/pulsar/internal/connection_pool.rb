# frozen_string_literal: true

require "uri"

require_relative "connection"

module Pulsar
  module Internal
    # ConnectionPool manages shared connections to Pulsar brokers.
    #
    # Unlike traditional connection pools where connections are checked out
    # and returned, Pulsar connections are long-lived and shared by multiple
    # producers and consumers. This pool provides:
    #
    # - Thread-safe lazy connection creation
    # - One connection per broker (configurable)
    # - Automatic reconnection on failure
    # - Centralized connection lifecycle management
    #
    # Usage:
    #   pool = ConnectionPool.new(connection_timeout: 5, keep_alive_interval: 30)
    #   connection = pool.get_connection("pulsar://localhost:6650")
    #   # connection is shared - use it for producers/consumers
    #   pool.close_all
    class ConnectionPool
      # @param connection_timeout [Integer] timeout for establishing connections
      # @param keep_alive_interval [Integer] interval for keep-alive pings
      # @param max_connections_per_broker [Integer] max connections per broker
      def initialize(connection_timeout: Connection::DEFAULT_CONNECTION_TIMEOUT,
                     keep_alive_interval: Connection::DEFAULT_KEEP_ALIVE_INTERVAL,
                     max_connections_per_broker: 1)
        @connection_timeout = connection_timeout
        @keep_alive_interval = keep_alive_interval
        @max_connections_per_broker = max_connections_per_broker

        # Map of normalized_broker_url -> Array<Connection>
        @connections = {}
        @mutex = Mutex.new
        @closed = false

        # Round-robin index for multiple connections per broker
        @rr_index = {}
      end

      # Get a connection to the specified broker.
      #
      # Returns a shared connection to the broker. The connection is created
      # lazily on first request and reused for subsequent requests. If the
      # connection is closed, a new one is created.
      #
      # @param broker_url [String] broker URL (e.g., "pulsar://localhost:6650")
      # @return [Connection] a connected connection to the broker
      # @raise [ConnectionError] if connection fails
      # @raise [ConnectionClosedError] if pool is closed
      def get_connection(broker_url)
        raise Pulsar::ConnectionClosedError, "Connection pool is closed" if @closed

        normalized_url = normalize_url(broker_url)

        @mutex.synchronize do
          connections = @connections[normalized_url] ||= []

          # Find or create a ready connection
          connection = find_ready_connection(connections)

          if connection.nil? && connections.size < @max_connections_per_broker
            # Create new connection
            connection = create_connection(normalized_url)
            connections << connection
          elsif connection.nil?
            # All connections are down, reconnect one
            connection = reconnect_one(connections, normalized_url)
          end

          connection
        end
      end

      # Close all connections and mark the pool as closed.
      #
      # After calling this method, the pool cannot be used again.
      #
      # @return [void]
      def close_all
        @mutex.synchronize do
          return if @closed
          @closed = true

          @connections.each_value do |connections|
            connections.each do |connection|
              connection.close rescue nil
            end
          end
          @connections.clear
        end
      end

      # Check if the pool is closed
      # @return [Boolean]
      def closed?
        @closed
      end

      # Get the number of brokers with active connections
      # @return [Integer]
      def size
        @mutex.synchronize { @connections.size }
      end

      # Get total number of connections across all brokers
      # @return [Integer]
      def total_connections
        @mutex.synchronize do
          @connections.values.sum(&:size)
        end
      end

      private

      # Find a ready connection from the list using round-robin
      # @param connections [Array<Connection>]
      # @return [Connection, nil]
      def find_ready_connection(connections)
        return nil if connections.empty?

        # Simple round-robin selection among ready connections
        ready = connections.select(&:ready?)
        return nil if ready.empty?

        ready.first
      end

      # Reconnect one connection from the list
      # @param connections [Array<Connection>]
      # @param broker_url [String]
      # @return [Connection]
      def reconnect_one(connections, broker_url)
        # Remove the first closed connection and create a new one
        connections.reject!(&:closed?)

        connection = create_connection(broker_url)
        connections << connection
        connection
      end

      # Create and connect a new connection
      # @param broker_url [String]
      # @return [Connection]
      def create_connection(broker_url)
        host, port = parse_broker_url(broker_url)

        connection = Connection.new(
          host: host,
          port: port,
          connection_timeout: @connection_timeout,
          keep_alive_interval: @keep_alive_interval
        )
        connection.connect
        connection
      end

      # Normalize a broker URL for consistent lookup
      # @param broker_url [String]
      # @return [String]
      def normalize_url(broker_url)
        host, port = parse_broker_url(broker_url)
        "pulsar://#{host}:#{port}"
      end

      # Parse a broker URL into host and port
      # @param broker_url [String]
      # @return [Array<String, Integer>] [host, port]
      def parse_broker_url(broker_url)
        # Handle both "pulsar://host:port" and "host:port" formats
        url = broker_url.start_with?("pulsar://") ? broker_url : "pulsar://#{broker_url}"

        uri = URI.parse(url)
        host = uri.host
        port = uri.port || 6650

        raise ArgumentError, "Invalid broker URL: #{broker_url}" if host.nil? || host.empty?

        [host, port]
      end
    end
  end
end
