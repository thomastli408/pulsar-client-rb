# frozen_string_literal: true

require_relative "commands"
require_relative "service_url"
require_relative "topic_name"
require_relative "../errors"

module Pulsar
  module Internal
    # LookupService resolves topic names to broker addresses.
    #
    # When a producer or consumer wants to connect to a topic, it first needs
    # to discover which broker is serving that topic. The LookupService handles
    # this by:
    #
    # 1. Sending a LOOKUP command to the service URL broker
    # 2. Handling redirects (the initial broker may not be the owner)
    # 3. Following redirects up to MAX_REDIRECTS times
    # 4. Returning the final broker URL
    #
    # Usage:
    #   lookup = LookupService.new(connection_pool, service_url)
    #   broker_url = lookup.lookup("my-topic")
    #   # => "pulsar://broker2:6650"
    class LookupService
      # Maximum number of redirects to follow
      MAX_REDIRECTS = 20

      # Default lookup timeout in seconds
      DEFAULT_TIMEOUT = 30

      # @param connection_pool [ConnectionPool] pool for broker connections
      # @param service_url [ServiceUrl] the Pulsar service URL
      # @param timeout [Integer] timeout for lookup requests
      def initialize(connection_pool, service_url, timeout: DEFAULT_TIMEOUT)
        @connection_pool = connection_pool
        @service_url = service_url
        @timeout = timeout
      end

      # Look up the broker URL for a topic
      #
      # @param topic [String, TopicName] topic to look up
      # @return [String] broker URL (e.g., "pulsar://broker:6650")
      # @raise [LookupError] if lookup fails or max redirects exceeded
      # @raise [TimeoutError] if lookup times out
      def lookup(topic)
        topic_name = normalize_topic(topic)

        # Start with the service URL broker
        host, port = @service_url.next_host
        current_broker = "pulsar://#{host}:#{port}"
        authoritative = false

        redirects = 0

        loop do
          if redirects >= MAX_REDIRECTS
            raise Pulsar::LookupError, "Max redirects (#{MAX_REDIRECTS}) exceeded for topic: #{topic_name}"
          end

          result = send_lookup_request(current_broker, topic_name.to_s, authoritative: authoritative)

          case result[:type]
          when :connect
            # Found the broker
            return result[:broker_url]

          when :redirect
            # Follow redirect
            current_broker = result[:broker_url]
            authoritative = result[:authoritative]
            redirects += 1

          when :failed
            raise Pulsar::LookupError, "Lookup failed for topic #{topic_name}: #{result[:error]}"
          end
        end
      end

      # Look up the broker URL and return the connection
      #
      # Convenience method that performs lookup and returns a connection.
      #
      # @param topic [String, TopicName] topic to look up
      # @return [Connection] connection to the topic's broker
      # @raise [LookupError] if lookup fails
      # @raise [ConnectionError] if connection fails
      def lookup_connection(topic)
        broker_url = lookup(topic)
        @connection_pool.get_connection(broker_url)
      end

      private

      # Normalize topic to TopicName
      # @param topic [String, TopicName]
      # @return [TopicName]
      def normalize_topic(topic)
        case topic
        when TopicName
          topic
        when String
          TopicName.new(topic)
        else
          raise ArgumentError, "Invalid topic type: #{topic.class}"
        end
      end

      # Send a lookup request to a broker
      #
      # @param broker_url [String] broker to query
      # @param topic [String] fully qualified topic name
      # @param authoritative [Boolean] whether this is an authoritative lookup
      # @return [Hash] result with :type (:connect, :redirect, :failed) and relevant data
      def send_lookup_request(broker_url, topic, authoritative:)
        connection = @connection_pool.get_connection(broker_url)

        request_id = connection.next_request_id
        lookup_cmd = Commands.lookup(
          topic: topic,
          request_id: request_id,
          authoritative: authoritative
        )

        response = connection.send_request(lookup_cmd, timeout: @timeout)

        parse_lookup_response(response)
      end

      # Parse the lookup response
      #
      # @param response [Proto::CommandLookupTopicResponse]
      # @return [Hash] parsed result
      def parse_lookup_response(response)
        case response.response
        when :Connect
          {
            type: :connect,
            broker_url: normalize_broker_url(response.brokerServiceUrl),
            authoritative: response.authoritative
          }

        when :Redirect
          {
            type: :redirect,
            broker_url: normalize_broker_url(response.brokerServiceUrl),
            authoritative: response.authoritative
          }

        when :Failed
          {
            type: :failed,
            error: "#{response.error}: #{response.message}"
          }

        else
          {
            type: :failed,
            error: "Unknown lookup response type: #{response.response}"
          }
        end
      end

      # Normalize broker URL to consistent format
      #
      # The broker may return URLs in various formats. This ensures
      # we always use pulsar://host:port format.
      #
      # @param url [String] broker URL from response
      # @return [String] normalized URL
      def normalize_broker_url(url)
        return url if url.nil? || url.empty?

        # If it's already a pulsar:// URL, return as-is
        return url if url.start_with?("pulsar://")

        # Handle host:port format
        "pulsar://#{url}"
      end
    end
  end
end
