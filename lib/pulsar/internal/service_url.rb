# frozen_string_literal: true

require "uri"

module Pulsar
  module Internal
    # ServiceUrl parses and manages Pulsar service URLs.
    #
    # Supports URLs in the format:
    #   pulsar://host1:6650,host2:6650,host3:6650
    #
    # Features:
    # - Multiple hosts for failover/load balancing
    # - Round-robin host selection via #next_host
    # - Default port 6650 if not specified
    #
    # Note: Only pulsar:// URLs are supported (no TLS/pulsar+ssl://)
    #
    # Usage:
    #   url = ServiceUrl.new("pulsar://broker1:6650,broker2:6650")
    #   host, port = url.next_host  # Returns ["broker1", 6650]
    #   host, port = url.next_host  # Returns ["broker2", 6650]
    #   host, port = url.next_host  # Returns ["broker1", 6650] (wraps around)
    class ServiceUrl
      # Default Pulsar port
      DEFAULT_PORT = 6650

      # Supported schemes
      SCHEME = "pulsar"

      # @return [Array<Array<String, Integer>>] list of [host, port] pairs
      attr_reader :hosts

      # @return [String] original URL string
      attr_reader :url

      # Initialize a ServiceUrl from a URL string
      # @param url [String] Pulsar service URL (e.g., "pulsar://host1:6650,host2:6650")
      # @raise [ArgumentError] if URL is invalid or uses unsupported scheme
      def initialize(url)
        @url = url
        @hosts = parse_url(url)
        @index = 0
        @mutex = Mutex.new

        raise ArgumentError, "No hosts specified in URL: #{url}" if @hosts.empty?
      end

      # Get the next host using round-robin selection
      # @return [Array<String, Integer>] [host, port] pair
      def next_host
        @mutex.synchronize do
          host = @hosts[@index]
          @index = (@index + 1) % @hosts.size
          host
        end
      end

      # Get all hosts as broker URLs
      # @return [Array<String>] list of broker URLs
      def broker_urls
        @hosts.map { |host, port| "pulsar://#{host}:#{port}" }
      end

      # Get the first host (useful for single-host URLs)
      # @return [Array<String, Integer>] [host, port] pair
      def first_host
        @hosts.first
      end

      # Check if this is a multi-host URL
      # @return [Boolean]
      def multi_host?
        @hosts.size > 1
      end

      # @return [String] string representation
      def to_s
        @url
      end

      private

      # Parse the URL string into host/port pairs
      # @param url [String]
      # @return [Array<Array<String, Integer>>]
      def parse_url(url)
        raise ArgumentError, "URL cannot be nil or empty" if url.nil? || url.empty?

        # Check scheme
        unless url.start_with?("#{SCHEME}://")
          raise ArgumentError, "Invalid scheme. Only '#{SCHEME}://' is supported. Got: #{url}"
        end

        # Remove scheme
        host_part = url.sub("#{SCHEME}://", "")

        # Remove any path component
        host_part = host_part.split("/").first

        raise ArgumentError, "No hosts specified in URL: #{url}" if host_part.nil? || host_part.empty?

        # Split by comma for multiple hosts
        host_strings = host_part.split(",").map(&:strip)

        host_strings.map do |host_string|
          parse_host(host_string)
        end
      end

      # Parse a single host:port string
      # @param host_string [String] e.g., "localhost:6650" or "localhost"
      # @return [Array<String, Integer>] [host, port]
      def parse_host(host_string)
        raise ArgumentError, "Empty host in URL" if host_string.empty?

        if host_string.include?(":")
          parts = host_string.split(":")
          raise ArgumentError, "Invalid host format: #{host_string}" if parts.size != 2

          host = parts[0]
          port = parts[1].to_i

          raise ArgumentError, "Empty hostname in: #{host_string}" if host.empty?
          raise ArgumentError, "Invalid port in: #{host_string}" if port <= 0 || port > 65535

          [host, port]
        else
          [host_string, DEFAULT_PORT]
        end
      end
    end
  end
end
