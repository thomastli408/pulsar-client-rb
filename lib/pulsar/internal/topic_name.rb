# frozen_string_literal: true

module Pulsar
  module Internal
    # TopicName parses and represents Pulsar topic names.
    #
    # Full topic name format:
    #   {domain}://{tenant}/{namespace}/{topic}
    #
    # Where:
    # - domain: "persistent" or "non-persistent"
    # - tenant: tenant name (e.g., "public")
    # - namespace: namespace name (e.g., "default")
    # - topic: local topic name
    #
    # Short forms are expanded to full names:
    # - "my-topic" => "persistent://public/default/my-topic"
    # - "my-tenant/my-ns/my-topic" => "persistent://my-tenant/my-ns/my-topic"
    #
    # Usage:
    #   topic = TopicName.new("my-topic")
    #   topic.name       # => "persistent://public/default/my-topic"
    #   topic.domain     # => "persistent"
    #   topic.tenant     # => "public"
    #   topic.namespace  # => "default"
    #   topic.local_name # => "my-topic"
    class TopicName
      # Supported domains
      PERSISTENT = "persistent"
      NON_PERSISTENT = "non-persistent"
      VALID_DOMAINS = [PERSISTENT, NON_PERSISTENT].freeze

      # Default values for short-form topic names
      DEFAULT_DOMAIN = PERSISTENT
      DEFAULT_TENANT = "public"
      DEFAULT_NAMESPACE = "default"

      # Topic name pattern
      FULL_TOPIC_PATTERN = %r{\A(#{PERSISTENT}|#{NON_PERSISTENT})://([^/]+)/([^/]+)/(.+)\z}

      # @return [String] domain ("persistent" or "non-persistent")
      attr_reader :domain

      # @return [String] tenant name
      attr_reader :tenant

      # @return [String] namespace name
      attr_reader :namespace

      # @return [String] local topic name (without domain/tenant/namespace)
      attr_reader :local_name

      # @return [String] fully qualified topic name
      attr_reader :name

      # Initialize a TopicName from a topic string
      # @param topic [String] topic name (short or full form)
      # @raise [ArgumentError] if topic name is invalid
      def initialize(topic)
        raise ArgumentError, "Topic name cannot be nil or empty" if topic.nil? || topic.empty?

        parse_topic(topic)
        @name = build_full_name
      end

      # Check if this is a persistent topic
      # @return [Boolean]
      def persistent?
        @domain == PERSISTENT
      end

      # Check if this is a non-persistent topic
      # @return [Boolean]
      def non_persistent?
        @domain == NON_PERSISTENT
      end

      # Get the namespace path (tenant/namespace)
      # @return [String]
      def namespace_path
        "#{@tenant}/#{@namespace}"
      end

      # Get the full namespace name with domain
      # @return [String]
      def full_namespace
        "#{@domain}://#{@tenant}/#{@namespace}"
      end

      # @return [String] fully qualified topic name
      def to_s
        @name
      end

      # Check equality with another TopicName or string
      # @param other [TopicName, String]
      # @return [Boolean]
      def ==(other)
        case other
        when TopicName
          @name == other.name
        when String
          @name == TopicName.new(other).name
        else
          false
        end
      end

      alias eql? ==

      # Hash code for use in Hash keys
      # @return [Integer]
      def hash
        @name.hash
      end

      private

      # Parse the topic string into components
      # @param topic [String]
      def parse_topic(topic)
        topic = topic.strip

        # Check for full topic name format (with domain://)
        if topic.include?("://")
          parse_full_topic(topic)
          return
        end

        # Check for short forms
        parts = topic.split("/")

        case parts.size
        when 1
          # Just topic name: "my-topic"
          @domain = DEFAULT_DOMAIN
          @tenant = DEFAULT_TENANT
          @namespace = DEFAULT_NAMESPACE
          @local_name = parts[0]
        when 3
          # tenant/namespace/topic format
          @domain = DEFAULT_DOMAIN
          @tenant = parts[0]
          @namespace = parts[1]
          @local_name = parts[2]
        else
          raise ArgumentError, "Invalid topic name format: #{topic}. " \
            "Expected 'topic', 'tenant/namespace/topic', or '{domain}://tenant/namespace/topic'"
        end

        validate_components
      end

      # Parse a full topic name with domain://
      # @param topic [String]
      def parse_full_topic(topic)
        # Extract domain
        domain_end = topic.index("://")
        @domain = topic[0...domain_end]

        # Validate domain
        unless VALID_DOMAINS.include?(@domain)
          raise ArgumentError, "Invalid domain: #{@domain}. Must be one of: #{VALID_DOMAINS.join(', ')}"
        end

        # Extract the rest: tenant/namespace/local_name
        rest = topic[(domain_end + 3)..]

        # Split by / but preserve slashes in local_name (last component)
        parts = rest.split("/", 3)

        if parts.size < 3
          raise ArgumentError, "Invalid topic name format: #{topic}. " \
            "Full topic name must be '{domain}://tenant/namespace/topic'"
        end

        @tenant = parts[0]
        @namespace = parts[1]
        @local_name = parts[2]

        validate_components
      end

      # Build the fully qualified topic name
      # @return [String]
      def build_full_name
        "#{@domain}://#{@tenant}/#{@namespace}/#{@local_name}"
      end

      # Validate all components are non-empty
      def validate_components
        raise ArgumentError, "Domain cannot be empty" if @domain.nil? || @domain.empty?
        raise ArgumentError, "Tenant cannot be empty" if @tenant.nil? || @tenant.empty?
        raise ArgumentError, "Namespace cannot be empty" if @namespace.nil? || @namespace.empty?
        raise ArgumentError, "Topic local name cannot be empty" if @local_name.nil? || @local_name.empty?

        unless VALID_DOMAINS.include?(@domain)
          raise ArgumentError, "Invalid domain: #{@domain}. Must be one of: #{VALID_DOMAINS.join(', ')}"
        end
      end
    end
  end
end
