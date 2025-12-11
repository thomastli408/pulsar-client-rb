# frozen_string_literal: true

module Pulsar
  # Base error class for all Pulsar client errors
  class Error < StandardError; end

  # Raised when connection to the broker fails
  class ConnectionError < Error; end

  # Raised when an operation times out
  class TimeoutError < Error; end

  # Raised when the topic cannot be found
  class TopicNotFoundError < Error; end

  # Raised when a producer operation fails
  class ProducerError < Error; end

  # Raised when a consumer operation fails
  class ConsumerError < Error; end

  # Raised when topic lookup fails
  class LookupError < Error; end

  # Raised when subscription is busy (e.g., exclusive subscription already exists)
  class SubscriptionBusyError < Error; end

  # Raised when the connection is closed unexpectedly
  class ConnectionClosedError < ConnectionError; end

  # Raised when handshake with broker fails
  class HandshakeError < ConnectionError; end

  # Raised when the broker returns an error response
  class BrokerError < Error
    attr_reader :error_code, :error_message

    def initialize(error_code, error_message)
      @error_code = error_code
      @error_message = error_message
      super("Broker error: #{error_code} - #{error_message}")
    end
  end
end
