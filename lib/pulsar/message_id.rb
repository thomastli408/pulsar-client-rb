# frozen_string_literal: true

require_relative "internal/proto/PulsarApi_pb"

module Pulsar
  # MessageId uniquely identifies a message in Pulsar.
  #
  # A message ID consists of:
  # - ledger_id: The ledger where the message is stored
  # - entry_id: The entry within the ledger
  # - partition: The partition index (-1 for non-partitioned topics)
  # - batch_index: The index within a batch (-1 for non-batched messages)
  #
  # Usage:
  #   # From a send receipt
  #   message_id = MessageId.from_proto(receipt.message_id)
  #   puts message_id  # => "123:456:-1:-1"
  #
  #   # Create directly
  #   message_id = MessageId.new(ledger_id: 123, entry_id: 456)
  class MessageId
    # @return [Integer] ledger ID
    attr_reader :ledger_id

    # @return [Integer] entry ID
    attr_reader :entry_id

    # @return [Integer] partition index (-1 for non-partitioned)
    attr_reader :partition

    # @return [Integer] batch index (-1 for non-batched)
    attr_reader :batch_index

    # Initialize a new message ID
    #
    # @param ledger_id [Integer] ledger ID
    # @param entry_id [Integer] entry ID
    # @param partition [Integer] partition index (default: -1)
    # @param batch_index [Integer] batch index (default: -1)
    def initialize(ledger_id:, entry_id:, partition: -1, batch_index: -1)
      @ledger_id = ledger_id
      @entry_id = entry_id
      @partition = partition
      @batch_index = batch_index
    end

    # Convert to string representation
    #
    # @return [String] string in format "ledger_id:entry_id:partition:batch_index"
    def to_s
      "#{ledger_id}:#{entry_id}:#{partition}:#{batch_index}"
    end

    # Create MessageId from protobuf MessageIdData
    #
    # @param message_id_data [Proto::MessageIdData] protobuf message ID
    # @return [MessageId]
    def self.from_proto(message_id_data)
      new(
        ledger_id: message_id_data.ledgerId,
        entry_id: message_id_data.entryId,
        partition: message_id_data.partition,
        batch_index: message_id_data.batch_index
      )
    end

    # Convert to protobuf MessageIdData
    #
    # @return [Proto::MessageIdData]
    def to_proto
      Proto::MessageIdData.new(
        ledgerId: @ledger_id,
        entryId: @entry_id,
        partition: @partition,
        batch_index: @batch_index
      )
    end

    # Check equality with another MessageId
    #
    # @param other [MessageId]
    # @return [Boolean]
    def ==(other)
      return false unless other.is_a?(MessageId)

      ledger_id == other.ledger_id &&
        entry_id == other.entry_id &&
        partition == other.partition &&
        batch_index == other.batch_index
    end

    alias eql? ==

    # Generate hash code for use in Hash keys
    #
    # @return [Integer]
    def hash
      [ledger_id, entry_id, partition, batch_index].hash
    end

    # Compare with another MessageId for ordering
    #
    # @param other [MessageId]
    # @return [Integer] -1, 0, or 1
    def <=>(other)
      return nil unless other.is_a?(MessageId)

      result = ledger_id <=> other.ledger_id
      return result unless result.zero?

      result = entry_id <=> other.entry_id
      return result unless result.zero?

      result = partition <=> other.partition
      return result unless result.zero?

      batch_index <=> other.batch_index
    end

    include Comparable
  end
end
