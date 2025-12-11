# frozen_string_literal: true

require_relative "buffer"
require_relative "checksum"
require_relative "proto/PulsarApi_pb"

# Implements Pulsar Protocol's Framing
# https://pulsar.apache.org/docs/3.0.x/developing-binary-protocol/#framing
module Pulsar
  module Internal
    # Commands handles encoding and decoding of Pulsar wire protocol frames.
    #
    # Wire format for simple commands:
    #   [4 bytes: total_size] [4 bytes: cmd_size] [cmd_size bytes: BaseCommand protobuf]
    #
    # Wire format for message commands (send/receive):
    #   [4 bytes: total_size]
    #   [4 bytes: cmd_size]
    #   [cmd_size bytes: CommandSend/Message protobuf]
    #   [2 bytes: magic (0x0e01)]
    #   [4 bytes: checksum]
    #   [4 bytes: metadata_size]
    #   [metadata_size bytes: MessageMetadata protobuf]
    #   [payload bytes]
    module Commands
      TOTAL_SIZE_BYTES = 4
      CMD_SIZE_BYTES = 4
      METADATA_SIZE_BYTES = 4

      # Command types that carry message payloads
      PAYLOAD_COMMANDS = [
        :SEND,
        :MESSAGE
      ].freeze

      class << self
        # Encode a BaseCommand into a wire protocol frame (without payload)
        # @param base_command [Proto::BaseCommand] the command to encode
        # @return [String] wire protocol frame bytes (binary)
        def encode_command(base_command)
          cmd_bytes = Proto::BaseCommand.encode(base_command)
          cmd_size = cmd_bytes.bytesize
          frame_size = CMD_SIZE_BYTES + cmd_size

          buffer = Buffer.new(TOTAL_SIZE_BYTES + frame_size)
          buffer.write_uint32(frame_size)
          buffer.write_uint32(cmd_size)
          buffer.write(cmd_bytes)
          buffer.to_bytes
        end

        # Encode a message with payload (for producer send)
        # @param command_send [Proto::CommandSend] the send command
        # @param metadata [Proto::MessageMetadata] message metadata
        # @param payload [String] message payload bytes
        # @return [String] wire protocol frame bytes (binary)
        def encode_message(command_send, metadata, payload)
          # Build the base command with CommandSend
          base_command = Proto::BaseCommand.new(
            type: :SEND,
            send: command_send
          )
          cmd_bytes = Proto::BaseCommand.encode(base_command)

          metadata_bytes = Proto::MessageMetadata.encode(metadata)
          payload = payload.b if payload.encoding != Encoding::BINARY

          # Build checksummed portion: [4 bytes metadata_size] [metadata] [payload]
          checksummed_buffer = Buffer.new(METADATA_SIZE_BYTES + metadata_bytes.bytesize + payload.bytesize)
          checksummed_buffer.write_uint32(metadata_bytes.bytesize)
          checksummed_buffer.write(metadata_bytes)
          checksummed_buffer.write(payload)
          checksummed_data = checksummed_buffer.to_bytes

          # Calculate checksum over metadata_size + metadata + payload
          checksum = Checksum.crc32c(checksummed_data)

          # Calculate total frame size
          # frame_size = cmd_size + command + magic + checksum + metadata_size + metadata + payload
          payload_section_size = Checksum::MAGIC_SIZE + Checksum::CHECKSUM_SIZE + checksummed_data.bytesize
          frame_size = CMD_SIZE_BYTES + cmd_bytes.bytesize + payload_section_size

          # Build the complete frame
          buffer = Buffer.new(TOTAL_SIZE_BYTES + frame_size)
          buffer.write_uint32(frame_size)
          buffer.write_uint32(cmd_bytes.bytesize)
          buffer.write(cmd_bytes)
          buffer.write_uint16(Checksum::MAGIC_CRC32C)
          buffer.write_uint32(checksum)
          buffer.write(checksummed_data)
          buffer.to_bytes
        end

        # Decode a frame from a buffer
        # @param buffer [Buffer] buffer containing frame data
        # @return [Hash, nil] decoded frame with :command and optional :metadata/:payload,
        #                     or nil if not enough data
        # @raise [FrameError] if frame is malformed
        def decode_frame(buffer)
          # Need at least frame_size header
          return nil if buffer.readable_bytes < TOTAL_SIZE_BYTES

          # Mark position for potential rollback
          buffer.mark

          # Read frame size
          frame_size = buffer.read_uint32

          # Check if we have the complete frame
          if buffer.readable_bytes < frame_size
            buffer.reset_to_mark
            return nil
          end

          # Read command size
          cmd_size = buffer.read_uint32

          # Read and decode command
          cmd_bytes = buffer.read(cmd_size)
          base_command = Proto::BaseCommand.decode(cmd_bytes)

          result = { command: base_command }

          # Check if this command type has a payload
          remaining = frame_size - CMD_SIZE_BYTES - cmd_size
          if remaining > 0 && PAYLOAD_COMMANDS.include?(base_command.type)
            payload_data = decode_payload(buffer, remaining)
            result.merge!(payload_data)
          elsif remaining > 0
            # Skip any unexpected remaining bytes
            buffer.skip(remaining)
          end

          # TODO: use better logger
          puts "#{Time.now} [Pulsar::Internal::Commands#decode_frame] Decoded Frame: #{result.inspect}"

          result
        end

        # Build a BaseCommand with the appropriate type and command set
        # @param type [Symbol] command type (e.g., :CONNECT, :PRODUCER, :SUBSCRIBE)
        # @param command [Google::Protobuf::MessageExts] the specific command message
        # @return [Proto::BaseCommand] built command
        def base_command(type, command)
          base = Proto::BaseCommand.new(type: type)

          # Map command type to field name
          field_name = type_to_field_name(type)
          base.public_send("#{field_name}=", command)

          base
        end

        # Build a CONNECT command
        # @param client_version [String] client identifier string
        # @param protocol_version [Integer] protocol version number
        # @return [Proto::BaseCommand]
        def connect(client_version:, protocol_version: 20)
          connect_cmd = Proto::CommandConnect.new(
            client_version: client_version,
            protocol_version: protocol_version
          )
          base_command(:CONNECT, connect_cmd)
        end

        # Build a PING command
        # @return [Proto::BaseCommand]
        def ping
          base_command(:PING, Proto::CommandPing.new)
        end

        # Build a PONG command
        # @return [Proto::BaseCommand]
        def pong
          base_command(:PONG, Proto::CommandPong.new)
        end

        # Build a PRODUCER command
        # @param producer_id [Integer] unique producer ID
        # @param request_id [Integer] request correlation ID
        # @param topic [String] fully qualified topic name
        # @param producer_name [String, nil] optional producer name
        # @return [Proto::BaseCommand]
        def producer(producer_id:, request_id:, topic:, producer_name: nil)
          producer_cmd = Proto::CommandProducer.new(
            topic: topic,
            producer_id: producer_id,
            request_id: request_id
          )
          producer_cmd.producer_name = producer_name if producer_name
          base_command(:PRODUCER, producer_cmd)
        end

        # Build a SUBSCRIBE command
        # @param consumer_id [Integer] unique consumer ID
        # @param request_id [Integer] request correlation ID
        # @param topic [String] fully qualified topic name
        # @param subscription [String] subscription name
        # @param sub_type [Symbol] subscription type (:Shared, :Failover, :Exclusive)
        # @param consumer_name [String, nil] optional consumer name
        # @param initial_position [Symbol] :Latest or :Earliest
        # @return [Proto::BaseCommand]
        def subscribe(consumer_id:, request_id:, topic:, subscription:, sub_type: :Shared,
                      consumer_name: nil, initial_position: :Latest)
          subscribe_cmd = Proto::CommandSubscribe.new(
            topic: topic,
            subscription: subscription,
            subType: sub_type,
            consumer_id: consumer_id,
            request_id: request_id,
            initialPosition: initial_position
          )
          subscribe_cmd.consumer_name = consumer_name if consumer_name
          base_command(:SUBSCRIBE, subscribe_cmd)
        end

        # Build a FLOW command
        # @param consumer_id [Integer] consumer ID
        # @param message_permits [Integer] number of messages the consumer can receive
        # @return [Proto::BaseCommand]
        def flow(consumer_id:, message_permits:)
          flow_cmd = Proto::CommandFlow.new(
            consumer_id: consumer_id,
            messagePermits: message_permits
          )
          base_command(:FLOW, flow_cmd)
        end

        # Build an ACK command
        # @param consumer_id [Integer] consumer ID
        # @param message_id [Proto::MessageIdData] message ID to acknowledge
        # @param ack_type [Symbol] :Individual or :Cumulative
        # @return [Proto::BaseCommand]
        def ack(consumer_id:, message_id:, ack_type: :Individual)
          ack_cmd = Proto::CommandAck.new(
            consumer_id: consumer_id,
            ack_type: ack_type,
            message_id: [message_id]
          )
          base_command(:ACK, ack_cmd)
        end

        # Build a REDELIVER_UNACKNOWLEDGED_MESSAGES command
        # @param consumer_id [Integer] consumer ID
        # @param message_ids [Array<Proto::MessageIdData>] message IDs to redeliver (empty = all)
        # @return [Proto::BaseCommand]
        def redeliver_unacknowledged(consumer_id:, message_ids: [])
          redeliver_cmd = Proto::CommandRedeliverUnacknowledgedMessages.new(
            consumer_id: consumer_id,
            message_ids: message_ids
          )
          base_command(:REDELIVER_UNACKNOWLEDGED_MESSAGES, redeliver_cmd)
        end

        # Build a CLOSE_PRODUCER command
        # @param producer_id [Integer] producer ID to close
        # @param request_id [Integer] request correlation ID
        # @return [Proto::BaseCommand]
        def close_producer(producer_id:, request_id:)
          close_cmd = Proto::CommandCloseProducer.new(
            producer_id: producer_id,
            request_id: request_id
          )
          base_command(:CLOSE_PRODUCER, close_cmd)
        end

        # Build a CLOSE_CONSUMER command
        # @param consumer_id [Integer] consumer ID to close
        # @param request_id [Integer] request correlation ID
        # @return [Proto::BaseCommand]
        def close_consumer(consumer_id:, request_id:)
          close_cmd = Proto::CommandCloseConsumer.new(
            consumer_id: consumer_id,
            request_id: request_id
          )
          base_command(:CLOSE_CONSUMER, close_cmd)
        end

        # Build a LOOKUP command
        # @param topic [String] topic to lookup
        # @param request_id [Integer] request correlation ID
        # @param authoritative [Boolean] whether this is an authoritative lookup
        # @return [Proto::BaseCommand]
        def lookup(topic:, request_id:, authoritative: false)
          lookup_cmd = Proto::CommandLookupTopic.new(
            topic: topic,
            request_id: request_id,
            authoritative: authoritative
          )
          base_command(:LOOKUP, lookup_cmd)
        end

        # Build message metadata for sending
        # @param producer_name [String] name of the producer
        # @param sequence_id [Integer] sequence ID for this message
        # @param publish_time [Integer] publish timestamp in milliseconds
        # @param properties [Hash<String, String>] message properties
        # @param partition_key [String, nil] optional partition key
        # @param event_time [Integer, nil] optional event time in milliseconds
        # @return [Proto::MessageMetadata]
        def message_metadata(producer_name:, sequence_id:, publish_time: nil,
                             properties: {}, partition_key: nil, event_time: nil)
          metadata = Proto::MessageMetadata.new(
            producer_name: producer_name,
            sequence_id: sequence_id,
            publish_time: publish_time || (Time.now.to_f * 1000).to_i
          )

          properties.each do |key, value|
            metadata.properties << Proto::KeyValue.new(key: key.to_s, value: value.to_s)
          end

          metadata.partition_key = partition_key if partition_key
          metadata.event_time = event_time if event_time

          metadata
        end

        # Build a CommandSend for producing messages
        # @param producer_id [Integer] producer ID
        # @param sequence_id [Integer] sequence ID for this message
        # @param num_messages [Integer] number of messages (1 for non-batched)
        # @return [Proto::CommandSend]
        def send_command(producer_id:, sequence_id:, num_messages: 1)
          Proto::CommandSend.new(
            producer_id: producer_id,
            sequence_id: sequence_id,
            num_messages: num_messages
          )
        end

        # Build a message ID proto
        # @param ledger_id [Integer] ledger ID
        # @param entry_id [Integer] entry ID
        # @param partition [Integer] partition index (-1 for non-partitioned)
        # @param batch_index [Integer] batch index (-1 for non-batched)
        # @return [Proto::MessageIdData]
        def message_id(ledger_id:, entry_id:, partition: -1, batch_index: -1)
          Proto::MessageIdData.new(
            ledgerId: ledger_id,
            entryId: entry_id,
            partition: partition,
            batch_index: batch_index
          )
        end

        private

        # Decode payload section (magic + checksum + metadata + payload)
        # @param buffer [Buffer] buffer positioned at start of payload section
        # @param length [Integer] length of payload section
        # @return [Hash] hash with :metadata and :payload keys
        def decode_payload(buffer, length)
          start_position = buffer.read_position

          # Check for magic number
          if Checksum.has_checksum?(buffer)
            # Read and verify checksum
            magic = buffer.read_uint16
            expected_checksum = buffer.read_uint32

            checksummed_length = length - Checksum::TOTAL_SIZE
            checksummed_data = buffer.peek(checksummed_length)
            actual_checksum = Checksum.crc32c(checksummed_data)

            unless actual_checksum == expected_checksum
              raise ChecksumError, "Checksum mismatch: expected 0x#{expected_checksum.to_s(16)}, got 0x#{actual_checksum.to_s(16)}"
            end

            # Parse metadata
            metadata_size = buffer.read_uint32
            metadata_bytes = buffer.read(metadata_size)
            metadata = Proto::MessageMetadata.decode(metadata_bytes)

            # Remaining bytes are payload
            payload_size = checksummed_length - METADATA_SIZE_BYTES - metadata_size
            payload = buffer.read(payload_size)
          else
            # No checksum, parse directly (older protocol)
            metadata_size = buffer.read_uint32
            metadata_bytes = buffer.read(metadata_size)
            metadata = Proto::MessageMetadata.decode(metadata_bytes)

            # Remaining bytes are payload
            payload_size = length - METADATA_SIZE_BYTES - metadata_size
            payload = buffer.read(payload_size)
          end

          { metadata: metadata, payload: payload }
        end

        # Map command type enum to field name
        # @param type [Symbol] command type
        # @return [String] field name on BaseCommand
        def type_to_field_name(type)
          case type
          when :CONNECT then "connect"
          when :CONNECTED then "connected"
          when :SUBSCRIBE then "subscribe"
          when :PRODUCER then "producer"
          when :SEND then "send"
          when :SEND_RECEIPT then "send_receipt"
          when :SEND_ERROR then "send_error"
          when :MESSAGE then "message"
          when :ACK then "ack"
          when :ACK_RESPONSE then "ackResponse"
          when :FLOW then "flow"
          when :UNSUBSCRIBE then "unsubscribe"
          when :SUCCESS then "success"
          when :ERROR then "error"
          when :CLOSE_PRODUCER then "close_producer"
          when :CLOSE_CONSUMER then "close_consumer"
          when :PRODUCER_SUCCESS then "producer_success"
          when :PING then "ping"
          when :PONG then "pong"
          when :REDELIVER_UNACKNOWLEDGED_MESSAGES then "redeliverUnacknowledgedMessages"
          when :LOOKUP then "lookupTopic"
          when :LOOKUP_RESPONSE then "lookupTopicResponse"
          when :PARTITIONED_METADATA then "partitionMetadata"
          when :PARTITIONED_METADATA_RESPONSE then "partitionMetadataResponse"
          when :CONSUMER_STATS then "consumerStats"
          when :CONSUMER_STATS_RESPONSE then "consumerStatsResponse"
          when :REACHED_END_OF_TOPIC then "reachedEndOfTopic"
          when :SEEK then "seek"
          when :GET_LAST_MESSAGE_ID then "getLastMessageId"
          when :GET_LAST_MESSAGE_ID_RESPONSE then "getLastMessageIdResponse"
          when :ACTIVE_CONSUMER_CHANGE then "active_consumer_change"
          when :GET_TOPICS_OF_NAMESPACE then "getTopicsOfNamespace"
          when :GET_TOPICS_OF_NAMESPACE_RESPONSE then "getTopicsOfNamespaceResponse"
          when :GET_SCHEMA then "getSchema"
          when :GET_SCHEMA_RESPONSE then "getSchemaResponse"
          when :AUTH_CHALLENGE then "authChallenge"
          when :AUTH_RESPONSE then "authResponse"
          when :GET_OR_CREATE_SCHEMA then "getOrCreateSchema"
          when :GET_OR_CREATE_SCHEMA_RESPONSE then "getOrCreateSchemaResponse"
          when :TOPIC_MIGRATED then "topicMigrated"
          else
            raise ArgumentError, "Unknown command type: #{type}"
          end
        end
      end
    end

    # Error raised when frame parsing fails
    class FrameError < StandardError; end
  end
end
