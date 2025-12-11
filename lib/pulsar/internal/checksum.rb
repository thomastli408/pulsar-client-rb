# frozen_string_literal: true

require "digest/crc32c"

module Pulsar
  module Internal
    # Checksum provides CRC32-C checksum calculation for the Pulsar wire protocol.
    # Pulsar uses CRC32-C (Castagnoli) for message integrity verification.
    module Checksum
      # Magic number identifying CRC32-C checksum in the wire protocol
      # This is placed before the checksum in message frames
      MAGIC_CRC32C = 0x0e01
      MAGIC_SIZE = 2
      CHECKSUM_SIZE = 4

      TOTAL_SIZE = MAGIC_SIZE + CHECKSUM_SIZE

      class << self
        # Calculate CRC32-C checksum for the given data
        # @param data [String] binary data to checksum
        # @return [Integer] 32-bit CRC32-C checksum value
        def crc32c(data)
          Digest::CRC32c.checksum(data)
        end

        # Verify that data matches the expected checksum
        # @param data [String] binary data to verify
        # @param expected_checksum [Integer] expected CRC32-C checksum value
        # @return [Boolean] true if checksums match
        def verify(data, expected_checksum)
          crc32c(data) == expected_checksum
        end

        # Calculate checksum and return it with the magic number prefix
        # Used when encoding messages with checksums
        # @param data [String] binary data to checksum
        # @return [String] 6-byte string: 2-byte magic + 4-byte checksum (binary)
        def encode(data)
          checksum = crc32c(data)
          [MAGIC_CRC32C, checksum].pack("nN")
        end

        # Parse magic number and checksum from a buffer
        # @param buffer [Buffer] buffer positioned at the start of the checksum area
        # @return [Array<Integer, Integer>] [magic, checksum]
        def decode(buffer)
          magic = buffer.read_uint16
          checksum = buffer.read_uint32
          [magic, checksum]
        end

        # Check if data has a valid magic number prefix at the current position
        # @param buffer [Buffer] buffer to check
        # @return [Boolean] true if magic number is present
        def has_checksum?(buffer)
          return false if buffer.readable_bytes < MAGIC_SIZE
          magic = buffer.peek(MAGIC_SIZE).unpack1("n")
          magic == MAGIC_CRC32C
        end

        # Verify checksum from a buffer and return the verified data portion
        # @param buffer [Buffer] buffer positioned at the magic number
        # @param data_length [Integer] length of data after magic+checksum to verify
        # @return [Boolean] true if checksum is valid
        # @raise [ChecksumError] if magic number is incorrect or checksum doesn't match
        def verify_from_buffer(buffer, data_length)
          magic = buffer.read_uint16
          unless magic == MAGIC_CRC32C
            raise ChecksumError, "Invalid magic number: expected 0x#{MAGIC_CRC32C.to_s(16)}, got 0x#{magic.to_s(16)}"
          end

          expected_checksum = buffer.read_uint32
          data = buffer.peek(data_length)
          actual_checksum = crc32c(data)

          unless actual_checksum == expected_checksum
            raise ChecksumError, "Checksum mismatch: expected 0x#{expected_checksum.to_s(16)}, got 0x#{actual_checksum.to_s(16)}"
          end

          true
        end

        # Resume CRC calculation by updating with additional data
        # This is useful for calculating checksum over multiple pieces of data
        # @param crc [Digest::CRC32c, nil] existing CRC state or nil to start fresh
        # @param data [String] additional data to include
        # @return [Digest::CRC32c] updated CRC state
        def update(crc, data)
          crc ||= Digest::CRC32c.new
          crc.update(data)
          crc
        end

        # Finalize a CRC calculation and return the checksum value
        # @param crc [Digest::CRC32c] CRC state to finalize
        # @return [Integer] 32-bit checksum value
        def finalize(crc)
          crc.checksum
        end
      end
    end

    # Error raised when checksum verification fails
    class ChecksumError < StandardError; end
  end
end
