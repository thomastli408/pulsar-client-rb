# frozen_string_literal: true

module Pulsar
  module Internal
    # Buffer provides a byte buffer for reading and writing binary data
    # in the Pulsar wire protocol. All multi-byte integers are encoded
    # in big-endian (network byte order).
    class Buffer
      # @return [Integer] current read position in the buffer
      attr_reader :read_position

      # @return [Integer] current write position in the buffer
      attr_reader :write_position

      # Initialize a new buffer
      # @param initial_capacity [Integer] initial buffer size in bytes
      def initialize(initial_capacity = 4096)
        @data = String.new(capacity: initial_capacity)
        @data.force_encoding(Encoding::BINARY)
        @read_position = 0
        @write_position = 0
      end

      # Write a 16-bit unsigned integer in big-endian format
      # @param value [Integer] value to write (0-65535)
      # @return [self]
      def write_uint16(value)
        ensure_capacity(2)
        @data[@write_position, 2] = [value].pack("n")
        @write_position += 2
        self
      end

      # Write a 32-bit unsigned integer in big-endian format
      # @param value [Integer] value to write (0-4294967295)
      # @return [self]
      def write_uint32(value)
        ensure_capacity(4)
        @data[@write_position, 4] = [value].pack("N")
        @write_position += 4
        self
      end

      # Write a 64-bit unsigned integer in big-endian format
      # @param value [Integer] value to write
      # @return [self]
      def write_uint64(value)
        ensure_capacity(8)
        @data[@write_position, 8] = [value].pack("Q>")
        @write_position += 8
        self
      end

      # Write raw bytes to the buffer
      # @param bytes [String] bytes to write
      # @return [self]
      def write(bytes)
        bytes = bytes.b if bytes.encoding != Encoding::BINARY
        ensure_capacity(bytes.bytesize)
        @data[@write_position, bytes.bytesize] = bytes
        @write_position += bytes.bytesize
        self
      end

      # Read a 16-bit unsigned integer in big-endian format
      # @return [Integer] the value read
      # @raise [BufferUnderflowError] if not enough bytes available
      def read_uint16
        ensure_readable(2)
        value = @data[@read_position, 2].unpack1("n")
        @read_position += 2
        value
      end

      # Read a 32-bit unsigned integer in big-endian format
      # @return [Integer] the value read
      # @raise [BufferUnderflowError] if not enough bytes available
      def read_uint32
        ensure_readable(4)
        value = @data[@read_position, 4].unpack1("N")
        @read_position += 4
        value
      end

      # Read a 64-bit unsigned integer in big-endian format
      # @return [Integer] the value read
      # @raise [BufferUnderflowError] if not enough bytes available
      def read_uint64
        ensure_readable(8)
        value = @data[@read_position, 8].unpack1("Q>")
        @read_position += 8
        value
      end

      # Read raw bytes from the buffer
      # @param length [Integer] number of bytes to read
      # @return [String] bytes read (binary encoding)
      # @raise [BufferUnderflowError] if not enough bytes available
      def read(length)
        ensure_readable(length)
        bytes = @data[@read_position, length]
        @read_position += length
        bytes
      end

      # Peek at bytes without advancing read position
      # @param length [Integer] number of bytes to peek
      # @return [String] bytes peeked (binary encoding)
      # @raise [BufferUnderflowError] if not enough bytes available
      def peek(length)
        ensure_readable(length)
        @data[@read_position, length]
      end

      # Skip bytes without reading them
      # @param length [Integer] number of bytes to skip
      # @return [self]
      # @raise [BufferUnderflowError] if not enough bytes available
      def skip(length)
        ensure_readable(length)
        @read_position += length
        self
      end

      # Get the number of readable bytes remaining
      # @return [Integer] number of bytes available for reading
      def readable_bytes
        @write_position - @read_position
      end

      # Check if there are any readable bytes
      # @return [Boolean] true if there are bytes to read
      def readable?
        readable_bytes > 0
      end

      # Get the buffer contents as a string (from read position to write position)
      # @return [String] the buffer contents (binary encoding)
      def to_s
        @data[@read_position, readable_bytes]
      end

      # Get all written data (from start to write position)
      # @return [String] all written data (binary encoding)
      def to_bytes
        @data[0, @write_position]
      end

      # Clear the buffer, resetting read and write positions
      # @return [self]
      def clear
        @read_position = 0
        @write_position = 0
        self
      end

      # Reset read position to beginning
      # @return [self]
      def rewind
        @read_position = 0
        self
      end

      # Compact the buffer by discarding already-read bytes
      # Moves unread data to the beginning of the buffer
      # @return [self]
      def compact
        if @read_position > 0
          unread = readable_bytes
          if unread > 0
            @data[0, unread] = @data[@read_position, unread]
          end
          @read_position = 0
          @write_position = unread
        end
        self
      end

      # Create a buffer from existing bytes
      # @param bytes [String] bytes to initialize the buffer with
      # @return [Buffer] a new buffer containing the bytes
      def self.from_bytes(bytes)
        buffer = new(bytes.bytesize)
        buffer.write(bytes)
        buffer.rewind
        buffer
      end

      # Mark the current read position
      # @return [Integer] the marked position
      def mark
        @mark_position = @read_position
      end

      # Reset read position to the last marked position
      # @return [self]
      # @raise [RuntimeError] if no mark has been set
      def reset_to_mark
        raise "No mark set" unless @mark_position
        @read_position = @mark_position
        self
      end

      private

      # Ensure there's enough capacity for writing
      # @param additional [Integer] number of additional bytes needed
      def ensure_capacity(additional)
        required = @write_position + additional
        if @data.bytesize < required
          # Grow by at least doubling or to required size
          new_size = [(@data.bytesize * 2), required].max
          @data << ("\x00" * (new_size - @data.bytesize))
        end
      end

      # Ensure there are enough bytes to read
      # @param length [Integer] number of bytes needed
      # @raise [BufferUnderflowError] if not enough bytes available
      def ensure_readable(length)
        if readable_bytes < length
          raise BufferUnderflowError, "Not enough bytes: need #{length}, have #{readable_bytes}"
        end
      end
    end

    # Error raised when attempting to read more bytes than available
    class BufferUnderflowError < StandardError; end
  end
end
