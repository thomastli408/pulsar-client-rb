# frozen_string_literal: true

require "spec_helper"
require "pulsar/internal/connection_pool"

RSpec.describe Pulsar::Internal::ConnectionPool do
  describe "#initialize" do
    it "creates a pool with default settings" do
      pool = described_class.new
      expect(pool.closed?).to be false
      expect(pool.size).to eq(0)
    end

    it "accepts custom connection settings" do
      pool = described_class.new(
        connection_timeout: 10,
        keep_alive_interval: 60,
        max_connections_per_broker: 2
      )
      expect(pool).to be_a(described_class)
    end
  end

  describe "#closed?" do
    it "returns false for new pool" do
      pool = described_class.new
      expect(pool.closed?).to be false
    end
  end

  describe "#size" do
    it "returns 0 for empty pool" do
      pool = described_class.new
      expect(pool.size).to eq(0)
    end
  end

  describe "#total_connections" do
    it "returns 0 for empty pool" do
      pool = described_class.new
      expect(pool.total_connections).to eq(0)
    end
  end

  describe "#close_all" do
    it "marks pool as closed" do
      pool = described_class.new
      pool.close_all
      expect(pool.closed?).to be true
    end

    it "can be called multiple times safely" do
      pool = described_class.new
      pool.close_all
      pool.close_all
      expect(pool.closed?).to be true
    end
  end

  describe "#get_connection" do
    it "raises error when pool is closed" do
      pool = described_class.new
      pool.close_all

      expect {
        pool.get_connection("pulsar://localhost:6650")
      }.to raise_error(Pulsar::ConnectionClosedError, /pool is closed/)
    end

    it "raises error for invalid broker URL" do
      pool = described_class.new

      expect {
        pool.get_connection("")
      }.to raise_error(ArgumentError, /Invalid broker URL/)
    end
  end

  describe "URL parsing" do
    it "parses host:port format" do
      pool = described_class.new

      # Use a non-routable address to test parsing without connecting
      expect {
        pool.get_connection("192.0.2.1:6650")  # TEST-NET address
      }.to raise_error(Pulsar::TimeoutError)

      pool.close_all
    end

    it "parses pulsar:// URL format" do
      pool = described_class.new

      expect {
        pool.get_connection("pulsar://192.0.2.1:6650")
      }.to raise_error(Pulsar::TimeoutError)

      pool.close_all
    end

    it "uses default port 6650 when not specified" do
      pool = described_class.new

      # Parse should work, connection should timeout
      expect {
        pool.get_connection("pulsar://192.0.2.1")
      }.to raise_error(Pulsar::TimeoutError)

      pool.close_all
    end
  end

  # Integration tests require a running Pulsar broker
  describe "integration tests", skip: ENV["PULSAR_URL"].nil? do
    let(:pulsar_url) { ENV.fetch("PULSAR_URL", "pulsar://localhost:6650") }

    describe "#get_connection" do
      it "creates and returns a connection" do
        pool = described_class.new

        connection = pool.get_connection(pulsar_url)

        expect(connection).to be_a(Pulsar::Internal::Connection)
        expect(connection.ready?).to be true
        expect(pool.size).to eq(1)
        expect(pool.total_connections).to eq(1)

        pool.close_all
      end

      it "returns the same connection for same broker" do
        pool = described_class.new

        conn1 = pool.get_connection(pulsar_url)
        conn2 = pool.get_connection(pulsar_url)

        expect(conn1).to be(conn2)
        expect(pool.total_connections).to eq(1)

        pool.close_all
      end

      it "normalizes URLs and reuses connections" do
        pool = described_class.new
        uri = URI.parse(pulsar_url)

        # Get connection with full URL
        conn1 = pool.get_connection(pulsar_url)

        # Get connection with just host:port
        conn2 = pool.get_connection("#{uri.host}:#{uri.port}")

        expect(conn1).to be(conn2)
        expect(pool.total_connections).to eq(1)

        pool.close_all
      end

      it "creates separate connections for different brokers" do
        pool = described_class.new

        # This test would need two different brokers
        # For now we just test that one connection is created
        conn = pool.get_connection(pulsar_url)
        expect(conn.ready?).to be true

        pool.close_all
      end
    end

    describe "#close_all" do
      it "closes all connections" do
        pool = described_class.new

        connection = pool.get_connection(pulsar_url)
        expect(connection.ready?).to be true

        pool.close_all

        expect(pool.closed?).to be true
        expect(connection.closed?).to be true
      end
    end

    describe "reconnection" do
      it "creates new connection when previous one is closed" do
        pool = described_class.new

        conn1 = pool.get_connection(pulsar_url)
        expect(conn1.ready?).to be true

        # Simulate connection failure
        conn1.close

        # Should create a new connection
        conn2 = pool.get_connection(pulsar_url)
        expect(conn2.ready?).to be true
        expect(conn2).not_to be(conn1)

        pool.close_all
      end
    end

    describe "thread safety" do
      it "handles concurrent connection requests" do
        pool = described_class.new

        threads = 10.times.map do
          Thread.new do
            conn = pool.get_connection(pulsar_url)
            expect(conn.ready?).to be true
            conn
          end
        end

        connections = threads.map(&:value)

        # All threads should get the same connection (single connection per broker)
        expect(connections.uniq.size).to eq(1)
        expect(pool.total_connections).to eq(1)

        pool.close_all
      end
    end
  end
end
