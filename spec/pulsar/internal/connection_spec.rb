# frozen_string_literal: true

require "spec_helper"
require "pulsar/internal/connection"

RSpec.describe Pulsar::Internal::Connection do
  let(:host) { "localhost" }
  let(:port) { 6650 }

  describe "#initialize" do
    it "creates a connection in INIT state" do
      connection = described_class.new(host: host, port: port)
      expect(connection.state).to eq(Pulsar::Internal::Connection::State::INIT)
      expect(connection.host).to eq(host)
      expect(connection.port).to eq(port)
    end

    it "uses default port 6650" do
      connection = described_class.new(host: host)
      expect(connection.port).to eq(6650)
    end

    it "accepts custom connection timeout" do
      connection = described_class.new(host: host, connection_timeout: 5)
      expect(connection).to be_a(described_class)
    end

    it "accepts custom keep-alive interval" do
      connection = described_class.new(host: host, keep_alive_interval: 60)
      expect(connection).to be_a(described_class)
    end
  end

  describe "#ready?" do
    it "returns false when connection is in INIT state" do
      connection = described_class.new(host: host, port: port)
      expect(connection.ready?).to be false
    end
  end

  describe "#closed?" do
    it "returns false when connection is in INIT state" do
      connection = described_class.new(host: host, port: port)
      expect(connection.closed?).to be false
    end
  end

  describe "#next_request_id" do
    it "returns incrementing request IDs" do
      connection = described_class.new(host: host, port: port)
      id1 = connection.next_request_id
      id2 = connection.next_request_id
      id3 = connection.next_request_id

      expect(id2).to eq(id1 + 1)
      expect(id3).to eq(id2 + 1)
    end
  end

  describe "#register_producer / #unregister_producer" do
    it "registers and unregisters producer handlers" do
      connection = described_class.new(host: host, port: port)
      handler = double("producer_handler")

      connection.register_producer(1, handler)
      connection.unregister_producer(1)
      # No error should be raised
    end
  end

  describe "#register_consumer / #unregister_consumer" do
    it "registers and unregisters consumer handlers" do
      connection = described_class.new(host: host, port: port)
      handler = double("consumer_handler")

      connection.register_consumer(1, handler)
      connection.unregister_consumer(1)
      # No error should be raised
    end
  end

  describe "#on_close" do
    it "registers close callbacks" do
      connection = described_class.new(host: host, port: port)
      callback_called = false

      connection.on_close { callback_called = true }

      # Callback should be registered but not called yet
      expect(callback_called).to be false
    end
  end

  # Integration tests require a running Pulsar broker
  describe "integration tests", skip: ENV["PULSAR_URL"].nil? do
    let(:pulsar_host) { URI.parse(ENV.fetch("PULSAR_URL", "pulsar://localhost:6650")).host }
    let(:pulsar_port) { URI.parse(ENV.fetch("PULSAR_URL", "pulsar://localhost:6650")).port }

    describe "#connect" do
      it "successfully connects to Pulsar broker" do
        connection = described_class.new(host: pulsar_host, port: pulsar_port)
        connection.connect

        expect(connection.ready?).to be true
        expect(connection.state).to eq(Pulsar::Internal::Connection::State::READY)
        expect(connection.server_version).not_to be_nil

        connection.close
        expect(connection.closed?).to be true
      end

      it "raises error when connecting twice" do
        connection = described_class.new(host: pulsar_host, port: pulsar_port)
        connection.connect

        expect { connection.connect }.to raise_error(Pulsar::ConnectionError, /already established/)

        connection.close
      end
    end

    describe "#close" do
      it "closes an established connection" do
        connection = described_class.new(host: pulsar_host, port: pulsar_port)
        connection.connect

        connection.close

        expect(connection.closed?).to be true
        expect(connection.state).to eq(Pulsar::Internal::Connection::State::CLOSED)
      end

      it "calls on_close callbacks" do
        connection = described_class.new(host: pulsar_host, port: pulsar_port)
        callback_called = false
        callback_error = :not_called

        connection.on_close do |error|
          callback_called = true
          callback_error = error
        end

        connection.connect
        connection.close

        expect(callback_called).to be true
        expect(callback_error).to be_nil
      end
    end

    describe "#send_command" do
      it "raises error when connection is not ready" do
        connection = described_class.new(host: pulsar_host, port: pulsar_port)

        ping_cmd = Pulsar::Internal::Commands.ping

        expect { connection.send_command(ping_cmd) }.to raise_error(Pulsar::ConnectionClosedError)
      end

      it "sends PING command successfully" do
        connection = described_class.new(host: pulsar_host, port: pulsar_port)
        connection.connect

        ping_cmd = Pulsar::Internal::Commands.ping
        expect { connection.send_command(ping_cmd) }.not_to raise_error

        connection.close
      end
    end
  end

  # Tests for connection error handling
  describe "error handling" do
    it "raises ConnectionError when host is unreachable" do
      connection = described_class.new(
        host: "192.0.2.1", # TEST-NET, should be unreachable
        port: port,
        connection_timeout: 1
      )

      expect { connection.connect }.to raise_error(Pulsar::TimeoutError)
    end

    it "raises ConnectionError when connection is refused" do
      connection = described_class.new(
        host: "127.0.0.1",
        port: 65534, # Unlikely to have anything listening
        connection_timeout: 1
      )

      expect { connection.connect }.to raise_error(Pulsar::ConnectionError)
    end
  end
end
