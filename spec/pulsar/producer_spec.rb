# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pulsar::Producer do
  let(:connection_pool) { instance_double(Pulsar::Internal::ConnectionPool) }
  let(:lookup_service) { instance_double(Pulsar::Internal::LookupService) }
  let(:connection) { instance_double(Pulsar::Internal::Connection) }
  let(:topic) { "test-topic" }

  describe "class constants" do
    it "defines default send timeout" do
      expect(described_class::DEFAULT_SEND_TIMEOUT).to eq(30)
    end

    it "defines default max pending messages" do
      expect(described_class::DEFAULT_MAX_PENDING).to eq(1000)
    end
  end

  # Unit tests with mocked dependencies
  describe "with mocked connection" do
    let(:producer_success) do
      double("producer_success",
        producer_name: "test-producer-name",
        last_sequence_id: 0,
        schema_version: nil
      )
    end

    before do
      allow(lookup_service).to receive(:lookup_connection).and_return(connection)
      allow(connection).to receive(:register_producer)
      allow(connection).to receive(:unregister_producer)
      allow(connection).to receive(:next_request_id).and_return(1)
      allow(connection).to receive(:send_request).and_return(producer_success)
      allow(connection).to receive(:ready?).and_return(true)
    end

    describe "#initialize" do
      it "creates a producer for a topic" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )

        expect(producer.topic).to eq("persistent://public/default/#{topic}")
        expect(producer.ready?).to be true
        expect(producer.closed?).to be false
      end

      it "accepts custom options" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic,
          options: {
            name: "my-producer",
            send_timeout: 60,
            max_pending_messages: 500
          }
        )

        expect(producer.ready?).to be true
      end

      it "expands short topic names" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: "short-topic"
        )

        expect(producer.topic).to eq("persistent://public/default/short-topic")
      end

      it "preserves full topic names" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: "persistent://my-tenant/my-ns/my-topic"
        )

        expect(producer.topic).to eq("persistent://my-tenant/my-ns/my-topic")
      end
    end

    describe "#closed?" do
      it "returns false for new producer" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )

        expect(producer.closed?).to be false
      end
    end

    describe "#close" do
      it "marks producer as closed" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )
        producer.close

        expect(producer.closed?).to be true
        expect(producer.ready?).to be false
      end

      it "can be called multiple times safely" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )
        producer.close
        producer.close

        expect(producer.closed?).to be true
      end
    end

    describe "#send" do
      it "raises error when producer is closed" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )
        producer.close

        message = Pulsar::ProducerMessage.new(payload: "test")

        expect {
          producer.send(message)
        }.to raise_error(Pulsar::ProducerError, /closed/)
      end

      it "raises error for nil message" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )

        expect {
          producer.send(nil)
        }.to raise_error(ArgumentError, /nil/)
      end

      it "raises error for non-ProducerMessage" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )

        expect {
          producer.send("not a message")
        }.to raise_error(ArgumentError, /ProducerMessage/)
      end

      it "raises error for message with nil payload" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )

        message = Pulsar::ProducerMessage.new(payload: "test")
        message.payload = nil

        expect {
          producer.send(message)
        }.to raise_error(ArgumentError, /payload/)
      end
    end

    describe "#handle_send_receipt" do
      it "processes send receipts" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )

        receipt = double("receipt",
          sequence_id: 0,
          message_id: double("message_id",
            ledgerId: 123,
            entryId: 456,
            partition: -1,
            batch_index: -1
          )
        )

        # Should not raise
        producer.handle_send_receipt(receipt)
      end
    end

    describe "#handle_send_error" do
      it "processes send errors" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )

        error = double("error",
          sequence_id: 0,
          error: :UnknownError,
          message: "Test error"
        )

        # Should not raise
        producer.handle_send_error(error)
      end
    end

    describe "#handle_close" do
      it "marks producer as not ready" do
        producer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic
        )

        producer.handle_close

        expect(producer.ready?).to be false
      end
    end
  end

  # Integration tests require a running Pulsar broker
  describe "integration tests", skip: ENV["PULSAR_URL"].nil? do
    let(:pulsar_url) { ENV.fetch("PULSAR_URL", "pulsar://localhost:6650") }
    let(:service_url) { Pulsar::Internal::ServiceUrl.new(pulsar_url) }
    let(:real_connection_pool) { Pulsar::Internal::ConnectionPool.new }
    let(:real_lookup_service) { Pulsar::Internal::LookupService.new(real_connection_pool, service_url) }
    let(:test_topic) { "test-producer-#{Time.now.to_i}-#{rand(1000)}" }

    after do
      real_connection_pool.close_all
    end

    describe "#initialize" do
      it "creates a ready producer" do
        producer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic
        )

        expect(producer.ready?).to be true
        expect(producer.name).not_to be_nil
        expect(producer.producer_id).to be_a(Integer)

        producer.close
      end
    end

    describe "#send" do
      it "sends a simple message" do
        producer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic
        )

        message = Pulsar::ProducerMessage.new(payload: "Hello, Pulsar!")
        message_id = producer.send(message)

        expect(message_id).to be_a(Pulsar::MessageId)
        expect(message_id.ledger_id).to be > 0

        producer.close
      end

      it "sends message with properties" do
        producer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic
        )

        message = Pulsar::ProducerMessage.new(
          payload: "Message with properties",
          key: "test-key",
          properties: { "foo" => "bar", "baz" => "qux" }
        )
        message_id = producer.send(message)

        expect(message_id).to be_a(Pulsar::MessageId)

        producer.close
      end

      it "sends message with event time" do
        producer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic
        )

        event_time = Time.now
        message = Pulsar::ProducerMessage.new(
          payload: "Message with event time",
          event_time: event_time
        )
        message_id = producer.send(message)

        expect(message_id).to be_a(Pulsar::MessageId)

        producer.close
      end

      it "sends multiple messages" do
        producer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic
        )

        message_ids = []
        5.times do |i|
          message = Pulsar::ProducerMessage.new(payload: "Message #{i}")
          message_ids << producer.send(message)
        end

        expect(message_ids.size).to eq(5)
        message_ids.each { |id| expect(id).to be_a(Pulsar::MessageId) }

        producer.close
      end
    end

    describe "#close" do
      it "closes the producer" do
        producer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic
        )

        producer.close

        expect(producer.closed?).to be true
        expect(producer.ready?).to be false
      end
    end
  end
end
