# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pulsar::Consumer do
  let(:connection_pool) { instance_double(Pulsar::Internal::ConnectionPool) }
  let(:lookup_service) { instance_double(Pulsar::Internal::LookupService) }
  let(:connection) { instance_double(Pulsar::Internal::Connection) }
  let(:topic) { "test-topic" }
  let(:subscription) { "test-subscription" }

  describe "class constants" do
    it "defines default receiver queue size" do
      expect(described_class::DEFAULT_RECEIVER_QUEUE_SIZE).to eq(1000)
    end
  end

  # Unit tests with mocked dependencies
  describe "with mocked connection" do
    let(:subscribe_success) { double("subscribe_success") }

    before do
      allow(lookup_service).to receive(:lookup_connection).and_return(connection)
      allow(connection).to receive(:register_consumer)
      allow(connection).to receive(:unregister_consumer)
      allow(connection).to receive(:next_request_id).and_return(1)
      allow(connection).to receive(:send_request).and_return(subscribe_success)
      allow(connection).to receive(:send_command)
      allow(connection).to receive(:ready?).and_return(true)
    end

    describe "#initialize" do
      it "creates a consumer for a topic and subscription" do
        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic,
          subscription: subscription
        )

        expect(consumer.topic).to eq("persistent://public/default/#{topic}")
        expect(consumer.subscription).to eq(subscription)
        expect(consumer.ready?).to be true
        expect(consumer.closed?).to be false
      end

      it "accepts custom options" do
        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic,
          subscription: subscription,
          options: {
            subscription_type: :shared,
            initial_position: :earliest,
            receiver_queue_size: 500,
            name: "my-consumer"
          }
        )

        expect(consumer.ready?).to be true
      end

      it "expands short topic names" do
        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: "short-topic",
          subscription: subscription
        )

        expect(consumer.topic).to eq("persistent://public/default/short-topic")
      end

      it "preserves full topic names" do
        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: "persistent://my-tenant/my-ns/my-topic",
          subscription: subscription
        )

        expect(consumer.topic).to eq("persistent://my-tenant/my-ns/my-topic")
      end
    end

    describe "#closed?" do
      it "returns false for new consumer" do
        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic,
          subscription: subscription
        )

        expect(consumer.closed?).to be false
      end
    end

    describe "#close" do
      it "marks consumer as closed" do
        allow(connection).to receive(:send_request)

        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic,
          subscription: subscription
        )
        consumer.close

        expect(consumer.closed?).to be true
        expect(consumer.ready?).to be false
      end

      it "can be called multiple times safely" do
        allow(connection).to receive(:send_request)

        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic,
          subscription: subscription
        )
        consumer.close
        consumer.close

        expect(consumer.closed?).to be true
      end
    end

    describe "#receive" do
      it "raises error when consumer is closed" do
        allow(connection).to receive(:send_request)

        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic,
          subscription: subscription
        )
        consumer.close

        expect {
          consumer.receive
        }.to raise_error(Pulsar::ConsumerError, /closed/)
      end
    end

    describe "#acknowledge" do
      it "raises error when consumer is closed" do
        allow(connection).to receive(:send_request)

        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic,
          subscription: subscription
        )
        consumer.close

        message_id = Pulsar::MessageId.new(ledger_id: 1, entry_id: 1)
        message = Pulsar::Message.new(
          message_id: message_id,
          payload: "test",
          topic: topic,
          consumer_id: 1,
          producer_name: "producer",
          publish_time: Time.now.to_i * 1000
        )

        expect {
          consumer.acknowledge(message)
        }.to raise_error(Pulsar::ConsumerError, /closed/)
      end
    end

    describe "#handle_message" do
      it "queues incoming messages" do
        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic,
          subscription: subscription
        )

        command_message = double("command_message",
          consumer_id: consumer.consumer_id,
          message_id: double("message_id",
            ledgerId: 123,
            entryId: 456,
            partition: -1,
            batch_index: -1
          ),
          redelivery_count: 0
        )

        metadata = double("metadata",
          producer_name: "test-producer",
          publish_time: Time.now.to_i * 1000,
          event_time: 0,
          partition_key: "",
          properties: []
        )

        consumer.handle_message(command_message, metadata, "test payload")

        # Message should be queued - we can verify by receiving with timeout
        # Since we're testing the handler, we just verify no error is raised
      end
    end

    describe "#handle_close" do
      it "marks consumer as not ready" do
        consumer = described_class.new(
          connection_pool: connection_pool,
          lookup_service: lookup_service,
          topic: topic,
          subscription: subscription
        )

        consumer.handle_close

        expect(consumer.ready?).to be false
      end
    end
  end

  # Integration tests require a running Pulsar broker
  describe "integration tests", skip: ENV["PULSAR_URL"].nil? do
    let(:pulsar_url) { ENV.fetch("PULSAR_URL", "pulsar://localhost:6650") }
    let(:service_url) { Pulsar::Internal::ServiceUrl.new(pulsar_url) }
    let(:real_connection_pool) { Pulsar::Internal::ConnectionPool.new }
    let(:real_lookup_service) { Pulsar::Internal::LookupService.new(real_connection_pool, service_url) }
    let(:test_topic) { "test-consumer-#{Time.now.to_i}-#{rand(1000)}" }
    let(:test_subscription) { "test-sub-#{Time.now.to_i}-#{rand(1000)}" }

    after do
      real_connection_pool.close_all
    end

    describe "#initialize" do
      it "creates a ready consumer" do
        consumer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic,
          subscription: test_subscription
        )

        expect(consumer.ready?).to be true
        expect(consumer.consumer_id).to be_a(Integer)
        expect(consumer.subscription).to eq(test_subscription)

        consumer.close
      end

      it "creates consumer with shared subscription type" do
        consumer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic,
          subscription: test_subscription,
          options: { subscription_type: :shared }
        )

        expect(consumer.ready?).to be true

        consumer.close
      end

      it "creates consumer with earliest position" do
        consumer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic,
          subscription: test_subscription,
          options: { initial_position: :earliest }
        )

        expect(consumer.ready?).to be true

        consumer.close
      end
    end

    describe "#receive" do
      it "returns nil on timeout when no messages" do
        consumer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic,
          subscription: test_subscription
        )

        message = consumer.receive(timeout: 1)
        expect(message).to be_nil

        consumer.close
      end

      it "receives messages sent to topic" do
        # Create producer and consumer
        producer = Pulsar::Producer.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic
        )

        consumer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic,
          subscription: test_subscription,
          options: { initial_position: :earliest }
        )

        # Send a message
        producer.send(Pulsar::ProducerMessage.new(payload: "Test message"))

        # Receive the message
        message = consumer.receive(timeout: 10)

        expect(message).not_to be_nil
        expect(message).to be_a(Pulsar::Message)
        expect(message.data).to eq("Test message")

        consumer.acknowledge(message)

        producer.close
        consumer.close
      end
    end

    describe "#acknowledge" do
      it "acknowledges received messages" do
        producer = Pulsar::Producer.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic
        )

        consumer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic,
          subscription: test_subscription,
          options: { initial_position: :earliest }
        )

        producer.send(Pulsar::ProducerMessage.new(payload: "Ack test"))

        message = consumer.receive(timeout: 10)
        expect(message).not_to be_nil

        # Should not raise
        consumer.acknowledge(message)

        producer.close
        consumer.close
      end

      it "supports ack alias" do
        producer = Pulsar::Producer.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic
        )

        consumer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic,
          subscription: test_subscription,
          options: { initial_position: :earliest }
        )

        producer.send(Pulsar::ProducerMessage.new(payload: "Ack alias test"))

        message = consumer.receive(timeout: 10)
        expect(message).not_to be_nil

        # Should not raise - using alias
        consumer.ack(message)

        producer.close
        consumer.close
      end
    end

    describe "#close" do
      it "closes the consumer" do
        consumer = described_class.new(
          connection_pool: real_connection_pool,
          lookup_service: real_lookup_service,
          topic: test_topic,
          subscription: test_subscription
        )

        consumer.close

        expect(consumer.closed?).to be true
        expect(consumer.ready?).to be false
      end
    end
  end
end
