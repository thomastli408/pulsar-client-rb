# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pulsar do
  it "has a version number" do
    expect(Pulsar::VERSION).not_to be nil
  end

  it "defines error classes" do
    expect(Pulsar::Error).to be < StandardError
    expect(Pulsar::ClientError).to be < Pulsar::Error
    expect(Pulsar::ConnectionError).to be < Pulsar::Error
    expect(Pulsar::TimeoutError).to be < Pulsar::Error
    expect(Pulsar::ProducerError).to be < Pulsar::Error
    expect(Pulsar::ConsumerError).to be < Pulsar::Error
  end
end

RSpec.describe Pulsar::Client do
  let(:service_url) { "pulsar://localhost:6650" }

  describe "#initialize" do
    it "creates a client with service URL" do
      # Use mocks to avoid actual connection
      allow_any_instance_of(Pulsar::Internal::ConnectionPool).to receive(:get_connection)

      client = described_class.new(service_url)

      expect(client.service_url).to eq(service_url)
      expect(client.closed?).to be false
    end

    it "accepts custom options" do
      client = described_class.new(service_url, {
        connection_timeout: 5,
        keep_alive_interval: 60,
        operation_timeout: 15
      })

      expect(client.service_url).to eq(service_url)
    end

    it "raises error for invalid service URL" do
      expect {
        described_class.new("http://localhost:6650")
      }.to raise_error(ArgumentError, /Only 'pulsar:\/\/'/)
    end

    it "raises error for empty service URL" do
      expect {
        described_class.new("")
      }.to raise_error(ArgumentError)
    end
  end

  describe "#closed?" do
    it "returns false for new client" do
      client = described_class.new(service_url)
      expect(client.closed?).to be false
    end
  end

  describe "#close" do
    it "marks client as closed" do
      client = described_class.new(service_url)
      client.close
      expect(client.closed?).to be true
    end

    it "can be called multiple times safely" do
      client = described_class.new(service_url)
      client.close
      client.close
      expect(client.closed?).to be true
    end
  end

  describe "#create_producer" do
    it "raises error when client is closed" do
      client = described_class.new(service_url)
      client.close

      expect {
        client.create_producer("my-topic")
      }.to raise_error(Pulsar::ClientError, /closed/)
    end
  end

  describe "#subscribe" do
    it "raises error when client is closed" do
      client = described_class.new(service_url)
      client.close

      expect {
        client.subscribe("my-topic", "my-subscription")
      }.to raise_error(Pulsar::ClientError, /closed/)
    end
  end

  # Integration tests require a running Pulsar broker
  describe "integration tests", skip: ENV["PULSAR_URL"].nil? do
    let(:pulsar_url) { ENV.fetch("PULSAR_URL", "pulsar://localhost:6650") }
    let(:test_topic) { "test-client-#{Time.now.to_i}-#{rand(1000)}" }
    let(:test_subscription) { "test-sub-#{Time.now.to_i}-#{rand(1000)}" }

    describe "#create_producer" do
      it "creates a producer for a topic" do
        client = described_class.new(pulsar_url)

        producer = client.create_producer(test_topic)

        expect(producer).to be_a(Pulsar::Producer)
        expect(producer.ready?).to be true
        expect(producer.topic).to include(test_topic)

        producer.close
        client.close
      end

      it "creates multiple producers" do
        client = described_class.new(pulsar_url)

        producer1 = client.create_producer(test_topic)
        producer2 = client.create_producer("#{test_topic}-2")

        expect(producer1.ready?).to be true
        expect(producer2.ready?).to be true

        producer1.close
        producer2.close
        client.close
      end
    end

    describe "#subscribe" do
      it "creates a consumer with subscription" do
        client = described_class.new(pulsar_url)

        consumer = client.subscribe(test_topic, test_subscription)

        expect(consumer).to be_a(Pulsar::Consumer)
        expect(consumer.ready?).to be true
        expect(consumer.topic).to include(test_topic)
        expect(consumer.subscription).to eq(test_subscription)

        consumer.close
        client.close
      end

      it "creates consumer with options" do
        client = described_class.new(pulsar_url)

        consumer = client.subscribe(test_topic, test_subscription, {
          subscription_type: :shared,
          initial_position: :earliest
        })

        expect(consumer.ready?).to be true

        consumer.close
        client.close
      end
    end

    describe "#close" do
      it "closes all producers and consumers" do
        client = described_class.new(pulsar_url)

        producer = client.create_producer(test_topic)
        consumer = client.subscribe(test_topic, test_subscription)

        expect(producer.ready?).to be true
        expect(consumer.ready?).to be true

        client.close

        expect(client.closed?).to be true
        expect(producer.closed?).to be true
        expect(consumer.closed?).to be true
      end
    end

    describe "send and receive" do
      it "sends and receives messages" do
        client = described_class.new(pulsar_url)

        producer = client.create_producer(test_topic)
        consumer = client.subscribe(test_topic, test_subscription, {
          initial_position: :earliest
        })

        # Send a message
        message = Pulsar::ProducerMessage.new(payload: "Hello from Client spec!")
        message_id = producer.send(message)
        expect(message_id).to be_a(Pulsar::MessageId)

        # Receive the message
        received = consumer.receive(timeout: 10)
        expect(received).not_to be_nil
        expect(received.data).to eq("Hello from Client spec!")

        consumer.acknowledge(received)

        producer.close
        consumer.close
        client.close
      end
    end
  end
end
