# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pulsar::Internal::DLQRouter do
  let(:client) { instance_double(Pulsar::Client) }
  let(:policy) { Pulsar::DLQPolicy.new(max_redeliveries: 3) }
  let(:source_topic) { "persistent://public/default/my-topic" }
  let(:subscription) { "my-subscription" }
  let(:producer) { instance_double(Pulsar::Producer) }

  let(:message_id) { Pulsar::MessageId.new(ledger_id: 123, entry_id: 456) }
  let(:message) do
    Pulsar::Message.new(
      message_id: message_id,
      payload: "test payload",
      topic: source_topic,
      consumer_id: 1,
      producer_name: "producer",
      publish_time: Time.now.to_i * 1000,
      redelivery_count: 3,
      key: "partition-key",
      properties: { "original" => "property" }
    )
  end

  describe "#initialize" do
    it "creates a router with policy" do
      router = described_class.new(
        client: client,
        policy: policy,
        source_topic: source_topic,
        subscription: subscription
      )

      expect(router.dlq_topic).to eq("#{source_topic}-#{subscription}-DLQ")
    end

    it "uses custom DLQ topic from policy" do
      custom_policy = Pulsar::DLQPolicy.new(
        max_redeliveries: 3,
        dead_letter_topic: "custom-dlq"
      )

      router = described_class.new(
        client: client,
        policy: custom_policy,
        source_topic: source_topic,
        subscription: subscription
      )

      expect(router.dlq_topic).to eq("custom-dlq")
    end
  end

  describe "#should_route_to_dlq?" do
    let(:router) do
      described_class.new(
        client: client,
        policy: policy,
        source_topic: source_topic,
        subscription: subscription
      )
    end

    it "returns true when redelivery_count equals max_redeliveries" do
      message_at_max = Pulsar::Message.new(
        message_id: message_id,
        payload: "test",
        topic: source_topic,
        consumer_id: 1,
        producer_name: "producer",
        publish_time: Time.now.to_i * 1000,
        redelivery_count: 3
      )

      expect(router.should_route_to_dlq?(message_at_max)).to be true
    end

    it "returns true when redelivery_count exceeds max_redeliveries" do
      message_over_max = Pulsar::Message.new(
        message_id: message_id,
        payload: "test",
        topic: source_topic,
        consumer_id: 1,
        producer_name: "producer",
        publish_time: Time.now.to_i * 1000,
        redelivery_count: 5
      )

      expect(router.should_route_to_dlq?(message_over_max)).to be true
    end

    it "returns false when redelivery_count is below max_redeliveries" do
      message_below_max = Pulsar::Message.new(
        message_id: message_id,
        payload: "test",
        topic: source_topic,
        consumer_id: 1,
        producer_name: "producer",
        publish_time: Time.now.to_i * 1000,
        redelivery_count: 2
      )

      expect(router.should_route_to_dlq?(message_below_max)).to be false
    end

    it "returns false when redelivery_count is 0" do
      fresh_message = Pulsar::Message.new(
        message_id: message_id,
        payload: "test",
        topic: source_topic,
        consumer_id: 1,
        producer_name: "producer",
        publish_time: Time.now.to_i * 1000,
        redelivery_count: 0
      )

      expect(router.should_route_to_dlq?(fresh_message)).to be false
    end
  end

  describe "#route_to_dlq" do
    let(:router) do
      described_class.new(
        client: client,
        policy: policy,
        source_topic: source_topic,
        subscription: subscription
      )
    end

    let(:dlq_message_id) { Pulsar::MessageId.new(ledger_id: 789, entry_id: 101) }

    before do
      allow(client).to receive(:create_producer).and_return(producer)
      allow(producer).to receive(:send).and_return(dlq_message_id)
    end

    it "creates DLQ producer lazily" do
      expect(client).to receive(:create_producer).with(router.dlq_topic).once

      router.route_to_dlq(message)
    end

    it "sends message to DLQ with original payload" do
      expect(producer).to receive(:send) do |dlq_msg|
        expect(dlq_msg.payload).to eq("test payload")
        dlq_message_id
      end

      router.route_to_dlq(message)
    end

    it "preserves original message key" do
      expect(producer).to receive(:send) do |dlq_msg|
        expect(dlq_msg.key).to eq("partition-key")
        dlq_message_id
      end

      router.route_to_dlq(message)
    end

    it "preserves original message properties" do
      expect(producer).to receive(:send) do |dlq_msg|
        expect(dlq_msg.properties["original"]).to eq("property")
        dlq_message_id
      end

      router.route_to_dlq(message)
    end

    it "adds DLQ metadata properties" do
      expect(producer).to receive(:send) do |dlq_msg|
        expect(dlq_msg.properties["PULSAR_DLQ_ORIGINAL_TOPIC"]).to eq(source_topic)
        expect(dlq_msg.properties["PULSAR_DLQ_ORIGINAL_SUBSCRIPTION"]).to eq(subscription)
        expect(dlq_msg.properties["PULSAR_DLQ_ORIGINAL_MESSAGE_ID"]).to eq(message_id.to_s)
        expect(dlq_msg.properties["PULSAR_DLQ_REDELIVERY_COUNT"]).to eq("3")
        dlq_message_id
      end

      router.route_to_dlq(message)
    end

    it "returns the DLQ message ID" do
      result = router.route_to_dlq(message)

      expect(result).to eq(dlq_message_id)
    end

    it "reuses the same producer for multiple messages" do
      expect(client).to receive(:create_producer).once.and_return(producer)

      router.route_to_dlq(message)
      router.route_to_dlq(message)
      router.route_to_dlq(message)
    end
  end

  describe "#close" do
    let(:router) do
      described_class.new(
        client: client,
        policy: policy,
        source_topic: source_topic,
        subscription: subscription
      )
    end

    it "closes the DLQ producer if created" do
      allow(client).to receive(:create_producer).and_return(producer)
      allow(producer).to receive(:send).and_return(Pulsar::MessageId.new(ledger_id: 1, entry_id: 1))

      # Create the producer
      router.route_to_dlq(message)

      expect(producer).to receive(:close)
      router.close
    end

    it "does nothing if producer was never created" do
      # Should not raise
      router.close
    end

    it "can be called multiple times safely" do
      allow(client).to receive(:create_producer).and_return(producer)
      allow(producer).to receive(:send).and_return(Pulsar::MessageId.new(ledger_id: 1, entry_id: 1))
      allow(producer).to receive(:close)

      router.route_to_dlq(message)
      router.close
      router.close # Second call should not raise
    end
  end
end
