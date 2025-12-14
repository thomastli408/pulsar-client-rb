# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pulsar::ProducerMessage do
  describe "#initialize" do
    it "creates a message with payload" do
      message = described_class.new(payload: "Hello, World!")

      expect(message.payload).to eq("Hello, World!")
      expect(message.key).to be_nil
      expect(message.properties).to eq({})
      expect(message.event_time).to be_nil
    end

    it "accepts all optional parameters" do
      event_time = Time.now
      message = described_class.new(
        payload: "test",
        key: "my-key",
        properties: { "foo" => "bar" },
        event_time: event_time
      )

      expect(message.payload).to eq("test")
      expect(message.key).to eq("my-key")
      expect(message.properties).to eq({ "foo" => "bar" })
      expect(message.event_time).to eq(event_time)
    end

    it "defaults properties to empty hash when nil" do
      message = described_class.new(payload: "test", properties: nil)

      expect(message.properties).to eq({})
    end
  end

  describe "#payload_bytes" do
    it "returns payload as binary string" do
      message = described_class.new(payload: "Hello")

      bytes = message.payload_bytes
      expect(bytes).to be_a(String)
      expect(bytes.encoding).to eq(Encoding::ASCII_8BIT)
      expect(bytes).to eq("Hello")
    end

    it "handles non-string payloads" do
      message = described_class.new(payload: 12345)

      bytes = message.payload_bytes
      expect(bytes).to eq("12345")
    end
  end

  describe "#event_time_millis" do
    it "returns nil when event_time is nil" do
      message = described_class.new(payload: "test")

      expect(message.event_time_millis).to be_nil
    end

    it "converts Time to milliseconds" do
      time = Time.at(1234567890.123)
      message = described_class.new(payload: "test", event_time: time)

      expect(message.event_time_millis).to eq(1234567890123)
    end

    it "returns integer event_time as-is" do
      message = described_class.new(payload: "test", event_time: 1234567890123)

      expect(message.event_time_millis).to eq(1234567890123)
    end

    it "raises error for invalid event_time type" do
      message = described_class.new(payload: "test", event_time: "invalid")

      expect { message.event_time_millis }.to raise_error(ArgumentError, /Time or Integer/)
    end
  end

  describe "attribute accessors" do
    it "allows modifying payload" do
      message = described_class.new(payload: "original")
      message.payload = "modified"

      expect(message.payload).to eq("modified")
    end

    it "allows modifying key" do
      message = described_class.new(payload: "test")
      message.key = "new-key"

      expect(message.key).to eq("new-key")
    end

    it "allows modifying properties" do
      message = described_class.new(payload: "test")
      message.properties = { "new" => "props" }

      expect(message.properties).to eq({ "new" => "props" })
    end

    it "allows modifying event_time" do
      message = described_class.new(payload: "test")
      new_time = Time.now
      message.event_time = new_time

      expect(message.event_time).to eq(new_time)
    end
  end
end

RSpec.describe Pulsar::Message do
  let(:message_id) { Pulsar::MessageId.new(ledger_id: 123, entry_id: 456) }
  let(:publish_time) { (Time.now.to_f * 1000).to_i }

  describe "#initialize" do
    it "creates a message with required parameters" do
      message = described_class.new(
        message_id: message_id,
        payload: "test payload",
        topic: "my-topic",
        consumer_id: 1,
        producer_name: "my-producer",
        publish_time: publish_time
      )

      expect(message.message_id).to eq(message_id)
      expect(message.payload).to eq("test payload")
      expect(message.topic).to eq("my-topic")
      expect(message.consumer_id).to eq(1)
      expect(message.producer_name).to eq("my-producer")
      expect(message.publish_time).to eq(publish_time)
      expect(message.redelivery_count).to eq(0)
      expect(message.key).to be_nil
      expect(message.properties).to eq({})
      expect(message.event_time).to be_nil
    end

    it "accepts all optional parameters" do
      event_time = (Time.now.to_f * 1000).to_i

      message = described_class.new(
        message_id: message_id,
        payload: "test",
        topic: "topic",
        consumer_id: 1,
        producer_name: "producer",
        publish_time: publish_time,
        redelivery_count: 2,
        key: "partition-key",
        properties: { "foo" => "bar" },
        event_time: event_time
      )

      expect(message.redelivery_count).to eq(2)
      expect(message.key).to eq("partition-key")
      expect(message.properties).to eq({ "foo" => "bar" })
      expect(message.event_time).to eq(event_time)
    end
  end

  describe "#data" do
    it "is an alias for payload" do
      message = described_class.new(
        message_id: message_id,
        payload: "my data",
        topic: "topic",
        consumer_id: 1,
        producer_name: "producer",
        publish_time: publish_time
      )

      expect(message.data).to eq("my data")
      expect(message.data).to eq(message.payload)
    end
  end

  describe "#publish_time_as_time" do
    it "converts publish_time to Time object" do
      timestamp = 1234567890123
      message = described_class.new(
        message_id: message_id,
        payload: "test",
        topic: "topic",
        consumer_id: 1,
        producer_name: "producer",
        publish_time: timestamp
      )

      time = message.publish_time_as_time
      expect(time).to be_a(Time)
      expect(time.to_f).to be_within(0.001).of(1234567890.123)
    end
  end

  describe "#event_time_as_time" do
    it "returns nil when event_time is nil" do
      message = described_class.new(
        message_id: message_id,
        payload: "test",
        topic: "topic",
        consumer_id: 1,
        producer_name: "producer",
        publish_time: publish_time
      )

      expect(message.event_time_as_time).to be_nil
    end

    it "returns nil when event_time is 0" do
      message = described_class.new(
        message_id: message_id,
        payload: "test",
        topic: "topic",
        consumer_id: 1,
        producer_name: "producer",
        publish_time: publish_time,
        event_time: 0
      )

      expect(message.event_time_as_time).to be_nil
    end

    it "converts event_time to Time object" do
      event_timestamp = 1234567890123
      message = described_class.new(
        message_id: message_id,
        payload: "test",
        topic: "topic",
        consumer_id: 1,
        producer_name: "producer",
        publish_time: publish_time,
        event_time: event_timestamp
      )

      time = message.event_time_as_time
      expect(time).to be_a(Time)
      expect(time.to_f).to be_within(0.001).of(1234567890.123)
    end
  end

  describe "#redelivered?" do
    it "returns false when redelivery_count is 0" do
      message = described_class.new(
        message_id: message_id,
        payload: "test",
        topic: "topic",
        consumer_id: 1,
        producer_name: "producer",
        publish_time: publish_time,
        redelivery_count: 0
      )

      expect(message.redelivered?).to be false
    end

    it "returns true when redelivery_count is > 0" do
      message = described_class.new(
        message_id: message_id,
        payload: "test",
        topic: "topic",
        consumer_id: 1,
        producer_name: "producer",
        publish_time: publish_time,
        redelivery_count: 1
      )

      expect(message.redelivered?).to be true
    end
  end

  describe "#to_s" do
    it "returns string representation" do
      message = described_class.new(
        message_id: message_id,
        payload: "test",
        topic: "my-topic",
        consumer_id: 1,
        producer_name: "producer",
        publish_time: publish_time,
        redelivery_count: 2
      )

      str = message.to_s
      expect(str).to include("Message")
      expect(str).to include("my-topic")
      expect(str).to include("redelivery=2")
    end
  end
end

RSpec.describe Pulsar::MessageId do
  describe "#initialize" do
    it "creates a message ID with required parameters" do
      id = described_class.new(ledger_id: 123, entry_id: 456)

      expect(id.ledger_id).to eq(123)
      expect(id.entry_id).to eq(456)
      expect(id.partition).to eq(-1)
      expect(id.batch_index).to eq(-1)
    end

    it "accepts optional partition and batch_index" do
      id = described_class.new(
        ledger_id: 123,
        entry_id: 456,
        partition: 2,
        batch_index: 5
      )

      expect(id.partition).to eq(2)
      expect(id.batch_index).to eq(5)
    end
  end

  describe "#to_s" do
    it "returns formatted string" do
      id = described_class.new(ledger_id: 123, entry_id: 456)

      expect(id.to_s).to eq("123:456:-1:-1")
    end

    it "includes partition and batch_index" do
      id = described_class.new(
        ledger_id: 123,
        entry_id: 456,
        partition: 2,
        batch_index: 5
      )

      expect(id.to_s).to eq("123:456:2:5")
    end
  end

  describe "#==" do
    it "compares equal IDs" do
      id1 = described_class.new(ledger_id: 123, entry_id: 456)
      id2 = described_class.new(ledger_id: 123, entry_id: 456)

      expect(id1).to eq(id2)
    end

    it "compares unequal IDs" do
      id1 = described_class.new(ledger_id: 123, entry_id: 456)
      id2 = described_class.new(ledger_id: 123, entry_id: 789)

      expect(id1).not_to eq(id2)
    end

    it "returns false for non-MessageId" do
      id = described_class.new(ledger_id: 123, entry_id: 456)

      expect(id).not_to eq("123:456:-1:-1")
    end

    it "considers all fields" do
      id1 = described_class.new(ledger_id: 123, entry_id: 456, partition: 1, batch_index: 0)
      id2 = described_class.new(ledger_id: 123, entry_id: 456, partition: 2, batch_index: 0)

      expect(id1).not_to eq(id2)
    end
  end

  describe "#hash" do
    it "returns same hash for equal IDs" do
      id1 = described_class.new(ledger_id: 123, entry_id: 456)
      id2 = described_class.new(ledger_id: 123, entry_id: 456)

      expect(id1.hash).to eq(id2.hash)
    end

    it "can be used as Hash key" do
      id1 = described_class.new(ledger_id: 123, entry_id: 456)
      id2 = described_class.new(ledger_id: 123, entry_id: 456)

      hash = { id1 => "value" }
      expect(hash[id2]).to eq("value")
    end
  end

  describe "#<=>" do
    it "compares by ledger_id first" do
      id1 = described_class.new(ledger_id: 100, entry_id: 999)
      id2 = described_class.new(ledger_id: 200, entry_id: 1)

      expect(id1 <=> id2).to eq(-1)
      expect(id2 <=> id1).to eq(1)
    end

    it "compares by entry_id when ledger_id is equal" do
      id1 = described_class.new(ledger_id: 100, entry_id: 100)
      id2 = described_class.new(ledger_id: 100, entry_id: 200)

      expect(id1 <=> id2).to eq(-1)
    end

    it "returns 0 for equal IDs" do
      id1 = described_class.new(ledger_id: 100, entry_id: 100)
      id2 = described_class.new(ledger_id: 100, entry_id: 100)

      expect(id1 <=> id2).to eq(0)
    end

    it "returns nil for non-MessageId" do
      id = described_class.new(ledger_id: 100, entry_id: 100)

      expect(id <=> "not an id").to be_nil
    end

    it "supports Comparable methods" do
      id1 = described_class.new(ledger_id: 100, entry_id: 100)
      id2 = described_class.new(ledger_id: 200, entry_id: 100)

      expect(id1 < id2).to be true
      expect(id2 > id1).to be true
      expect(id1 <= id1).to be true
      expect(id1 >= id1).to be true
    end
  end

  describe "#to_proto" do
    it "converts to protobuf MessageIdData" do
      id = described_class.new(
        ledger_id: 123,
        entry_id: 456,
        partition: 2,
        batch_index: 5
      )

      proto = id.to_proto

      expect(proto).to be_a(Pulsar::Proto::MessageIdData)
      expect(proto.ledgerId).to eq(123)
      expect(proto.entryId).to eq(456)
      expect(proto.partition).to eq(2)
      expect(proto.batch_index).to eq(5)
    end
  end

  describe ".from_proto" do
    it "creates MessageId from protobuf" do
      proto = Pulsar::Proto::MessageIdData.new(
        ledgerId: 123,
        entryId: 456,
        partition: 2,
        batch_index: 5
      )

      id = described_class.from_proto(proto)

      expect(id.ledger_id).to eq(123)
      expect(id.entry_id).to eq(456)
      expect(id.partition).to eq(2)
      expect(id.batch_index).to eq(5)
    end
  end
end
