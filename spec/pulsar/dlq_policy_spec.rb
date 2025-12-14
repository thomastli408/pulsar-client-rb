# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pulsar::DLQPolicy do
  describe "#initialize" do
    it "creates a policy with required max_redeliveries" do
      policy = described_class.new(max_redeliveries: 3)

      expect(policy.max_redeliveries).to eq(3)
      expect(policy.dead_letter_topic).to be_nil
      expect(policy.initial_subscription_name).to be_nil
    end

    it "accepts custom dead_letter_topic" do
      policy = described_class.new(
        max_redeliveries: 5,
        dead_letter_topic: "custom-dlq"
      )

      expect(policy.max_redeliveries).to eq(5)
      expect(policy.dead_letter_topic).to eq("custom-dlq")
    end

    it "accepts initial_subscription_name" do
      policy = described_class.new(
        max_redeliveries: 3,
        initial_subscription_name: "dlq-sub"
      )

      expect(policy.initial_subscription_name).to eq("dlq-sub")
    end

    it "raises error for nil max_redeliveries" do
      expect {
        described_class.new(max_redeliveries: nil)
      }.to raise_error(ArgumentError, /must be positive/)
    end

    it "raises error for zero max_redeliveries" do
      expect {
        described_class.new(max_redeliveries: 0)
      }.to raise_error(ArgumentError, /must be positive/)
    end

    it "raises error for negative max_redeliveries" do
      expect {
        described_class.new(max_redeliveries: -1)
      }.to raise_error(ArgumentError, /must be positive/)
    end

    it "raises error for non-integer max_redeliveries" do
      expect {
        described_class.new(max_redeliveries: "3")
      }.to raise_error(ArgumentError, /must be positive/)
    end
  end

  describe "#generate_dlq_topic" do
    context "when dead_letter_topic is set" do
      it "returns the configured topic" do
        policy = described_class.new(
          max_redeliveries: 3,
          dead_letter_topic: "my-custom-dlq"
        )

        topic = policy.generate_dlq_topic("original-topic", "my-subscription")

        expect(topic).to eq("my-custom-dlq")
      end
    end

    context "when dead_letter_topic is not set" do
      it "generates topic name from original topic and subscription" do
        policy = described_class.new(max_redeliveries: 3)

        topic = policy.generate_dlq_topic("my-topic", "my-subscription")

        expect(topic).to eq("my-topic-my-subscription-DLQ")
      end

      it "handles fully qualified topic names" do
        policy = described_class.new(max_redeliveries: 3)

        topic = policy.generate_dlq_topic(
          "persistent://tenant/namespace/my-topic",
          "my-sub"
        )

        expect(topic).to eq("persistent://tenant/namespace/my-topic-my-sub-DLQ")
      end
    end
  end
end
