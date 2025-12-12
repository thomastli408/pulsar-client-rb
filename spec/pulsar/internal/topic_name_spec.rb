# frozen_string_literal: true

require "spec_helper"
require "pulsar/internal/topic_name"

RSpec.describe Pulsar::Internal::TopicName do
  describe "#initialize" do
    context "with full topic name" do
      it "parses persistent topic" do
        topic = described_class.new("persistent://my-tenant/my-ns/my-topic")

        expect(topic.domain).to eq("persistent")
        expect(topic.tenant).to eq("my-tenant")
        expect(topic.namespace).to eq("my-ns")
        expect(topic.local_name).to eq("my-topic")
        expect(topic.name).to eq("persistent://my-tenant/my-ns/my-topic")
      end

      it "parses non-persistent topic" do
        topic = described_class.new("non-persistent://my-tenant/my-ns/my-topic")

        expect(topic.domain).to eq("non-persistent")
        expect(topic.tenant).to eq("my-tenant")
        expect(topic.namespace).to eq("my-ns")
        expect(topic.local_name).to eq("my-topic")
      end

      it "handles topic names with special characters" do
        topic = described_class.new("persistent://tenant/ns/my-topic-with-dashes_and_underscores")

        expect(topic.local_name).to eq("my-topic-with-dashes_and_underscores")
      end

      it "handles topic names with slashes" do
        topic = described_class.new("persistent://tenant/ns/path/to/topic")

        expect(topic.local_name).to eq("path/to/topic")
      end
    end

    context "with short topic name (just topic)" do
      it "uses defaults for single name" do
        topic = described_class.new("my-topic")

        expect(topic.domain).to eq("persistent")
        expect(topic.tenant).to eq("public")
        expect(topic.namespace).to eq("default")
        expect(topic.local_name).to eq("my-topic")
        expect(topic.name).to eq("persistent://public/default/my-topic")
      end
    end

    context "with tenant/namespace/topic format" do
      it "uses default domain" do
        topic = described_class.new("my-tenant/my-ns/my-topic")

        expect(topic.domain).to eq("persistent")
        expect(topic.tenant).to eq("my-tenant")
        expect(topic.namespace).to eq("my-ns")
        expect(topic.local_name).to eq("my-topic")
        expect(topic.name).to eq("persistent://my-tenant/my-ns/my-topic")
      end
    end

    context "with invalid input" do
      it "raises error for nil" do
        expect { described_class.new(nil) }.to raise_error(ArgumentError, /nil or empty/)
      end

      it "raises error for empty string" do
        expect { described_class.new("") }.to raise_error(ArgumentError, /nil or empty/)
      end

      it "raises error for invalid format (two parts)" do
        expect { described_class.new("tenant/topic") }.to raise_error(ArgumentError, /Invalid topic name format/)
      end

      it "raises error for invalid domain" do
        expect { described_class.new("invalid://tenant/ns/topic") }.to raise_error(ArgumentError, /Invalid domain/)
      end

      it "raises error for empty tenant" do
        expect { described_class.new("persistent:///ns/topic") }.to raise_error(ArgumentError, /Tenant cannot be empty/)
      end

      it "raises error for empty namespace" do
        expect { described_class.new("persistent://tenant//topic") }.to raise_error(ArgumentError, /Namespace cannot be empty/)
      end

      it "raises error for empty local name" do
        expect { described_class.new("persistent://tenant/ns/") }.to raise_error(ArgumentError, /local name cannot be empty/)
      end
    end
  end

  describe "#persistent?" do
    it "returns true for persistent topics" do
      topic = described_class.new("persistent://tenant/ns/topic")
      expect(topic.persistent?).to be true
    end

    it "returns false for non-persistent topics" do
      topic = described_class.new("non-persistent://tenant/ns/topic")
      expect(topic.persistent?).to be false
    end
  end

  describe "#non_persistent?" do
    it "returns true for non-persistent topics" do
      topic = described_class.new("non-persistent://tenant/ns/topic")
      expect(topic.non_persistent?).to be true
    end

    it "returns false for persistent topics" do
      topic = described_class.new("persistent://tenant/ns/topic")
      expect(topic.non_persistent?).to be false
    end
  end

  describe "#namespace_path" do
    it "returns tenant/namespace" do
      topic = described_class.new("persistent://my-tenant/my-ns/topic")
      expect(topic.namespace_path).to eq("my-tenant/my-ns")
    end
  end

  describe "#full_namespace" do
    it "returns domain://tenant/namespace" do
      topic = described_class.new("persistent://my-tenant/my-ns/topic")
      expect(topic.full_namespace).to eq("persistent://my-tenant/my-ns")
    end
  end

  describe "#to_s" do
    it "returns the full topic name" do
      topic = described_class.new("my-topic")
      expect(topic.to_s).to eq("persistent://public/default/my-topic")
    end
  end

  describe "#==" do
    it "compares two TopicName objects" do
      topic1 = described_class.new("my-topic")
      topic2 = described_class.new("persistent://public/default/my-topic")

      expect(topic1).to eq(topic2)
    end

    it "compares with string" do
      topic = described_class.new("my-topic")

      expect(topic).to eq("persistent://public/default/my-topic")
    end

    it "returns false for different topics" do
      topic1 = described_class.new("topic1")
      topic2 = described_class.new("topic2")

      expect(topic1).not_to eq(topic2)
    end
  end

  describe "#hash" do
    it "returns same hash for equivalent topics" do
      topic1 = described_class.new("my-topic")
      topic2 = described_class.new("persistent://public/default/my-topic")

      expect(topic1.hash).to eq(topic2.hash)
    end

    it "can be used as Hash key" do
      topic1 = described_class.new("my-topic")
      topic2 = described_class.new("persistent://public/default/my-topic")

      hash = { topic1 => "value" }
      expect(hash[topic2]).to eq("value")
    end
  end
end
