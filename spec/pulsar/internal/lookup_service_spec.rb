# frozen_string_literal: true

require "spec_helper"
require "pulsar/internal/lookup_service"

RSpec.describe Pulsar::Internal::LookupService do
  let(:connection_pool) { instance_double(Pulsar::Internal::ConnectionPool) }
  let(:service_url) { Pulsar::Internal::ServiceUrl.new("pulsar://localhost:6650") }

  describe "#initialize" do
    it "creates a lookup service" do
      lookup = described_class.new(connection_pool, service_url)
      expect(lookup).to be_a(described_class)
    end

    it "accepts custom timeout" do
      lookup = described_class.new(connection_pool, service_url, timeout: 60)
      expect(lookup).to be_a(described_class)
    end
  end

  describe "#lookup" do
    let(:connection) { instance_double(Pulsar::Internal::Connection) }
    let(:lookup_service) { described_class.new(connection_pool, service_url) }

    before do
      allow(connection_pool).to receive(:get_connection).and_return(connection)
      allow(connection).to receive(:next_request_id).and_return(1)
    end

    context "when broker responds with Connect" do
      it "returns the broker URL" do
        response = double(
          "response",
          response: :Connect,
          brokerServiceUrl: "pulsar://broker1:6650",
          authoritative: true
        )
        allow(connection).to receive(:send_request).and_return(response)

        result = lookup_service.lookup("my-topic")

        expect(result).to eq("pulsar://broker1:6650")
      end

      it "normalizes short topic names" do
        response = double(
          "response",
          response: :Connect,
          brokerServiceUrl: "pulsar://broker1:6650",
          authoritative: true
        )
        allow(connection).to receive(:send_request).and_return(response)

        # Should expand "my-topic" to full name
        expect(connection).to receive(:send_request) do |cmd, **_opts|
          expect(cmd.lookupTopic.topic).to eq("persistent://public/default/my-topic")
          response
        end

        lookup_service.lookup("my-topic")
      end

      it "accepts TopicName objects" do
        topic = Pulsar::Internal::TopicName.new("persistent://tenant/ns/topic")
        response = double(
          "response",
          response: :Connect,
          brokerServiceUrl: "pulsar://broker1:6650",
          authoritative: true
        )
        allow(connection).to receive(:send_request).and_return(response)

        result = lookup_service.lookup(topic)

        expect(result).to eq("pulsar://broker1:6650")
      end
    end

    context "when broker responds with Redirect" do
      it "follows redirects" do
        redirect_response = double(
          "redirect",
          response: :Redirect,
          brokerServiceUrl: "pulsar://broker2:6650",
          authoritative: true
        )
        final_response = double(
          "final",
          response: :Connect,
          brokerServiceUrl: "pulsar://broker2:6650",
          authoritative: true
        )

        call_count = 0
        allow(connection).to receive(:send_request) do
          call_count += 1
          call_count == 1 ? redirect_response : final_response
        end

        result = lookup_service.lookup("my-topic")

        expect(result).to eq("pulsar://broker2:6650")
      end

      it "raises error when max redirects exceeded" do
        redirect_response = double(
          "redirect",
          response: :Redirect,
          brokerServiceUrl: "pulsar://broker2:6650",
          authoritative: false
        )
        allow(connection).to receive(:send_request).and_return(redirect_response)

        expect {
          lookup_service.lookup("my-topic")
        }.to raise_error(Pulsar::LookupError, /Max redirects/)
      end
    end

    context "when broker responds with Failed" do
      it "raises LookupError" do
        failed_response = double(
          "failed",
          response: :Failed,
          error: :TopicNotFound,
          message: "Topic not found"
        )
        allow(connection).to receive(:send_request).and_return(failed_response)

        expect {
          lookup_service.lookup("my-topic")
        }.to raise_error(Pulsar::LookupError, /Lookup failed.*TopicNotFound/)
      end
    end
  end

  describe "#lookup_connection" do
    let(:connection) { instance_double(Pulsar::Internal::Connection) }
    let(:broker_connection) { instance_double(Pulsar::Internal::Connection) }
    let(:lookup_service) { described_class.new(connection_pool, service_url) }

    it "returns connection to the looked-up broker" do
      response = double(
        "response",
        response: :Connect,
        brokerServiceUrl: "pulsar://broker1:6650",
        authoritative: true
      )

      allow(connection).to receive(:next_request_id).and_return(1)
      allow(connection).to receive(:send_request).and_return(response)

      # First call for lookup, second for getting broker connection
      call_count = 0
      allow(connection_pool).to receive(:get_connection) do |url|
        call_count += 1
        if call_count == 1
          connection  # service URL connection
        else
          broker_connection  # broker connection
        end
      end

      result = lookup_service.lookup_connection("my-topic")

      expect(result).to eq(broker_connection)
    end
  end

  # Integration tests require a running Pulsar broker
  describe "integration tests", skip: ENV["PULSAR_URL"].nil? do
    let(:pulsar_url) { ENV.fetch("PULSAR_URL", "pulsar://localhost:6650") }
    let(:real_service_url) { Pulsar::Internal::ServiceUrl.new(pulsar_url) }
    let(:real_connection_pool) { Pulsar::Internal::ConnectionPool.new }
    let(:lookup_service) { described_class.new(real_connection_pool, real_service_url) }

    after do
      real_connection_pool.close_all
    end

    describe "#lookup" do
      it "looks up a topic and returns broker URL" do
        result = lookup_service.lookup("test-lookup-topic")

        expect(result).to start_with("pulsar://")
        expect(result).to match(/:\d+$/)
      end
    end

    describe "#lookup_connection" do
      it "returns a ready connection" do
        connection = lookup_service.lookup_connection("test-lookup-topic")

        expect(connection).to be_a(Pulsar::Internal::Connection)
        expect(connection.ready?).to be true
      end
    end
  end
end
