# frozen_string_literal: true

require "spec_helper"
require "pulsar/internal/service_url"

RSpec.describe Pulsar::Internal::ServiceUrl do
  describe "#initialize" do
    it "parses single host URL" do
      url = described_class.new("pulsar://localhost:6650")

      expect(url.hosts).to eq([["localhost", 6650]])
      expect(url.multi_host?).to be false
    end

    it "parses multi-host URL" do
      url = described_class.new("pulsar://host1:6650,host2:6651,host3:6652")

      expect(url.hosts).to eq([
        ["host1", 6650],
        ["host2", 6651],
        ["host3", 6652]
      ])
      expect(url.multi_host?).to be true
    end

    it "uses default port 6650 when not specified" do
      url = described_class.new("pulsar://localhost")

      expect(url.hosts).to eq([["localhost", 6650]])
    end

    it "handles mixed port specifications" do
      url = described_class.new("pulsar://host1:6650,host2,host3:6652")

      expect(url.hosts).to eq([
        ["host1", 6650],
        ["host2", 6650],
        ["host3", 6652]
      ])
    end

    it "handles whitespace around hosts" do
      url = described_class.new("pulsar://host1:6650, host2:6651 , host3:6652")

      expect(url.hosts).to eq([
        ["host1", 6650],
        ["host2", 6651],
        ["host3", 6652]
      ])
    end

    it "raises error for nil URL" do
      expect { described_class.new(nil) }.to raise_error(ArgumentError, /nil or empty/)
    end

    it "raises error for empty URL" do
      expect { described_class.new("") }.to raise_error(ArgumentError, /nil or empty/)
    end

    it "raises error for invalid scheme" do
      expect { described_class.new("http://localhost:6650") }.to raise_error(ArgumentError, /Only 'pulsar:\/\/'/)
    end

    it "raises error for pulsar+ssl scheme (not supported)" do
      expect { described_class.new("pulsar+ssl://localhost:6650") }.to raise_error(ArgumentError, /Only 'pulsar:\/\/'/)
    end

    it "raises error for URL without hosts" do
      expect { described_class.new("pulsar://") }.to raise_error(ArgumentError, /No hosts/)
    end

    it "raises error for invalid port" do
      expect { described_class.new("pulsar://localhost:abc") }.to raise_error(ArgumentError, /Invalid port/)
    end

    it "raises error for port out of range" do
      expect { described_class.new("pulsar://localhost:99999") }.to raise_error(ArgumentError, /Invalid port/)
    end
  end

  describe "#next_host" do
    it "returns host/port pair" do
      url = described_class.new("pulsar://localhost:6650")
      host, port = url.next_host

      expect(host).to eq("localhost")
      expect(port).to eq(6650)
    end

    it "round-robins through hosts" do
      url = described_class.new("pulsar://host1:6650,host2:6651,host3:6652")

      expect(url.next_host).to eq(["host1", 6650])
      expect(url.next_host).to eq(["host2", 6651])
      expect(url.next_host).to eq(["host3", 6652])
      expect(url.next_host).to eq(["host1", 6650])  # wraps around
    end

    it "is thread-safe" do
      url = described_class.new("pulsar://host1:6650,host2:6651,host3:6652")

      hosts = []
      threads = 10.times.map do
        Thread.new { hosts << url.next_host }
      end
      threads.each(&:join)

      # All hosts should be valid
      hosts.each do |host, port|
        expect(%w[host1 host2 host3]).to include(host)
        expect([6650, 6651, 6652]).to include(port)
      end
    end
  end

  describe "#first_host" do
    it "returns the first host" do
      url = described_class.new("pulsar://host1:6650,host2:6651")

      expect(url.first_host).to eq(["host1", 6650])
      expect(url.first_host).to eq(["host1", 6650])  # always first
    end
  end

  describe "#broker_urls" do
    it "returns list of broker URLs" do
      url = described_class.new("pulsar://host1:6650,host2:6651")

      expect(url.broker_urls).to eq([
        "pulsar://host1:6650",
        "pulsar://host2:6651"
      ])
    end
  end

  describe "#to_s" do
    it "returns the original URL" do
      original = "pulsar://localhost:6650"
      url = described_class.new(original)

      expect(url.to_s).to eq(original)
    end
  end
end
