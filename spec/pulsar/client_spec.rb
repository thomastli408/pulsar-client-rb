# frozen_string_literal: true

require "spec_helper"

RSpec.describe Pulsar do
  it "has a version number" do
    expect(Pulsar::Client::VERSION).not_to be nil
  end

  it "defines error classes" do
    expect(Pulsar::Error).to be < StandardError
    expect(Pulsar::ConnectionError).to be < Pulsar::Error
    expect(Pulsar::TimeoutError).to be < Pulsar::Error
  end
end
