#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Create a producer and send messages
#
# Usage:
#   PULSAR_URL=pulsar://localhost:6650 bundle exec ruby examples/send_message.rb

require_relative "../lib/pulsar/client"

pulsar_url = ENV.fetch("PULSAR_URL", "pulsar://localhost:6650")
topic = ARGV[0] || "test-producer-topic"

puts "Connecting to Pulsar at #{pulsar_url}..."

# Create the required components
service_url = Pulsar::Internal::ServiceUrl.new(pulsar_url)
connection_pool = Pulsar::Internal::ConnectionPool.new
lookup_service = Pulsar::Internal::LookupService.new(connection_pool, service_url)

begin
  puts "Creating producer for topic: #{topic}"
  puts "-" * 50

  producer = Pulsar::Producer.new(
    connection_pool: connection_pool,
    lookup_service: lookup_service,
    topic: topic
  )

  puts "Producer created: #{producer.name}"
  puts "-" * 50

  # Synchronous send
  puts "\n=== Synchronous Send ==="
  message = Pulsar::ProducerMessage.new(
    payload: "Hello, Pulsar!",
    key: "my-key",
    properties: { "source" => "ruby-client", "example" => "sync" }
  )

  message_id = producer.send(message)
  puts "Sent message: #{message_id}"

  # Send a few more messages
  5.times do |i|
    msg = Pulsar::ProducerMessage.new(payload: "Message #{i + 1}")
    mid = producer.send(msg)
    puts "Sent message #{i + 1}: #{mid}"
  end

  puts "\nAll messages sent!"
  puts "-" * 50

  puts "\nClosing producer..."
  producer.close
  puts "Producer closed."

rescue Pulsar::Error => e
  puts "Pulsar error: #{e.message}"
  exit 1
rescue => e
  puts "Error: #{e.class}: #{e.message}"
  puts e.backtrace.first(10).join("\n")
  exit 1
ensure
  connection_pool.close_all
  puts "Connection pool closed."
end
