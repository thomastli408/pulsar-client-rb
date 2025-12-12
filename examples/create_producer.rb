#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Create a producer and print the success message from the broker
#
# Usage:
#   PULSAR_URL=pulsar://localhost:6650 bundle exec ruby examples/create_producer.rb

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

  puts "-" * 50
  puts "Producer created successfully!"
  puts "  Topic: #{producer.topic}"
  puts "  Name: #{producer.name}"
  puts "  Producer ID: #{producer.producer_id}"
  puts "  Ready: #{producer.ready?}"
  puts "  producer.inspect: #{producer.inspect}"

  puts "\nClosing producer..."
  producer.close
  puts "Producer closed."

rescue Pulsar::Error => e
  puts "Pulsar error: #{e.message}"
  exit 1
rescue => e
  puts "Error: #{e.class}: #{e.message}"
  puts e.backtrace.first(5).join("\n")
  exit 1
ensure
  connection_pool.close_all
  puts "Connection pool closed."
end
