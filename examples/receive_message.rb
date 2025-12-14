#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Create a consumer and receive messages
#
# Usage:
#   PULSAR_URL=pulsar://localhost:6650 bundle exec ruby examples/receive_message.rb
#
# To produce messages, run in another terminal:
#   bundle exec ruby examples/send_message.rb

require_relative "../lib/pulsar/client"

pulsar_url = ENV.fetch("PULSAR_URL", "pulsar://localhost:6650")
topic = ARGV[0] || "test-producer-topic"
subscription = ARGV[1] || "test-subscription"

puts "Connecting to Pulsar at #{pulsar_url}..."

# Create the required components
service_url = Pulsar::Internal::ServiceUrl.new(pulsar_url)
connection_pool = Pulsar::Internal::ConnectionPool.new
lookup_service = Pulsar::Internal::LookupService.new(connection_pool, service_url)

begin
  puts "Creating consumer for topic: #{topic}"
  puts "Subscription: #{subscription}"
  puts "-" * 50

  consumer = Pulsar::Consumer.new(
    connection_pool: connection_pool,
    lookup_service: lookup_service,
    topic: topic,
    subscription: subscription,
    options: {
      subscription_type: :shared,
      initial_position: :earliest
    }
  )

  puts "Consumer created!"
  puts "  Topic: #{consumer.topic}"
  puts "  Subscription: #{consumer.subscription}"
  puts "  Consumer ID: #{consumer.consumer_id}"
  puts "  Ready: #{consumer.ready?}"
  puts "-" * 50

  puts "\nWaiting for messages (Ctrl+C to stop)..."
  puts ""

  message_count = 0

  loop do
    # Receive with 5 second timeout
    message = consumer.receive(timeout: 5)

    if message.nil?
      puts "No message received (timeout), waiting..."
      next
    end

    message_count += 1

    puts "=== Message #{message_count} ==="
    puts "  Message ID: #{message.message_id}"
    puts "  Payload: #{message.data}"
    puts "  Producer: #{message.producer_name}"
    puts "  Publish Time: #{message.publish_time_as_time}"
    puts "  Redelivery Count: #{message.redelivery_count}"

    if message.properties.any?
      puts "  Properties:"
      message.properties.each do |key, value|
        puts "    #{key}: #{value}"
      end
    end

    if message.key
      puts "  Key: #{message.key}"
    end

    # Acknowledge the message
    consumer.acknowledge(message)
    puts "  [ACKNOWLEDGED]"
    puts ""
  end

rescue Interrupt
  puts "\nShutting down..."
rescue Pulsar::Error => e
  puts "Pulsar error: #{e.message}"
  exit 1
rescue => e
  puts "Error: #{e.class}: #{e.message}"
  puts e.backtrace.first(10).join("\n")
  exit 1
ensure
  if defined?(consumer) && consumer
    puts "Closing consumer..."
    consumer.close
    puts "Consumer closed."
  end
  connection_pool.close_all
  puts "Connection pool closed."
end
