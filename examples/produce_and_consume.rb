#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Produce and consume messages using Pulsar::Client
#
# Usage:
#   PULSAR_URL=pulsar://localhost:6650 bundle exec ruby examples/produce_and_consume.rb
#

require_relative "../lib/pulsar/client"

pulsar_url = ENV.fetch("PULSAR_URL", "pulsar://localhost:6650")
topic = ARGV[0] || "test-rb-driver-topic"
subscription = ARGV[1] || "test-subscription"

puts "Connecting to Pulsar at #{pulsar_url}..."

# Create the Pulsar client
client = Pulsar::Client.new(pulsar_url)

begin
  total_message_count = 5
  consumed_message_count = 0
  consumer_count = 2
  consumer_mutex = Mutex.new

  threads = []

  # Producer thread
  threads << Thread.new do
    producer = client.create_producer(topic)
    puts "Producer created: #{producer.name}"

    total_message_count.times do |i|
      msg = Pulsar::ProducerMessage.new(payload: "Message #{i + 1} from producer")
      mid = producer.send(msg)
      puts "Sent message #{i + 1}: #{mid}"
      sleep 1 + rand(3)
    end

    producer.close
  end

  # Consumer threads
  consumer_count.times do
    threads << Thread.new do
      consumer = client.subscribe(topic, subscription, {
        subscription_type: :shared,
        initial_position: :earliest
      })
      puts "Consumer created: #{consumer.consumer_id}"

      loop do
        done = consumer_mutex.synchronize do
          puts "Consumer #{consumer.consumer_id} checking consumed count: #{consumed_message_count}/#{total_message_count}"
          consumed_message_count >= total_message_count
        end
        if done
          puts "All messages consumed, exiting consumer #{consumer.consumer_id}."
          break
        end

        begin
          message = consumer.receive(timeout: 1)
        rescue Pulsar::TimeoutError
          message = nil
        end

        if message.nil?
          puts "No message received (timeout), waiting..."
          next
        end

        consumer_mutex.synchronize do
          consumed_message_count += 1
        end

        puts "=== Message #{consumed_message_count} Received by Consumer #{consumer.consumer_id} ==="
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
    end
  end

  threads.each(&:join)

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
  puts "Closing client..."
  client.close
  puts "Client closed."
end
