#!/usr/bin/env ruby
# frozen_string_literal: true

# Example: Negative acknowledgment and Dead Letter Queue
#
# This example demonstrates:
# 1. Using negative_acknowledge (nack) to trigger message redelivery
# 2. Configuring a Dead Letter Queue (DLQ) policy
# 3. Messages automatically routed to DLQ after max redeliveries
# 4. Processing DLQ messages
#
# Usage:
#   PULSAR_URL=pulsar://localhost:6650 bundle exec ruby examples/nack_and_dlq.rb

require_relative "../lib/pulsar/client"

pulsar_url = ENV.fetch("PULSAR_URL", "pulsar://localhost:6650")
topic = "test-dlq-topic"
subscription = "test-dlq-subscription"
max_redeliveries = 3

puts "=" * 60
puts "Negative Acknowledgment and Dead Letter Queue Example"
puts "=" * 60
puts
puts "Configuration:"
puts "  Pulsar URL: #{pulsar_url}"
puts "  Topic: #{topic}"
puts "  Subscription: #{subscription}"
puts "  Max Redeliveries: #{max_redeliveries}"
puts

# Create the Pulsar client
client = Pulsar::Client.new(pulsar_url)

begin
  # Configure DLQ policy - after max_redeliveries failed attempts,
  # message will be routed to the dead letter topic
  dlq_policy = Pulsar::DLQPolicy.new(
    max_redeliveries: max_redeliveries,
    dead_letter_topic: "#{topic}-DLQ" # Optional: auto-generated if nil
  )

  # Create producer
  producer = client.create_producer(topic)
  puts "[Producer] Created producer: #{producer.name}"

  # Create consumer with DLQ policy
  consumer = client.subscribe(topic, subscription, {
    subscription_type: :shared,
    initial_position: :earliest,
    dlq_policy: dlq_policy
  })
  puts "[Consumer] Subscribed with DLQ policy (max_redeliveries: #{max_redeliveries})"
  puts

  # Send test messages
  puts "=" * 60
  puts "Sending test messages..."
  puts "=" * 60

  # Message 1: Will be processed successfully
  producer.send(Pulsar::ProducerMessage.new(
    payload: "Good message - will be acknowledged",
    properties: { "type" => "good" }
  ))
  puts "[Producer] Sent: Good message"

  # Message 2: Will fail and be nacked until routed to DLQ
  producer.send(Pulsar::ProducerMessage.new(
    payload: "Bad message - will fail processing",
    properties: { "type" => "bad" }
  ))
  puts "[Producer] Sent: Bad message (will be nacked)"

  # Message 3: Will be explicitly sent to DLQ
  producer.send(Pulsar::ProducerMessage.new(
    payload: "Poison message - send directly to DLQ",
    properties: { "type" => "poison" }
  ))
  puts "[Producer] Sent: Poison message (will go directly to DLQ)"
  puts

  # Process messages
  puts "=" * 60
  puts "Processing messages..."
  puts "=" * 60

  processed = 0
  nack_counts = Hash.new(0)

  while processed < 3
    message = consumer.receive(timeout: 5)

    if message.nil?
      puts "[Consumer] No message received (timeout)"
      next
    end

    msg_type = message.properties["type"]
    puts
    puts "[Consumer] Received message:"
    puts "  Payload: #{message.data}"
    puts "  Type: #{msg_type}"
    puts "  Redelivery Count: #{message.redelivery_count}"

    case msg_type
    when "good"
      # Process successfully
      consumer.acknowledge(message)
      puts "  -> Acknowledged successfully"
      processed += 1

    when "bad"
      # Simulate processing failure - nack the message
      nack_counts[message.message_id.to_s] += 1

      if message.redelivery_count < max_redeliveries
        consumer.negative_acknowledge(message)
        puts "  -> NACK (redelivery #{message.redelivery_count + 1}/#{max_redeliveries})"
        puts "     Message will be redelivered..."
      else
        # DLQ routing happens automatically via receive() when redelivery_count >= max_redeliveries
        # This branch won't actually be reached because receive() handles it
        puts "  -> Routed to DLQ automatically"
        processed += 1
      end

    when "poison"
      # Immediately send to DLQ without retrying
      dlq_msg_id = consumer.send_to_dlq(message)
      puts "  -> Sent directly to DLQ: #{dlq_msg_id}"
      processed += 1

    else
      consumer.acknowledge(message)
      processed += 1
    end
  end

  # Wait a bit for the bad message to be redelivered and eventually hit DLQ
  puts
  puts "Waiting for bad message to exhaust redeliveries..."
  sleep 2

  # Try to receive more (the bad message after being nacked multiple times)
  loop do
    message = consumer.receive(timeout: 3)
    break if message.nil?

    puts
    puts "[Consumer] Received redelivered message:"
    puts "  Payload: #{message.data}"
    puts "  Redelivery Count: #{message.redelivery_count}"

    if message.redelivery_count >= max_redeliveries
      puts "  -> Will be routed to DLQ automatically on next receive"
      # The DLQ routing happened before we received it (if >= max_redeliveries)
      # This is handled inside receive()
    else
      consumer.negative_acknowledge(message)
      puts "  -> NACK again"
    end
  end

  producer.close
  consumer.close

  # Now consume from the DLQ
  puts
  puts "=" * 60
  puts "Processing Dead Letter Queue..."
  puts "=" * 60

  dlq_topic = "#{topic}-DLQ"
  dlq_consumer = client.subscribe(dlq_topic, "dlq-processor", {
    subscription_type: :shared,
    initial_position: :earliest
  })
  puts "[DLQ Consumer] Subscribed to: #{dlq_topic}"
  puts

  dlq_count = 0
  loop do
    message = dlq_consumer.receive(timeout: 3)
    break if message.nil?

    dlq_count += 1
    puts "[DLQ] Message #{dlq_count}:"
    puts "  Payload: #{message.data}"
    puts "  Original Topic: #{message.properties['PULSAR_DLQ_ORIGINAL_TOPIC']}"
    puts "  Original Subscription: #{message.properties['PULSAR_DLQ_ORIGINAL_SUBSCRIPTION']}"
    puts "  Original Message ID: #{message.properties['PULSAR_DLQ_ORIGINAL_MESSAGE_ID']}"
    puts "  Redelivery Count: #{message.properties['PULSAR_DLQ_REDELIVERY_COUNT']}"
    puts "  Original Type: #{message.properties['type']}"

    # Acknowledge the DLQ message after processing
    dlq_consumer.acknowledge(message)
    puts "  -> Acknowledged"
    puts
  end

  dlq_consumer.close

  puts "=" * 60
  puts "Summary:"
  puts "  Messages processed from DLQ: #{dlq_count}"
  puts "=" * 60

rescue Interrupt
  puts "\nShutting down..."
rescue Pulsar::Error => e
  puts "Pulsar error: #{e.message}"
  puts e.backtrace.first(5).join("\n")
  exit 1
rescue => e
  puts "Error: #{e.class}: #{e.message}"
  puts e.backtrace.first(10).join("\n")
  exit 1
ensure
  puts
  puts "Closing client..."
  client.close
  puts "Done."
end
