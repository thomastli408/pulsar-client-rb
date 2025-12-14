# Pulsar Client for Ruby
> Status: Early / Not yet battle-tested <br>
> This library is under early development and has not been proven in production environments.

A pure Ruby client library for [Apache Pulsar](https://pulsar.apache.org/), the distributed messaging and streaming platform. This client is developed independently of C++ bindings, making it easy to install and use in any Ruby environment.

## Features

- Pure Ruby implementation (no C++ bindings required)
- Producer for sending messages to topics, supporting both standalone and partitioned topics
- Consumer for receiving messages with subscription support
- Multiple subscription types: Shared, Exclusive, Failover
- Message properties and partition keys
- Automatic connection management and keep-alive
- Connection pooling for efficient resource usage
- Thread-safe operations

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'pulsar-client'
```

And then execute:

```bash
bundle install
```

Or install it yourself as:

```bash
gem install pulsar-client
```

## Requirements

- Ruby >= 2.3.0
- Apache Pulsar broker (tested with Pulsar 3.x)

## Quick Start

```ruby
require 'pulsar/client'

# Create a client
client = Pulsar::Client.new("pulsar://localhost:6650")

# Create a producer and send messages
producer = client.create_producer("my-topic")
message_id = producer.send(Pulsar::ProducerMessage.new(payload: "Hello, Pulsar!"))
puts "Sent message: #{message_id}"
producer.close

# Create a consumer and receive messages
consumer = client.subscribe("my-topic", "my-subscription")
message = consumer.receive(timeout: 10)
if message
  puts "Received: #{message.data}"
  consumer.acknowledge(message)
end
consumer.close

# Clean up
client.close
```

## Usage

### Creating a Client

```ruby
# Basic client
client = Pulsar::Client.new("pulsar://localhost:6650")

# With options
client = Pulsar::Client.new("pulsar://localhost:6650", {
  connection_timeout: 10,      # Connection timeout in seconds (default: 10)
  keep_alive_interval: 30,     # Keep-alive interval in seconds (default: 30)
  operation_timeout: 30        # Default operation timeout in seconds (default: 30)
})

# Multiple brokers for failover
client = Pulsar::Client.new("pulsar://broker1:6650,broker2:6650,broker3:6650")
```

### Producer

```ruby
# Create a producer
producer = client.create_producer("my-topic")

# With options
producer = client.create_producer("my-topic", {
  name: "my-producer",           # Producer name (auto-generated if nil)
  send_timeout: 30,              # Send timeout in seconds (default: 30)
  max_pending_messages: 1000     # Max pending messages (default: 1000)
})

# Send a simple message
message_id = producer.send(Pulsar::ProducerMessage.new(payload: "Hello!"))

# Send a message with properties
message = Pulsar::ProducerMessage.new(
  payload: "Hello with metadata!",
  key: "partition-key",          # Partition key for routing
  properties: {                  # Custom properties
    "source" => "ruby-client",
    "version" => "1.0"
  },
  event_time: Time.now           # Event time (optional)
)
message_id = producer.send(message)

# Close when done
producer.close
```

### Consumer

```ruby
# Create a consumer with a subscription
consumer = client.subscribe("my-topic", "my-subscription")

# With options
consumer = client.subscribe("my-topic", "my-subscription", {
  subscription_type: :shared,      # :shared, :exclusive, or :failover (default: :shared)
  initial_position: :latest,       # :latest or :earliest (default: :latest)
  receiver_queue_size: 1000,       # Max messages to prefetch (default: 1000)
  name: "my-consumer"              # Consumer name (auto-generated if nil)
})

# Receive messages (blocking)
message = consumer.receive

# Receive with timeout (returns nil on timeout)
message = consumer.receive(timeout: 10)

# Process the message
if message
  puts "Message ID: #{message.message_id}"
  puts "Payload: #{message.data}"
  puts "Producer: #{message.producer_name}"
  puts "Publish Time: #{message.publish_time_as_time}"
  puts "Properties: #{message.properties}"
  puts "Key: #{message.key}" if message.key
  puts "Redelivered: #{message.redelivered?}"

  # Acknowledge the message
  consumer.acknowledge(message)
  # or use the alias
  consumer.ack(message)
end

# Close when done
consumer.close
```

### Subscription Types

- **`:shared`** - Multiple consumers can attach to the same subscription. Messages are distributed round-robin across consumers.

[//]: # (- **`:exclusive`** - Only one consumer can attach to the subscription. Other consumers will fail.)
[//]: # (- **`:failover`** - Multiple consumers can attach, but only one receives messages. Others take over on failure.)

### Initial Position

- **`:latest`** - Start consuming from the latest message (default)
- **`:earliest`** - Start consuming from the earliest available message

### Topic Names

Topics can be specified in short form or fully qualified:

```ruby
# Short form (uses default tenant/namespace: public/default)
producer = client.create_producer("my-topic")

# Fully qualified
producer = client.create_producer("persistent://my-tenant/my-namespace/my-topic")
```

### Error Handling

```ruby
begin
  client = Pulsar::Client.new("pulsar://localhost:6650")
  producer = client.create_producer("my-topic")
  producer.send(Pulsar::ProducerMessage.new(payload: "Hello!"))
rescue Pulsar::ConnectionError => e
  puts "Connection failed: #{e.message}"
rescue Pulsar::TimeoutError => e
  puts "Operation timed out: #{e.message}"
rescue Pulsar::ProducerError => e
  puts "Producer error: #{e.message}"
rescue Pulsar::ConsumerError => e
  puts "Consumer error: #{e.message}"
rescue Pulsar::Error => e
  puts "Pulsar error: #{e.message}"
ensure
  client&.close
end
```

### Thread Safety

The client, producers, and consumers are thread-safe. You can share a single client across multiple threads and create producers/consumers from different threads.

```ruby
client = Pulsar::Client.new("pulsar://localhost:6650")

threads = []

# Producer thread
threads << Thread.new do
  producer = client.create_producer("my-topic")
  10.times do |i|
    producer.send(Pulsar::ProducerMessage.new(payload: "Message #{i}"))
  end
  producer.close
end

# Consumer threads
2.times do
  threads << Thread.new do
    consumer = client.subscribe("my-topic", "shared-sub", {
      subscription_type: :shared
    })
    loop do
      message = consumer.receive(timeout: 5)
      break unless message
      puts "Received: #{message.data}"
      consumer.ack(message)
    end
    consumer.close
  end
end

threads.each(&:join)
client.close
```

[//]: # (## API Reference)

[//]: # ()
[//]: # (### Pulsar::Client)

[//]: # ()
[//]: # (| Method | Description |)

[//]: # (|--------|-------------|)

[//]: # (| `new&#40;service_url, options = {}&#41;` | Create a new client |)

[//]: # (| `create_producer&#40;topic, options = {}&#41;` | Create a producer for a topic |)

[//]: # (| `subscribe&#40;topic, subscription, options = {}&#41;` | Subscribe to a topic |)

[//]: # (| `close` | Close all producers, consumers, and connections |)

[//]: # (| `closed?` | Check if the client is closed |)

[//]: # ()
[//]: # (### Pulsar::Producer)

[//]: # ()
[//]: # (| Method | Description |)

[//]: # (|--------|-------------|)

[//]: # (| `send&#40;message&#41;` | Send a message synchronously, returns MessageId |)

[//]: # (| `close` | Close the producer |)

[//]: # (| `closed?` | Check if the producer is closed |)

[//]: # (| `ready?` | Check if the producer is ready |)

[//]: # (| `topic` | Get the topic name |)

[//]: # (| `name` | Get the producer name |)

[//]: # ()
[//]: # (### Pulsar::Consumer)

[//]: # ()
[//]: # (| Method | Description |)

[//]: # (|--------|-------------|)

[//]: # (| `receive&#40;timeout: nil&#41;` | Receive a message &#40;blocking or with timeout&#41; |)

[//]: # (| `acknowledge&#40;message&#41;` / `ack&#40;message&#41;` | Acknowledge a message |)

[//]: # (| `acknowledge_id&#40;message_id&#41;` / `ack_id&#40;message_id&#41;` | Acknowledge by message ID |)

[//]: # (| `close` | Close the consumer |)

[//]: # (| `closed?` | Check if the consumer is closed |)

[//]: # (| `ready?` | Check if the consumer is ready |)

[//]: # (| `topic` | Get the topic name |)

[//]: # (| `subscription` | Get the subscription name |)

[//]: # ()
[//]: # (### Pulsar::ProducerMessage)

[//]: # ()
[//]: # (| Attribute | Description |)

[//]: # (|-----------|-------------|)

[//]: # (| `payload` | Message payload &#40;String&#41; |)

[//]: # (| `key` | Partition key &#40;optional&#41; |)

[//]: # (| `properties` | Message properties &#40;Hash&#41; |)

[//]: # (| `event_time` | Event time &#40;Time or Integer milliseconds&#41; |)

[//]: # ()
[//]: # (### Pulsar::Message)

[//]: # ()
[//]: # (| Attribute | Description |)

[//]: # (|-----------|-------------|)

[//]: # (| `message_id` | Unique message identifier |)

[//]: # (| `data` / `payload` | Message payload |)

[//]: # (| `key` | Partition key |)

[//]: # (| `properties` | Message properties |)

[//]: # (| `producer_name` | Name of the producer |)

[//]: # (| `publish_time` | Publish time in milliseconds |)

[//]: # (| `publish_time_as_time` | Publish time as Time object |)

[//]: # (| `event_time` | Event time in milliseconds |)

[//]: # (| `event_time_as_time` | Event time as Time object |)

[//]: # (| `topic` | Topic name |)

[//]: # (| `redelivery_count` | Number of redeliveries |)

[//]: # (| `redelivered?` | Whether the message was redelivered |)

## Current Limitations

- Negative acknowledgment (nack) is not yet implemented
- TLS/SSL connections are not yet supported (only `pulsar://` URLs)
- Async send is not yet exposed (internally used, but API is sync-only)
- Reader API is not yet implemented
- Batch message sending is not yet supported
- Schema support is not yet implemented

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`.

### Generating Protobuf

```bash
bundle exec rake proto
```

### Running Examples

Start a Pulsar standalone cluster:

```bash
docker run -it -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:latest bin/pulsar standalone
```

Run the example:

```bash
bundle exec ruby examples/produce_and_consume.rb
```

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/thomasli/pulsar-client-rb.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
