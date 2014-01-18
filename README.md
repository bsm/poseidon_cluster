# Poseidon Cluster [![Build Status](https://travis-ci.org/bsm/poseidon_cluster.png?branch=master)](https://travis-ci.org/bsm/poseidon_cluster) [![Coverage Status](https://coveralls.io/repos/bsm/poseidon_cluster/badge.png)](https://coveralls.io/r/bsm/poseidon_cluster)

Poseidon Cluster is a cluster extension the excellent [Poseidon](http://github.com/bpot/poseidon) Ruby client for Kafka 0.8+. It implements the distribution concept of self-rebalancing *Consumer Groups* and supports the consumption of a single topic from multiple instances.

Consumer group instances share a common group name, and each message published to a topic is delivered to one instance within each subscribing consumer group. Consumer instances can be in separate processes or on separate machines.

## Usage

Launch a consumer group:

```ruby
require 'poseidon_cluster'

consumer = Poseidon::ConsumerGroup.new(
            "my-group",                               # Group name
            ["kafka1.host:9092", "kafka2.host:9092"], # Kafka brokers
            ["kafka1.host:2181", "kafka2.host:2181"], # Zookeepers hosts
            "my-topic")                               # Topic name

consumer.partitions # => [0, 1, 2, 3] - all partitions of 'my-topic'
consumer.claimed    # => [0, 1] - partitions this instance has claimed
```

Fetch a bulk of messages, auto-commit the offset:

```ruby
consumer.fetch do |partition, bulk|
  bulk.each do |m|
    puts "Fetched '#{m.value}' at #{m.offset} from #{partition}"
  end
end
```

Get the offset for a partition:

```ruby
consumer.offset(0) # => 320 - current offset from partition 0
```

Fetch more messages, commit manually:

```ruby
consumer.fetch commit: false do |partition, bulk|
  bulk.each do |m|
    puts "Fetched '#{m.value}' at #{m.offset} from #{partition}"
  end

  consumer.commit partition, bulk.last.offset+1 unless bulk.empty?
end
```

Initiate a fetch-loop, consume indefinitely:

```ruby
consumer.fetch_loop do |partition, bulk|
  bulk.each do |m|
    puts "Fetched '#{m.value}' at #{m.offset} from #{partition}"
  end
end
```

For more details and information, please see the [Poseidon::ConsumerGroup](http://rubydoc.info/github/bsm/poseidon_cluster/Poseidon/ConsumerGroup) documentation and the [Examples](https://github.com/bsm/poseidon_cluster/tree/master/examples).

## Running Tests

The test suite will automatically download, configure and run Kafka locally, you only need a JRE. Run the suite via:

```bash
bundle exec rake spec
```

## Licence

```
Copyright (c) 2014 Black Square Media

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```
