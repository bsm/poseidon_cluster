=begin

 PLEASE NOTE

 This example uses threads, but you could equally use fork or run your
 consumer groups from completely separate process and from multiple machines.

=end
require 'poseidon_cluster'

# Create a consumer group
group1 = Poseidon::ConsumerGroup.new "my-group", ["host1:9092", "host2:9092"], ["host1:2181", "host2:2181"], "my-topic"

# Start consuming "my-topic" in a background thread
thread1 = Thread.new do
  group1.fetch_loop do |partition, messages|
    puts "Consumer #1 fetched #{messages.size} from #{partition}"
  end
end

# Create a second consumer group
group2 = Poseidon::ConsumerGroup.new "my-group", ["host1:9092", "host2:9092"], ["host1:2181", "host2:2181"], "my-topic"

# Now consuming all partitions of "my-topic" in parallel
thread2 = Thread.new do
  group2.fetch_loop do |partition, messages|
    puts "Consumer #2 fetched #{messages.size} from #{partition}"
  end
end

# Join threads, loop forever
[thread1, thread2].each(&:join)

