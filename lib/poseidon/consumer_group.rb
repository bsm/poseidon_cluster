# A ConsumerGroup operates on all partitions of a single topic. The goal is to ensure
# each topic message is consumed only once, no matter of the number of consumer instances within
# a cluster, as described in: http://kafka.apache.org/documentation.html#distributionimpl.
#
# The ConsumerGroup internally creates multiple PartitionConsumer instances. It uses Zookkeper
# and follows a simple consumer rebalancing algorithms which allows all the consumers
# in a group to come into consensus on which consumer is consuming which partitions. Each
# ConsumerGroup can 'claim' 0-n partitions and will consume their messages until another
# ConsumerGroup instance joins or leaves the cluster.
#
# Please note: ConsumerGroups themselves don't implement any threading or concurrency.
# When consuming messages, they simply round-robin across the claimed partitions. If you wish
# to parallelize consumption simply create multiple ConsumerGroups instances. The built-in
# concensus algorithm will automatically rebalance the available partitions between them and you
# can then decide for yourself if you want to run them in multiple thread or processes, ideally
# on multiple boxes.
#
# Unlike stated in the Kafka documentation, consumer rebalancing is *only* triggered on each
# addition or removal of consumers within the same group, while the addition of broker nodes
# and/or partition *does currently not trigger* a rebalancing cycle.
#
# @api public
class Poseidon::ConsumerGroup
  DEFAULT_CLAIM_TIMEOUT = 10

  # Poseidon::ConsumerGroup::Consumer is internally used by Poseidon::ConsumerGroup.
  # Don't invoke it directly.
  #
  # @api private
  class Consumer < ::Poseidon::PartitionConsumer

    # @attr_reader [Integer] partition Consumer partition
    attr_reader :partition

    # @api private
    def initialize(group, partition, options = {})
      broker = group.leader(partition)
      offset = group.offset(partition)
      super group.id, broker.host, broker.port, group.topic, partition, offset, options
    end

  end

  # @attr_reader [String] name Group name
  attr_reader :name

  # @attr_reader [String] topic Topic name
  attr_reader :topic

  # @attr_reader [Poseidon::BrokerPool] pool Broker pool
  attr_reader :pool

  # @attr_reader [ZK::Client] zk Zookeeper client
  attr_reader :zk

  # @attr_reader [Hash] options Consumer options
  attr_reader :options

  # Create a new consumer group, which processes all partition of the specified topic.
  #
  # @param [String] name Group name
  # @param [Array<String>] brokers A list of known brokers, e.g. ["localhost:9092"]
  # @param [Array<String>] zookeepers A list of known zookeepers, e.g. ["localhost:2181"]
  # @param [String] topic Topic to operate on
  # @param [Hash] options Partition consumer options, see Poseidon::PartitionConsumer#initialize
  #
  # @api public
  def initialize(name, brokers, zookeepers, topic, options = {})
    @name      = name
    @topic     = topic
    @zk        = ::ZK.new(zookeepers.join(","))
    @options   = options
    @consumers = []
    @pool      = ::Poseidon::BrokerPool.new(id, brokers)
    @mutex     = Mutex.new

    # Register instance
    registries.each do |_, path|
      zk.mkdir_p(path)
    end
    zk.create(consumer_path, "{}", ephemeral: true)
    zk.register(registries[:consumer]) {|_| rebalance! }

    # Rebalance
    rebalance!
  end

  # @return [String] a globally unique identifier
  def id
    @id ||= [name, ::Socket.gethostname, ::Process.pid, ::Time.now.to_i, ::Poseidon::Cluster.inc!].join("-")
  end

  # @return [Hash<Symbol,String>] registry paths
  def registries
    @registries ||= {
      consumer: "/consumers/#{name}/ids",
      owner:    "/consumers/#{name}/owners/#{topic}",
      offset:   "/consumers/#{name}/offsets/#{topic}",
    }
  end

  # @return [Poseidon::ClusterMetadata] cluster metadata
  def metadata
    @metadata ||= Poseidon::ClusterMetadata.new.tap {|m| m.update pool.fetch_metadata([topic]) }
  end

  # @return [Poseidon::TopicMetadata] topic metadata
  def topic_metadata
    @topic_metadata ||= metadata.metadata_for_topics([topic])[topic]
  end

  # Reloads metadata/broker/partition information
  def reload
    @metadata = @topic_metadata = nil
    metadata
    self
  end

  # Closes the consumer group gracefully, only really useful in tests
  # @api private
  def close
    @mutex.synchronize do
      release_all!
      zk.close
    end
  end

  # @param [Integer] partition
  # @return [Poseidon::Protocol::Broker] the leader for the given partition
  def leader(partition)
    metadata.lead_broker_for_partition(topic, partition)
  end

  # @param [Integer] partition
  # @return [Integer] the latest stored offset for the given partition
  def offset(partition)
    data, _ = zk.get offset_path(partition), ignore: :no_node
    data.to_i
  end

  # Commits the latest offset for a partition
  # @param [Integer] partition
  # @param [Integer] offset
  def commit(partition, offset)
    zk.set offset_path(partition), offset.to_s
  rescue ZK::Exceptions::NoNode
    zk.create offset_path(partition), offset.to_s, ignore: :node_exists
  end

  # Sorted partitions by broker address (so partitions on the same broker are clustered together)
  # @return [Array<Poseidon::Protocol::PartitionMetadata>] sorted partitions
  def partitions
    topic_metadata.partitions.sort_by do |part|
      broker = metadata.brokers[part.leader]
      [broker.host, broker.port].join(":")
    end
  end

  # Partitions currently claimed and consumed by this group instance
  # @return [Array<Integer>] partition IDs
  def claimed
    @consumers.map(&:partition).sort
  end

  # Checks out a single partition consumer. Round-robins between claimed partitions.
  #
  # @yield [consumer] The processing block
  # @yieldparam [Consumer] consumer The consumer instance
  # @yieldreturn [Boolean] return false to stop auto-commit
  #
  # @param [Hash] opts
  # @option opts [Boolean] :commit Automatically commit consumer offset (default: true)
  #
  # @api public
  def checkout(opts = {})
    @mutex.synchronize do
      consumer = @consumers.shift
      break unless consumer

      @consumers.push(consumer)
      result = yield(consumer)

      unless opts[:commit] == false || result == false
        commit consumer.partition, consumer.offset
      end
    end
    nil
  end

  # Convenience method to fetch messages from the broker.
  # Round-robins between claimed partitions.
  #
  # @yield [partition, messages] The processing block
  # @yieldparam [Integer] partition The source partition
  # @yieldparam [Array<Message>] messages The fetched messages
  # @yieldreturn [Boolean] return false to stop commit
  #
  # @param [Hash] opts
  # @option opts [Boolean] :commit Automatically commit consumed offset (default: true)
  #
  # @api public
  def fetch(opts = {})
    checkout(opts) do |consumer|
      yield consumer.partition, consumer.fetch
    end
  end

  protected

    # Rebalance algorithm:
    #
    # * let CG be all consumers in the same group that consume topic T
    # * let PT be all partitions producing topic T
    # * sort CG
    # * sort PT (so partitions on the same broker are clustered together)
    # * let POS be our index position in CG and let N = size(PT)/size(CG)
    # * assign partitions from POS*N to (POS+1)*N-1
    def rebalance!
      @mutex.synchronize do
        reload
        cg  = zk.children(registries[:consumer], watch: true).sort
        pt  = partitions
        pos = cg.index(id)
        n   = pt.size / cg.size
        n   = 1 if n < 1

        first = pos*n
        last  = (pos+1)*n-1

        release_all!
        (pt[first..last] || []).each do |part|
          consumer = claim!(part.id)
          @consumers.push(consumer)
        end
      end
    end

    # Release all consumer claims
    def release_all!
      @consumers.each {|c| release!(c.partition) }
      @consumers.clear
    end

    # Claim the ownership of the partition for this consumer
    # @raise [Timeout::Error]
    def claim!(partition)
      path = claim_path(partition)
      Timeout.timeout(options[:claim_timout] || DEFAULT_CLAIM_TIMEOUT) do
        sleep(0.01) while zk.create(path, id, ephemeral: true, ignore: :node_exists).nil?
      end
      Consumer.new(self, partition, options.dup)
    end

    # Release ownership of the partition
    def release!(partition)
      zk.delete claim_path(partition), ignore: :no_node
    end

  private

    # @return [String] zookeeper ownership claim path
    def claim_path(partition)
      "#{registries[:owner]}/#{partition}"
    end

    # @return [String] zookeeper offset storage path
    def offset_path(partition)
      "#{registries[:offset]}/#{partition}"
    end

    # @return [String] zookeeper consumer registration path
    def consumer_path
      "#{registries[:consumer]}/#{id}"
    end

end
