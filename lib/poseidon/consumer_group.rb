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
  DEFAULT_CLAIM_TIMEOUT = 30
  DEFAULT_LOOP_DELAY = 1

  # Poseidon::ConsumerGroup::Consumer is internally used by Poseidon::ConsumerGroup.
  # Don't invoke it directly.
  #
  # @api private
  class Consumer < ::Poseidon::PartitionConsumer

    # @attr_reader [Integer] partition consumer partition
    attr_reader :partition

    # @api private
    def initialize(group, partition, options = {})
      broker = group.leader(partition)
      offset = group.offset(partition)
      offset = (options[:trail] ? :latest_offset : :earliest_offset) if offset == 0
      options.delete(:trail)
      super group.id, broker.host, broker.port, group.topic, partition, offset, options
    end

  end

  # @param [Integer] pnum number of partitions size
  # @param [Array<String>] cids consumer IDs
  # @param [String] id consumer ID
  # @return [Range, NilClass] selectable range, if any
  def self.pick(pnum, cids, id)
    cids = cids.sort
    pos  = cids.index(id)
    cid_cnt = cids.length
    return unless pos && pos < cid_cnt
    remainder  = pnum % cid_cnt
    step = secondary_step = pnum / cid_cnt
    even_step_parts = pnum
    until even_step_parts % cid_cnt == 0
      even_step_parts += 1
      step = even_step_parts / cid_cnt
      secondary_step = step - 1
    end
    secondary_step_start = remainder*step
    if pos < remainder
      frst = pos*step
      last = (pos+1)*step-1
    else
      new_pos = pos-remainder
      frst = secondary_step_start + new_pos*secondary_step
      last = secondary_step_start + (new_pos+1)*secondary_step-1
    end
    last = pnum-1 if last > pnum-1
    return if last < 0 || last < frst

    (frst..last)
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
  # @param [Hash] options Consumer options
  # @option options [Integer] :max_bytes Maximum number of bytes to fetch. Default: 1048576 (1MB)
  # @option options [Integer] :max_wait_ms How long to block until the server sends us data. Default: 100 (100ms)
  # @option options [Integer] :min_bytes Smallest amount of data the server should send us. Default: 0 (Send us data as soon as it is ready)
  # @option options [Integer] :claim_timeout Maximum number of seconds to wait for a partition claim. Default: 10
  # @option options [Integer] :loop_delay Number of seconds to delay the next fetch (in #fetch_loop) if nothing was returned. Default: 1
  # @option options [Integer] :socket_timeout_ms broker connection wait timeout in ms. Default: 10000
  # @option options [Boolean] :register Automatically register instance and start consuming. Default: true
  # @option options [Boolean] :trail Starts reading messages from the latest partitions offsets and skips 'old' messages . Default: false
  #
  # @api public
  def initialize(name, brokers, zookeepers, topic, options = {})
    @name       = name
    @topic      = topic
    @zk         = ::ZK.new(zookeepers.join(","))
    # Poseidon::BrokerPool doesn't provide default value for this option
    # Configuring default value like this isn't beautiful, though.. by kssminus
    options[:socket_timeout_ms] ||= 10000
    @options    = options
    @consumers  = []
    @pool       = ::Poseidon::BrokerPool.new(id, brokers, options[:socket_timeout_ms])
    @mutex      = Mutex.new
    @registered = false
    @logger = @options.delete(:logger) || Logger.new(STDOUT)
    @log_level = @options.delete(:log_level) || :debug

    register! unless options.delete(:register) == false
  end

  # @return [String] a globally unique identifier
  def id
    @id ||= [name, Poseidon::Cluster.guid].join("-")
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

  # @return [Boolean] true if registered
  def registered?
    @registered
  end

  # @return [Boolean] true if registration was successful, false if already registered
  def register!
    return false if registered?

    # Register instance
    registries.each do |_, path|
      zk.mkdir_p(path)
    end
    zk.create(consumer_path, "{}", ephemeral: true)
    zk.register(registries[:consumer]) {|_| rebalance! }

    # Rebalance
    rebalance!
    @registered = true
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
    @mutex.synchronize { release_all! }
    zk.close
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
    return [] unless topic_metadata

    topic_metadata.available_partitions.sort_by do |part|
      part.id
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
  # @return [Boolean] true if a consumer was checked out, false if none could be claimed
  #
  # @example
  #
  #   ok = group.checkout do |consumer|
  #     puts "Checked out consumer for partition #{consumer.partition}"
  #   end
  #   ok # => true if the block was run, false otherwise
  #
  # @api public
  def checkout(opts = {})
    consumer = nil
    commit   = @mutex.synchronize do
      @consumers.rotate!
      return false unless (consumer = @consumers.first)

      yield consumer
    end

    unless opts[:commit] == false || commit == false
      commit consumer.partition, consumer.offset
    end

    true
  end

  # Convenience method to fetch messages from the broker.
  # Round-robins between claimed partitions.
  #
  # @yield [partition, messages] The processing block
  # @yieldparam [Integer] partition The source partition
  # @yieldparam [Array<Message>] messages The fetched messages
  # @yieldreturn [Boolean] return false to prevent auto-commit
  #
  # @param [Hash] opts
  # @option opts [Boolean] :commit Automatically commit consumed offset (default: true)
  # @return [Boolean] true if messages were fetched, false if none could be claimed
  #
  # @example
  #
  #   ok = group.fetch do |n, messages|
  #     puts "Fetched #{messages.size} messages for partition #{n}"
  #   end
  #   ok # => true if the block was run, false otherwise
  #
  # @api public
  def fetch(opts = {})
    checkout(opts) do |consumer|
      yield consumer.partition, consumer.fetch
    end
  end

  # Initializes an infinite fetch loop. This method blocks!
  #
  # Will wait for `loop_delay` seconds after each failed fetch. This may happen when there is
  # no new data or when the consumer hasn't claimed any partitions.
  #
  # SPECIAL ATTENTION:
  # When 'breaking out' of the loop, you must do it before processing the messages, as the
  # the last offset will not be committed. Please see examples below.
  #
  # @yield [partition, messages] The processing block
  # @yieldparam [Integer] partition The source partition, may be -1 if no partitions are claimed
  # @yieldparam [Array<Message>] messages The fetched messages
  # @yieldreturn [Boolean] return false to prevent auto-commit
  #
  # @param [Hash] opts
  # @option opts [Boolean] :commit Automatically commit consumed offset (default: true)
  # @option opts [Boolean] :loop_delay Delay override in seconds after unsuccessful fetch.
  #
  # @example
  #
  #   group.fetch_loop do |n, messages|
  #     puts "Fetched #{messages.size} messages for partition #{n}"
  #   end
  #   puts "Done" # => this code is never reached
  #
  # @example Stopping the loop (wrong)
  #
  #   counts = Hash.new(0)
  #   group.fetch_loop do |n, messages|
  #     counts[n] += messages.size
  #     puts "Status: #{counts.inspect}"
  #     break if counts[0] > 100
  #   end
  #   puts "Result: #{counts.inspect}"
  #   puts "Offset: #{group.offset(0)}"
  #
  #   # Output:
  #   # Status: {0=>30}
  #   # Status: {0=>60}
  #   # Status: {0=>90}
  #   # Status: {0=>120}
  #   # Result: {0=>120}
  #   # Offset: 90      # => Last offset was not committed!
  #
  # @example Stopping the loop (correct)
  #
  #   counts = Hash.new(0)
  #   group.fetch_loop do |n, messages|
  #     break if counts[0] > 100
  #     counts[n] += messages.size
  #     puts "Status: #{counts.inspect}"
  #   end
  #   puts "Result: #{counts.inspect}"
  #   puts "Offset: #{group.offset(0)}"
  #
  #   # Output:
  #   # Status: {0=>30}
  #   # Status: {0=>60}
  #   # Status: {0=>90}
  #   # Status: {0=>120}
  #   # Result: {0=>120}
  #   # Offset: 120
  #
  # @api public
  def fetch_loop(opts = {})
    delay = opts[:loop_delay] || options[:loop_delay] || DEFAULT_LOOP_DELAY

    loop do
      mp = false
      ok = fetch(opts) do |n, messages|
        mp = !messages.empty?
        yield n, messages
      end

      # Yield over an empty array if nothing claimed,
      # to allow user to e.g. break out of the loop
      unless ok
        yield -1, []
      end

      # Sleep if either not claimes or nothing returned
      unless ok && mp
        sleep delay
      end
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
      return if @pending

      @pending = true
      @mutex.synchronize do
        @pending = nil

        reload

        ids = zk.children(registries[:consumer], watch: true)
        pms = partitions
        owned_range = self.class.pick(pms.size, ids, id)

        if owned_range
          owned_partitions = pms[owned_range].map(&:id)
          already_claimed_partitions, unclaimed_partitions = owned_partitions.partition { |p| claimed.include?(p) }

          partitions_to_release = claimed - already_claimed_partitions - unclaimed_partitions

          @logger.send(@log_level, "[Kafka::Consumer] keeping partitions - #{name} already_claimed_partitions: #{already_claimed_partitions}")
          @logger.send(@log_level, "[Kafka::Consumer] releasing partitions - #{name} partitions_to_release: #{partitions_to_release}")

          partitions_to_release.each do |c|
            release!(c)
          end

          @consumers.reject! do |c|
            if partitions_to_release.include?(c.partition)
              c = nil
              true
            end
          end

          @logger.send(@log_level, "[Kafka::Consumer] claiming partitions - #{name} unclaimed_partitions: #{unclaimed_partitions}")

          unclaimed_partitions.each { |p| claim!(p) }

          @logger.send(@log_level, "[Kafka::Consumer] rebalanced! - #{name} claimed: #{claimed}")
        else
          @logger.send(@log_level, "[Kafka::Consumer] rebalanced! - #{name} releasing all consumers: #{claimed}")
          release_all!
        end
      end
    end

    # Release all consumer claims
    def release_all!
      @consumers.each {|c| release!(c.partition) }
      @consumers.clear
    end

  private

    # Claim the ownership of the partition for this consumer
    def claim!(partition)
      return if claimed.include?(partition)

      path = claim_path(partition)

      begin
        zk.create(path, id, ephemeral: true)
        @consumers.push(Poseidon::ConsumerGroup::Consumer.new(self, partition, options.dup))
      rescue ZK::Exceptions::NodeExists
        node_subscription = zk.register(path) do |event|
          if event.node_deleted?
            if claim!(partition)
              @logger.send(@log_level, "[Kafka::Consumer] watch trigger - #{name}, claimed partition: #{partition}, claimed: #{claimed}")
            end
          end
        end

        unless zk.exists?(path, watch: true)
          node_subscription.unsubscribe
          claim!(partition)
        end

        nil
      end
    end


    # Release ownership of the partition
    def release!(partition)
      zk.delete claim_path(partition), ignore: :no_node
    end

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
