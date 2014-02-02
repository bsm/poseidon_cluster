require 'spec_helper'

describe Poseidon::ConsumerGroup do

  def new_group
    group = described_class.new "my-group", ["localhost:29092", "localhost:29091"], ["localhost:22181"], TOPIC_NAME
    groups.push(group)
    group
  end

  def fetch_response(n)
    set = Poseidon::MessageSet.new
    n.times {|i| set << Poseidon::Message.new(value: "value", key: "key", offset: i) }
    pfr = Poseidon::Protocol::PartitionFetchResponse.new(0, 0, 100, set)
    tfr = Poseidon::Protocol::TopicFetchResponse.new(TOPIC_NAME, [pfr])
    Poseidon::Protocol::FetchResponse.new(nil, [tfr])
  end

  let :groups do
    []
  end

  let :brokers do
    [ Poseidon::Protocol::Broker.new(1, "localhost", 29092),   # id,host,port
      Poseidon::Protocol::Broker.new(2, "localhost", 29091), ]
  end

  let :partitions do
    [ Poseidon::Protocol::PartitionMetadata.new(0, 0, 1, [1,2], []), # err,id,leader,replicas,isr
      Poseidon::Protocol::PartitionMetadata.new(0, 1, 2, [1,2], []), ]
  end

  let :topics do
    [ Poseidon::TopicMetadata.new(Poseidon::Protocol::TopicMetadataStruct.new(0, TOPIC_NAME, partitions)) ]
  end

  let :metadata do
    Poseidon::Protocol::MetadataResponse.new nil, brokers.dup, topics.dup
  end

  subject { new_group }
  before do
    Poseidon::ConsumerGroup.any_instance.stub(:sleep)
    Poseidon::BrokerPool.any_instance.stub(:fetch_metadata_from_broker).and_return(metadata)
    Poseidon::Connection.any_instance.stub(:fetch).with{|_, _, req| req[0].partition_fetches[0].partition == 0 }.and_return(fetch_response(10))
    Poseidon::Connection.any_instance.stub(:fetch).with{|_, _, req| req[0].partition_fetches[0].partition == 1 }.and_return(fetch_response(5))
  end
  after do
    subject.zk.rm_rf "/consumers/#{subject.name}"
    groups.each(&:close)
  end

  it               { should be_registered }
  its(:name)       { should == "my-group" }
  its(:topic)      { should == TOPIC_NAME }
  its(:pool)       { should be_instance_of(Poseidon::BrokerPool) }
  its(:id)         { should match(/\Amy-group\-[\w\-\.]+?\-\d{1,5}\-\d{10}\-\d{1,3}\z/) }
  its(:zk)         { should be_instance_of(ZK::Client::Threaded) }

  its(:claimed)        { should == [0, 1] }
  its(:metadata)       { should be_instance_of(Poseidon::ClusterMetadata) }
  its(:topic_metadata) { should be_instance_of(Poseidon::TopicMetadata) }
  its(:registries)     { should == {
    consumer: "/consumers/my-group/ids",
    owner:    "/consumers/my-group/owners/my-topic",
    offset:   "/consumers/my-group/offsets/my-topic",
  }}

  its("metadata.brokers.keys") { should =~ [1,2] }
  its("topic_metadata.partition_count") { should == 2 }

  it "should register with zookeeper" do
    subject.zk.children("/consumers/my-group/ids").should include(subject.id)
    stat = subject.zk.stat("/consumers/my-group/ids")
    stat.ephemeral_owner.should be(0)

    data, stat = subject.zk.get("/consumers/my-group/ids/#{subject.id}")
    data.should == "{}"
    stat.num_children.should == 0
    stat.ephemeral_owner.should > 0
  end

  it "should sort partitions by leader address" do
    subject.partitions.map(&:id).should == [1, 0]
  end

  it "should not fail if topic doesn't exist" do
    no_topics = Poseidon::Protocol::MetadataResponse.new nil, brokers.dup, []
    Poseidon::BrokerPool.any_instance.stub(:fetch_metadata_from_broker).and_return(no_topics)

    subject.partitions.should == []
    subject.claimed.should == []
  end

  it "should return the offset for each partition" do
    subject.offset(0).should == 0
    subject.offset(1).should == 0
    subject.offset(2).should == 0
    subject.fetch {|*| true }
    subject.offset(0).should == 0
    subject.offset(1).should == 5
    subject.offset(2).should == 0
  end

  it "should return the leader for a partition" do
    subject.leader(0).should == brokers[0]
    subject.leader(1).should == brokers[1]
    subject.leader(2).should be_nil
  end

  it "should checkout individual partition consumers (atomically)" do
    subject.checkout {|c| c.partition.should == 1 }.should be_true
    subject.checkout {|c| c.partition.should == 0 }.should be_true

    n = 0
    a = Thread.new do
      100.times { subject.checkout {|_| n+=1 } }
      Thread.pass
      100.times { subject.checkout {|_| n+=1 } }
    end
    b = Thread.new do
      100.times { subject.checkout {|_| n+=1 } }
      Thread.pass
      100.times { subject.checkout {|_| n+=1 } }
    end
    [a, b].each &:join
    n.should == 400
  end

  describe "rebalance" do

    it "should watch out for new consumers joining/leaving" do
      subject.should_receive(:rebalance!).twice.and_call_original
      new_group.should_receive(:rebalance!).once.and_call_original
      new_group
    end

    it "should distribute available partitions between consumers" do
      subject.claimed.should == [0, 1]

      b = new_group
      wait_for { subject.claimed.size > 0 }
      wait_for { b.claimed.size > 0 }
      subject.claimed.should == [1]
      b.claimed.should == [0]

      c = new_group
      b.close
      wait_for { b.claimed.size < 0 }
      wait_for { c.claimed.size > 0 }

      subject.claimed.should == [1]
      b.claimed.should == []
      c.claimed.should == [0]
    end

    it "should allocate partitions correctly" do
      subject.claimed.should == [0, 1]

      b = new_group
      wait_for { subject.claimed.size > 0 }
      wait_for { b.claimed.size > 0 }
      subject.claimed.should == [1]
      b.claimed.should == [0]

      c = new_group
      b.close
      wait_for { b.claimed.size < 0 }
      wait_for { c.claimed.size > 0 }

      subject.claimed.should == [1]
      b.claimed.should == []
      c.claimed.should == [0]
    end

  end

  describe "fetch" do

    it "should return messages from owned partitions" do
      subject.fetch do |n, msg|
        n.should == 1
        msg.size.should == 5
      end.should be_true

      subject.fetch do |n, msg|
        n.should == 0
        msg.size.should == 10
      end.should be_true

      subject.fetch do |n, msg|
        n.should == 1
        msg.size.should == 5
      end.should be_true
    end

    it "should auto-commit fetched offset" do
      -> {
        subject.fetch {|n, _| n.should == 1 }
      }.should change { subject.offset(1) }.from(0).to(5)
    end

    it "should skip auto-commits if requested" do
      -> {
        subject.fetch(commit: false) {|n, _| n.should == 1 }
      }.should_not change { subject.offset(1) }
    end

    it "should skip auto-commits if block results in false" do
      -> {
        subject.fetch {|n, _| n.should == 1; false }
      }.should_not change { subject.offset(1) }
    end

    it "should return false when trying to fetch messages without a claim" do
      no_topics = Poseidon::Protocol::MetadataResponse.new nil, brokers.dup, []
      Poseidon::BrokerPool.any_instance.stub fetch_metadata_from_broker: no_topics

      subject.claimed.should == []
      subject.fetch {|*|  }.should be_false
    end

    it "should return true even when no messages were fetched" do
      Poseidon::Connection.any_instance.stub fetch: fetch_response(0)
      subject.fetch {|*|  }.should be_true
    end

  end

  describe "fetch_loop" do

    it "should fetch indefinitely" do
      total, cycles = 0, 0
      subject.fetch_loop do |_, m|
        total += m.size
        break if (cycles+=1) > 2
      end
      total.should == 20
      cycles.should == 3
    end

    it "should delay fetch was unsuccessful" do
      subject.stub fetch: false

      cycles = 0
      subject.should_receive(:sleep).with(1)
      subject.fetch_loop do |n, m|
        n.should == -1
        m.should == []
        break if (cycles+=1) > 1
      end
    end

    it "should delay fetch didn't yield any results" do
      subject.stub(:fetch).and_yield(3, []).and_return(true)

      cycles = 0
      subject.should_receive(:sleep).with(1)
      subject.fetch_loop do |n, m|
        n.should == 3
        m.should == []
        break if (cycles+=1) > 1
      end
    end

  end

  describe "pick" do

    { [3, ["N1", "N2", "N3"], "N1"]       => (0..0),
      [3, ["N1", "N2", "N3"], "N2"]       => (1..1),
      [3, ["N1", "N2", "N3"], "N3"]       => (2..2),
      [4, ["N2", "N4", "N3", "N1"], "N3"] => (2..2),
      [3, ["N1", "N2", "N3"], "N4"]       => nil,
      [5, ["N1", "N2", "N3"], "N1"]       => (0..1),
      [5, ["N1", "N2", "N3"], "N2"]       => (2..3),
      [5, ["N1", "N2", "N3"], "N3"]       => (4..4),
      [5, ["N1", "N2", "N3"], "N4"]       => nil,
      [2, ["N1", "N2"], "N9"]             => nil,
      [1, ["N1", "N2", "N3"], "N1"]       => (0..0),
      [1, ["N1", "N2", "N3"], "N2"]       => nil,
      [1, ["N1", "N2", "N3"], "N3"]       => nil,
      [5, ["N1", "N2"], "N1"]             => (0..2),
      [5, ["N1", "N2"], "N2"]             => (3..4),
    }.each do |args, expected|
      it "should pick #{expected.inspect} from #{args.inspect}" do
        described_class.pick(*args).should == expected
      end
    end

  end
end
