require 'spec_helper'

describe Poseidon::ConsumerGroup do

  def fetch_response(n)
    set = Poseidon::MessageSet.new
    n.times {|i| set << Poseidon::Message.new(value: "value", key: "key", offset: i) }
    pfr = Poseidon::Protocol::PartitionFetchResponse.new(0, 0, 100, set)
    tfr = Poseidon::Protocol::TopicFetchResponse.new("mytopic", [pfr])
    Poseidon::Protocol::FetchResponse.new(nil, [tfr])
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
    [ Poseidon::TopicMetadata.new(Poseidon::Protocol::TopicMetadataStruct.new(0, "mytopic", partitions)) ]
  end

  let :metadata do
    Poseidon::Protocol::MetadataResponse.new nil, brokers.dup, topics.dup
  end

  let :zk_client do
    double "ZK", mkdir_p: nil, get: nil, set: nil, delete: nil, create: "/path", register: nil, children: ["my-group-UNIQUEID"], close: nil
  end

  let(:group) { described_class.new "my-group", ["localhost:29092", "localhost:29091"], ["localhost:22181"], "mytopic" }
  subject     { group }

  before do
    allow(ZK).to receive_messages(new: zk_client)
    allow(Poseidon::Cluster).to receive_messages(guid: "UNIQUEID")
    allow_any_instance_of(Poseidon::ConsumerGroup).to receive(:sleep)
    allow_any_instance_of(Poseidon::PartitionConsumer).to receive_messages(resolve_offset_if_necessary: 0)
    allow_any_instance_of(Poseidon::BrokerPool).to receive_messages(fetch_metadata_from_broker: metadata)

    allow_any_instance_of(Poseidon::Connection).to receive(:fetch).with(10000, 1, ->req { req[0].partition_fetches[0].partition == 0 }).and_return(fetch_response(10))
    allow_any_instance_of(Poseidon::Connection).to receive(:fetch).with(10000, 1, ->req { req[0].partition_fetches[0].partition == 1 }).and_return(fetch_response(5))
  end

  it               { should be_registered }
  its(:name)       { should == "my-group" }
  its(:topic)      { should == "mytopic" }
  its(:pool)       { should be_instance_of(Poseidon::BrokerPool) }
  its(:id)         { should == "my-group-UNIQUEID" }
  its(:zk)         { should be(zk_client) }

  its(:claimed)        { should == [0, 1] }
  its(:metadata)       { should be_instance_of(Poseidon::ClusterMetadata) }
  its(:topic_metadata) { should be_instance_of(Poseidon::TopicMetadata) }
  its(:registries)     { should == {
    consumer: "/consumers/my-group/ids",
    owner:    "/consumers/my-group/owners/mytopic",
    offset:   "/consumers/my-group/offsets/mytopic",
  }}

  its("metadata.brokers.keys") { should =~ [1,2] }
  its("topic_metadata.partition_count") { should == 2 }

  it "should register with zookeeper and rebalance" do
    zk_client.should_receive(:mkdir_p).with("/consumers/my-group/ids")
    zk_client.should_receive(:mkdir_p).with("/consumers/my-group/owners/mytopic")
    zk_client.should_receive(:mkdir_p).with("/consumers/my-group/offsets/mytopic")
    zk_client.should_receive(:create).with("/consumers/my-group/ids/my-group-UNIQUEID", "{}", ephemeral: true)
    zk_client.should_receive(:register).with("/consumers/my-group/ids")
    described_class.any_instance.should_receive :rebalance!

    subject
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
    zk_client.should_receive(:get).with("/consumers/my-group/offsets/mytopic/0", ignore: :no_node).and_return([nil])
    subject.offset(0).should == 0

    zk_client.should_receive(:get).with("/consumers/my-group/offsets/mytopic/1", ignore: :no_node).and_return(["21", nil])
    subject.offset(1).should == 21

    zk_client.should_receive(:get).with("/consumers/my-group/offsets/mytopic/2", ignore: :no_node).and_return(["0", nil])
    subject.offset(2).should == 0
  end

  it "should return the leader for a partition" do
    subject.leader(0).should == brokers[0]
    subject.leader(1).should == brokers[1]
    subject.leader(2).should be_nil
  end

  it "should checkout individual partition consumers (atomically)" do
    subject.checkout {|c| c.partition.should == 1 }.should be_truthy
    subject.checkout {|c| c.partition.should == 0 }.should be_truthy

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

  describe "consumer" do
    subject { described_class::Consumer.new group, 1 }
    before  { group.stub(:offset).with(1).and_return(432) }

    it { should be_a(Poseidon::PartitionConsumer) }
    its(:offset) { should == 432 }

    it 'should start with the earliest offset if none stored' do
      group.unstub(:offset)
      subject.offset.should == :earliest_offset
    end

    it 'should start with the latest offset if none stored and in trailing mode' do
      group.unstub(:offset)
      trailing_consumer = described_class::Consumer.new group, 1, {trail: true}
      trailing_consumer.offset.should == :latest_offset
    end

  end

  describe "rebalance" do

    it "should watch out for new consumers joining/leaving" do
      described_class.any_instance.should_receive(:rebalance!)
      subject
    end

    it "should distribute available partitions between consumers" do
      subject.claimed.should == [0, 1]
      zk_client.stub children: ["my-group-UNIQUEID", "my-group-OTHERID"]
      -> { subject.send :rebalance! }.should change { subject.claimed }.to([0])
      zk_client.stub children: ["my-group-UNIQUEID", "my-group-OTHERID", "my-group-THIRDID"]
      -> { subject.send :rebalance! }.should change { subject.claimed }.to([])
    end

    it "should allocate partitions correctly" do
      subject.claimed.should == [0, 1]

      zk_client.stub children: ["my-group-UNIQUEID", "my-group-ZID"]
      zk_client.should_receive(:delete).with("/consumers/my-group/owners/mytopic/1", ignore: :no_node)
      -> { subject.send :rebalance! }.should change { subject.claimed }.to([1])

      zk_client.stub children: ["my-group-UNIQUEID", "my-group-ZID", "my-group-AID"]
      -> { subject.send :rebalance! }.should change { subject.claimed }.to([0])
    end

  end

  describe "fetch" do

    it "should return messages from claimed partitions" do
      subject.fetch do |n, msg|
        n.should == 1
        msg.size.should == 5
      end.should be_truthy

      subject.fetch do |n, msg|
        n.should == 0
        msg.size.should == 10
      end.should be_truthy

      subject.fetch do |n, msg|
        n.should == 1
        msg.size.should == 5
      end.should be_truthy
    end

    it "should auto-commit fetched offset" do
      zk_client.should_receive(:set).with("/consumers/my-group/offsets/mytopic/1", "5")
      subject.fetch {|n, _| n.should == 1 }
    end

    it "should skip auto-commits if requested" do
      zk_client.should_not_receive(:set)
      subject.fetch(commit: false) {|n, _| n.should == 1 }
    end

    it "should skip auto-commits if block results in false" do
      zk_client.should_not_receive(:set)
      subject.fetch {|n, _| n.should == 1; false }
    end

    it "should return false when trying to fetch messages without a claim" do
      no_topics = Poseidon::Protocol::MetadataResponse.new nil, brokers.dup, []
      Poseidon::BrokerPool.any_instance.stub fetch_metadata_from_broker: no_topics

      subject.claimed.should == []
      subject.fetch {|*|  }.should be_falsey
    end

    it "should return true even when no messages were fetched" do
      Poseidon::Connection.any_instance.stub fetch: fetch_response(0)
      subject.fetch {|*|  }.should be_truthy
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
