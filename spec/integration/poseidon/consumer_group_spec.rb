require 'spec_helper'

describe Poseidon::ConsumerGroup, integration: true do

  def new_group(max_bytes = 1024*8)
    described_class.new "my-group", ["localhost:29092"], ["localhost:22181"], TOPIC_NAME, max_bytes: max_bytes
  end

  subject { new_group }
  after   { subject.zk.rm_rf "/consumers/#{subject.name}" }

  let(:consumed) { Hash.new(0) }

  def total
    consumed.values.inject(0, :+)
  end

  def stored_offsets
    { 0 => subject.offset(0), 1 => subject.offset(1) }
  end

  describe "small batches" do
    before { sleep 1 }

    it "should consume messages from all partitions" do
      subject.fetch {|n, msgs| consumed[n] += msgs.size }
      consumed.should == { 0 => 115 }
      subject.fetch {|n, msgs| consumed[n] += msgs.size }
      consumed.should == { 0 => 115, 1 => 115 }
      subject.fetch {|n, msgs| consumed[n] += msgs.size }
      consumed.should == { 0 => 230, 1 => 115 }
      subject.fetch {|n, msgs| consumed[n] += msgs.size }
      consumed.should == { 0 => 230, 1 => 230 }
      subject.fetch {|n, msgs| consumed[n] += msgs.size }
      consumed.should == { 0 => 336, 1 => 230 }
      subject.fetch {|n, msgs| consumed[n] += msgs.size }
      consumed.should == { 0 => 336, 1 => 340 }
      subject.fetch {|n, msgs| consumed[n] += msgs.size }
      consumed.should == { 0 => 336, 1 => 340 }
      stored_offsets.should == { 0 => 336, 1 => 340 }

      total.should == 676
    end

  end

  describe "large batches" do
    subject { new_group 1024 * 1024 * 10 }

    it "should consume messages from all partitions" do
      subject.fetch {|n, msgs| consumed[n] += msgs.size }
      consumed.should == { 0 => 336 }
      subject.fetch {|n, msgs| consumed[n] += msgs.size }
      consumed.should == { 0 => 336, 1 => 340 }
      subject.fetch {|n, msgs| consumed[n] += msgs.size }
      consumed.should == { 0 => 336, 1 => 340 }
      stored_offsets.should == { 0 => 336, 1 => 340 }

      total.should == 676
    end
  end

  describe "fuzzing" do
    subject       { new_group 512 }
    let!(:second) { new_group 512 }
    let!(:third)  { new_group 512 }
    let!(:fourth) { new_group 512 }

    def thread(group, count)
      Thread.new do
        sum = 0
        count.times do |i|
          group.fetch {|_, msgs| sum += msgs.size }
          sleep(rand / 100)
          Thread.pass
        end
        group.close unless count > 99
        sum
      end
    end

    it "should consume from multiple sources" do
      a = thread(subject, 100)
      b = thread(second, 3)
      c = thread(third, 6)
      d = thread(fourth, 100)

      vals = [a,b,c,d].map(&:value)
      vals.inject(0, :+).should == 676
    end

  end

end
