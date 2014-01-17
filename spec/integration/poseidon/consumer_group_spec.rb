require 'spec_helper'

describe Poseidon::ConsumerGroup, integration: true do

  def new_group(max_bytes = 1024*8, name = TOPIC_NAME)
    described_class.new "my-group", ["localhost:29092"], ["localhost:22181"], name, max_bytes: max_bytes
  end

  subject { new_group }
  after   { subject.zk.rm_rf "/consumers/#{subject.name}" }

  let(:consumed) { Hash.new(0) }

  def stored_offsets
    { 0 => subject.offset(0), 1 => subject.offset(1) }
  end

  describe "small batches" do

    it "should consume messages from all partitions" do
      5.times do
        subject.fetch {|n, msgs| consumed[n] += msgs.size }
      end
      consumed.values.inject(0, :+).should < 676

      5.times do
        subject.fetch {|n, msgs| consumed[n] += msgs.size }
      end
      consumed.keys.should =~ [0, 1]
      consumed.values.inject(0, :+).should == 676
      consumed.should == stored_offsets
    end

  end

  describe "large batches" do
    subject { new_group 1024 * 1024 * 10 }

    it "should consume messages from all partitions" do
      5.times do
        subject.fetch {|n, msgs| consumed[n] += msgs.size }
      end
      consumed.keys.should =~ [0, 1]
      consumed.values.inject(0, :+).should == 676
      consumed.should == stored_offsets
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

  describe "multi-process fuzzing", slow: true do
    before do
      producer = Poseidon::Producer.new(["localhost:29092"], "my-producer")
      payload  = "data" * 10
      100.times do
        messages = (0...1000).map do |i|
          Poseidon::MessageToSend.new("slow-topic", payload, i.to_s)
        end
        producer.send_messages(messages)
      end
    end

    it 'should consume correctly' do
      read, write = IO.pipe
      pid1 = fork do
        group = new_group(64*1024, "slow-topic")
        10.times do
          5.times { group.fetch {|_, m| write.write "1:#{m.size}\n" }}
          sleep(1)
        end
      end
      pid2 = fork do
        group = new_group(32*1024, "slow-topic")
        5.times do
          10.times { group.fetch {|_, m| write.write "2:#{m.size}\n" }}
          sleep(1)
        end
      end
      pid3 = fork do
        group = new_group(8*1024, "slow-topic")
        5.times do
          50.times { group.fetch {|_, m| write.write "3:#{m.size}\n" }}
        end
      end
      Process.wait(pid2)

      pid4 = fork do
        group = new_group(8*1024, "slow-topic")
        5.times do
          50.times { group.fetch {|_, m| write.write "4:#{m.size}\n" }}
        end
      end
      pid5 = fork do
        group = new_group(32*1024, "slow-topic")
        5.times do
          50.times { group.fetch {|_, m| write.write "5:#{m.size}\n" }}
          sleep(2)
        end
      end
      Process.wait(pid1)
      Process.wait(pid3)
      Process.wait(pid4)
      Process.wait(pid5)
      write.close
      raw = read.read
      read.close

      stats = raw.lines.inject(Hash.new(0)) do |res, line|
        pid, count = line.chomp.split(":")
        res[pid.to_i] += count.to_i
        res
      end
      stats.keys.size.should be_within(1).of(4)
      stats.values.inject(0, :+).should == 100_000
    end

  end
end
