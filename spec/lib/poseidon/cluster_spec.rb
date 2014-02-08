require 'spec_helper'

describe Poseidon::Cluster do

  it 'should generate incremented numbers (atomically)' do
    num = described_class.inc!
    (described_class.inc! - num).should == 1

    (0...5).map do
      Thread.new { 100.times { described_class.inc! }}
    end.each &:join
    (described_class.inc! - num).should == 502
  end

  it 'should generate GUIDs' do
    described_class.guid.should match(/\A[\w\-\.]+?\-\d{1,5}\-\d{10}\-\d{1,3}\z/)
  end

end
