require 'socket'
require 'timeout'
require 'zk'
require 'poseidon'
require 'thread'

module Poseidon::Cluster
  MAX_INT32 = 0x7fffffff
  @@sem = Mutex.new
  @@inc = 0

  # @return [Integer] an incremented number
  # @api private
  def self.inc!
    @@sem.synchronize { @@inc += 1; @@inc = 1 if @@inc > MAX_INT32; @@inc }
  end

  # @return [String] an globally unique identifier
  # @api private
  def self.guid
    [::Socket.gethostname, ::Process.pid, ::Time.now.to_i, inc!].join("-")
  end

end

%w|consumer_group|.each do |name|
  require "poseidon/#{name}"
end
