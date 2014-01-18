require 'poseidon_cluster'
require 'rspec'
require 'fileutils'
require 'pathname'

TOPIC_NAME  = "my-topic"
KAFKA_LOCAL = File.expand_path("../kafka_2.8.0-0.8.0", __FILE__)
KAFKA_ROOT  = Pathname.new(ENV["KAFKA_ROOT"] || KAFKA_LOCAL)

module Poseidon::SpecHelper

  def wait_for(&truth)
    100.times do
      break if truth.call
      sleep(0.01)
    end
  end

end

RSpec.configure do |c|
  c.include Poseidon::SpecHelper
  c.filter_run_excluding slow: true unless ENV["SLOW"] == "1"

  c.before :suite do
    kafka_bin = KAFKA_ROOT.join("bin", "kafka-server-start.sh")
    kafka_cfg = KAFKA_ROOT.join("config", "server-poseidon.properties")
    zookp_bin = KAFKA_ROOT.join("bin", "zookeeper-server-start.sh")
    zookp_cfg = KAFKA_ROOT.join("config", "zookeeper-poseidon.properties")

    if KAFKA_ROOT.to_s == KAFKA_LOCAL && !kafka_bin.file?
      puts "---> Downloading Kafka"
      target = Pathname.new(File.expand_path("../", __FILE__))
      system("cd #{target} && curl http://www.us.apache.org/dist/kafka/0.8.0/kafka_2.8.0-0.8.0.tar.gz | tar xz") ||
        raise("Unable to download Kafka")

      kafka_cfg.open("w") do |f|
        f.write KAFKA_ROOT.join("config", "server.properties").read.sub("=9092", "=29092").sub(":2181", ":22181").sub("/tmp/kafka-logs", "/tmp/kafka-logs-poseidon")
      end
      zookp_cfg.open("w") do |f|
        f.write KAFKA_ROOT.join("config", "zookeeper.properties").read.sub("=2181", "=22181")
      end
    end

    # Ensure all required files are present
    [kafka_bin, zookp_bin, kafka_cfg, zookp_cfg].each do |path|
      raise "Unable to locate #{path}. File does not exist!" unless path.file?
    end

    # Start Zookeeper & Kafka
    $ZOOKP_PID = spawn zookp_bin.to_s, zookp_cfg.to_s, out: '/dev/null'
    $KAFKA_PID = spawn kafka_bin.to_s, kafka_cfg.to_s, out: '/dev/null'
  end

  c.after :suite do
    Process.kill :TERM, $KAFKA_PID if $KAFKA_PID
    Process.kill :TERM, $ZOOKP_PID if $ZOOKP_PID
    FileUtils.rm_rf "/tmp/kafka-logs-poseidon"
  end

end
