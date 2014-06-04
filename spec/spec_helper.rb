require 'poseidon_cluster'
require 'rspec'
require 'rspec/its'
require 'coveralls'
Coveralls.wear_merged!

RSpec.configure do |c|
  c.expect_with :rspec do |c|
    c.syntax = [:expect, :should]
  end
  c.mock_with :rspec do |c|
    c.syntax = [:expect, :should]
  end
end
