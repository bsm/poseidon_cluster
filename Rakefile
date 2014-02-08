require 'bundler/gem_tasks'

require 'rspec/core/rake_task'
RSpec::Core::RakeTask.new(:spec)

require 'yard'
require 'yard/rake/yardoc_task'
YARD::Rake::YardocTask.new

require 'coveralls/rake/task'
Coveralls::RakeTask.new
namespace :spec do
  task coveralls: [:spec, 'coveralls:push']
end

desc "Run full integration test scenario"
task :scenario do
  load File.expand_path("../scenario/run.rb", __FILE__)
end

task default: :spec
