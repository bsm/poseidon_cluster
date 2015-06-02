Gem::Specification.new do |s|
  s.required_ruby_version = '>= 1.9.1'
  s.required_rubygems_version = ">= 1.8.0"

  s.name        = File.basename(__FILE__, '.gemspec')
  s.summary     = "Poseidon cluster extensions"
  s.description = "Cluster extensions for Poseidon, a producer and consumer implementation for Kafka >= 0.8"
  s.version     = "0.3.2"

  s.authors     = ["Black Square Media"]
  s.email       = "info@blacksquaremedia.com"
  s.homepage    = "https://github.com/bsm/poseidon_cluster"

  s.require_path = 'lib'
  s.files        = `git ls-files`.split("\n")
  s.test_files   = `git ls-files -- {test,spec,features,scenario}/*`.split("\n")

  s.add_dependency "poseidon", ">= 0.0.5", "<0.1.0"
  s.add_dependency "zk"

  s.add_development_dependency "rake"
  s.add_development_dependency "bundler"
  s.add_development_dependency "rspec"
  s.add_development_dependency "rspec-its"
  s.add_development_dependency "yard"
  s.add_development_dependency "coveralls"

end
