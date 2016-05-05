# -*- encoding: utf-8 -*-

lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'vertica/version'

Gem::Specification.new do |s|
  s.name        = "vertica"
  s.summary     = "Pure Ruby library for interacting with Vertica"
  s.description = "Query Vertica with ruby"
  s.homepage    = "https://github.com/wvanbergen/vertica"
  s.license     = "MIT"
  s.version     = Vertica::VERSION

  s.authors = ["Jeff Smick", "Matt Bauer", "Willem van Bergen"]
  s.email   = ["sprsquish@gmail.com", "matt@ciderapps.com", "willem@railsdoctors.com"]

  s.extra_rdoc_files = ["README.md"]
  s.require_paths = ["lib"]

  s.files = `git ls-files`.split($/)
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  s.add_development_dependency 'rake'
  s.add_development_dependency 'yard'
  s.add_development_dependency 'minitest', '~> 5'
end
