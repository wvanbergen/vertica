require 'rubygems'
require 'bundler/setup'

require 'yaml'
require 'test/unit'

require 'vertica'

hash = YAML.load(File.read(File.expand_path("../connection.yml", __FILE__)))
TEST_CONNECTION_HASH = hash.inject(Hash.new) { |carry, (k, v)| carry[k.to_sym] = v; carry }
