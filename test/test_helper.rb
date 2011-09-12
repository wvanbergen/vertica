require 'rubygems'
require 'bundler/setup'

require 'yaml'
require 'test/unit'

require 'vertica'

TEST_CONNECTION_HASH = YAML.load(File.read(File.expand_path("../connection.yml", __FILE__)))
