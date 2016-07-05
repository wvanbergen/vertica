require 'bundler/setup'

require 'yaml'
require 'minitest/autorun'
require 'minitest/pride'
require 'mocha/mini_test'

require 'vertica'

TEST_CONNECTION_FILENAME = File.expand_path("../connection.yml", __FILE__)

if File.exist?(TEST_CONNECTION_FILENAME)
  TEST_CONNECTION_HASH = YAML.load(File.read(TEST_CONNECTION_FILENAME))
else
  puts
  puts "Create #{TEST_CONNECTION_FILENAME} with connection info "
  puts "for your Vertica test database in order to run the test suite."
  puts
  exit(1)
end
