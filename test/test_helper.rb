require "test/unit"
$:.unshift(File.dirname(__FILE__) + '/../lib')
require "vertica"
 
class Test::Unit::TestCase

  TEST_CONNECTION_USER     = 'dbadmin'
  TEST_CONNECTION_PASSWORD = 'clitheur'
  TEST_CONNECTION_HOST     = 'ec2-174-129-157-242.compute-1.amazonaws.com'
  TEST_CONNECTION_PORT     = 5433
  TEST_CONNECTION_DATABASE = 'db'
  
  TEST_CONNECTION_HASH = {
    :user     => TEST_CONNECTION_USER,
    :password => TEST_CONNECTION_PASSWORD,
    :host     => TEST_CONNECTION_HOST,
    :port     => TEST_CONNECTION_PORT,
    :database => TEST_CONNECTION_DATABASE
  }

end

class StringIO
  include Vertica::BitHelper
end
