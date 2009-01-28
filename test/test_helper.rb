require "test/unit"
$:.unshift(File.dirname(__FILE__) + '/../lib')
require "vertica"
 
class Test::Unit::TestCase
  
  TEST_CONNECTION_USER     = 'user'
  TEST_CONNECTION_PASSWORD = 'password'
  TEST_CONNECTION_HOST     = 'server'
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
