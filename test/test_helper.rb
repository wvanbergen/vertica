require "test/unit"

$:.unshift(File.dirname(__FILE__) + '/../lib')
require "vertica"

class Test::Unit::TestCase

  TEST_CONNECTION_HASH = {
    :user     => '',
    :password => '',
    :host     => '',
    :port     => '',
    :database => ''
  }

end
