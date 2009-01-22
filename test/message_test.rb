require File.join(File.dirname(__FILE__), "test_helper")

class MessageTest < Test::Unit::TestCase  
  
  def test_startup
    s = StringIO.new
    Vertica::Messages::Startup.new('dbadmin', 'db').to_bytes(s)
    assert_equal %Q[\000\000\000"\000\003\000\000user\000dbadmin\000database\000db\000\000], s.string
  end

  def test_query
    s = StringIO.new
    Vertica::Messages::Query.new("SELECT * FROM USERS").to_bytes(s)
    assert_equal %Q[Q\000\000\000\030SELECT * FROM USERS\000], s.string
  end

end
