require File.join(File.dirname(__FILE__), "test_helper")

class ConnectionTest < Test::Unit::TestCase

  def test_new_with_hash
    conn =  Vertica::Connection.new('localhost', 5433, nil, 'db', 'dbadmin', 'clitheur')
    assert conn
    conn.close
  end

  def test_reset
    conn =  Vertica::Connection.new('localhost', 5433, nil, 'db', 'dbadmin', 'clitheur')
    assert conn
    conn.reset
    assert conn
    conn.close
  end

end

