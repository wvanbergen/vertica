require File.join(File.dirname(__FILE__), "test_helper")

class ConnectionTest < Test::Unit::TestCase

  def test_new_with_args
    conn = Vertica::Connection.new('localhost', 5433, nil, 'db', 'dbadmin', 'clitheur')
    assert conn
    conn.close
  end

  def test_new_with_string
    conn = Vertica::Connection.new('user=dbadmin password=clitheur hostaddr=127.0.0.1 port=5433 dbname=db')
    assert conn
    conn.close
  end

  def test_new_async
    conn = Vertica::Connection.new('user=dbadmin password=clitheur hostaddr=127.0.0.1 port=5433 dbname=db', true)
    assert conn
    loop do
      break if conn.poll == :ok
    end
    conn.close
  end

  def test_reset
    conn = Vertica::Connection.new('localhost', 5433, nil, 'db', 'dbadmin', 'clitheur')
    assert conn
    conn.reset
    assert conn
    conn.close
  end

  def test_reset_async
    conn = Vertica::Connection.new('localhost', 5433, nil, 'db', 'dbadmin', 'clitheur')
    assert conn
    conn.reset(true)
    loop do
      break if conn.reset_poll == :ok
    end
    assert conn
    conn.close
  end

  def connection
    conn = Vertica::Connection.new('localhost', 5433, nil, 'db', 'dbadmin', 'clitheur')
    assert conn
    yield conn
    conn.close
  end

  def test_db
    connection { |c| assert_equal "db", c.db }
  end

  def test_user
    connection { |c| assert_equal "dbadmin", c.user }
  end

  def test_password
    connection { |c| assert_equal "clitheur", c.password }
  end

  def test_host
    connection { |c| assert_equal "localhost", c.host }
  end

  def test_port
    connection { |c| assert_equal "5433", c.port }
  end

  def test_options
    connection { |c| assert_equal "", c.options }
  end

  def test_status
    connection { |c| assert_equal :ok, c.status }
  end

  def test_transaction_status
    connection { |c| assert_equal :idle, c.transaction_status }
  end

  def test_parameter_status
    connection { |c| assert_equal "8.0", c.parameter_status("server_version") }
    connection { |c| assert_equal nil, c.parameter_status("is_superuser") }
  end

  def test_protocol_version
    connection { |c| assert_equal 3, c.protocol_version }
  end
  
  def test_server_version
    connection { |c| assert_equal 80000, c.server_version }
  end
  
  def test_error_message
    connection { |c| assert_equal "", c.error_message }
  end
  
  def test_socket
    connection { |c| assert c.socket > 0 }
  end
  
  def test_backend_pid
    connection { |c| assert c.backend_pid > 0 }
  end
  
  def test_ssl?
    connection { |c| assert !c.ssl? }
  end
  
end

