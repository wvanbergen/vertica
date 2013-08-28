require 'test_helper'

class ConnectionTest < Test::Unit::TestCase
  
  def teardown
    @connection.close if @connection
  end

  def assert_valid_open_connection(connection)
    assert connection.opened?
    assert !connection.closed?

    # connection variables
    assert connection.backend_pid
    assert connection.backend_key
    assert connection.transaction_status
    
    # parameters
    assert connection.parameters.kind_of?(Hash)
    assert connection.parameters.include?('server_version')
  end

  def assert_valid_closed_connection(connection)
    assert !connection.opened?
    assert connection.closed?
    assert_equal({}, connection.parameters)
    assert_nil connection.backend_pid
    assert_nil connection.backend_key
    assert_nil connection.transaction_status
  end

  def test_opening_and_closing_connection
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    assert_valid_open_connection(connection)

    connection.close
    assert_valid_closed_connection(connection)    
  end
  
  def test_connection_with_ssl
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH.merge(:ssl => true))
    assert_valid_open_connection(connection)
    assert connection.ssl?

    connection.close
    assert_valid_closed_connection(connection)
    assert !connection.ssl?
    
  rescue Vertica::Error::SSLNotSupported => e
    puts "\nThe test server doesn't support SSL, so SSL connections could not be tested."
  end

  def test_reset_connection
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    original_backend_pid, original_backend_key = connection.backend_pid, connection.backend_key

    connection.reset_connection

    assert_valid_open_connection(connection)
    assert_not_equal original_backend_pid, connection.backend_pid
    assert_not_equal original_backend_key, connection.backend_key
    assert_equal :no_transaction, connection.transaction_status
  end
  
  def test_interruptable_connection
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH.merge(:interruptable => true))
    assert connection.interruptable?, "The connection should be interruptable!"
  end

  def test_new_with_error_response
    assert_raises Vertica::Error::ConnectionError do
      Vertica::Connection.new(TEST_CONNECTION_HASH.merge('database' => 'nonexistant_db'))
    end
  end
  
  def test_connection_inspect_should_not_print_password
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    inspected_string = connection.inspect
    assert_no_match /:password=>#{TEST_CONNECTION_HASH[:password]}/, inspected_string
  end

  def test_connection_timed_out_error
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    connection.options[:read_timeout] = 0.01
    assert_raises(Vertica::Error::TimedOutError) {connection.query("SELECT SLEEP(1)")}
    assert connection.closed?
  end

  def test_automatically_reconnects
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    connection.close
    assert_equal(1, connection.query("SELECT 1").the_value)
  end

  def test_socket_write_error
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    class << connection.socket
      def write_nonblock(foo)
        raise Errno::ETIMEDOUT
      end
    end

    assert_raises(Vertica::Error::ConnectionError) {connection.query('select 1')}
    assert connection.closed?
  end

  def test_socket_read_error
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    class << connection.socket
      def read_nonblock(foo)
        raise Errno::ETIMEDOUT
      end
    end

    assert_raises(Vertica::Error::ConnectionError) {connection.query('select 1')}
    assert connection.closed?
  end

  def test_concurrent_access
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    t = Thread.new { connection.query("SELECT 1") }
    sleep(0.01)
    
    assert connection.busy?
    assert_raises(Vertica::Error::SynchronizeError) { connection.query('SELECT 1') }
    
    t.join
    assert connection.ready_for_query?
    connection.close
  end
end
