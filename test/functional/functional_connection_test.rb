require 'test_helper'

class FunctionalConnectionTest < Minitest::Test
  def test_opening_and_closing_connection
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    assert_valid_open_connection(connection)

    connection.close
    assert_valid_closed_connection(connection)
  end

  def test_connection_with_ssl
    connection = Vertica::Connection.new(ssl: true, **TEST_CONNECTION_HASH)
    assert_valid_open_connection(connection)
    assert connection.ssl?

    connection.close
    assert_valid_closed_connection(connection)
    assert !connection.ssl?

  rescue Vertica::Error::SSLNotSupported
    puts "\nThe test server doesn't support SSL, so SSL connections could not be tested."
  end

  def test_interruptable_connection
    connection = Vertica::Connection.new(interruptable: true, **TEST_CONNECTION_HASH)
    assert connection.interruptable?, "The connection should be interruptable!"
  end

  def test_interrupt_on_non_interruptable_connection
    connection = Vertica::Connection.new(interruptable: false, **TEST_CONNECTION_HASH)
    assert_raises(Vertica::Error::InterruptImpossible) { connection.interrupt }
  end

  def test_new_with_error_response
    assert_raises(Vertica::Error::ConnectionError) do
      Vertica::Connection.new(TEST_CONNECTION_HASH.merge(database: 'nonexistant_db'))
    end
  end

  def test_initialize_connection_with_search_path
    connection = Vertica::Connection.new(search_path: 'public', **TEST_CONNECTION_HASH)
    assert_equal "public, v_catalog, v_monitor, v_internal", connection_setting(connection, 'search_path')

    connection = Vertica::Connection.new(search_path: ['v_catalog', 'v_monitor', 'v_internal'], **TEST_CONNECTION_HASH)
    assert_equal "v_catalog, v_monitor, v_internal", connection_setting(connection, 'search_path')
  end

  def test_initialize_connection_with_role
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    available_roles = connection.query('show available roles').fetch(0, 'setting').split(', ')

    connection = Vertica::Connection.new(role: :none, **TEST_CONNECTION_HASH)
    enabled_roles = connection.query('show enabled roles').fetch(0, 'setting').split(', ')
    assert_equal [], enabled_roles

    connection = Vertica::Connection.new(role: :all, **TEST_CONNECTION_HASH)
    enabled_roles = connection.query('show enabled roles').fetch(0, 'setting').split(', ')
    assert_equal available_roles, enabled_roles
  end

  def test_initialize_connection_with_timezone
    connection = Vertica::Connection.new(timezone: 'America/Toronto', **TEST_CONNECTION_HASH)
    assert_equal 'America/Toronto', connection_setting(connection, 'timezone')
  end

  def test_connection_inspect_should_not_print_password
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    inspected_string = connection.inspect
    assert inspected_string !~ /:password=>#{TEST_CONNECTION_HASH[:password]}/
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
    assert_equal 1, connection.query("SELECT 1").the_value
  end

  def test_socket_connection_error
    TCPSocket.expects(:new).raises(Errno::ECONNREFUSED)

    assert_raises(Vertica::Error::ConnectionError) { Vertica::Connection.new(TEST_CONNECTION_HASH) }
  end

  def test_socket_read_error_during_initialization
    TCPSocket.any_instance.expects(:read_nonblock).raises(Errno::ETIMEDOUT)

    assert_raises(Vertica::Error::ConnectionError) { Vertica::Connection.new(TEST_CONNECTION_HASH) }
    end

  def test_socket_write_error_during_query
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)

    TCPSocket.any_instance.expects(:write_nonblock).raises(Errno::ETIMEDOUT)

    assert_raises(Vertica::Error::ConnectionError) { connection.query('select 1') }
    assert connection.closed?
  end

  def test_socket_read_error_during_query
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)

    TCPSocket.any_instance.expects(:read_nonblock).raises(Errno::ETIMEDOUT)

    assert_raises(Vertica::Error::ConnectionError) { connection.query('select 1') }
    assert connection.closed?
  end

  def test_concurrent_access
    connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    t = Thread.new { connection.query("SELECT sleep(1)") }
    sleep(0.1)

    assert connection.busy?, "The connection should be busy while executing a query"
    assert_raises(Vertica::Error::SynchronizeError) { connection.query('SELECT 1') }

    t.join
    assert connection.ready_for_query?, "The connection should be available again."
    connection.close
  end

  def test_autocommit_enabled
    connection = Vertica::Connection.new(autocommit: true, **TEST_CONNECTION_HASH)
    connection.query("DROP TABLE IF EXISTS test_ruby_vertica_autocommit_table CASCADE;")
    connection.query("CREATE TABLE test_ruby_vertica_autocommit_table (id int, name varchar(100))")
    connection.query("CREATE PROJECTION IF NOT EXISTS test_ruby_vertica_autoccommit_table_p (id, name) AS SELECT * FROM test_ruby_vertica_autocommit_table SEGMENTED BY HASH(id) ALL NODES OFFSET 1")
    connection.query("INSERT INTO test_ruby_vertica_autocommit_table VALUES (1, 'willem')")

    # The inserted record should be visible during the session in which the record was inserted.
    assert_equal 1, connection.query('SELECT COUNT(*) FROM test_ruby_vertica_autocommit_table').value

    connection.close

    # The inserted record should be persisted even after starting a new session, even without calling commit.
    connection = Vertica::Connection.new(autocommit: true, **TEST_CONNECTION_HASH)
    assert_equal 1, connection.query('SELECT COUNT(*) FROM test_ruby_vertica_autocommit_table').value
  end

  def test_user_instead_of_username_for_backwards_compatibility
    hash = TEST_CONNECTION_HASH.clone
    hash[:user] = hash.delete(:username)

    connection = Vertica::Connection.new(hash)
    assert_valid_open_connection(connection)
  end

  private

  def connection_setting(connection, setting)
    connection.query("SHOW ALL").detect { |row| row['name'] == setting }.fetch('setting')
  end

  def assert_valid_open_connection(connection)
    assert connection.opened?
    refute connection.closed?

    # connection variables
    assert connection.transaction_status

    # parameters
    assert connection.parameters.kind_of?(Hash)
    assert connection.parameters.include?('server_version')
  end

  def assert_valid_closed_connection(connection)
    refute connection.opened?
    assert connection.closed?
    assert_equal({}, connection.parameters)
    assert_nil connection.transaction_status
  end
end
