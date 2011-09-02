require 'test_helper'

class ConnectionTest < Test::Unit::TestCase
  
  def teardown
    @connection.close if @connection
  end
  
  def test_new_connection
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)

    assert !@connection.parameters.empty?
    assert @connection.backend_pid
    assert @connection.backend_key
    assert @connection.transaction_status
    assert @connection.opened?
    assert !@connection.closed?
    
    # parameters
    assert @connection.parameters.kind_of?(Hash)
    assert @connection.parameters.include?('server_version')
    
    assert_equal [], @connection.notifications
  end
  
  def test_close_connection
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    @connection.close

    assert !@connection.opened?
    assert @connection.closed?
    assert_equal({}, @connection.parameters)
    assert_nil @connection.backend_pid
    assert_nil @connection.backend_key
    assert_nil @connection.transaction_status
  end
  
  def test_connection_with_ssl
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH.merge(:ssl => true))

    assert @connection.ssl?
    assert !@connection.parameters.empty?
    assert @connection.backend_pid
    assert @connection.backend_key
    assert @connection.transaction_status

    @connection.close
    
    assert_equal({}, @connection.parameters)
    assert_nil @connection.backend_pid
    assert_nil @connection.backend_key
    assert_nil @connection.transaction_status
  end

  def test_reset
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    assert !@connection.parameters.empty?
    assert @connection.backend_pid
    assert @connection.backend_key
    assert @connection.transaction_status
    @connection.reset
    assert_equal({}, @connection.parameters)
    assert_nil @connection.backend_pid
    assert_nil @connection.backend_key
    assert_nil @connection.transaction_status
  end

  def test_new_with_error_response
    assert_raises Vertica::Error::MessageError do
      Vertica::Connection.new(TEST_CONNECTION_HASH.merge(:database => 'nonexistant_db'))
    end
  end
end
