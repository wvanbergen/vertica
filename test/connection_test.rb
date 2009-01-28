require File.join(File.dirname(__FILE__), "test_helper")

class ConnectionTest < Test::Unit::TestCase  
  
  def test_new
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    assert !c.parameters.empty?
    assert c.backend_pid
    assert c.backend_key
    assert c.transaction_status
    c.close
    assert_equal({}, c.parameters)
    assert_nil c.backend_pid
    assert_nil c.backend_key
    assert_nil c.transaction_status
  end
  
  def test_new_with_ssl
    c = Vertica::Connection.new(TEST_CONNECTION_HASH.merge(:ssl => true))
    assert !c.parameters.empty?
    assert c.backend_pid
    assert c.backend_key
    assert c.transaction_status
    c.close
    assert_equal({}, c.parameters)
    assert_nil c.backend_pid
    assert_nil c.backend_key
    assert_nil c.transaction_status
  end
  
  def test_new_with_error_response
    assert_raises Vertica::Error::MessageError do
      Vertica::Connection.new(TEST_CONNECTION_HASH.merge(:database => 'nonexistant_db'))
    end
  end
  
  def test_select_query_with_results
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    r = c.query("SELECT * FROM USERS")
    assert_equal 1, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal 'id', r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal 'name', r.columns[1].name
    assert_equal [[1, 'matt']], r.rows
    c.close
  end
  
  def test_select_query_with_no_results
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    r = c.query("SELECT * FROM USERS WHERE 1 != 1")
    assert_equal 0, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal 'id', r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal 'name', r.columns[1].name
    assert_equal [], r.rows
    c.close
  end
  
  # test execute (delete)
  # test insert (get ROWID)
  
  def test_empty_query
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    r = c.query("")
    assert_equal 0, r.row_count
    assert_equal 0, r.columns.length
    assert_equal [], r.columns
    assert_equal [], r.rows
    c.close
  end

  # test cancel
  # test process message lst
  # test COPY IN/OUT
end
