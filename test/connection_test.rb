require File.join(File.dirname(__FILE__), "test_helper")

class ConnectionTest < Test::Unit::TestCase  
  
  def test_new
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    assert !c.parameters.empty?
    assert c.backend_pid
    assert c.backend_key
    assert c.transaction_status
    assert c.opened?
    assert !c.closed?
    assert_equal({"server_version" => "8.0"}, c.parameters)
    assert_equal([], c.notifications)
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
  
  def test_reset
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    assert !c.parameters.empty?
    assert c.backend_pid
    assert c.backend_key
    assert c.transaction_status
    c.reset
    assert_equal({}, c.parameters)
    assert_nil c.backend_pid
    assert_nil c.backend_key
    assert_nil c.transaction_status
    c.close
  end
  
  def test_new_with_error_response
    assert_raises Vertica::Error::MessageError do
      Vertica::Connection.new(TEST_CONNECTION_HASH.merge(:database => 'nonexistant_db'))
    end
  end
  
  def test_select_query_with_results
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    r = c.query("SELECT * FROM test_table")
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
    r = c.query("SELECT * FROM test_table WHERE 1 != 1")
    assert_equal 0, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal 'id', r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal 'name', r.columns[1].name
    assert_equal [], r.rows
    c.close
  end
  
  def test_delete_of_no_rows
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    r = c.query("DELETE FROM test_table WHERE 1 != 1")
    assert_equal 1, r.row_count
    assert_equal 1, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal 'OUTPUT', r.columns[0].name
    assert_equal [[0]], r.rows
    c.close
  end  
  
  def test_insert
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    r = c.query("INSERT INTO test_table VALUES (2, 'stefanie')")
    assert_equal 1, r.row_count
    assert_equal 1, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal 'OUTPUT', r.columns[0].name
    assert_equal [[1]], r.rows
    c.close
  end
  
  def test_delete_of_a_row
    test_insert
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    r = c.query("DELETE FROM test_table WHERE id = 2")
    assert_equal 1, r.row_count
    assert_equal 1, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal 'OUTPUT', r.columns[0].name
    assert_equal [[0]], r.rows
    c.close
  end  
    
  def test_empty_query
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    assert_raises ArgumentError do
      r = c.query("")
    end
    assert_raises ArgumentError do
      r = c.query(nil)
    end
    c.close
  end
  
  def test_cancel
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    Vertica::Connection.cancel(c)
    c.close
  end
  
  def test_prepared_statement_with_no_params
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    c.prepare("my_ps", "SELECT * FROM test_table")
    r = c.execute_prepared("my_ps")
    assert_equal 1, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal 'id', r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal 'name', r.columns[1].name
    assert_equal [[1, 'matt']], r.rows
    c.close    
  end
  
  def test_prepared_statement_with_one_param
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    c.prepare("my_ps", "SELECT * FROM test_table WHERE id = ?", 1)
    r = c.execute_prepared("my_ps", 1)
    assert_equal 1, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal 'id', r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal 'name', r.columns[1].name
    assert_equal [[1, 'matt']], r.rows
    c.close    
  end
  
  def test_prepared_statement_with_two_params
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    c.prepare("my_ps", "SELECT * FROM test_table WHERE id = ? OR id = ?", 2)
    r = c.execute_prepared("my_ps", 1, 3)
    assert_equal 1, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal 'id', r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal 'name', r.columns[1].name
    assert_equal [[1, 'matt']], r.rows
    c.close    
  end

  def test_double_select
    c = Vertica::Connection.new(TEST_CONNECTION_HASH)
    5.times do
      r = c.query("SELECT * FROM test_table")
      assert_equal 1, r.row_count
      assert_equal 2, r.columns.length
      assert_equal :in, r.columns[0].data_type
      assert_equal 'id', r.columns[0].name
      assert_equal :varchar, r.columns[1].data_type
      assert_equal 'name', r.columns[1].name
      assert_equal [[1, 'matt']], r.rows
    end
    c.close
  end

  # test parameters

end
