require 'test_helper'

class QueryTest < Test::Unit::TestCase
  
  def setup
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    @connection.query("CREATE TABLE IF NOT EXISTS test_table (id int, name varchar(100))")
    @connection.query("CREATE PROJECTION IF NOT EXISTS test_table_p (id, name) AS SELECT * FROM test_table SEGMENTED BY HASH(id) ALL NODES OFFSET 1")
    @connection.query("INSERT INTO test_table VALUES (1, 'matt')")
    @connection.query("COMMIT")
  end
  
  def teardown
    @connection.query("DROP TABLE IF EXISTS test_table CASCADE;")
    @connection.query("COMMIT")
    @connection.close
  end
  
  def test_select_query_with_results_as_hash
    r = @connection.query("SELECT * FROM test_table")
    assert_equal 1, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :integer, r.columns[0].data_type
    assert_equal :id, r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal :name, r.columns[1].name
    
    assert_equal [{:id => 1, :name => "matt"}], r.rows
  end
  
  def test_select_query_with_results_as_array
    @connection.row_style = :array
    r = @connection.query("SELECT * FROM test_table")
    assert_equal 1, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :integer, r.columns[0].data_type
    assert_equal :id, r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal :name, r.columns[1].name
    
    assert_equal [[1, "matt"]], r.rows
  end
  
  
  def test_select_query_with_no_results
    r = @connection.query("SELECT * FROM test_table WHERE 1 != 1")
    assert_equal 0, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :integer, r.columns[0].data_type
    assert_equal :id, r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal :name, r.columns[1].name
    assert_equal [], r.rows
  end
  
  def test_insert
    r = @connection.query("INSERT INTO test_table VALUES (2, 'stefanie')")
    assert_equal 1, r.row_count
    assert_equal 1, r.columns.length
    assert_equal :integer, r.columns[0].data_type
    assert_equal :OUTPUT, r.columns[0].name
    assert_equal [{:OUTPUT => 1}], r.rows
  end
  
  
  def test_delete_of_no_rows
    r = @connection.query("DELETE FROM test_table WHERE 1 != 1")
    assert_equal 1, r.row_count
    assert_equal 1, r.columns.length
    assert_equal :integer, r.columns[0].data_type
    assert_equal :OUTPUT, r.columns[0].name
    assert_equal [{:OUTPUT => 0}], r.rows
  end
  
  def test_delete_of_a_row
    r = @connection.query("DELETE FROM test_table WHERE id = 1")
    assert_equal 1, r.row_count
    assert_equal 1, r.columns.length
    assert_equal :integer, r.columns[0].data_type
    assert_equal :OUTPUT, r.columns[0].name
    assert_equal [{:OUTPUT => 1}], r.rows
  end
  
  def test_empty_query
    assert_raises Vertica::Error::QueryError do
      @connection.query("")
    end
    assert_raises Vertica::Error::QueryError do
      @connection.query(nil)
    end
    assert_raises Vertica::Error::QueryError do 
      @connection.query("-- just a SQL comment")
    end
  end
  
  def test_cleanup_after_select
    3.times do
      r = @connection.query("SELECT * FROM test_table")
      assert_equal 1, r.row_count
      assert_equal 2, r.columns.length
      assert_equal :integer, r.columns[0].data_type
      assert_equal :id, r.columns[0].name
      assert_equal :varchar, r.columns[1].data_type
      assert_equal :name, r.columns[1].name
      assert_equal [{:id => 1, :name => "matt"}], r.rows
    end
  end  
  
  def test_sql_error
    assert_raises Vertica::Error::QueryError do 
      @connection.query("SELECT * FROM nonexistingfdg")
    end
    assert_raises Vertica::Error::QueryError do 
      @connection.query("BLAH")
    end
  end
  
  def test_cancel
    Vertica::Connection.cancel(@connection)
  end
end
