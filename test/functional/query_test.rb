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
  
  def test_select_query_with_results
    r = @connection.query("SELECT * FROM test_table")
    assert_equal 1, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal :id, r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal :name, r.columns[1].name
    
    # assert_equal [[1, 'matt']], r.rows
    assert_equal [{:id => 1, :name => "matt"}], r.rows
  end
  
  def test_select_query_with_no_results
    r = @connection.query("SELECT * FROM test_table WHERE 1 != 1")
    assert_equal 0, r.row_count
    assert_equal 2, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal :id, r.columns[0].name
    assert_equal :varchar, r.columns[1].data_type
    assert_equal :name, r.columns[1].name
    assert_equal [], r.rows
  end
  
  def test_insert
    r = @connection.query("INSERT INTO test_table VALUES (2, 'stefanie')")
    assert_equal 1, r.row_count
    assert_equal 1, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal :OUTPUT, r.columns[0].name
    assert_equal [{:OUTPUT => 1}], r.rows
  end
  
  
  def test_delete_of_no_rows
    r = @connection.query("DELETE FROM test_table WHERE 1 != 1")
    assert_equal 1, r.row_count
    assert_equal 1, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal :OUTPUT, r.columns[0].name
    assert_equal [{:OUTPUT => 0}], r.rows
  end
  
  def test_delete_of_a_row
    r = @connection.query("DELETE FROM test_table WHERE id = 1")
    assert_equal 1, r.row_count
    assert_equal 1, r.columns.length
    assert_equal :in, r.columns[0].data_type
    assert_equal :OUTPUT, r.columns[0].name
    assert_equal [{:OUTPUT => 1}], r.rows
  end
  
  def test_empty_query
    assert_raises ArgumentError do
      @connection.query("")
    end
    assert_raises ArgumentError do
      @connection.query(nil)
    end
  end

  # FIXME: now hangs forever
  # def test_non_query
  #   @connection.query("--just a comment")
  # end
  
  def test_sql_error
    assert_raises Vertica::Error::MessageError do 
      @connection.query("SELECT * FROM nonexisting")
    end
    assert_raises Vertica::Error::MessageError do 
      @connection.query("BLAH")
    end
  end
  
  def test_cancel
    Vertica::Connection.cancel(@connection)
  end

  # def test_prepared_statement_with_no_params
  #   @connection.prepare("my_ps", "SELECT * FROM test_table")
  #   r = @connection.execute_prepared("my_ps")
  #   assert_equal 1, r.row_count
  #   assert_equal 2, r.columns.length
  #   assert_equal :in, r.columns[0].data_type
  #   assert_equal :id, r.columns[0].name
  #   assert_equal :varchar, r.columns[1].data_type
  #   assert_equal :name, r.columns[1].name
  #   assert_equal [{:id => 1, :name => "matt"}], r.rows
  # end
  #
  # def test_prepared_statement_with_one_param
  #   c = Vertica::Connection.new(TEST_CONNECTION_HASH)
  #   c.prepare("my_ps", "SELECT * FROM test_table WHERE id = ?", 1)
  #   r = c.execute_prepared("my_ps", 1)
  #   assert_equal 1, r.row_count
  #   assert_equal 2, r.columns.length
  #   assert_equal :in, r.columns[0].data_type
  #   assert_equal 'id', r.columns[0].name
  #   assert_equal :varchar, r.columns[1].data_type
  #   assert_equal 'name', r.columns[1].name
  #   assert_equal [[1, 'matt']], r.rows
  #   c.close    
  # end
  # 
  # def test_prepared_statement_with_two_params
  #   c = Vertica::Connection.new(TEST_CONNECTION_HASH)
  #   c.prepare("my_ps", "SELECT * FROM test_table WHERE id = ? OR id = ?", 2)
  #   r = c.execute_prepared("my_ps", 1, 3)
  #   assert_equal 1, r.row_count
  #   assert_equal 2, r.columns.length
  #   assert_equal :in, r.columns[0].data_type
  #   assert_equal 'id', r.columns[0].name
  #   assert_equal :varchar, r.columns[1].data_type
  #   assert_equal 'name', r.columns[1].name
  #   assert_equal [[1, 'matt']], r.rows
  #   c.close
  # end

  def test_cleanup_after_select
    5.times do
      r = @connection.query("SELECT * FROM test_table")
      assert_equal 1, r.row_count
      assert_equal 2, r.columns.length
      assert_equal :in, r.columns[0].data_type
      assert_equal :id, r.columns[0].name
      assert_equal :varchar, r.columns[1].data_type
      assert_equal :name, r.columns[1].name
      assert_equal [{:id => 1, :name => "matt"}], r.rows
    end
  end
end
