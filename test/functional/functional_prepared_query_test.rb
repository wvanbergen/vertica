require 'test_helper'

class FunctionalQueryTest < Minitest::Test
  
  def setup
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    @connection.query("DROP TABLE IF EXISTS test_ruby_vertica_table CASCADE;")
    @connection.query("CREATE TABLE test_ruby_vertica_table (id int, name varchar(100))")
  end

  def test_basic_prepared_query
    r = @connection.prepare('select 2').execute

    assert_equal 1, r.size
    assert_equal 2, r[0][0]
  end

  def test_streaming_prepared_query
    rows = []
    @connection.prepare('select 2').execute do |r|
      rows << r
    end

    assert_equal 1, rows.size
    assert_equal 2, rows[0][0]
  end

  def test_query_with_parameters
    r = @connection.prepare('select ?::int').execute(5)

    assert_equal 1, r.size
    assert_equal 5, r[0][0]
  end

  def test_reuse_same_query_with_different_parameters
    pq = @connection.prepare('select ?::int')

    r = pq.execute(5)
    assert_equal 1, r.size
    assert_equal 5, r[0][0]

    r = pq.execute(6)
    assert_equal 1, r.size
    assert_equal 6, r[0][0]
  end

  def test_prepared_dml_queries
    r = @connection.query('select * from test_ruby_vertica_table where id=1')
    assert_equal 0, r.size

    @connection.prepare('insert into test_ruby_vertica_table values(?, ?)').execute(1, 'abcd')
    r = @connection.query('select * from test_ruby_vertica_table where id=1')
    assert_equal 1, r.size
    assert_equal 'abcd', r[0][:name]

    @connection.prepare('update test_ruby_vertica_table set name=? where id=?').execute('abcd2', 1)

    r = @connection.query('select * from test_ruby_vertica_table where id=1')
    assert_equal 1, r.size
    assert_equal 'abcd2', r[0][:name]

    @connection.prepare('delete from test_ruby_vertica_table where id=?').execute(1)
    r = @connection.query('select * from test_ruby_vertica_table where id=1')
    assert_equal 0, r.size
  end

  def test_empty_query
    assert_raises Vertica::Error::EmptyQueryError do
      @connection.prepare('')
    end  

    assert_raises Vertica::Error::EmptyQueryError do
      @connection.prepare('/* test */')
    end  
  end

  def test_invalid_query
    assert_raises Vertica::Error::MissingRelation do
      @connection.prepare("select * from non_existing")
    end
  end

  def test_incorrect_parameters
    e = assert_raises Vertica::Error::QueryError do
      @connection.prepare("select 1 where 1=?").execute
    end

    assert_match /Insufficient data left in message/, e.message

    e = assert_raises Vertica::Error::QueryError do
      @connection.prepare("select 1 where 1=?").execute(1, 2)
    end

    assert_match /Bind message has 2 parameter formats but 1 parameters/, e.message
  end
end