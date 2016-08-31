require 'test_helper'

class FunctionalPreparedQueryTest < Minitest::Test
  
  def setup
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    @connection.query("DROP TABLE IF EXISTS test_ruby_vertica_table CASCADE;")
    @connection.query("CREATE TABLE test_ruby_vertica_table (id int, name varchar(100))")
    @connection.query("DROP TABLE IF EXISTS pq_conversions_table CASCADE;")
    @connection.query <<-SQL
          CREATE TABLE IF NOT EXISTS pq_conversions_table (
            "int_field" int,
            "varchar_field" varchar(100),
            "char_field" char(10),
            "long_varchar_field" long varchar,
            "date_field" date,
            "timestamp_field" timestamp,
            "timestamptz_field" timestamptz,
            "boolean_field" boolean,
            "float_field" float,
            "numeric_field" numeric(10, 2),
            "binary_field" varbinary
          )
        SQL
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

  def test_parameter_types
    today = Date.today
    now = Time.now.round
    binary_data = ['d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21'].pack('H*')
    decimal = BigDecimal.new('1.12')

    @connection.prepare("INSERT INTO pq_conversions_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .execute(123, 'hello world', 'hello', 'hello world', today, now, now, false, -1.123, decimal, binary_data)

    r = @connection.query('select * from pq_conversions_table where int_field = 123')  

    assert_equal 1, r.size
    row = r[0]
    assert_equal 123, row[:int_field]
    assert_equal 'hello world', row[:varchar_field]
    assert_equal 'hello     ', row[:char_field]
    assert_equal 'hello world', row[:long_varchar_field]
    assert_equal today, row[:date_field]
    assert_equal now, row[:timestamp_field]
    assert_equal now, row[:timestamptz_field]
    assert_equal false, row[:boolean_field]
    assert_equal -1.123, row[:float_field]
    assert_equal decimal, row[:numeric_field]
    assert_equal binary_data, row[:binary_field]
  end

  def test_parameter_types_condition
    today = Date.today
    now = Time.now.round
    binary_data = ['d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21'].pack('H*')
    decimal = BigDecimal.new('1.12')

    @connection.prepare("INSERT INTO pq_conversions_table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
      .execute(124, 'hello world', 'hello', 'hello world', today, now, now, true, -1.123, decimal, binary_data)

    pq = @connection.prepare("select * from pq_conversions_table where int_field = ?")
    
    assert_equal 1, pq.execute(124).size  
    assert_equal 0, pq.execute(125).size  

    pq = @connection.prepare("select * from pq_conversions_table where int_field > ?")

    assert_equal 0, pq.execute(124).size  
    assert_equal 1, pq.execute(123).size  

    pq = @connection.prepare("select * from pq_conversions_table where int_field = 124 and varchar_field = ?")

    assert_equal 1, pq.execute('hello world').size  
    assert_equal 0, pq.execute('hello world2').size  

    pq = @connection.prepare("select * from pq_conversions_table where int_field = 124 and char_field = ?")

    assert_equal 1, pq.execute('hello     ').size  
    assert_equal 1, pq.execute('hello').size  
    assert_equal 0, pq.execute('hello2').size  

    pq = @connection.prepare("select * from pq_conversions_table where int_field = 124 and date_field = ?")

    assert_equal 1, pq.execute(today).size  
    assert_equal 0, pq.execute(Date.today - 1).size  

    pq = @connection.prepare("select * from pq_conversions_table where int_field = 124 and date_field > ?")

    assert_equal 0, pq.execute(today).size  
    assert_equal 1, pq.execute(Date.today - 1).size

    pq = @connection.prepare("select * from pq_conversions_table where int_field = 124 and timestamp_field = ?")

    assert_equal 1, pq.execute(now).size  
    assert_equal 0, pq.execute(now - 1).size

    pq = @connection.prepare("select * from pq_conversions_table where int_field = 124 and timestamptz_field > ?")

    assert_equal 0, pq.execute(now).size  
    assert_equal 1, pq.execute(now - 1).size  

    pq = @connection.prepare("select * from pq_conversions_table where int_field = 124 and boolean_field = ?")

    assert_equal 1, pq.execute(true).size  
    assert_equal 0, pq.execute(false).size  

    pq = @connection.prepare("select * from pq_conversions_table where int_field = 124 and float_field = ?")

    assert_equal 1, pq.execute(-1.123).size  
    assert_equal 0, pq.execute(-1.3).size  

    pq = @connection.prepare("select * from pq_conversions_table where int_field = 124 and numeric_field = ?")

    assert_equal 1, pq.execute(decimal).size  
    assert_equal 0, pq.execute(decimal - 1).size  

    pq = @connection.prepare("select * from pq_conversions_table where int_field = 124 and binary_field = ?")

    assert_equal 1, pq.execute(binary_data).size  
    assert_equal 0, pq.execute(binary_data + '3').size  
  end

  def test_raise_when_connection_is_in_use
    assert_raises(Vertica::Error::SynchronizeError) do
      @connection.prepare("SELECT 1 UNION SELECT ?::int").execute(2) do |record|
        @connection.prepare("SELECT 3").execute
      end
    end
  end
end