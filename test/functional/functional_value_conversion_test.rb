# encoding : UTF-8
require 'test_helper'

class FunctionalValueConversionTest < Minitest::Test

  def setup
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)

    @connection.query <<-SQL
      CREATE TABLE IF NOT EXISTS conversions_table (
        "int_field" int,
        "varchar_field" varchar(100),
        "char_field" char(10),
        "long_varchar_field" long varchar,
        "date_field" date,
        "timestamp_field" timestamp,
        "timestamptz_field" timestamptz,
        "time_field" time,
        "interval_field" interval,
        "boolean_field" boolean,
        "float_field" float,
        "numeric_field" numeric(10, 2),
        "binary_field" varbinary
      )
    SQL
  end


  def teardown
    @connection.query("DROP TABLE IF EXISTS conversions_table CASCADE;")
    @connection.close
  end

  def test_deserialize_values_from_table
    @connection.query <<-SQL
      INSERT INTO conversions_table VALUES (
          123,
          'hello world',
          'hello',
          'hello world',
          '2010-01-01',
          '2010-01-01 12:00:00.123456',
          '2010-01-01 12:00:00 +0930',
          '12:00:00',
          INTERVAL '1 DAY',
          TRUE,
          -1.123,
          1.12345,
          HEX_TO_BINARY('d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21')
      )
    SQL

    result = @connection.query("SELECT * FROM conversions_table LIMIT 1")

    assert_equal 1, result.size
    assert_equal 123, result.fetch(0, 'int_field')
    assert_equal 'hello world', result.fetch(0, 'varchar_field')
    assert_equal 'hello     ', result.fetch(0, 'char_field')
    assert_equal 'hello world', result.fetch(0, 'long_varchar_field')
    assert_equal Date.parse('2010-01-01'), result.fetch(0, 'date_field')
    assert_equal Time.new(2010, 1, 1, 12, 0, BigDecimal.new("0.123456")), result.fetch(0, 'timestamp_field')
    assert_equal Time.new(2010, 1, 1, 12, 0, 0, '+09:30'), result.fetch(0, 'timestamptz_field')
    assert_equal "12:00:00", result.fetch(0, 'time_field')
    assert_equal "1", result.fetch(0, 'interval_field')
    assert_equal true, result.fetch(0, 'boolean_field')
    assert_equal -1.123, result.fetch(0, 'float_field')
    assert_equal BigDecimal.new('1.12'), result.fetch(0, 'numeric_field')
    assert_equal ['d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21'].pack('H*'), result.fetch(0, 'binary_field')
  end

  def test_nil_values_from_table
    @connection.query("INSERT INTO conversions_table VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")
    result = @connection.query("SELECT * FROM conversions_table LIMIT 1")
    assert_equal 1, result.size
    assert result[0].all?(&:nil?)
  end

  def test_deserialize_float_values
    result = @connection.query(<<-SQL)
      SELECT 0::float,
             -0::float,
             1.1::float,
             -1.1::float,
             1.0::float / 0.0::float,
             -1.0::float / 0.0::float,
             1.0::float / 0.0::float - 1.0::float / 0.0::float,
             NULL::float
    SQL

    assert_equal 1, result.size
    assert_equal 0.0, result[0, 0]
    assert_equal -0.0, result[0, 1]
    assert_equal 1.1, result[0, 2]
    assert_equal -1.1, result[0, 3]
    assert_equal Float::INFINITY, result[0, 4]
    assert_equal -Float::INFINITY, result[0, 5]
    assert result[0, 6].equal?(Float::NAN)
    assert_nil result[0, 7]
  end

  def test_deserialize_numeric_values
    result = @connection.query(<<-SQL)
      SELECT '1'::numeric(5,2),
             '1.12'::numeric(5,2),
             '1.1234'::numeric(5,2),
             '1.1234'::numeric,
             '0'::numeric,
             '-0'::numeric,
             NULL::numeric
    SQL

    assert_equal 1, result.size
    assert_equal BigDecimal.new('1.00'), result.fetch(0, 0)
    assert_equal BigDecimal.new('1.12'), result.fetch(0, 1)
    assert_equal BigDecimal.new('1.12'), result.fetch(0, 2)
    assert_equal BigDecimal.new('1.1234'), result.fetch(0, 3)
    assert_equal BigDecimal.new('0'), result.fetch(0, 4)
    assert_equal BigDecimal.new('-0'), result.fetch(0, 5)
    assert_nil result.fetch(0, 6)
  end

  def test_deserialize_string_values
    assert_equal 'åßç∂ë', @connection.query("SELECT 'åßç∂ë'").the_value
    assert_equal Encoding::UTF_8, @connection.query("SELECT 'åßç∂ë'").the_value.encoding
  end

  def test_deserialize_binary_values
    assert_equal ['d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21'].pack('H*'), @connection.query("SELECT HEX_TO_BINARY('d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21')").the_value
    assert_equal Encoding::BINARY, @connection.query("SELECT HEX_TO_BINARY('d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21')").the_value.encoding
  end

  def test_deserialize_timestamp_values_with_utc_timezone_connection
    @connection.query("SET TIMEZONE TO 'UTC'")

    result = @connection.query(<<-SQL)
      SELECT '2013-01-02 14:15:16'::timestamp,
             '2013-01-02 14:15:16 America/Toronto'::timestamp,
             '2013-01-02 14:15:16 +9:30'::timestamp,
             '2013-01-02 14:15:16'::timestamptz,
             '2013-01-02 14:15:16 America/Toronto'::timestamptz,
             '2013-01-02 14:15:16 +9:30'::timestamptz,
             NULL::timestamp,
             NULL::timestamptz
    SQL

    assert_equal Time.new(2013, 1, 2, 14, 15, 16), result.fetch(0, 0)
    assert_equal Time.new(2013, 1, 2, 19, 15, 16), result.fetch(0, 1)
    assert_equal Time.new(2013, 1, 2,  4, 45, 16), result.fetch(0, 2)
    assert_equal Time.new(2013, 1, 2, 14, 15, 16, '+00:00'), result.fetch(0, 3)
    assert_equal Time.new(2013, 1, 2, 19, 15, 16, '+00:00'), result.fetch(0, 4)
    assert_equal Time.new(2013, 1, 2,  4, 45, 16, '+00:00'), result.fetch(0, 5)
    assert_nil result.fetch(0, 6)
    assert_nil result.fetch(0, 7)
  end

  def test_deserialize_timestamp_values_with_toronto_timezone_connection
    @connection.query("SET TIMEZONE TO 'America/Toronto'")

    result = @connection.query(<<-SQL)
      SELECT '2013-01-02 14:15:16'::timestamp,
             '2013-01-02 14:15:16 America/Toronto'::timestamp,
             '2013-01-02 14:15:16 +9:30'::timestamp,
             '2013-01-02 14:15:16'::timestamptz,
             '2013-01-02 14:15:16 America/Toronto'::timestamptz,
             '2013-01-02 14:15:16 +9:30'::timestamptz,
             NULL::timestamp,
             NULL::timestamptz
    SQL

    assert_equal Time.new(2013, 1, 2, 14, 15, 16), result.fetch(0, 0)
    assert_equal Time.new(2013, 1, 2, 14, 15, 16), result.fetch(0, 1)
    assert_equal Time.new(2013, 1, 1, 23, 45, 16), result.fetch(0, 2)
    assert_equal Time.new(2013, 1, 2, 14, 15, 16, '-05:00'), result.fetch(0, 3)
    assert_equal Time.new(2013, 1, 2, 14, 15, 16, '-05:00'), result.fetch(0, 4)
    assert_equal Time.new(2013, 1, 1, 23, 45, 16, '-05:00'), result.fetch(0, 5)
    assert_nil result.fetch(0, 6)
    assert_nil result.fetch(0, 7)
  end
end
