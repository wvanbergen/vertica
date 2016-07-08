# encoding : UTF-8
require 'test_helper'

class FunctionalValueConversionTest < Minitest::Test

  def setup
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)

    @connection.query <<-SQL
      CREATE TABLE IF NOT EXISTS conversions_table (
        "int_field" int,
        "string_field" varchar(100),
        "date_field" date,
        "timestamp_field" timestamp,
        "timestamptz_field" timestamptz,
        "time_field" time,
        "interval_field" interval,
        "boolean_field" boolean,
        "float_field" float,
        "float_zero" float,
        "binary_field" varbinary,
        "long_varchar_field" long varchar
      )
    SQL
  end


  def teardown
    @connection.query("DROP TABLE IF EXISTS conversions_table CASCADE;")
    @connection.close
  end

  def test_value_conversions
    @connection.query <<-SQL
      INSERT INTO conversions_table VALUES (
          123,
          'hello world',
          '2010-01-01',
          '2010-01-01 12:00:00.123456',
          '2010-01-01 12:00:00 +0930',
          '12:00:00',
          INTERVAL '1 DAY',
          TRUE,
          1.0,
          0.0,
          HEX_TO_BINARY('d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21'),
          'hello world'
      )
    SQL

    result = @connection.query <<-SQL
      SELECT *,
             float_field / float_zero as infinity,
             float_field / float_zero - float_field / float_zero as nan
        FROM conversions_table LIMIT 1
    SQL

    assert_equal 1, result.size
    assert_equal [
      123,
      'hello world',
      Date.parse('2010-01-01'),
      Time.new(2010, 1, 1, 12, 0, BigDecimal.new("0.123456")),
      Time.new(2010, 1, 1, 12, 0, 0, '+09:30'),
      "12:00:00",
      "1",
      true,
      1.0,
      0.0,
      ['d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21'].pack('H*'),
      'hello world',
      Float::INFINITY,
      Float::NAN
    ], result[0].to_a
  end

  def test_nil_conversions
    @connection.query "INSERT INTO conversions_table VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    result = @connection.query "SELECT * FROM conversions_table LIMIT 1"
    assert_equal 1, result.size
    assert_equal [nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil], result[0].to_a
  end

  def test_string_decoding
    assert_equal 'åßç∂ë', @connection.query("SELECT 'åßç∂ë'").the_value
    assert_equal Encoding::UTF_8, @connection.query("SELECT 'åßç∂ë'").the_value.encoding
  end

  def test_binary_decoding
    assert_equal ['d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21'].pack('H*'), @connection.query("SELECT HEX_TO_BINARY('d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21')").the_value
    assert_equal Encoding::BINARY, @connection.query("SELECT HEX_TO_BINARY('d09fd180d0b8d0b2d0b5d1822c2068656c6c6f21')").the_value.encoding
  end

  def test_timestamp_decoding_with_utc_timezone_connection
    @connection.query("SET TIMEZONE TO 'UTC'")

    assert_equal Time.new(2013, 1, 2, 14, 15, 16), @connection.query("SELECT '2013-01-02 14:15:16'::timestamp").the_value
    assert_equal Time.new(2013, 1, 2, 19, 15, 16), @connection.query("SELECT '2013-01-02 14:15:16 America/Toronto'::timestamp").the_value
    assert_equal Time.new(2013, 1, 2,  4, 45, 16), @connection.query("SELECT '2013-01-02 14:15:16 +9:30'::timestamp").the_value

    assert_equal Time.new(2013, 1, 2, 14, 15, 16, '+00:00'), @connection.query("SELECT '2013-01-02 14:15:16'::timestamptz").the_value
    assert_equal Time.new(2013, 1, 2, 19, 15, 16, '+00:00'), @connection.query("SELECT '2013-01-02 14:15:16 America/Toronto'::timestamptz").the_value
    assert_equal Time.new(2013, 1, 2,  4, 45, 16, '+00:00'), @connection.query("SELECT '2013-01-02 14:15:16 +09:30'::timestamptz").the_value
  end

  def test_timestamp_decoding_with_toronto_timezone_connection
    @connection.query("SET TIMEZONE TO 'America/Toronto'")

    assert_equal Time.new(2013, 1, 2, 14, 15, 16), @connection.query("SELECT '2013-01-02 14:15:16'::timestamp").the_value
    assert_equal Time.new(2013, 1, 2, 14, 15, 16), @connection.query("SELECT '2013-01-02 14:15:16 America/Toronto'::timestamp").the_value
    assert_equal Time.new(2013, 1, 1, 23, 45, 16), @connection.query("SELECT '2013-01-02 14:15:16 +9:30'::timestamp").the_value

    assert_equal Time.new(2013, 1, 2, 14, 15, 16, '-05:00'), @connection.query("SELECT '2013-01-02 14:15:16'::timestamptz").the_value
    assert_equal Time.new(2013, 1, 2, 14, 15, 16, '-05:00'), @connection.query("SELECT '2013-01-02 14:15:16 America/Toronto'::timestamptz").the_value
    assert_equal Time.new(2013, 1, 1, 23, 45, 16, '-05:00'), @connection.query("SELECT '2013-01-02 14:15:16 +09:30'::timestamptz").the_value
  end
end
