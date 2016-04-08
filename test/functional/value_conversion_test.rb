# encoding : UTF-8
require 'test_helper'

class ValueConversionTest < Minitest::Test
  
  def setup
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH.merge(:row_style => :array))

    @connection.query <<-SQL
      CREATE TABLE IF NOT EXISTS conversions_table (
        "int_field" int, 
        "string_field" varchar(100),
        "date_field" date,
        "timestamp_field" timestamp,
        "time_field" time,
        "interval_field" interval,
        "boolean_field" boolean,
        "float_field" float,
        "float_zero" float
      )
    SQL
  end
  
  
  def teardown
    @connection.query("DROP TABLE IF EXISTS conversions_table CASCADE;")
    @connection.close
  end
  
  def test_value_conversions
    @connection.query "INSERT INTO conversions_table VALUES (123, 'hello world', '2010-01-01', '2010-01-01 12:00:00', '12:00:00', INTERVAL '1 DAY', TRUE, 1.0, 0.0)"
    result = @connection.query "SELECT *,
                                       float_field / float_zero as infinity,
                                       float_field / float_zero - float_field / float_zero as nan
                                FROM conversions_table LIMIT 1"
    assert_equal result.rows.length, 1
    assert_equal [
      123, 
      'hello world', 
      Date.parse('2010-01-01'), 
      Time.parse('2010-01-01 12:00:00'),
      "12:00:00", 
      "1", 
      true,
      1.0,
      0.0,
      Float::INFINITY,
      Float::NAN], result.rows.first
  end
  
  def test_nil_conversions
    @connection.query "INSERT INTO conversions_table VALUES (NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    result = @connection.query "SELECT * FROM conversions_table LIMIT 1"
    assert_equal result.rows.length, 1
    assert_equal [nil, nil, nil, nil, nil, nil, nil, nil, nil], result.rows.first
  end
  
  def test_string_encoding
    assert_equal 'åßç∂ë', @connection.query("SELECT 'åßç∂ë'").the_value
  end
end
