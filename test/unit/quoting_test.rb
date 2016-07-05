require 'test_helper'

class QuotingTest < Minitest::Test
  def test_quote_identifier
    assert_equal '"test"', Vertica.quote_identifier(:test)
    assert_equal '"te""st"', Vertica.quote_identifier('te"st')
    assert_equal '"te""""st"', Vertica.quote_identifier('te""st')
  end

  def test_quote_strings
    assert_equal "'test'", Vertica.quote('test')
    assert_equal "'te''st'", Vertica.quote("te'st")
    assert_equal "'te''''st'", Vertica.quote("te''st")
    assert_equal "'test'", Vertica.quote(:test)
  end

  def test_quote_null_and_booleans
    assert_equal 'NULL', Vertica.quote(nil)
    assert_equal 'TRUE', Vertica.quote(true)
    assert_equal 'FALSE', Vertica.quote(false)
  end

  def test_quote_numerics
    assert_equal '1', Vertica.quote(1)
    assert_equal '1.1', Vertica.quote(1.1)
    assert_equal '1.1', Vertica.quote(BigDecimal.new('1.1'))
  end

  def test_quote_date_and_timestamps
    assert_equal "'2010-02-27'::date", Vertica.quote(Date.parse('2010-02-27'))
    assert_equal "'2010-02-27T12:44:25.000000+0000'::timestamptz", Vertica.quote(DateTime.parse('2010-02-27 12:44:25'))
    assert_equal "'2010-02-27T12:44:25.000000+0800'::timestamptz", Vertica.quote(DateTime.parse('2010-02-27 12:44:25 +8'))

    assert_equal "'2010-02-27T12:44:25.123400+0000'::timestamptz", Vertica.quote(Time.utc(2010,2, 27, 12, 44, BigDecimal.new('25.1234')))
    assert_equal "'2010-02-27T12:44:25.123400-0500'::timestamptz", Vertica.quote(Time.new(2010,2, 27, 12, 44, BigDecimal.new('25.1234'), '-05:00'))
    assert_equal "'2010-02-27T12:44:25.123400+0900'::timestamptz", Vertica.quote(Time.new(2010,2, 27, 12, 44, BigDecimal.new('25.1234'), '+09:00'))
  end

  def test_quote_array
    assert_equal "NULL, 1, TRUE, 'test'", Vertica.quote([nil, 1, true, 'test'])
  end
end
