require 'test_helper'

class QuotingTest < Test::Unit::TestCase

  def test_quote_identifier
    assert_equal '"test"', Vertica.quote_identifier(:test)
    assert_equal '"te""st"', Vertica.quote_identifier('te"st')
    assert_equal '"te""""st"', Vertica.quote_identifier('te""st')
  end

  def test_quote
    assert_equal "'test'", Vertica.quote('test')
    assert_equal "'te''st'", Vertica.quote("te'st")
    assert_equal "'te''''st'", Vertica.quote("te''st")
    assert_equal "'test'", Vertica.quote(:test)

    assert_equal 'NULL', Vertica.quote(nil)
    assert_equal 'TRUE', Vertica.quote(true)
    assert_equal 'FALSE', Vertica.quote(false)

    assert_equal '1', Vertica.quote(1)
    assert_equal '1.1', Vertica.quote(1.1)
    assert_equal '1.1', Vertica.quote(BigDecimal.new('1.1'))

    assert_equal "'2010-02-27'::date", Vertica.quote(Date.parse('2010-02-27'))
    assert_equal "'2010-02-27 12:44:25'::timestamp", Vertica.quote(DateTime.parse('2010-02-27 12:44:25'))
    assert_equal "'2010-02-27 12:44:25'::timestamp", Vertica.quote(Time.utc(2010,2, 27, 12, 44, 25))
    
    assert_equal "NULL, 1, TRUE, 'test'", Vertica.quote([nil, 1, true, 'test'])
  end
end
