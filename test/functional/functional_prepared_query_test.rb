require 'test_helper'

class FunctionalQueryTest < Minitest::Test
  
  def setup
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
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
end