require 'test_helper'

class ResultTest < Minitest::Test
  def setup
    @result = Vertica::Result.build(
      row_description: [
        Vertica::Column.new(name: 'a', data_type_oid: 6),
        Vertica::Column.new(name: 'b', data_type_oid: 6),
      ],
      rows: [
        [1, 2],
        [3, 4],
      ],
      tag: 'SELECT'
    )
  end

  def test_basics
    assert_equal 'SELECT', @result.tag
    assert_equal 2, @result.size
    refute_predicate @result, :empty?
  end

  def test_fetch_row
    assert_equal [1, 2], @result.fetch(0).to_a
    assert_equal [3, 4], @result.fetch(1).to_a

    assert_raises(IndexError) { @result.fetch(-1234) }
    assert_raises(IndexError) { @result.fetch(1234) }
  end

  def test_fetch_row_with_negative_index
    assert_equal @result.fetch(0), @result.fetch(-(@result.size) + 0)
    assert_equal @result.fetch(1), @result.fetch(-(@result.size) + 1)
  end

  def test_fetch_value_by_column_index
    assert_equal 1, @result.fetch(0, 0)
    assert_equal 2, @result.fetch(0, 1)
    assert_raises(IndexError) { @result.fetch(0, 2) }
    assert_raises(IndexError) { @result.fetch(0, -3) }
  end

  def test_fetch_value_by_column_name
    assert_equal 1, @result.fetch(0, :a)
    assert_equal 2, @result.fetch(0, 'b')
    assert_raises(KeyError) { @result.fetch(0, :c) }
  end

  def test_value
    assert_equal @result.value, @result[0, 0]
  end

  def test_enumerable
    sum_a = @result.inject(0) { |carry, row| carry + row[:a] }
    sum_b = @result.inject(0) { |carry, row| carry + row[1] }

    assert_equal 4, sum_a
    assert_equal 6, sum_b
  end
end
