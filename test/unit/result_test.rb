require 'test_helper'

class ResultTest < Minitest::Test
  def setup
    @hash_result = Vertica::Result.new(
      row_description: [
        Vertica::Column.new(name: 'a', data_type_oid: 6),
        Vertica::Column.new(name: 'b', data_type_oid: 6),
      ],
      rows: [
        { a: 1, b: 2},
        { a: 3, b: 4},
      ],
      tag: 'SELECT'
    )

    @array_result = Vertica::Result.new(
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
    assert_equal 'SELECT', @hash_result.tag
    assert_equal 'SELECT', @array_result.tag

    assert_equal 2, @hash_result.size
    assert_equal 2, @array_result.size

    refute_predicate @hash_result, :empty?
    refute_predicate @array_result, :empty?
  end

  def test_fetch_row
    assert_equal Hash[:a, 1, :b, 2], @hash_result.fetch(0)
    assert_equal Hash[:a, 3, :b, 4], @hash_result.fetch(1)

    assert_equal [1, 2], @array_result.fetch(0)
    assert_equal [3, 4], @array_result.fetch(1)

    assert_raises(IndexError) { @hash_result.fetch(-1234) }
    assert_raises(IndexError) { @hash_result.fetch(1234) }

    assert_raises(IndexError) { @array_result.fetch(-1234) }
    assert_raises(IndexError) { @array_result.fetch(1234) }
  end

  def test_fetch_row_with_negative_index
    assert_equal @hash_result.fetch(0), @hash_result.fetch(-(@hash_result.size) + 0)
    assert_equal @hash_result.fetch(1), @hash_result.fetch(-(@hash_result.size) + 1)
  end

  def test_fetch_value_by_column_index
    assert_equal 1, @hash_result.fetch(0, 0)
    assert_equal 2, @hash_result.fetch(0, 1)
    assert_raises(IndexError) { @hash_result.fetch(0, 2) }
    assert_raises(IndexError) { @hash_result.fetch(0, -3) }

    assert_equal 1, @array_result.fetch(0, 0)
    assert_equal 2, @array_result.fetch(0, 1)
    assert_raises(IndexError) { @array_result.fetch(0, 2) }
    assert_raises(IndexError) { @array_result.fetch(0, -3) }
  end

  def test_fetch_value_by_column_name
    assert_equal 1, @hash_result.fetch(0, :a)
    assert_equal 2, @hash_result.fetch(0, 'b')
    assert_raises(KeyError) { @hash_result.fetch(0, :c) }

    assert_equal 1, @array_result.fetch(0, :a)
    assert_equal 2, @array_result.fetch(0, 'b')
    assert_raises(KeyError) { @array_result.fetch(0, :c) }
  end

  def test_value
    assert_equal @hash_result.value, @hash_result[0, 0]
    assert_equal @array_result.value, @array_result[0, 0]
  end


  def test_enumerable
    sum_a = @hash_result.inject(0) { |carry, row| carry + row[:a] }
    sum_b = @array_result.inject(0) { |carry, row| carry + row[1] }

    assert_equal 4, sum_a
    assert_equal 6, sum_b
  end
end
