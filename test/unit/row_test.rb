require 'test_helper'

class RowTest < Minitest::Test
  def setup
    @message = Vertica::Protocol::RowDescription.new("\x00\x02id\x00\x00\np8\x00\x01\x00\x00\x00\x06\x00\b\xFF\xFF\xFF\xFF\x00\x00name\x00\x00\np8\x00\x02\x00\x00\x00\t\xFF\xFF\x00\x00\x00h\x00\x00")

    @column1 = Vertica::Column.build(@message.fields[0])
    @column2 = Vertica::Column.build(@message.fields[1])

    @row_description = Vertica::RowDescription.build([@column1, @column2])
  end

  def test_fetch
    row = Vertica::Row.new(@row_description, [123, 'test'])
    assert_equal 123, row[0]
    assert_equal 123, row.fetch('id')
    assert_equal 'test', row.fetch(1)
    assert_equal 'test', row[:name]
  end

  def test_to_a
    row = Vertica::Row.new(@row_description, [123, 'test'])
    assert_equal [123, 'test'], row.to_a
  end

  def test_to_h
    row = Vertica::Row.new(@row_description, [123, 'test'])
    hash = { 'id' => 123, 'name' => 'test' }
    assert_equal hash, row.to_h
  end

  def test_to_h_with_duplicate_column_name_raises
    row_description = Vertica::RowDescription.build([@column1, @column1])
    row = Vertica::Row.new(row_description, [123, 456])

    assert_raises(Vertica::Error::DuplicateColumnName) { row.to_h }
  end

  def test_eql?
    rd1 = Vertica::RowDescription.build([@column1, @column2])
    rd2 = Vertica::RowDescription.build([@column1, @column1])

    row = Vertica::Row.new(rd1, [123, 'test'])

    assert_equal row, Vertica::Row.new(rd1, [123, 'test'])
    refute_equal rd1, Vertica::Row.new(rd1, [124, 'test'])
    refute_equal rd1, Vertica::Row.new(rd2, [123, 'test'])
    refute_equal rd1, nil
  end

  def test_inspect
    row = Vertica::Row.new(@row_description, [123, 'test'])
    assert_equal "#<Vertica::Row[123, \"test\"]>", row.inspect
  end
end
