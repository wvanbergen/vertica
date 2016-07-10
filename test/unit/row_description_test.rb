require 'test_helper'

class RowDescriptionTest < Minitest::Test
  def setup
    @message = Vertica::Protocol::RowDescription.new("\x00\x02id\x00\x00\np8\x00\x01\x00\x00\x00\x06\x00\b\xFF\xFF\xFF\xFF\x00\x00name\x00\x00\np8\x00\x02\x00\x00\x00\t\xFF\xFF\x00\x00\x00h\x00\x00")

    @column1 = Vertica::Column.new(@message.fields[0])
    @column2 = Vertica::Column.new(@message.fields[1])

    @row_description = Vertica::RowDescription.build([@column1, @column2])
  end

  def test_build_from_row_description_message
    assert_equal Vertica::RowDescription.build(@message), @row_description
  end

  def test_build_from_self_returns_self
    assert Vertica::RowDescription.build(@row_description).eql?(@row_description)
  end

  def test_build_from_nil_return_nil
    assert_nil Vertica::RowDescription.build(nil)
  end

  def test_size
    assert_equal 2, @row_description.length
    assert_equal 2, @row_description.size
  end

  def test_column_with_index
    assert_equal [@column1, 0], @row_description.column_with_index(0)
    assert_equal [@column2, 1], @row_description.column_with_index('name')
  end

  def test_finding_columns_by_index
    assert_equal @column1, @row_description[0]
    assert_equal @column2, @row_description.column(1)
  end

  def test_finding_columns_by_name
    assert_equal @column1, @row_description['id']
    assert_equal @column2, @row_description.column('name')

    assert_equal @column1, @row_description.column(:id)
    assert_equal @column2, @row_description[:name]
  end

  def test_to_a
    assert_equal [@column1, @column2], @row_description.to_a
  end

  def test_to_h
    hash = { 'id' => @column1, 'name' => @column2}
    assert_equal hash, @row_description.to_h
  end

  def test_to_h_with_duplicate_column_name_raises
    row_description = Vertica::RowDescription.build([@column1, @column1])
    assert_raises(Vertica::Error::DuplicateColumnName) { row_description.to_h }
  end

  def test_build_row
    row1 = @row_description.build_row([1, 'name'])
    row2 = @row_description.build_row(id: 1, name: 'name')

    assert_equal row1, row2
  end
end
