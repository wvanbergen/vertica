require 'test_helper'

class ColumnTest < Minitest::Test

  def test_initialize_from_row_description
    field_description = {
      :name               => "OUTPUT",
      :table_oid          => 0,
      :attribute_number   => 0,
      :data_type_oid      => 6,
      :data_type_size     => 8,
      :data_type_modifier => 8,
      :format_code        => 0
    }

    column = Vertica::Column.new(field_description)
    assert_equal 'OUTPUT', column.name
    assert_equal :integer, column.data_type
    assert_equal 8, column.data_type_modifier
  end

  def test_unknown_type_oid
    field_description = {
      :name               => "OUTPUT",
      :table_oid          => 0,
      :attribute_number   => 0,
      :data_type_oid      => 123456,
      :data_type_size     => 8,
      :data_type_modifier => 8,
      :format_code        => 0
    }

    assert_raises(Vertica::Error::UnknownTypeError) { Vertica::Column.new(field_description) }
  end

  def test_integer_converter
    field_description = {
      :name               => "OUTPUT",
      :table_oid          => 0,
      :attribute_number   => 0,
      :data_type_oid      => 6,
      :data_type_size     => 8,
      :data_type_modifier => 8,
      :format_code        => 0
    }

    column = Vertica::Column.new(field_description)
    assert_equal :integer, column.data_type
    assert_equal 1234, column.convert('1234')
  end
end
