require 'test_helper'

class ColumnTest < Minitest::Test
  def setup
    @field_description = {
      :name               => "OUTPUT",
      :table_oid          => 0,
      :attribute_number   => 0,
      :data_type_oid      => 6,
      :data_type_size     => 8,
      :data_type_modifier => 8,
      :data_format        => 0
    }
  end

  def test_initialize_from_row_description
    column = Vertica::Column.new(@field_description)
    assert_equal 'OUTPUT', column.name
    assert_equal 'integer', column.data_type.name
    assert_equal 8, column.data_type.modifier
    assert_equal 8, column.data_type.size
  end

  def test_unknown_type_oid
    field_description = @field_description.merge(data_type_oid: 123456)
    assert_raises(Vertica::Error::UnknownTypeError) { Vertica::Column.new(field_description) }
  end

  def test_equality
    column = Vertica::Column.new(@field_description)

    assert_equal column, Vertica::Column.new(@field_description)
    refute_equal column, Vertica::Column.new(@field_description.merge(name: 'other'))
    refute_equal column, Vertica::Column.new(@field_description.merge(data_format: 1))
  end
end
