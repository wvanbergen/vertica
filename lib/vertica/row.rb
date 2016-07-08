class Vertica::Row
  include Enumerable

  attr_reader :row_description

  def initialize(row_description, values)
    @row_description, @values = row_description, values
  end

  def each(&block)
    @values.each(&block)
  end

  def fetch(name_or_index)
    @values.fetch(column_index(name_or_index))
  end

  alias_method :[], :fetch

  # Returns an array representation of the row. The values will
  # be ordered like the order of the fields in the {#row_description}.
  # @return [Array]
  def to_a
    @values
  end

  # Returns a hash representation of this rows, using the name of the
  # fields as keys.
  # @param symbolize_keys [true, false] Set to true to use symbols instead
  #   of strings for the field names.
  # @return [Hash]
  def to_h(symbolize_keys: false)
    @row_description.inject({}) do |carry, column|
      key = symbolize_keys ? column.name.to_sym : column.name
      carry[key] = fetch(column.name)
      carry
    end
  end

  def eql?(other)
    other.kind_of?(Vertica::Row) && other.row_description == row_description && other.to_a == to_a
  end

  alias_method :==, :eql?

  def hash
    [row_description, values].hash
  end

  def inspect
    "<Vertica::Row#{@values.inspect}>"
  end

  private

  def column(name_or_index)
    column_with_index(name_or_index).fetch(0)
  end

  def column_index(name_or_index)
    column_with_index(name_or_index).fetch(1)
  end

  def column_with_index(name_or_index)
    @row_description.column_with_index(name_or_index)
  end
end
