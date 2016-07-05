class Vertica::Row
  include Enumerable

  def initialize(row_description, values)
    @row_description, @values = row_description, values
  end

  def each(&block)
    @values.each(&block)
  end

  def fetch(name_or_index)
    @values.fetch(column_index(name_or_index))
  end

  def inspect
    "<Vertica::Row#{@values.inspect}>"
  end

  alias_method :[], :fetch

  def to_a
    @values.to_a
  end

  def to_h(symbolize_keys: false)
    @row_description.inject({}) do |carry, column|
      key = symbolize_keys ? column.name.to_sym : column.name
      carry.merge(key => fetch(column.name))
    end
  end

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
