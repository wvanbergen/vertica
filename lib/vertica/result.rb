class Vertica::Result
  include Enumerable

  attr_reader :row_description
  attr_reader :rows
  attr_reader :tag

  def initialize(row_description: nil, rows: [], tag: nil)
    @row_description, @rows, @tag = Vertica::RowDescription.build(row_description), rows, tag
  end

  def each(&block)
    @rows.each(&block)
  end

  def empty?
    @rows.empty?
  end

  def size
    @rows.size
  end

  alias_method :count, :size
  alias_method :length, :size

  def fetch(row_index, col = nil)
    row = rows.fetch(row_index)
    return row if col.nil?

    column, index = row_description.column_with_index(col)
    case row
      when Hash; row.fetch(column.name.to_sym)
      when Array; row.fetch(index)
    end
  end

  alias_method :[], :fetch

  def value
    fetch(0, 0)
  end

  alias_method :the_value, :value

  alias_method :columns, :row_description
end
