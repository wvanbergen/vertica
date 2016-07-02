class Vertica::Result
  include Enumerable

  attr_reader :columns
  attr_reader :rows
  attr_reader :tag

  def initialize(columns: nil, rows: [], tag: nil)
    @columns, @rows, @tag = columns, rows, tag
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

    column, index = find_column_with_index(col)
    case row
      when Hash; row.fetch(column.name)
      when Array; row.fetch(index)
    end
  end

  alias_method :[], :fetch

  def value
    fetch(0, 0)
  end

  alias_method :the_value, :value

  protected

  def find_column_with_index(col)
    index = case col
      when Integer; col
      when String, Symbol; columns.find_index { |c| c.name.to_s == col.to_s } or raise KeyError, "No column found with name #{col}"
      else raise ArgumentError, "#{col.inspect} is not a valid column identifier"
    end
    [columns.fetch(index), index]
  end
end
