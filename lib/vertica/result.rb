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

  def [](row, col = nil)
    row = rows.fetch(0)
    return row if col.nil?

    column = find_column(col)
    case row
      when Hash; row.fetch(column.name)
      when Array; row.fetch(column.attribute_number)
    end
  end

  def value
    self[0, 0]
  end

  alias_method :the_value, :value

  protected

  def find_column(col)
    case col
      when Integer; columns.fetch(col)
      when String, Symbol; columns.detect { |c| c.name.to_s == col }
      else raise ArgumentError, "#{col.inspect} is not a valid column identifier"
    end
  end
end
