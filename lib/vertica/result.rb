class Vertica::Result
  include Enumerable

  attr_reader :row_description
  attr_reader :rows
  attr_reader :tag

  def initialize(row_description: nil, rows: nil, tag: nil)
    @row_description, @rows, @tag = row_description, rows, tag
  end

  def each(&block)
    @rows.each(&block)
  end

  def empty?
    @rows.empty?
  end

  def size
    @rows.length
  end

  alias_method :count, :size
  alias_method :length, :size

  def fetch(row_index, col = nil)
    row = rows.fetch(row_index)
    return row if col.nil?
    row.fetch(col)
  end

  alias_method :[], :fetch

  def value
    fetch(0, 0)
  end

  alias_method :the_value, :value

  alias_method :columns, :row_description

  def self.build(row_description: nil, rows: [], tag: nil)
    row_description = Vertica::RowDescription.build(row_description)
    rows = rows.map { |values| row_description.build_row(values) }
    new(row_description: row_description, rows: rows, tag: tag)
  end
end
