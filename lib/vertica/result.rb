class Vertica::Result
  include Enumerable

  attr_reader :columns
  attr_reader :rows
  attr_accessor :tag, :notice

  def initialize(row_handler: nil, row_style: :hash)
    @row_style = row_style
    @row_handler = row_handler || lambda { |record| buffer_row(record) }
    @rows = []
  end

  def descriptions=(message)
    @columns = message.fields.map { |fd| Vertica::Column.new(fd) }
  end

  def handle_row(message)
    @row_handler.call(format_row(message))
  end

  def format_row(row_data)
    send("format_row_as_#{@row_style}", row_data)
  end

  def format_row_as_array(row_data)
    row = []
    row_data.values.each_with_index do |value, idx|
      row << columns.fetch(idx).convert(value)
    end
    row
  end

  def format_row_as_hash(row_data)
    row = {}
    row_data.values.each_with_index do |value, idx|
      col = columns.fetch(idx)
      row[col.name] = col.convert(value)
    end
    row
  end

  def buffer_row(row)
    @rows << row
  end

  def each_row(&block)
    @rows.each(&block)
  end

  def empty?
    @rows.empty?
  end

  def the_value
    if empty?
      nil
    else
      @row_style == :array ? rows[0][0] : rows[0][columns[0].name]
    end
  end

  def [](row, col = nil)
    col.nil? ? row[row] : rows[row][col]
  end

  alias_method :each, :each_row

  def row_count
    @rows.size
  end

  alias_method :size, :row_count
  alias_method :length, :row_count
end
