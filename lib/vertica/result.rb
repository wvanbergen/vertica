class Vertica::Result
  include Enumerable

  attr_reader :columns
  attr_reader :rows
  attr_accessor :tag

  def initialize(row_handler: nil, row_style: :hash)
    @row_style = row_style
    @row_handler = row_handler || lambda { |row| buffer_row(row) }
    @rows = []
  end

  def each(&block)
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
    col.nil? ? rows[row] : rows.fetch([row], {}).fetch(col)
  end

  def size
    @rows.size
  end

  alias_method :count, :size
  alias_method :length, :size

  # @private
  def handle_row_description(message)
    @columns = message.fields.map { |fd| Vertica::Column.new(fd) }
  end

  # @private
  def handle_data_row(message)
    @row_handler.call(format_row(message))
  end

  private

  def format_row(message)
    send("format_row_as_#{@row_style}", message)
  end

  def format_row_as_array(message)
    row = []
    message.values.each_with_index do |value, idx|
      row << columns.fetch(idx).convert(value)
    end
    row
  end

  def format_row_as_hash(message)
    row = {}
    message.values.each_with_index do |value, idx|
      col = columns.fetch(idx)
      row[col.name] = col.convert(value)
    end
    row
  end

  def buffer_row(row)
    @rows << row
  end
end
