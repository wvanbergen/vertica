class Vertica::Result
  include Enumerable

  attr_reader :columns
  attr_reader :rows
  attr_accessor :tag

  def initialize(row_handler: nil)
    @row_handler = row_handler || lambda { |row| buffer_row(row) }
    @rows = []
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


  # @private
  def handle_row_description(message)
    @columns = message.fields.map { |fd| Vertica::Column.new(fd) }
  end

  # @private
  def handle_data_row(message)
    @row_handler.call(format_row(message))
  end

  protected

  def find_column(col)
    case col
      when Integer; columns.fetch(col)
      when String, Symbol; columns.detect { |c| c.name.to_s == col }
      else raise ArgumentError, "#{col.inspect} is not a valid column identifier"
    end
  end

  def format_row(message)
    raise NotImplementedError
  end

  def buffer_row(row)
    @rows << row
  end

  class ArrayResult < Vertica::Result
    def format_row(message)
      row = []
      message.values.each_with_index do |value, idx|
        row << columns.fetch(idx).convert(value)
      end
      row
    end
  end

  class HashResult < Vertica::Result
    def format_row(message)
      row = {}
      message.values.each_with_index do |value, idx|
        col = columns.fetch(idx)
        row[col.name] = col.convert(value)
      end
      row
    end
  end
end
