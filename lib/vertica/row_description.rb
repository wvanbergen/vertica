class Vertica::RowDescription
  include Enumerable

  def self.build(columns)
    case columns
    when Vertica::Protocol::RowDescription
      new(columns.fields.map { |fd| Vertica::Column.new(fd) })
    when Vertica::RowDescription
      columns
    when Array
      new(columns)
    when nil
      nil
    else
      raise ArgumentError, "Invalid list of columns: #{columns.inspect}"
    end
  end

  def initialize(columns)
    @columns = columns
  end

  def column(name_or_index)
    column_with_index(name_or_index).first
  end

  alias_method :[], :column

  def column_with_index(name_or_index)
    columns_index.fetch(name_or_index)
  end

  def each(&block)
    @columns.each(&block)
  end

  def size
    @columns.size
  end

  def to_a
    @columns.clone
  end

  def to_h
    @columns.inject({}) do |carry, column|
      carry.merge(column.name => column)
    end
  end

  alias_method :length, :size

  def eql?(other)
    self.class === other && other.to_a == self.to_a
  end

  alias_method :==, :eql?

  def hash
    @columns.hash
  end

  protected

  def columns_index
    @columns_index ||= begin
      result = {}
      @columns.each_with_index do |column, index|
        result[index]              = [column, index]
        result[column.name.to_s]   = [column, index]
        result[column.name.to_sym] = [column, index]
      end
      result
    end
  end
end
