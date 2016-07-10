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

  alias_method :length, :size

  def to_a
    @columns.clone
  end

  def to_h(symbolize_keys: false)
    @columns.inject({}) do |carry, column|
      key = symbolize_keys ? column.name.to_sym : column.name
      raise Vertica::Error::DuplicateColumnName, "Column with name #{key} occurs more than once in this row description." if carry.key?(key)
      carry[key] = column
      carry
    end
  end

  def build_row(values)
    case values
    when Vertica::Row
      raise ArgumentError, "Row description of provided row does match this row description" if values.row_description != self
      values

    when Vertica::Protocol::DataRow
      converted_values = @columns.map.with_index do |column, index|
        column.convert(values.values.fetch(index))
      end
      Vertica::Row.new(self, converted_values)

    when Array
      raise ArgumentError, "Number of values does not match row description" if values.size != size
      Vertica::Row.new(self, values)

    when Hash
      raise ArgumentError, "Number of values does not match row description" if values.size != size
      values_as_array = @columns.map { |column| values[column.name] || values[column.name.to_sym] }
      Vertica::Row.new(self, values_as_array)

    else
      raise ArgumentError, "Don't know how to build a row from a #{values.class.name} instance"
    end
  end

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
