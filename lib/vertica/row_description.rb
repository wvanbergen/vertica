# Class that describes the shape of a query result. It contains an ordered list
# of {Vertica::Column} instances, which describes the columns that will apear for
# every {Vertica::Row} part of the result of a query.
#
# @see Vertica::Row
# @see Vertica::Result
class Vertica::RowDescription
  include Enumerable

  # Builds a new Vertica::RowDescription instance given a list of columns.
  # @param columns [Vertica::Protocol::RowDescription, Vertica::RowDescription, Array<Vertica::Column>]
  #   An object that describes the list of columns.
  # @return [Vertica::RowDescription]
  # @raise [ArgumentError] If no Vertica::RowDescription could be constructed given the provided argument.
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

  # @param columns [Array<Vertica::Column>] The list of columns as they will
  #   appear in the rows of a query result.
  # @see Vertica::RowDescription.build
  def initialize(columns)
    @columns = columns
  end

  # Returns a column in this row description.
  # @param name_or_index [String, Symbol, Integer] The name of the column, or
  #   the index of the column in this row description.
  # @return [Vertica::Column]
  # @raise [KeyError] if the column could not be found in this row description.
  def column(name_or_index)
    column_with_index(name_or_index).first
  end

  alias_method :[], :column

  # Returns a column, accompanied by the index of that column in this row description.
  # @param name_or_index [String, Symbol, Integer] The name of the column, or
  #   the index of the column in this row description.
  # @return [Array<Vertica::Column, Integer>]
  # @raise [KeyError] if the column could not be found in this row description.
  def column_with_index(name_or_index)
    columns_index.fetch(name_or_index)
  end

  # Iterates over the columns in this row description.
  # @yield The provided block will be called with every column.
  # @yieldparam column [Vertica::Column]
  # @return [void]
  def each(&block)
    @columns.each(&block)
  end

  # @return [Integer] Returns the number of columns in this row description.
  def size
    @columns.size
  end

  alias_method :length, :size

  # @return [Array<Vertica::Column>] Returns the columns of this row description as an array.
  def to_a
    @columns.clone
  end

  # @param symbolize_keys [Boolean] Whether to use symbols instead of strings as keys.
  # @return [Hash] Returns the columns of this row description as a hash, index by the
  #   column name.
  # @raise [Vertica::Error::DuplicateColumnName] If the row description contains multiple
  #   columns with the same name
  def to_h(symbolize_keys: false)
    @columns.inject({}) do |carry, column|
      key = symbolize_keys ? column.name.to_sym : column.name
      raise Vertica::Error::DuplicateColumnName, "Column with name #{key} occurs more than once in this row description." if carry.key?(key)
      carry[key] = column
      carry
    end
  end

  # Builds a {Vertica::Row} instance given a list of values that conforms to
  # this row description.
  # @param [Array, Hash, Vertica::Protocol::DataRow] values A list of values. The number
  #   of values should match the number of columns in the row description.
  # @raise [ArgumentError] An ArgumentErrpr is raised if the number of values does not match
  #   the row description, or if a type is provided that it cannot handle.
  # @return [Vertica::Row] The row instance.
  def build_row(values)
    case values
    when Vertica::Row
      raise ArgumentError, "Row description of provided row does match this row description" if values.row_description != self
      values

    when Vertica::Protocol::DataRow
      raise ArgumentError, "Number of values does not match row description" if values.values.size != size
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

  # @return [Boolean] Returns true iff this record is equal to the other provided object
  def eql?(other)
    self.class === other && other.to_a == self.to_a
  end

  alias_method :==, :eql?

  # @return [Integer]  Returns a hash digtest of this object.
  def hash
    @columns.hash
  end

  # Returns a user-consumable string representation of this row description.
  # @return [String]
  def inspect
    "<Vertica::RowDescription[#{@columns.map(&:name).join(', ')}]>"
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
