# Class to represent a row returns by a query.
#
# Row instances can either be yielded to the block passed to {Vertica::Connection#query},
# or be part of a buffered {Vertica::Result} returned by {Vertica::Connection#query}.
#
# @attr_reader row_description [Vertica::RowDescription] The ordered list of columns this
#   row conforms to.
#
# @see Vertica::RowDescription#build_row
class Vertica::Row
  include Enumerable

  attr_reader :row_description

  # Initializes a new row instance for a given row description and values. The
  # number of values MUST match the number of columns in the row description.
  # @param row_description [Vertica::RowDescription] The row description the
  #   values will conform to.
  # @param values [Array] The values for the columns in the row description
  # @see Vertica::RowDescription#build_row
  def initialize(row_description, values)
    @row_description, @values = row_description, values
  end

  # Iterates over the values in this row.
  # @yield [value] The provided block will get called with the values in the order of
  #   the columns in the row_description.
  def each(&block)
    @values.each(&block)
  end

  # Fetches a value from this row.
  # @param name_or_index [Symbol, String, Integer] The name of the column as string or symbol,
  #   or the index of the column in the row description.
  # @raise KeyError A KeyError is raised if the field connot be found.
  def fetch(name_or_index)
    @values.fetch(column_index(name_or_index))
  end

  alias_method :[], :fetch

  # Returns an array representation of the row. The values will
  # be ordered like the order of the fields in the {#row_description}.
  # @return [Array]
  def to_a
    @values
  end

  # Returns a hash representation of this rows, using the name of the
  # fields as keys.
  # @param symbolize_keys [true, false] Set to true to use symbols instead
  #   of strings for the field names.
  # @return [Hash]
  # @raise [Vertica::Error::DuplicateColumnName] If the row contains multiple
  #   columns with the same name
  def to_h(symbolize_keys: false)
    @row_description.inject({}) do |carry, column|
      key = symbolize_keys ? column.name.to_sym : column.name
      raise Vertica::Error::DuplicateColumnName, "Column with name #{key} occurs more than once in this row." if carry.key?(key)
      carry[key] = fetch(column.name)
      carry
    end
  end

  # @return [Boolean] Returns true iff this record is equal to the other provided object
  def eql?(other)
    other.kind_of?(Vertica::Row) && other.row_description == row_description && other.to_a == to_a
  end

  alias_method :==, :eql?

  # @return [Integer] Returns a hash digtest of this object.
  def hash
    [row_description, values].hash
  end

  # Returns a user-consumable string representation of this row.
  # @return [String]
  def inspect
    "#<#{self.class.name}#{@values.inspect}>"
  end

  private

  def column(name_or_index)
    column_with_index(name_or_index).fetch(0)
  end

  def column_index(name_or_index)
    column_with_index(name_or_index).fetch(1)
  end

  def column_with_index(name_or_index)
    @row_description.column_with_index(name_or_index)
  end
end
