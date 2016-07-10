# Class that represents a buffered resultset.
#
# This class implements the Enumerable interface for easy iteration through
# the rows. You can also address specific values in the result using {#fetch}.
# To leanr more about the shape of the result {#row_description} will give you
# an ordered list of all the {Vertica::Column}s in this result.
#
# @example Iterating over the rows
#    result = connection.query("SELECT name, id")
#    result.each do |row|
#      puts "#{row[:id]}: #{row[:name]}"
#    end
#
# @example Fetching specific values in the result
#    result = connection.query('SELECT id, name FROM people WHERE id = 1')
#    name = result.fetch(0, 'name')
#    id = result[0,0]
#
# @example Retrieving the only value in a result
#    average_salery = connection.query("SELECT AVG(salery) FROM employees").the_value
#
# @attr_reader [Vertica::RowDescription] row_description The columns in the result.
# @attr_reader [String] tag The kind of SQL command that produced this reuslt.
#
# @see Vertica::Connection#query
class Vertica::Result
  include Enumerable

  attr_reader :row_description, :tag

  # Initializes a new Vertica::Result instance.
  #
  # The constructor assumes that the row description, and the list of rows match up.
  # If you're unsure, use {Vertica::Result.build} which will assert this is the case.
  #
  # @param row_description [Vertica::RowDescription] The description of the rows
  # @param rows [Array<Vertica::Row>] The array of rows
  # @param tag [String] The kind of command that returned this result.
  # @see Vertica::Result.build
  def initialize(row_description: nil, rows: nil, tag: nil)
    @row_description, @rows, @tag = row_description, rows, tag
  end

  # Iterates through the resultset. This class also includes the `Enumerable`
  # interface, which means you can use all the `Enumerable` methods like `map`
  # and `inject`  as well.
  # @yield The provided block will be called for every row in the resutset.
  # @yieldparam row [Vertica::Row]
  # @return [void]
  def each(&block)
    @rows.each(&block)
  end

  # @return [Boolean] Returns `true` if the result has no rows.
  def empty?
    @rows.empty?
  end

  # @return [Integer] The number of rows in this result
  def size
    @rows.length
  end

  alias_method :count, :size
  alias_method :length, :size

  # Retrieves a row or value from the result.
  # @return [Vertica::Row, Object]
  #
  # @overload fetch(row)
  #   Returns a row from the result.
  #   @param row [Integer] The 0-indexed row number.
  #   @raise [IndexError] if the row index is out of bounds.
  #   @return [Vertica::Row]
  # @overload fetch(row, column)
  #   Returns a singular value from the result.
  #   @param row [Integer] The 0-indexed row number.
  #   @param col [Symbol, String, Integer] The name or index of the column.
  #   @raise [IndexError] if the row index is out of bounds.
  #   @raise [KeyError] if the requested column is not part of the result
  #   @return The value at the given row and column in the result.
  def fetch(row, col = nil)
    row = @rows.fetch(row)
    return row if col.nil?
    row.fetch(col)
  end

  alias_method :[], :fetch

  # Shorthand to return the value of a query that only returns a single value.
  # @return The first value of the first row, i.e. `fetch(0, 0)`.
  def value
    fetch(0, 0)
  end

  alias_method :the_value, :value

  alias_method :columns, :row_description

  # Builds a {Vertica::Result} from a row description and a list of compatible rows.
  # @param row_description An object that can be built into a {Vertica::RowDescription}.
  #   See {Vertica::RowDescription.build} for more info
  # @param rows [Array] An array of objects that can be turned into a {Vertica::Row}.
  #   See {Vertica::RowDescription#build_row} for more info
  # @param tag [String] The type of SQL command that yielded the result.
  # @return [Vertica::Result]
  def self.build(row_description: nil, rows: [], tag: nil)
    row_description = Vertica::RowDescription.build(row_description)
    rows = rows.map { |values| row_description.build_row(values) }
    new(row_description: row_description, rows: rows, tag: tag)
  end
end
