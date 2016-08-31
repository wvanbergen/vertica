# The PreparedQuery class defines a query with parameters, that can be executed multiple times, given 
# different parameters every time.
#
# @note This class is for internal use only, you should never interact with this class directly.
#
# @see Vertica::Connection#prepare
class Vertica::PreparedQuery

  # Instantiates a new prepared query
  # @param connection [Vertica::Connection] The connection to use for the query
  # @param sql [String] The SQL statement to execute.
  def initialize(connection, sql)
    @connection, @sql, @name = connection, sql, connection.next_prepared_query_name
  end

  # Sends the query to the server for preparation
  # @raise [Vertica::Error::ConnectionError] if the connection between client and
  #   server fails.
  # @raise [Vertica::Error::QueryError] if the server cannot evaluate the query.
  def run
    @connection.write_message(Vertica::Protocol::Parse.new(@name, @sql, []))
    @connection.write_message(Vertica::Protocol::Describe.new(:prepared_statement, @name))
    @connection.write_message(Vertica::Protocol::Sync.new)
    @connection.write_message(Vertica::Protocol::Flush.new)

    begin
      process_message(message = @connection.read_message)
    end until message.kind_of?(Vertica::Protocol::ReadyForQuery)

    raise @error unless @error.nil?

    self
  end

  # Runs a prepared query against the database.
  #
  # @overload execute(parameter_values)
  #   Runs a query against the database, and return the full result as a {Vertica::Result}
  #   instance.
  #
  #   @note This requires the entire result to be buffered in memory, which may cause problems
  #     for queries with large results. Consider using the unbuffered version instead.
  #
  #   @param parameter_values [Array<Object>] The values of the parameters to bind for the prepared query
  #   @return [Vertica::Result]
  #   @raise [Vertica::Error::ConnectionError] The connection to the server failed.
  #   @raise [Vertica::Error::QueryError] The server sent an error response indicating that
  #     the provided query cannot be evaluated.
  #
  # @overload execute(parameter_values, &block)
  #   Runs a query against the database, and yield every {Vertica::Row row} to the provided
  #   block.
  #
  #   @param parameter_values [Array<Object>] The values of the parameters to bind for the prepared query
  #   @yield The provided block will be called for every row in the result.
  #   @yieldparam row [Vertica::Row]
  #   @return [void]
  #   @raise [Vertica::Error::ConnectionError] The connection to the server failed.
  #   @raise [Vertica::Error::QueryError] The server sent an error response indicating that
  #     the provided query cannot be evaluated.
  #
  def execute(*parameter_values, &block)
    @connection.send(:run_in_mutex, 
      Vertica::PreparedQueryExecutor.new(@connection, @name, @row_description, @parameter_types, parameter_values, block)
    )
  end
  
  private

  def process_message(message)
    case message
    when Vertica::Protocol::ErrorResponse
      @error = Vertica::Error::QueryError.from_error_response(message, @sql)
    when Vertica::Protocol::NoData
      @error = Vertica::Error::EmptyQueryError.new("A SQL string was expected, but the given string was blank or only contained SQL comments.")
    when Vertica::Protocol::RowDescription
      handle_row_description(message)
    when Vertica::Protocol::ParameterDescription
      handle_parameter_description(message)
    when Vertica::Protocol::ParseComplete
    else
      @connection.process_message(message)
    end
  end

  def handle_row_description(message)
    @row_description = Vertica::RowDescription.build(message)
  end

  def handle_parameter_description(message)
    @parameter_types = message.parameter_types
  end

end
