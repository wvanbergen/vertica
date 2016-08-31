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
    @connection.write_message(Vertica::Protocol::Flush.new)

    begin
      process_message(message = @connection.read_message)
    end until message.kind_of?(Vertica::Protocol::RowDescription) || @error

    raise @error unless @error.nil?

    self
  end

  def execute(parameter_values)
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
