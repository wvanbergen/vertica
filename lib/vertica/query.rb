# The Query class handles the state of the connection while a SQL query is being executed.
# The connection should call {#run} and it will block until the query has been handled by
# the connection, after which control will be given back to the {Connection} instance.
#
# @note This class is for internal use only, you should never interact with this class directly.
#
# @see Vertica::Connection#query
# @see Vertica::Connection#copy
class Vertica::Query

  include Vertica::QueryProcessor

  # Instantiates a new query
  # @param connection [Vertica::Connection] The connection to use for the query
  # @param sql [String] The SQL statement to execute.
  # @param row_handler [Proc, nil] Callback that will be called for every row that is returned.
  #   If no handler is provided, all rows will be buffered so a {Vertica::Result} can be returned.
  #
  # @param copy_handler [Proc, nil] Callback that will be called when the connection is ready
  #   to receive data for a `COPY ... FROM STDIN` statement.
  def initialize(connection, sql, row_handler: nil, copy_handler: nil)
    @connection, @sql = connection, sql
    @buffer = row_handler.nil? && copy_handler.nil? ? [] : nil
    @row_handler = row_handler || lambda { |row| buffer_row(row) }
    @copy_handler = copy_handler
    @row_description, @error = nil, nil
  end

  # Sends the query to the server, and processes the results.
  # @return [String] For an unbuffered query, the type of SQL command will be return as a string
  #   (e.g. `"SELECT"` or `"COPY"`).
  # @return [Vertica::Result] For a buffered query, this method will return a {Vertica::Result} instance
  # @raise [Vertica::Error::ConnectionError] if the connection between client and
  #   server fails.
  # @raise [Vertica::Error::QueryError] if the server cannot evaluate the query.
  def run
    @connection.write_message(Vertica::Protocol::Query.new(@sql))

    process_backend_messages
  end

  # @return [String] Returns a user-consumable string representation of this query instance.
  def inspect
    "#<Vertica::Query:#{object_id} sql=#{@sql.inspect}>"
  end

  private

  def process_message(message)
    case message
      when Vertica::Protocol::RowDescription
        handle_row_description(message)
      when Vertica::Protocol::CommandComplete
        handle_command_complete(message)
      else
        super(message)
    end
  end

  def handle_row_description(message)
    @row_description = Vertica::RowDescription.build(message)
  end

  def handle_command_complete(message)
    if buffer_rows?
      complete_operation(message.tag)
    else
      @result = message.tag
    end
  end
end
