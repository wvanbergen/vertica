# The PreparedQueryExecutor class handles the state of the connection while a prepared query is being executed.
#
# @note This class is for internal use only, you should never interact with this class directly.
#
# @see Vertica::Connection#prepare
# @see Vertica::PreparedQuery#execute
class Vertica::PreparedQueryExecutor

  attr_reader :connection, :sql, :row_handler, :copy_handler
  attr_accessor :error, :result, :row_description, :buffer
 
  include Vertica::QueryProcessor

  def initialize(connection, sql, prepared_query_name, row_description, parameter_types, parameter_values, row_handler)
    @connection, @sql, @prepared_query_name, @row_description, @parameter_types, @parameter_values = 
      connection, sql, prepared_query_name, row_description, parameter_types, parameter_values
    @row_handler = row_handler || lambda { |row| buffer_row(row) }
    @copy_handler = nil
    @buffer = row_handler.nil? ? [] : nil
    @error = nil
  end

  def run
    @connection.write_message(Vertica::Protocol::Bind.new('', @prepared_query_name, @parameter_types, @parameter_values))
    @connection.write_message(Vertica::Protocol::Execute.new('', 0))
    @connection.write_message(Vertica::Protocol::Sync.new)
    @connection.write_message(Vertica::Protocol::Flush.new)

    process_backend_messages
  end

  def process_message(message)
    case message
      when Vertica::Protocol::BindComplete
        handle_bind_complete(message)
      when Vertica::Protocol::PortalSuspended
        handle_portal_suspended(message)
      else
        super(message)
    end
  end

  def handle_bind_complete(message)
  end

  def handle_portal_suspended(message)
    if buffer_rows?
      complete_operation('')
    else
      @result = nil
    end

  end
end