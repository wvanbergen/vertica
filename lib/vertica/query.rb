class Vertica::Query

  attr_reader :connection, :sql
  attr_accessor :row_handler, :copy_handler, :row_style

  def initialize(connection, sql, options = {})
    @connection, @sql = connection, sql
    
    @row_style    = options[:row_style] || @connection.row_style || :hash
    @row_handler  = options[:row_handler] 
    @copy_handler = options[:copy_handler]
  end
  
  
  def run
    @connection.write Vertica::Messages::Query.new(@sql)
    result, error = nil, nil
    begin
      case message = @connection.read_message
      when Vertica::Messages::ErrorResponse
        error = message.error_message
      when Vertica::Messages::EmptyQueryResponse
        error = "The provided query was empty."
      when Vertica::Messages::RowDescription, Vertica::Messages::CommandComplete
        result = retreive_result(message, Vertica::Result.new(row_style))
      else
        @connection.process_message(message)
      end
    end until message.kind_of?(Vertica::Messages::ReadyForQuery)
    
    raise Vertica::Error::QueryError, error unless error.nil?
    return result
  end
  
  def retreive_result(message, result)
    until message.kind_of?(Vertica::Messages::CommandComplete)
      case message
      when Vertica::Messages::RowDescription
        result.descriptions = message
      when Vertica::Messages::DataRow
        record = result.format_row(message)
        result.add_row(record) if buffer_rows?
        @row_handler.call(record) if @row_handler
      else
        raise "Unexpected message: #{message}"
      end
      message = @connection.read_message
    end
    result.tag = message.tag
    return result
  end
  
  def buffer_rows?
    @row_handler.nil? && @copy_handler.nil?
  end
end
