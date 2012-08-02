class Vertica::Query

  attr_reader :connection, :sql, :result, :error
  attr_accessor :row_handler, :copy_handler, :row_style

  def initialize(connection, sql, options = {})
    @connection, @sql = connection, sql
    
    @row_style    = options[:row_style] || @connection.row_style || :hash
    @row_handler  = options[:row_handler] 
    @copy_handler = options[:copy_handler]

    @error  = nil
    @result = Vertica::Result.new(row_style)
  end
  
  
  def run
    @connection.write Vertica::Messages::Query.new(sql)
    
    begin
      process_message(message = @connection.read_message)
    end until message.kind_of?(Vertica::Messages::ReadyForQuery)

    raise error unless error.nil?
    return result
  end
  
  def write(data)
    @connection.write Vertica::Messages::CopyData.new(data)
    return self
  end
  
  alias_method :<<, :write
  
  protected
  
  def process_message(message)
    case message
    when Vertica::Messages::ErrorResponse
      @error = Vertica::Error::QueryError.from_error_response(message, @sql)
    when Vertica::Messages::EmptyQueryResponse
      @error = Vertica::Error::EmptyQueryError.new("A SQL string was expected, but the given string was blank or only contained SQL comments.")
    when Vertica::Messages::CopyInResponse
      handle_copy_from_stdin
    when Vertica::Messages::RowDescription
      result.descriptions = message
    when Vertica::Messages::DataRow
      handle_datarow(message)
    when Vertica::Messages::CommandComplete
      result.tag = message.tag
    else
      @connection.process_message(message)
    end
  end
  
  def handle_copy_from_stdin
    if copy_handler.nil?
      @connection.write Vertica::Messages::CopyFail.new('no handler provided')
    else
      begin
        if copy_handler.call(self) == :rollback
          @connection.write Vertica::Messages::CopyFail.new("rollback")
        else
          @connection.write Vertica::Messages::CopyDone.new
        end
      rescue => e
        @connection.write Vertica::Messages::CopyFail.new(e.message)
        raise
      end
    end
  end

  def handle_datarow(datarow_message)
    record = result.format_row(datarow_message)
    result.add_row(record) if buffer_rows?
    row_handler.call(record) if row_handler
  end
  
  def buffer_rows?
    row_handler.nil? && copy_handler.nil?
  end
end
