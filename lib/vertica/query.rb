class Vertica::Query

  attr_reader :connection, :sql, :result, :error

  def initialize(connection, sql, row_style: nil, row_handler: nil, copy_handler: nil)
    @connection, @sql = connection, sql

    row_style ||= connection.options.fetch(:row_style, :hash)
    @result = case row_style
      when :hash;  Vertica::Result::HashResult.new(row_handler: row_handler)
      when :array; Vertica::Result::ArrayResult.new(row_handler: row_handler)
      else raise ArgumentError, "Unknown row_style: #{row_style.inspect}"
    end

    @copy_handler = copy_handler

    @error  = nil
  end

  def run
    @connection.write_message(Vertica::Protocol::Query.new(sql))

    begin
      process_message(message = @connection.read_message)
    end until message.kind_of?(Vertica::Protocol::ReadyForQuery)

    raise error unless error.nil?
    return result
  end

  def to_s
    @sql
  end


  class CopyFromStdinWriter
    def initialize(connection)
      @connection = connection
    end

    def write(data)
      @connection.write_message(Vertica::Protocol::CopyData.new(data))
      return self
    end

    alias_method :<<, :write
  end
  private_constant :CopyFromStdinWriter

  protected

  def process_message(message)
    case message
    when Vertica::Protocol::ErrorResponse
      @error = Vertica::Error::QueryError.from_error_response(message, @sql)
    when Vertica::Protocol::EmptyQueryResponse
      @error = Vertica::Error::EmptyQueryError.new("A SQL string was expected, but the given string was blank or only contained SQL comments.")
    when Vertica::Protocol::CopyInResponse
      handle_copy_from_stdin
    when Vertica::Protocol::RowDescription
      result.handle_row_description(message)
    when Vertica::Protocol::DataRow
      result.handle_data_row(message)
    when Vertica::Protocol::CommandComplete
      result.tag = message.tag
    else
      @connection.process_message(message)
    end
  end

  def handle_copy_from_stdin
    if @copy_handler.nil?
      @connection.write_message(Vertica::Protocol::CopyFail.new('no handler provided'))
    else
      begin
        @copy_handler.call(CopyFromStdinWriter.new(connection))
        @connection.write_message(Vertica::Protocol::CopyDone.new)
      rescue => e
        @connection.write_message(Vertica::Protocol::CopyFail.new(e.message))
      end
    end
  end
end
