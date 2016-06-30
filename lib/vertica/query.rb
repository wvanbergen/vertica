class Vertica::Query

  attr_reader :connection, :sql, :error, :result

  def initialize(connection, sql, row_style: nil, row_handler: nil, copy_handler: nil)
    @connection, @sql = connection, sql
    row_style ||= connection.options.fetch(:row_style, :hash)
    @format_row_method = :"format_row_as_#{row_style}"
    @rows = [] if row_handler.nil?
    @row_handler = row_handler || lambda { |row| buffer_row(row) }
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

  protected

  def buffer_rows?
    @rows.is_a?(Array)
  end

  def process_message(message)
    case message
    when Vertica::Protocol::ErrorResponse
      @error = Vertica::Error::QueryError.from_error_response(message, @sql)
    when Vertica::Protocol::EmptyQueryResponse
      @error = Vertica::Error::EmptyQueryError.new("A SQL string was expected, but the given string was blank or only contained SQL comments.")
    when Vertica::Protocol::CopyInResponse
      handle_copy_in_response(message)
    when Vertica::Protocol::RowDescription
      handle_row_description(message)
    when Vertica::Protocol::DataRow
      handle_data_row(message)
    when Vertica::Protocol::CommandComplete
      handle_command_complete(message)
    else
      @connection.process_message(message)
    end
  end

  def handle_row_description(message)
    @columns = message.fields.map { |fd| Vertica::Column.new(fd) }
  end

  def handle_data_row(message)
    @row_handler.call(send(@format_row_method, message))
  end

  def handle_command_complete(message)
    if buffer_rows?
      @result = Vertica::Result.new(columns: @columns, rows: @rows, tag: message.tag)
      @columns, @rows = nil, nil
    else
      @result = message.tag
    end
  end

  def handle_copy_in_response(_message)
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

  def format_row_as_array(message)
    row = []
    message.values.each_with_index do |value, idx|
      row << @columns.fetch(idx).convert(value)
    end
    row
  end

  def format_row_as_hash(message)
    row = {}
    message.values.each_with_index do |value, idx|
      col = @columns.fetch(idx)
      row[col.name] = col.convert(value)
    end
    row
  end

  def buffer_row(row)
    @rows << row
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
end
