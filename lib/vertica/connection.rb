require 'socket'

class Vertica::Connection

  attr_reader :options, :notices, :transaction_status, :backend_pid, :backend_key, :parameters

  attr_accessor :row_style, :debug

  def self.cancel(existing_conn)
    conn = self.new(existing_conn.options.merge(:skip_startup => true))
    conn.write Vertica::Messages::CancelRequest.new(existing_conn.backend_pid, existing_conn.backend_key)
    conn.write Vertica::Messages::Flush.new
    conn.socket.close
  end

  # Opens a connectio the a Vertica server
  # @param [Hash] options The connection options to use.
  def initialize(options = {})
    reset_values

    @options = {}
    options.each { |key, value| @options[key.to_s.to_sym] = value }

    @notices = []

    @row_style = @options[:row_style] ? @options[:row_style] : :hash

    unless options[:skip_startup]
      write Vertica::Messages::Startup.new(@options[:user], @options[:database])
      process

      query("SET SEARCH_PATH TO #{options[:search_path]}") if options[:search_path]
      query("SET ROLE #{options[:role]}") if options[:role]
    end
  end

  def socket
    @socket ||= begin
      raw_socket = TCPSocket.new(@options[:host], @options[:port].to_s)
      if @options[:ssl]
        require 'openssl/ssl'
        raw_socket.write Vertica::Messages::SslRequest.new.to_bytes
        if raw_socket.read(1) == 'S'
          raw_socket = OpenSSL::SSL::SSLSocket.new(raw_socket, OpenSSL::SSL::SSLContext.new)
          raw_socket.sync = true
          raw_socket.connect
        else
          raise Error::ConnectionError.new("SSL requested but server doesn't support it.")
        end
      end
      
      raw_socket
    end
  end

  def ssl?
    socket.kind_of?(OpenSSL::SSL::SSLSocket)
  end

  def opened?
    @socket && @backend_pid && @transaction_status
  end

  def closed?
    !opened?
  end

  def write(message)
    raise ArgumentError, "invalid message: (#{message.inspect})" unless message.respond_to?(:to_bytes)
    puts "=> #{message.inspect}" if @debug
    socket.write message.to_bytes
  end

  def close
    write Vertica::Messages::Terminate.new
    socket.close
    @socket = nil
  rescue Errno::ENOTCONN # the backend closed the socket already
  ensure
    reset_values
  end

  def reset
    close if opened?
    reset_values
  end

  def query(query_string, &block)
    raise ArgumentError.new("Query string cannot be blank or empty.") if query_string.nil? || query_string.empty?
    reset_result
    write Vertica::Messages::Query.new(query_string)
    @process_row = block
    result = process(true)
    result unless @process_row
  end

  def prepare(name, query, params_count = 0)
    param_types = Array.new(params_count).fill(0)

    write Vertica::Messages::Parse.new(name, query, param_types)
    write Vertica::Messages::Describe.new(:prepared_statement, name)
    write Vertica::Messages::Sync.new
    write Vertica::Messages::Flush.new

    process
  end

  def execute_prepared(name, *param_values)
    portal_name = "" # use the unnamed portal
    max_rows    = 0  # return all rows

    reset_result

    write Vertica::Messages::Bind.new(portal_name, name, param_values)
    write Vertica::Messages::Execute.new(portal_name, max_rows)
    write Vertica::Messages::Sync.new

    result = process(true)

    # Close the portal
    write Vertica::Messages::Close.new(:portal, portal_name)
    write Vertica::Messages::Flush.new

    process

    # Return the result from the prepared statement
    result
  end

  protected


  def read_bytes(n)
    bytes = socket.read(n)
    raise Vertica::Error::ConnectionError.new("Couldn't read #{n} characters from socket.") if bytes.nil? || bytes.size != n
    return bytes
  end
  
  def read_message
    type = read_bytes(1)
    size = read_bytes(4).unpack('N').first
    raise Vertica::Error::MessageError.new("Bad message size: #{size}.") unless size >= 4
    msg = Vertica::Messages::BackendMessage.factory type, read_bytes(size - 4)
    puts "<= #{msg.inspect}" if @debug
    return msg
  end


  def process(return_result = false)
    result = return_result ? Vertica::Result.new(row_style) : nil
    loop do
      case message = read_message
      when Vertica::Messages::Authentication
        if message.code != Vertica::Messages::Authentication::OK
          write Vertica::Messages::Password.new(@options[:password], message.code, {:user => @options[:user], :salt => message.salt})
        end

      when Vertica::Messages::BackendKeyData
        @backend_pid = message.pid
        @backend_key = message.key

      when Vertica::Messages::DataRow
        @process_row.call(result.format_row(message)) if @process_row && result
        result.add_row(message) if result && !@process_row

      when Vertica::Messages::ErrorResponse
        error_class = result ? Vertica::Error::QueryError : Vertica::Error::ConnectionError
        raise error_class.new(message.error_message)


      when Vertica::Messages::NoticeResponse
        @notices << message.values

      when Vertica::Messages::ParameterStatus
        @parameters[message.name] = message.value

      when Vertica::Messages::ReadyForQuery
        @transaction_status = message.transaction_status
        break unless return_result

      when Vertica::Messages::RowDescription
        result.descriptions = message if result

      when Vertica::Messages::Unknown
        raise Error::MessageError.new("Unknown message type: #{message.message_id}")

      when  Vertica::Messages::BindComplete,
            Vertica::Messages::NoData,
            Vertica::Messages::EmptyQueryResponse,
            Vertica::Messages::ParameterDescription
        :nothing

      when  Vertica::Messages::CloseComplete,
            Vertica::Messages::CommandComplete,
            Vertica::Messages::ParseComplete,
            Vertica::Messages::PortalSuspended
        break
      end
    end

    result
  end

  def reset_values
    reset_result
    @parameters         = {}
    @backend_pid        = nil
    @backend_key        = nil
    @transaction_status = nil
    @socket             = nil
    @process_row        = nil
  end

  def reset_result
    @field_descriptions = []
    @field_values       = []
  end
end

require 'vertica/column'
require 'vertica/result'
require 'vertica/messages/message'
