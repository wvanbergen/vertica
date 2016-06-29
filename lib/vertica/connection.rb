require 'socket'

class Vertica::Connection

  attr_reader :transaction_status, :backend_pid, :backend_key, :parameters, :notice_handler, :session_id

  attr_reader :options

  def self.cancel(existing_conn)
    existing_conn.cancel
  end

  # Opens a connectio the a Vertica server
  # @param [Hash] options The connection options to use.
  def initialize(host: nil, port: 5433, username: nil, password: nil, database: nil, interruptable: false, ssl: nil, read_timeout: 600,row_style: :hash, debug: false, role: nil, search_path: nil, timezone: nil, skip_startup: false)
    reset_values
    @notice_handler = nil

    @options = {
      host: host,
      port: port.to_i,
      username: username,
      password: password,
      database: database,
      debug: debug,
      ssl: ssl,
      interruptable: interruptable,
      read_timeout: read_timeout,
      row_style: row_style,
      role: role,
      search_path: search_path,
      timezone: timezone
    }

    boot_connection unless skip_startup
  end

  def on_notice(&block)
    @notice_handler = block
  end

  def socket
    @socket ||= begin
      raw_socket = TCPSocket.new(@options[:host], @options[:port].to_i)
      if @options[:ssl]
        require 'openssl'
        raw_socket.write Vertica::Messages::SslRequest.new.to_bytes
        if raw_socket.read(1) == 'S'
          ssl_context = @options[:ssl].is_a?(OpenSSL::SSL::SSLContext) ? @options[:ssl] : OpenSSL::SSL::SSLContext.new
          raw_socket = OpenSSL::SSL::SSLSocket.new(raw_socket, ssl_context)
          raw_socket.sync = true
          raw_socket.connect
        else
          raise Vertica::Error::SSLNotSupported.new("SSL requested but server doesn't support it.")
        end
      end

      raw_socket
    end
  end

  def ssl?
    Object.const_defined?('OpenSSL') && @socket.kind_of?(OpenSSL::SSL::SSLSocket)
  end

  def opened?
    @socket && @backend_pid && @transaction_status
  end

  def closed?
    !opened?
  end

  def busy?
    @mutex.locked?
  end

  def ready_for_query?
    !busy?
  end

  def write_message(message)
    puts "=> #{message.inspect}" if options.fetch(:debug)
    write_bytes message.to_bytes
  rescue SystemCallError, IOError => e
    close_socket
    raise Vertica::Error::ConnectionError.new(e.message)
  end

  def close
    write_message Vertica::Messages::Terminate.new
  ensure
    close_socket
  end

  def close_socket
    socket.close
    @socket = nil
  rescue SystemCallError, IOError
  ensure
    reset_values
  end

  def reset_connection
    close
    boot_connection
  end

  def boot_connection
    startup_connection
    initialize_connection
  end

  def cancel
    conn = self.class.new(skip_startup: true, **options)
    conn.write_message Vertica::Messages::CancelRequest.new(backend_pid, backend_key)
    conn.write_message Vertica::Messages::Flush.new
    conn.socket.close
  end

  def interrupt
    raise Vertica::Error::InterruptImpossible, "Session cannopt be interrupted because the session ID is not known!" if session_id.nil?
    conn = self.class.new(options.merge(:interruptable => false, :role => nil, :search_path => nil))
    response = conn.query("SELECT CLOSE_SESSION(#{Vertica.quote(session_id)})").the_value
    conn.close
    return response
  end

  def interruptable?
    !session_id.nil?
  end

  def read_message
    type = read_bytes(1)
    size = read_bytes(4).unpack('N').first
    raise Vertica::Error::MessageError.new("Bad message size: #{size}.") unless size >= 4
    message = Vertica::Messages::BackendMessage.factory type, read_bytes(size - 4)
    puts "<= #{message.inspect}" if options.fetch(:debug)
    return message
  rescue SystemCallError, IOError => e
    close_socket
    raise Vertica::Error::ConnectionError.new(e.message)
  end

  def process_message(message)
    case message
    when Vertica::Messages::ErrorResponse
      raise Vertica::Error::ConnectionError.new(message.error_message)
    when Vertica::Messages::NoticeResponse
      @notice_handler.call(message) if @notice_handler
    when Vertica::Messages::BackendKeyData
      @backend_pid = message.pid
      @backend_key = message.key
    when Vertica::Messages::ParameterStatus
      @parameters[message.name] = message.value
    when Vertica::Messages::ReadyForQuery
      @transaction_status = message.transaction_status
      @mutex.unlock
    else
      raise Vertica::Error::MessageError, "Unhandled message: #{message.inspect}"
    end
  end

  def query(sql, options = {}, &block)
    job = Vertica::Query.new(self, sql, { :row_style => @options.fetch(:row_style) }.merge(options))
    job.row_handler = block if block_given?
    run_with_mutex(job)
  end

  def copy(sql, source = nil, &block)
    job = Vertica::Query.new(self, sql, :row_style => @options.fetch(:row_style))
    if block_given?
      job.copy_handler = block
    elsif source && File.exist?(source.to_s)
      job.copy_handler = lambda { |data| file_copy_handler(source, data) }
    elsif source.respond_to?(:read) && source.respond_to?(:eof?)
      job.copy_handler = lambda { |data| io_copy_handler(source, data) }
    end
    run_with_mutex(job)
  end

  def inspect
    safe_options = @options.reject{ |name, _| name == :password }
    "#<Vertica::Connection:#{object_id} @parameters=#{@parameters.inspect} @backend_pid=#{@backend_pid}, @backend_key=#{@backend_key}, @transaction_status=#{@transaction_status}, @socket=#{@socket}, @options=#{safe_options.inspect}, @row_style=#{@row_style}>"
  end

  protected

  def run_with_mutex(job)
    boot_connection if closed?
    if @mutex.try_lock
      begin
        job.run
      rescue StandardError
        @mutex.unlock if @mutex.locked?
        raise
      end
    else
      raise Vertica::Error::SynchronizeError.new(job)
    end
  end

  COPY_FROM_IO_BLOCK_SIZE = 1024 * 4096

  def file_copy_handler(input_file, output)
    File.open(input_file, 'r') do |input|
      while data = input.read(COPY_FROM_IO_BLOCK_SIZE)
        output << data
      end
    end
  end

  def io_copy_handler(input, output)
    until input.eof?
      output << input.read(COPY_FROM_IO_BLOCK_SIZE)
    end
  end

  def read_bytes(n)
    bytes = ""
    until bytes.length == n
      begin
        bytes << socket.read_nonblock(n - bytes.length)
      rescue IO::WaitReadable, IO::WaitWritable => wait_error
        io_select(wait_error)
        retry
      end
    end
    bytes
  end

  def write_bytes(bytes)
    bytes_written = socket.write_nonblock(bytes)
    write_bytes(bytes[bytes_written...bytes.size]) if bytes_written < bytes.size
  rescue IO::WaitReadable, IO::WaitWritable => wait_error
    io_select(wait_error)
    retry
  end

  def io_select(exception)
    readers, writers = nil, nil
    readers = [socket] if exception.is_a?(IO::WaitReadable)
    writers = [socket] if exception.is_a?(IO::WaitWritable)
    if IO.select(readers, writers, nil, @options[:read_timeout]).nil?
      close
      raise Vertica::Error::TimedOutError.new("Connection timed out.")
    end
  end

  def startup_connection
    write_message(Vertica::Messages::Startup.new(@options[:username], @options[:database]))
    message = nil
    begin
      case message = read_message
      when Vertica::Messages::Authentication
        if message.code != Vertica::Messages::Authentication::OK
          write_message(Vertica::Messages::Password.new(@options[:password], message.code, {:username => @options[:username], :salt => message.salt}))
        end
      else
        process_message(message)
      end
    end until message.kind_of?(Vertica::Messages::ReadyForQuery)
  end

  def initialize_connection
    query("SET SEARCH_PATH TO #{options[:search_path]}") if options[:search_path]
    query("SET ROLE #{options[:role]}") if options[:role]
    @session_id = query("SELECT session_id FROM v_monitor.current_session").the_value if options[:interruptable]
  end

  def reset_values
    @parameters         = {}
    @session_id         = nil
    @backend_pid        = nil
    @backend_key        = nil
    @transaction_status = nil
    @socket             = nil
    @mutex              = Mutex.new.lock
  end
end

require 'vertica/query'
require 'vertica/column'
require 'vertica/result'
require 'vertica/messages/message'
