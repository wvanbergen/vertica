require 'socket'

class Vertica::Connection

  attr_reader :transaction_status, :backend_pid, :backend_key, :parameters, :notice_handler, :session_id

  attr_reader :options

  def self.cancel(existing_conn)
    existing_conn.cancel
  end

  # Opens a connectio the a Vertica server
  # @param [Hash] options The connection options to use.
  def initialize(host: nil, port: 5433, username: nil, password: nil, database: nil, interruptable: false, ssl: nil, read_timeout: 600, debug: false, role: nil, search_path: nil, timezone: nil, skip_startup: false)
    reset_state
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
      role: role,
      search_path: search_path,
      timezone: timezone
    }

    boot_connection unless skip_startup
  end

  def on_notice(&block)
    @notice_handler = block
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

  def interruptable?
    !session_id.nil?
  end

  def query(sql, **kwargs, &block)
    row_handler = block_given? ? block : nil
    job = Vertica::Query.new(self, sql, row_handler: row_handler, **kwargs)
    run_with_mutex(job)
  end

  def copy(sql, source: nil, **kwargs, &block)
    copy_handler = if block_given?
      block
    elsif source && File.exist?(source.to_s)
      lambda { |data| file_copy_handler(source, data) }
    elsif source.respond_to?(:read) && source.respond_to?(:eof?)
      lambda { |data| io_copy_handler(source, data) }
    end

    job = Vertica::Query.new(self, sql, copy_handler: copy_handler, **kwargs)

    run_with_mutex(job)
  end

  def inspect
    safe_options = @options.reject { |name, _| name == :password }
    "#<Vertica::Connection:#{object_id} @parameters=#{@parameters.inspect} @backend_pid=#{@backend_pid}, @backend_key=#{@backend_key}, @transaction_status=#{@transaction_status}, @socket=#{@socket}, @options=#{safe_options.inspect}>"
  end

  def close
    write_message(Vertica::Protocol::Terminate.new)
  ensure
    close_socket
  end

  def cancel
    conn = self.class.new(skip_startup: true, **options)
    conn.write_message(Vertica::Protocol::CancelRequest.new(backend_pid, backend_key))
    conn.write_message(Vertica::Protocol::Flush.new)
    conn.close_socket
  end

  def interrupt
    raise Vertica::Error::InterruptImpossible, "Session cannopt be interrupted because the session ID is not known!" if session_id.nil?
    conn = self.class.new(options.merge(:interruptable => false, :role => nil, :search_path => nil))
    response = conn.query("SELECT CLOSE_SESSION(#{Vertica.quote(session_id)})").the_value
    conn.close
    return response
  end

  # @private
  def write_message(message)
    puts "=> #{message.inspect}" if options.fetch(:debug)
    write_bytes(message.to_bytes)
  rescue SystemCallError, IOError => e
    close_socket
    raise Vertica::Error::ConnectionError.new(e.message)
  end

  # @private
  def read_message
    type, size = read_bytes(5).unpack('aN')
    raise Vertica::Error::MessageError.new("Bad message size: #{size}.") unless size >= 4
    message = Vertica::Protocol::BackendMessage.factory(type, read_bytes(size - 4))
    puts "<= #{message.inspect}" if options.fetch(:debug)
    return message
  rescue SystemCallError, IOError => e
    close_socket
    raise Vertica::Error::ConnectionError.new(e.message)
  end

  # @private
  def process_message(message)
    case message
    when Vertica::Protocol::ErrorResponse
      raise Vertica::Error::ConnectionError.new(message.error_message)
    when Vertica::Protocol::NoticeResponse
      @notice_handler.call(message) if @notice_handler
    when Vertica::Protocol::BackendKeyData
      @backend_pid = message.pid
      @backend_key = message.key
    when Vertica::Protocol::ParameterStatus
      @parameters[message.name] = message.value
    when Vertica::Protocol::ReadyForQuery
      @transaction_status = message.transaction_status
      @mutex.unlock
    else
      raise Vertica::Error::MessageError, "Unhandled message: #{message.inspect}"
    end
  end

  protected

  def socket
    @socket ||= begin
      raw_socket = TCPSocket.new(@options[:host], @options[:port].to_i)
      if @options[:ssl]
        require 'openssl'
        raw_socket.write(Vertica::Protocol::SslRequest.new.to_bytes)
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

  DEFAULT_IO_COPY_HANDLER_BLOCK_SIZE = 1024 * 4096
  private_constant :DEFAULT_IO_COPY_HANDLER_BLOCK_SIZE

  def file_copy_handler(input_file, output)
    File.open(input_file, 'r') do |input|
      io_copy_handler(input, output)
    end
  end

  def io_copy_handler(input, output)
    until input.eof?
      output << input.read(DEFAULT_IO_COPY_HANDLER_BLOCK_SIZE)
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
    write_message(Vertica::Protocol::Startup.new(@options[:username], @options[:database]))
    message = nil
    begin
      case message = read_message
      when Vertica::Protocol::Authentication
        if message.code != Vertica::Protocol::Authentication::OK
          write_message(Vertica::Protocol::Password.new(@options[:password], auth_method: message.code, user: @options[:username], salt: message.salt))
        end
      else
        process_message(message)
      end
    end until message.kind_of?(Vertica::Protocol::ReadyForQuery)
  end

  def initialize_connection
    @session_id = query("SELECT session_id FROM v_monitor.current_session").the_value if options[:interruptable]
    initialize_connection_with_role
    initialize_connection_with_search_path
  end

  def initialize_connection_with_role
    case options[:role]
    when :all, :none, :default
      query("SET ROLE #{options[:role].to_s.upcase}")
    when String, Array
      query("SET ROLE #{Vertica.quote(options[:role])}")
    end
  end

  def initialize_connection_with_search_path
    query("SET SEARCH_PATH TO #{Vertica.quote(options[:search_path])}") if options[:search_path]
  end

  def close_socket
    @socket.close if @socket
  rescue SystemCallError, IOError
    # ignore
  ensure
    reset_state
  end

  def reset_connection
    close
    boot_connection
  end

  def boot_connection
    startup_connection
    initialize_connection
  end

  def reset_state
    @parameters         = {}
    @session_id         = nil
    @backend_pid        = nil
    @backend_key        = nil
    @transaction_status = nil
    @socket             = nil
    @mutex              = Mutex.new.lock
  end
end
