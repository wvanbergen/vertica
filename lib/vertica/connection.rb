require 'socket'

# A client for a Vertica server, which allows you to run queries against it.
#
# Use {Vertica.connect} to establish a connection. Then, the {#query} method will allow you
# to run SQL queries against the database. For `COPY FROM STDIN` queries, use the {#copy} method
# instead. You can use {#interrupt} to interrupt long running queries. {#close} will close the
# connection to the server.
#
# @attr_reader transaction_status [:no_transaction, :in_transaction, :failed_transaction] The current
#   transaction state of the session. This will be updated after every query.
# @attr_reader parameters [Hash<String, String>] Connection parameters as provided by the server.
# @attr_reader options [Hash] The connection options provided to the constructor. See {#initialize}.
#
# @example Running a buffered query against the database
#   connection = Vertica.connect(host: 'db_server', username: 'user', password: 'password', ...)
#   result = connection.query("SELECT id, name FROM my_table")
#   result.each do |row|
#     puts "Row: #row['id']: #{row['name']}"
#   end
#   connection.close
#
# @see Vertica.connect
# @see Vertica::Result
class Vertica::Connection

  attr_reader :transaction_status, :parameters, :options

  # Creates a connection the a Vertica server.
  # @param host [String] The hostname to connect to. E.g. `localhost`
  # @param port [Integer] The port to connect to. Defaults to `5433`.
  # @param username [String] The username for the session.
  # @param password [String] The password for the session.
  # @param interruptable [true, false] Whether to make this session interruptible. Setting this to true
  #   allows you to interrupt sessions and queries, but requires running a query during startup in order
  #   to obtain the session id.
  # @param ssl [OpenSSL::SSL::SSLContext, Boolean] Set this to an OpenSSL::SSL::SSLContext instance to
  #   require the connection to be encrypted using SSL/TLS. `true` will use the default SSL options.
  #   Not every server has support for SSL encryption. In that case you'll have to leave this to false.
  # @param read_timeout [Integer] The number of seconds to wait for data on the connection. You should
  #   set this to a sufficiently high value when executing complicated queries that require a long time
  #   to be evaluated.
  # @param role [Array<String>, :all, :none, :default] A list of additional roles to enable for the session. See the
  #   [Vertica documentation for `SET ROLE`](https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/SQLReferenceManual/Statements/SET/SETROLE.htm).
  # @param timezone [String] The timezone to use for the session. See the
  #   [Vertica documentation for `SET TIME ZONE`](https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/SQLReferenceManual/Statements/SET/SETTIMEZONE.htm).
  # @param search_path [Array<String>] A list of schemas to use as search path. See the
  #   [Vertica documentation for `SET SEARCH_PATH`](https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/SQLReferenceManual/Statements/SET/SETSEARCH_PATH.htm).
  # @param autocommit [Boolean] Enable autocommit on the session. See [the Vertica documentation](https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/ConnectingToHPVertica/vsql/AUTOCOMMIT.htm)
  #   for more information.
  # @param debug [Boolean] Setting this to true will log all the communication between client and server
  #   to STDOUT. Useful when developing this library.
  def initialize(host: nil, port: 5433, username: nil, password: nil, database: nil, interruptable: false, ssl: false, read_timeout: 600, debug: false, role: nil, search_path: nil, timezone: nil, autocommit: false, skip_startup: false, skip_initialize: false, user: nil)
    reset_state
    @notice_handler = nil

    @options = {
      host: host,
      port: port.to_i,
      username: username || user,
      password: password,
      database: database,
      debug: debug,
      ssl: ssl,
      interruptable: interruptable,
      read_timeout: read_timeout,
      role: role,
      search_path: search_path,
      timezone: timezone,
      autocommit: autocommit,
    }

    boot_connection(skip_initialize: skip_initialize) unless skip_startup
  end

  # @return [Boolean] Returns true iff the connection is encrypted.
  def ssl?
    Object.const_defined?('OpenSSL') && @socket.kind_of?(OpenSSL::SSL::SSLSocket)
  end

  # @return [Boolean] Returns true iff the connection to the server is opened.
  # @note The connection will be opened automatically if you use it.
  def opened?
    @socket && @backend_pid && @transaction_status
  end

  # @return [Boolean] Returns false iff the connection to the server is opened.
  # @note Even if the connection is closed, it will be opened automatically if you use it.
  def closed?
    !opened?
  end

  # @return [Boolean] Returns true iff the connection is in use.
  def busy?
    @mutex.locked?
  end

  # @return [Boolean] Returns true iff the connection is ready to handle queries.
  def ready_for_query?
    !busy?
  end

  # Returns true iff the connection can be interrupted.
  #
  # Connections can only be interrupted if the session ID is known, so it can
  # run `SELECT CLOSE_SESSION(session_id)` using a separate connection. By passing
  # `interruptable: true` as a connection parameter (see {#initialize}), the connection
  # will discover its session id before you can use it, allowing it to be interrupted.
  #
  # @return [Boolean] Returns true iff the connection can be interrupted.
  # @see {#interrupt}
  def interruptable?
    !session_id.nil?
  end

  # Runs a SQL query against the database.
  #
  # @overload query(sql)
  #   Runs a query against the database, and return the full result as a {Vertica::Result}
  #   instance.
  #
  #   @note This requires the entire result to be buffered in memory, which may cause problems
  #     for queries with large results. Consider using the unbuffered version instead.
  #
  #   @param sql [String] The SQL command to run.
  #   @return [Vertica::Result]
  #   @raise [Vertica::Error::ConnectionError] The connection to the server failed.
  #   @raise [Vertica::Error::QueryError] The server sent an error response indicating that
  #     the provided query cannot be evaluated.
  #
  # @overload query(sql, &block)
  #   Runs a query against the database, and yield every {Vertica::Row row} to the provided
  #   block.
  #
  #   @param sql [String] The SQL command to run.
  #   @yield The provided block will be called for every row in the result.
  #   @yieldparam row [Vertica::Row]
  #   @return [String] The kind of command that was executed, e.g. `"SELECT"`.
  #   @raise [Vertica::Error::ConnectionError] The connection to the server failed.
  #   @raise [Vertica::Error::QueryError] The server sent an error response indicating that
  #     the provided query cannot be evaluated.
  #
  # @see https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/SQLReferenceManual/Statements/SELECT/SELECT.htm
  #   Vertica's documentation for SELECT.
  def query(sql, &block)
    run_in_mutex(Vertica::Query.new(self, sql, row_handler: block))
  end

  # Loads data into Vertica using a `COPY table FROM STDIN` query.
  #
  # @param sql [String] The `COPY ... FROM STDIN` SQL command to run.
  # @param source [String, IO] The source of the data to be copied. This can either be a filename, or
  #   an IO object. If you don't specify a source, you'll need to provide a block that will provide the
  #   data to be copied.
  # @yield A block that will be called with a writer that you can provided data to. If an exception is
  #   raised in the block, the `COPY` command will be cancelled.
  # @yieldparam io [:write] An object that you can call write on to provide data to be loaded.
  # @return [String] The kind of command that was executed on the server. This should always be `"COPY"`.
  #
  # @example Loading data using an IO object as source
  #   connection = Vertica.connect(host: 'db_server', username: 'user', password: 'password', ...)
  #   File.open("filename.csv", "r") do |io|
  #     connection.copy("COPY my_table FROM STDIN ...", source: io)
  #   end
  #
  # @example Loading data using a filename as source
  #   connection = Vertica.connect(host: 'db_server', username: 'user', password: 'password', ...)
  #   connection.copy("COPY my_table FROM STDIN ...", source: "filename.csv")
  #
  # @example Loading data using a callback
  #   connection = Vertica.connect(host: 'db_server', username: 'user', password: 'password', ...)
  #   connection.copy("COPY my_table FROM STDIN ...") do |io|
  #     io.write("my data")
  #     io.write("more data")
  #   end
  #
  # @see https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/SQLReferenceManual/Statements/COPY/COPY.htm
  #   Vertica's documentation for COPY.
  def copy(sql, source: nil, &block)
    copy_handler = if block_given?
      block
    elsif source && File.exist?(source.to_s)
      lambda { |data| file_copy_handler(source, data) }
    elsif source.respond_to?(:read) && source.respond_to?(:eof?)
      lambda { |data| io_copy_handler(source, data) }
    end

    run_in_mutex(Vertica::Query.new(self, sql, copy_handler: copy_handler))
  end

  # Returns a user-consumable string representation of this row.
  # @return [String]
  def inspect
    safe_options = @options.reject { |name, _| name == :password }
    "#<Vertica::Connection:#{object_id} @parameters=#{@parameters.inspect} @backend_pid=#{@backend_pid}, @backend_key=#{@backend_key}, @transaction_status=#{@transaction_status}, @socket=#{@socket}, @options=#{safe_options.inspect}>"
  end

  # Closes the connection to the Vertica server.
  # @return [void]
  def close
    write_message(Vertica::Protocol::Terminate.new)
  ensure
    close_socket
  end

  # Cancels the current query.
  #
  # @note Vertica's protocol is based on the PostgreSQL protocol. This method to cancel sessions
  #   in PostgreSQL is accepted by the Vertica server, but I haven't actually observed queries
  #   actually being cancelled when using this method. Vertica provides an alternative method, by
  #   running `SELECT CLOSE_SESSION(session_id)` as a query on a different connection. See {#interrupt}.
  #
  # @return [void]
  # @see #interrupt
  def cancel
    conn = self.class.new(skip_startup: true, **options)
    conn.write_message(Vertica::Protocol::CancelRequest.new(backend_pid, backend_key))
    conn.write_message(Vertica::Protocol::Flush.new)
    conn.close_socket
  end

  # Interrupts this session to the Vertica server, which will cancel the running query.
  #
  # You'll have to call this method in a separate thread. It will open up a separate connection, and run
  # `SELECT CLOSE_SESSION(current_session_id)` to close the current session. In order to be able to do this
  # the client needs to know its session ID. You'll have to pass `interruptable: true` as a connection
  # parameter (see {#initialize}) to make sure the connection will request its session id, by running
  # `SELECT session_id FROM v_monitor.current_session` right after the connection is opened.
  #
  # @return [void]
  # @see #interruptable?
  # @see https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/SQLReferenceManual/Functions/VerticaFunctions/CLOSE_SESSION.htm
  #   Vertica's documentation for CLOSE_SESSION
  def interrupt
    raise Vertica::Error::InterruptImpossible, "Session cannopt be interrupted because the session ID is not known!" if session_id.nil?
    conn = self.class.new(skip_initialize: true, **options)
    conn.query("SELECT CLOSE_SESSION(#{Vertica.quote(session_id)})").the_value
  ensure
    conn.close if conn
  end

  # Installs a hanlder for notices that may be sent from the server to the client.
  #
  # You can only install one handler; if you call this method again it will replace the
  # previous handler.
  #
  # @return [void]
  def on_notice(&block)
    @notice_handler = block
  end

  # Writes a frontend message to the socket.
  # @note This method is for internal use only; you should not call it directly.
  # @return [void]
  # @raise [Vertica::Error::ConnectionError]
  # @private
  def write_message(message)
    puts "=> #{message.inspect}" if options.fetch(:debug)
    write_bytes(message.to_bytes)
  rescue SystemCallError, IOError => e
    close_socket
    raise Vertica::Error::ConnectionError.new(e.message)
  end

  # Reads a backend message from the socket.
  # @note This method is for internal use only; you should not call it directly.
  # @return [Vertica::Protocol::BackendMessage]
  # @raise [Vertica::Error::ConnectionError]
  # @private
  def read_message
    type, size = read_bytes(5).unpack('aN')
    puts "type is #{type}"
    raise Vertica::Error::MessageError.new("Bad message size: #{size}.") unless size >= 4
    message = Vertica::Protocol::BackendMessage.factory(type, read_bytes(size - 4))
    puts "<= #{message.inspect}" if options.fetch(:debug)
    return message
  rescue SystemCallError, IOError => e
    close_socket
    raise Vertica::Error::ConnectionError.new(e.message)
  end

  # Processes a backend message that was received from the socket.
  # @note This method is for internal use only; you should not call it directly.
  # @return [void]
  # @private
  def process_message(message)
    case message
    when Vertica::Protocol::ErrorResponse
      raise Vertica::Error::ConnectionError.new(message.error_message)
    when Vertica::Protocol::NoticeResponse
      @notice_handler.call(message) if @notice_handler
    when Vertica::Protocol::BackendKeyData
      puts message.pid, message.key
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

  attr_reader :backend_pid, :backend_key,  :session_id

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

  def run_in_mutex(job)
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
    initialize_connection_with_timezone
    initialize_connection_with_autocommit
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

  def initialize_connection_with_timezone
    query("SET TIME ZONE TO #{Vertica.quote(options[:timezone])}") if options[:timezone]
  end

  def initialize_connection_with_autocommit
    query("SET AUTOCOMMIT TO ON") if options[:autocommit]
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

  def boot_connection(skip_initialize: false)
    startup_connection
    initialize_connection unless skip_initialize
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
