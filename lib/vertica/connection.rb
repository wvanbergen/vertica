module Vertica
  class Connection

    STATUSES = {
      'I' => :no_transaction,
      'T' => :in_transaction,
      'E' => :failed_transaction
    }

    attr_reader :options, :notices, :transaction_status, :backend_pid, :backend_key, :notifications, :parameters

    attr_accessor :row_style

    def self.cancel(existing_conn)
      conn = self.new(existing_conn.options.merge(:skip_startup => true))
      conn.write Messages::CancelRequest.new(existing_conn.backend_pid, existing_conn.backend_key)
      conn.write Messages::Flush.new
      conn.socket.close
    end

    def initialize(options = {})
      reset_values

      @options = options
      @notices = []
      
      @row_style = @options[:row_style] ? @options[:row_style] : :hash

      unless options[:skip_startup]
        write Messages::Startup.new(@options[:user], @options[:database])
        process
        
        query("SET SEARCH_PATH TO #{options[:search_path]}") if options[:search_path]
        query("SET ROLE #{options[:role]}") if options[:role]
      end
    end

    def socket
      @socket ||= begin
        conn = TCPSocket.new(@options[:host], @options[:port].to_s)
        if @options[:ssl]
          conn.write Messages::SslRequest.new.to_bytes
          if conn.read(1) == 'S'
            conn = OpenSSL::SSL::SSLSocket.new(conn, OpenSSL::SSL::SSLContext.new)
            conn.sync = true
            conn.connect
          else
            raise Error::ConnectionError.new("SSL requested but server doesn't support it.")
          end
        end
        
        conn
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
      socket.write message.to_bytes
    end

    def close
      write Messages::Terminate.new
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
      write Messages::Query.new(query_string)
      @process_row = block
      result = process(true)
      result unless @process_row
    end

    def prepare(name, query, params_count = 0)
      param_types = Array.new(params_count).fill(0)

      write Messages::Parse.new(name, query, param_types)
      write Messages::Describe.new(:prepared_statement, name)
      write Messages::Sync.new
      write Messages::Flush.new

      process
    end

    def execute_prepared(name, *param_values)
      portal_name = "" # use the unnamed portal
      max_rows    = 0  # return all rows

      reset_result

      write Messages::Bind.new(portal_name, name, param_values)
      write Messages::Execute.new(portal_name, max_rows)
      write Messages::Sync.new

      result = process(true)

      # Close the portal
      write Messages::Close.new(:portal, portal_name)
      write Messages::Flush.new

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
      Messages::BackendMessage.factory type, read_bytes(size - 4)
    end


    def process(return_result = false)
      result = return_result ? Result.new(row_style) : nil
      loop do
        case message = read_message
        when Messages::Authentication
          if message.code != Messages::Authentication::OK
            write Messages::Password.new(@options[:password], message.code, {:user => @options[:user], :salt => message.salt})
          end

        when Messages::BackendKeyData
          @backend_pid = message.pid
          @backend_key = message.key

        when Messages::DataRow
          @process_row.call(result.format_row(message)) if @process_row && result
          result.add_row(message) if result && !@process_row

        when Messages::ErrorResponse
          error_class = result ? Vertica::Error::QueryError : Vertica::Error::ConnectionError
          raise error_class.new(message.error_message)
  

        when Messages::NoticeResponse
          @notices << message.values

        when Messages::NotificationResponse
          @notifications << Notification.new(message.pid, message.condition, message.additional_info)

        when Messages::ParameterStatus
          @parameters[message.name] = message.value

        when Messages::ReadyForQuery
          @transaction_status = STATUSES[message.transaction_status]
          break unless return_result

        when Messages::RowDescription
          result.descriptions = message if result

        when Messages::Unknown
          raise Error::MessageError.new("Unknown message type: #{message.message_id}")

        when  Messages::BindComplete,
              Messages::NoData,
              Messages::EmptyQueryResponse,
              Messages::ParameterDescription
          :nothing

        when  Messages::CloseComplete,
              Messages::CommandComplete,
              Messages::ParseComplete,
              Messages::PortalSuspended
          break
        end
      end

      result
    end

    def reset_values
      reset_notifications
      reset_result
      @parameters         = {}
      @backend_pid        = nil
      @backend_key        = nil
      @transaction_status = nil
      @socket             = nil
      @process_row        = nil
    end

    def reset_notifications
      @notifications      = []
    end

    def reset_result
      @field_descriptions = []
      @field_values       = []
    end

  end
end
