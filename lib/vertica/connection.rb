module Vertica

  class Connection

    STATUSES = {
      ?I => :no_transaction,
      ?T => :in_transaction,
      ?E => :failed_transaction
    }

    def self.cancel(existing_conn)
      conn = self.new(existing_conn.options.merge(:skip_startup => true))
      conn.write Messages::CancelRequest.new(existing_conn.backend_pid, existing_conn.backend_key)
      conn.write Messages::Flush.new
      conn.connection.close
    end

    def initialize(options = {})
      reset_values

      @options = options

      unless options[:skip_startup]
        connection.write Messages::Startup.new(@options[:user], @options[:database]).to_bytes
        process
      end
    end

    def connection
      @connection ||= begin
        conn = VerticaSocket.new(@options[:host], @options[:port].to_s)
        if @options[:ssl]
          conn.write Messages::SslRequest.new.to_bytes
          if conn.read_byte == ?S
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

    def opened?
      @connection && @backend_pid && @transaction_status
    end

    def closed?
      !opened?
    end

    def write(message)
      connection.write_message message
    end

    def close
      write Messages::Terminate.new
      connection.shutdown
      @connection = nil
    rescue Errno::ENOTCONN # the backend closed the connection already
    ensure
      reset_values
    end

    def reset
      close if opened?
      reset_values
    end

    def options
      @options.dup
    end

    def transaction_status
      @transaction_status
    end

    def backend_pid
      @backend_pid
    end

    def backend_key
      @backend_key
    end

    def notifications
      @notifications
    end

    def parameters
      @parameters.dup
    end

    def put_copy_data; raise NotImplementedError.new; end
    def put_copy_end;  raise NotImplementedError.new; end
    def get_copy_data; raise NotImplementedError.new; end

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

    def process(return_result = false)
      result = return_result ? Result.new : nil
      loop do
        case message = connection.read_message
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
          raise Error::MessageError.new(message.error)

        when Messages::NoticeResponse
          message.notices.each do |notice|
            @notices << Notice.new(notice[0], notice[1])
          end

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
        # when Messages::CopyData
        #   # nothing
        # when Messages::CopyDone
        #   # nothing
        # when Messages::CopyInResponse
        #   raise 'not done'
        # when Messages::CopyOutResponse
        #   raise 'not done'
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
      @connection         = nil
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
