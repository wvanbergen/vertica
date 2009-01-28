require 'uri'
require 'stringio'
require 'vertica/vertica_socket'
require 'vertica/messages/message'
require 'openssl/ssl'

module Vertica
  
  class Connection
    
    def initialize(options = {})
      reset_values

      @options = options
      establish_connection
      
      unless options[:skip_startup]
        Messages::Startup.new(@options[:user], @options[:database]).to_bytes(@conn)
        process
      end
    end
    
    def close
      raise_if_not_open
      Messages::Terminate.new.to_bytes(@conn)
      @conn.shutdown
    rescue Errno::ENOTCONN
      # the backend closed the connection already
    ensure
      reset_values
    end
    
    def reset
      close if opened?
      reset_values
      establish_connection
    end

    def options
      @options.dup
    end
    
    def transaction_status
      @transaction_status
    end

    def parameter_status
      @parameter_status.dup
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
    
    def opened?
      @conn && @backend_pid && @transaction_status
    end
    
    def closed?
      !opened?
    end
    
    def query(query_string)
      raise ArgumentError.new("Query string cannot be blank or empty.") if query_string.nil? || query_string.empty?
      raise_if_not_open
      reset_result

      Messages::Query.new(query_string).to_bytes(@conn)
      
      process(true)
    end
    
    def prepare(name, query, params_count = 0)
      raise_if_not_open
      
      param_types = Array.new(params_count).fill(0)
      
      Messages::Parse.new(name, query, param_types).to_bytes(@conn)
      Messages::Describe.new(:prepared_statement, name).to_bytes(@conn)
      Messages::Sync.new.to_bytes(@conn)
      Messages::Flush.new.to_bytes(@conn)
      
      process
    end
    
    def execute_prepared(name, *param_values)
      # raise NotImplementedError.new("Prepared statements are not supported in Vertica")
            
      raise_if_not_open
      
      portal_name = "" # use the unnamed portal
      max_rows    = 0  # return all rows

            
      reset_result
      
      Messages::Bind.new(portal_name, name, param_values).to_bytes(@conn)
      Messages::Execute.new(portal_name, max_rows).to_bytes(@conn)
      Messages::Sync.new.to_bytes(@conn)
      Messages::Flush.new.to_bytes(@conn)
            
      result = process(true)

      # Close the portal
      Messages::Close.new(:portal, portal_name).to_bytes(@conn)
      Messages::Flush.new.to_bytes(@conn)
      
      process
      
      # Return the result from the prepared statement
      result
    end

    def self.cancel(existing_conn)
      conn = new(existing_conn.options.merge(:skip_startup => true))
      Messages::CancelRequest.new(existing_conn.backend_pid, existing_conn.backend_key).to_bytes(conn.send(:conn))
      Messages::Flush.new.to_bytes(conn.send(:conn))
      conn.close
    end

    protected
    
    def establish_connection
      @conn = VerticaSocket.new(@options[:host], @options[:port].to_s)
      
      if @options[:ssl]
        Messages::SslRequest.new.to_bytes(@conn)
        if @conn.read_byte == ?S
          @conn = OpenSSL::SSL::SSLSocket.new(@conn, OpenSSL::SSL::SSLContext.new)
          @conn.sync = true
          @conn.connect
        else
          raise Error::ConnectionError.new("SSL requested but server doesn't support it.")
        end
      end      
    end
    
    def process(return_result = false)
      loop do
        message = Messages::BackendMessage.read(@conn)

        case message
        when Messages::Authentication
          if message.code != Messages::Authentication::OK
            Messages::Password.new(@options[:password], message.code, {:user => @options[:user], :salt => message.salt}).to_bytes(@conn)
          end
        when Messages::BackendKeyData
          @backend_pid = message.pid
          @backend_key = message.key
        when Messages::BindComplete
          break
        when Messages::CloseComplete
          break
        when Messages::CommandComplete
          break
        # when Messages::CopyData
        #   # nothing
        # when Messages::CopyDone
        #   # nothing        
        # when Messages::CopyInResponse
        #   raise 'not done'
        # when Messages::CopyOutResponse
        #   raise 'not done'
        when Messages::DataRow
          @field_values << message.fields
        when Messages::EmptyQueryResponse
          break
        when Messages::ErrorResponse
          raise Error::MessageError.new(message.error)
        when Messages::NoData
          :nothing
        when Messages::NoticeResponse
          message.notices.each do |notice|
            @notices << Notice.new(notice[0], notice[1])
          end
        when Messages::NotificationResponse
          @notifications << Notification.new(message.pid, message.condition, message.additional_info)        
        when Messages::ParameterDescription
          :nothing
        when Messages::ParameterStatus
          @parameters[message.name] = message.value
        when Messages::ParseComplete
          break
        when Messages::PortalSuspended
          break
        when Messages::ReadyForQuery
          @transaction_status = convert_transaction_status_to_sym(message.transaction_status)
          break
        when Messages::RowDescription
          @field_descriptions = message.fields
        when Messages::Unknown
          raise Error::MessageError.new("Unknown message type: #{message.message_id}")
        end
      end
      
      return_result ? Result.new(@field_descriptions, @field_values) : nil
    end
    
    def raise_if_not_open
      raise ConnectionError.new("connection doesn't exist or is already closed") if @conn.nil?
    end
    
    def reset_values
      reset_notifications
      reset_result
      @options            = {}
      @parameters         = {}
      @backend_pid        = nil
      @backend_key        = nil
      @transaction_status = nil
      @conn               = nil
    end
    
    def reset_notifications
      @notifications      = []
    end

    def reset_result
      @field_descriptions = []
      @field_values       = []
    end    
    
    def convert_transaction_status_to_sym(status)
      case status
      when ?I
        :no_transaction
      when ?T
        :in_transaction
      when ?E
        :failed_transaction
      else
        nil
      end
    end

    def conn
      @conn
    end

  end
end
