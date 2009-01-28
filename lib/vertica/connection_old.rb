require 'uri'
require 'stringio'
require 'vertica/vertica_socket'
require 'vertica/messages/message'
require 'openssl/ssl'

module Vertica
  
  class Connection
    attr_reader :parameters
    attr_reader :backend_pid
    attr_reader :backend_key
    attr_reader :transaction_status
    attr_reader :notifications
    attr_reader :notices
    attr_reader :options
    
    def initialize(options = {})
      reset
      
      @options = options
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
      
      startup_message = Messages::Startup.new(@options[:user], @options[:database])
      startup_message.to_bytes(@conn)

      loop do
        message = Messages::BackendMessage.read(@conn)

        case message
        when Messages::Authentication
          if message.code != Messages::Authentication::OK
            Messages::Password.new(@options[:password], message.code, {:user => @options[:user], :salt => message.salt}).to_bytes(@conn)
          end
        when Messages::ParameterStatus
          @parameters[message.name] = message.value
        when Messages::BackendKeyData
          @backend_pid = message.pid
          @backend_key = message.key
        when Messages::ReadyForQuery
          @transaction_status = convert_transaction_status_to_sym(message.transaction_status)
          break
        when Messages::NotificationResponse
          @notifications << Notification.new(message.pid, message.condition, message.additional_info)
        when Messages::ErrorResponse
          raise Error::MessageError.new(message.error)
        when Messages::Unknown
          raise Error::MessageError.new("Unknown message type: #{message.message_id}")
        end
      end
    end
    
    def terminate
      raise_if_not_open
      Messages::Terminate.new.to_bytes(@conn)
      @conn.shutdown
    rescue Errno::ENOTCONN
      # the backend closed the connection already
    ensure
      reset
    end
    
    def open?
      @conn && @backend_pid && @transaction_status
    end
    
    def query(query_string)
      raise_if_not_open
      Messages::Query.new(query_string).to_bytes(@conn)
      
      field_descriptions = []
      field_values       = []
      
      loop do
        message = Messages::BackendMessage.read(@conn)
        case message
        when Messages::DataRow
          field_values << message.fields
        when Messages::CommandComplete
          # nothing
        when Messages::ReadyForQuery
          @transaction_status = convert_transaction_status_to_sym(message.transaction_status)
          break
        when Messages::RowDescription
          field_descriptions = message.fields
        # when Messages::CopyInResponse
        #   raise 'not done'
        # when Messages::CopyOutResponse
        #   raise 'not done'
        when Messages::EmptyQueryResponse
          # do nothing
        when Messages::ErrorResponse
          raise Error::MessageError.new(message.error)
        when Messages::NoticeResponse
          message.notices.each do |notice|
            @notices << Notice.new(notice[0], notice[1])
          end
        when Messages::Unknown
          raise Error::MessageError.new("Unknown message type: #{message.message_id}")
        end
      end

      Result.new(field_descriptions, field_values)
    end
    
    def prepare(name, query, params_count = 0, *param_types)
      raise_if_not_open
      
      if param_types.nil? || param_types.empty?
        param_types = Array.new(params_count).fill(0)
      end
      
      Messages::Parse.new(name, query, param_types).to_bytes(@conn)
      Messages::Describe.new(:prepared_statement, name).to_bytes(@conn)
      Messages::Flush.new.to_bytes(@conn)
      
      loop do
        message = Messages::BackendMessage.read(@conn)
        case message
        when Messages::ParseComplete
          break
        when Messages::ParameterDescription
          # do nothing
        when Messages::ErrorResponse
          raise Error::MessageError.new(message.error)
        when Messages::Unknown
          raise Error::MessageError.new("Unknown message type: #{message.message_id}")
        end
      end
    end
    
    def bind(portal_name, prepared_statement_name, *param_values)
      raise_if_not_open
      
      Messages::Bind.new(portal_name, prepared_statement_name, param_values).to_bytes(@conn)
      Messages::Describe.new(:portal, portal_name).to_bytes(@conn)
      Messages::Flush.new.to_bytes(@conn)
      
      @bound_field_descriptions = []
      
      loop do
        message = Messages::BackendMessage.read(@conn)
        case message
        when Messages::BindComplete
          break
        when Messages::NoData
          # do nothing
        when Messages::RowDescription
          @bound_field_descriptions = message.fields
        when Messages::ErrorResponse
          raise Error::MessageError.new(message.error)
        when Messages::Unknown
          raise Error::MessageError.new("Unknown message type: #{message.message_id}")
        end
      end
    end
    
    def execute(portal_name, max_rows = 0)
      raise_if_not_open
      
      Messages::Execute.new(portal_name, max_rows).to_bytes(@conn)
      Messages::Flush.new.to_bytes(@conn)

      field_values       = []

      loop do
        message = Messages::BackendMessage.read(@conn)
        case message
        when Messages::CommandComplete
          break
        when Messages::DataRow
          field_values << message.fields
        when Messages::PortalSuspended
          break
        when Messages::EmptyQueryResponse
          break
        # when Messages::CopyInResponse
        #   raise 'not done'
        # when Messages::CopyOutResponse
        #   raise 'not done'
        when Messages::ErrorResponse
          raise Error::MessageError.new(message.error)
        when Messages::NoticeResponse
          message.notices.each do |notice|
            @notices << Notice.new(notice[0], notice[1])
          end
        when Messages::Unknown
          raise Error::MessageError.new("Unknown message type: #{message.message_id}")
        end
      end
      
      Messages::Sync.new.to_bytes(@conn)
      Messages::Flush.new.to_bytes(@conn)
      message = Messages::BackendMessage.read(@conn)
      raise Error::MessageError.new("Didn't receive a ReadyForQueue message as expected.") unless message.is_a?(Messages::ReadyForQuery)
      @transaction_status = convert_transaction_status_to_sym(message.transaction_status)
      
      Result.new(@bound_field_descriptions, field_values)
    end
    
    def close(close_type, close_name)
      raise_if_not_open
      
      Messages::Close.new(close_type, close_name).to_bytes(@conn)
      Messages::Flush.new.to_bytes(@conn)
      
      loop do
        message = Messages::BackendMessage.read(@conn)
        case message
        when Messages::CloseComplete
          break
        when Messages::ErrorResponse
          raise Error::MessageError.new(message.error)
        when Messages::NoticeResponse
          message.notices.each do |notice|
            @notices << Notice.new(notice[0], notice[1])
          end
        when Messages::Unknown
          raise Error::MessageError.new("Unknown message type: #{message.message_id}")
        end        
      end
    end

    def self.cancel(existing_conn)
      conn = new(existing_conn.options)
      Messages::Cancel(existing_conn.backend_pid, existing_conn.backend_key).to_bytes(conn)
      conn.terminate
    end

    protected
    
    def raise_if_not_open
      raise ConnectionError.new("connection doesn't exist or is already closed") if @conn.nil?
    end
    
    def reset
      reset_notifications
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

  end
end
