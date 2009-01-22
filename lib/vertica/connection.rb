require 'uri'
require 'stringio'
require 'vertica/vertica_socket'
require 'vertica/messages/message'

module Vertica
  
  class Connection
    attr_reader :parameters
    attr_reader :backend_pid
    attr_reader :backend_key
    attr_reader :transaction_status
    attr_reader :notifications
    attr_reader :notices
    
    def initialize(options = {})
      reset
      
      @conn = VerticaSocket.new(options[:host], options[:port].to_s)
      startup_message = Messages::Startup.new(options[:user], options[:database])
      startup_message.to_bytes(@conn)

      loop do
        message = Messages::BackendMessage.read(@conn)

        case message
        when Messages::Authentication
          if message.code != Messages::Authentication::OK
            Messages::Password.new(options[:password], message.code, {:user => options[:user], :salt => message.salt}).to_bytes(@conn)
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

    def close
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
    
    protected
    
    def raise_if_not_open
      raise ConnectionError.new("connection doesn't exist or is already closed") if @conn.nil?
    end
    
    def reset
      reset_notifications
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
