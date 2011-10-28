module Vertica
  module Messages

    class Message
      def self.message_id(message_id)
        self.send(:define_method, :message_id) { message_id }
      end

      def message_string(msg)
        msg = msg.join if msg.is_a?(Array)
        bytesize = msg.respond_to?(:bytesize) ? 4 + msg.bytesize : 4 + msg.size
        message_size = [bytesize].pack('N')
        message_id ? "#{message_id}#{message_size}#{msg}" : "#{message_size}#{msg}"
      end
    end

    class BackendMessage < Message
      MessageIdMap = {}

      def self.factory(type, data)
        #puts "factory reading message #{type} #{size} #{type.class}"
        if klass = MessageIdMap[type]           #explicitly use the char value, for 1.9 compat
          klass.new data
        else
          Messages::Unknown.new type, data
        end
      end

      def self.message_id(message_id)
        super
        MessageIdMap[message_id] = self          #explicitly use the char value, for 1.9 compat
      end

      def initialize(data)
      end
    end

    class FrontendMessage < Message
      def to_bytes
        message_string ''
      end
    end
  end
end

require 'vertica/messages/backend_messages/authentication'
require 'vertica/messages/backend_messages/backend_key_data'
require 'vertica/messages/backend_messages/bind_complete'
require 'vertica/messages/backend_messages/close_complete'
require 'vertica/messages/backend_messages/command_complete'
require 'vertica/messages/backend_messages/data_row'
require 'vertica/messages/backend_messages/empty_query_response'
require 'vertica/messages/backend_messages/notice_response'
require 'vertica/messages/backend_messages/error_response'
require 'vertica/messages/backend_messages/no_data'
require 'vertica/messages/backend_messages/parameter_description'
require 'vertica/messages/backend_messages/parameter_status'
require 'vertica/messages/backend_messages/parse_complete'
require 'vertica/messages/backend_messages/portal_suspended'
require 'vertica/messages/backend_messages/ready_for_query'
require 'vertica/messages/backend_messages/row_description'
require 'vertica/messages/backend_messages/copy_in_response'
require 'vertica/messages/backend_messages/unknown'

require 'vertica/messages/frontend_messages/bind'
require 'vertica/messages/frontend_messages/cancel_request'
require 'vertica/messages/frontend_messages/close'
require 'vertica/messages/frontend_messages/describe'
require 'vertica/messages/frontend_messages/execute'
require 'vertica/messages/frontend_messages/flush'
require 'vertica/messages/frontend_messages/parse'
require 'vertica/messages/frontend_messages/password'
require 'vertica/messages/frontend_messages/query'
require 'vertica/messages/frontend_messages/ssl_request'
require 'vertica/messages/frontend_messages/startup'
require 'vertica/messages/frontend_messages/sync'
require 'vertica/messages/frontend_messages/terminate'
require 'vertica/messages/frontend_messages/copy_done'
require 'vertica/messages/frontend_messages/copy_fail'
require 'vertica/messages/frontend_messages/copy_data'
