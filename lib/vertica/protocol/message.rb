module Vertica
  module Protocol

    class Message
      def self.message_id(message_id)
        self.send(:define_method, :message_id) { message_id }
      end
    end

    class BackendMessage < Message
      MessageIdMap = {}

      def self.factory(type, data)
        if klass = MessageIdMap[type]
          klass.new(data)
        else
          Protocol::Unknown.new(type, data)
        end
      end

      def self.message_id(message_id)
        super
        MessageIdMap[message_id] = self
      end

      def initialize(_data)
      end
    end

    class FrontendMessage < Message
      def to_bytes
        prepend_message_header(message_body)
      end

      protected

      def message_body
        ''
      end

      def prepend_message_header(msg)
        if message_id
          [message_id, 4 + msg.bytesize, msg].pack('aNa*')
        else
          [4 + msg.bytesize, msg].pack('Na*')
        end
      end
    end
  end
end

require 'vertica/protocol/backend/authentication'
require 'vertica/protocol/backend/backend_key_data'
require 'vertica/protocol/backend/bind_complete'
require 'vertica/protocol/backend/close_complete'
require 'vertica/protocol/backend/command_complete'
require 'vertica/protocol/backend/data_row'
require 'vertica/protocol/backend/empty_query_response'
require 'vertica/protocol/backend/notice_response'
require 'vertica/protocol/backend/error_response'
require 'vertica/protocol/backend/no_data'
require 'vertica/protocol/backend/parameter_description'
require 'vertica/protocol/backend/parameter_status'
require 'vertica/protocol/backend/parse_complete'
require 'vertica/protocol/backend/portal_suspended'
require 'vertica/protocol/backend/ready_for_query'
require 'vertica/protocol/backend/row_description'
require 'vertica/protocol/backend/copy_in_response'
require 'vertica/protocol/backend/unknown'

require 'vertica/protocol/frontend/bind'
require 'vertica/protocol/frontend/cancel_request'
require 'vertica/protocol/frontend/close'
require 'vertica/protocol/frontend/describe'
require 'vertica/protocol/frontend/execute'
require 'vertica/protocol/frontend/flush'
require 'vertica/protocol/frontend/parse'
require 'vertica/protocol/frontend/password'
require 'vertica/protocol/frontend/query'
require 'vertica/protocol/frontend/ssl_request'
require 'vertica/protocol/frontend/startup'
require 'vertica/protocol/frontend/sync'
require 'vertica/protocol/frontend/terminate'
require 'vertica/protocol/frontend/copy_done'
require 'vertica/protocol/frontend/copy_fail'
require 'vertica/protocol/frontend/copy_data'
