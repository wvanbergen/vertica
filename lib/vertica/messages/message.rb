module Vertica
  module Messages
    class Message
      LENGTH_SIZE   = 4
      
      class << self
        
        def message_id(message_id)
          self.const_set(:MESSAGE_ID, message_id) 
          class_eval "def message_id; MESSAGE_ID end"
        end
        
      end
    end
    
    class BackendMessage < Message
      MessageIdMap = {}
      
      attr_reader :size
      
      class << self
        def message_id(message_id)
          super
          MessageIdMap[message_id] = self
        end
        
        def read(stream)
          type = stream.read_byte
          size = stream.read_network_int32

          raise Vertica::Error::MessageError.new("Bad message size: #{size}") unless size >= 4

          message_klass = MessageIdMap[type]
          if message_klass.nil?
            Messages::Unknown.new(type)
          else
            message_klass.new(stream, size)
          end
        end
      
      end
      
      def initialize(stream, size)
        @size = size
      end

    end
    
    class FrontendMessage < Message
    end

  end
end

require 'vertica/messages/unknown'
require 'vertica/messages/error_response'
require 'vertica/messages/startup'
require 'vertica/messages/authentication'
require 'vertica/messages/password'
require 'vertica/messages/parameter_status'
require 'vertica/messages/backend_key_data'
require 'vertica/messages/ready_for_query'
require 'vertica/messages/terminate'
require 'vertica/messages/notification_response'
require 'vertica/messages/query'
require 'vertica/messages/notice_response'
require 'vertica/messages/row_description'
require 'vertica/messages/command_complete'
require 'vertica/messages/data_row'
require 'vertica/messages/empty_query_response'
