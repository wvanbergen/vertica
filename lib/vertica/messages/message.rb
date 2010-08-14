module Vertica
  module Messages
    class Message
      LENGTH_SIZE   = 4
      
      def self.message_id(message_id)
        self.const_set(:MESSAGE_ID, message_id) 
        class_eval "def message_id; MESSAGE_ID end"
      end
    end
    
    class BackendMessage < Message
      MessageIdMap = {}
      
      attr_reader :size
      
      def self.message_id(message_id)
        super
        MessageIdMap[message_id] = self
      end
      
      def self.read(stream)
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
      
      def initialize(stream, size)
        @size = size
      end

    end
    
    class FrontendMessage < Message
    end

  end
end
