module Vertica
  module Messages

    class Message
      def self.message_id(message_id)
        self.send(:define_method, :message_id) { message_id }
      end

      def message_string(msg)
        msg = msg.join if msg.is_a?(Array)
        size = (0.to_network_int32.size + msg.size).to_network_int32
        m_id = ''.to_byte             #in 1.9 it seems to write out message ids as numbers, handle this here
        if (message_id)
          m_id = message_id.chr
        end
        "#{m_id}#{size}#{msg}"
      end
    end

    class BackendMessage < Message
      MessageIdMap = {}

      attr_reader :size

      def self.factory(type, stream, size)
        #puts "factory reading message #{type} #{size} #{type.class}"
        if klass = MessageIdMap[type.chr]           #explicitly use the char value, for 1.9 compat
          klass.new stream, size
        else
          Messages::Unknown.new stream, size
        end
      end

      def self.message_id(message_id)
        super
        MessageIdMap[message_id.chr] = self          #explicitly use the char value, for 1.9 compat
      end

      def self.read(stream)
      end

      def initialize(stream, size)
        @size = size
      end
    end

    class FrontendMessage < Message
      def to_bytes
        message_string ''
      end
    end

  end
end
