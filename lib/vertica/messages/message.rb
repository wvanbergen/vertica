module Vertica
  module Messages

    class Message
      def self.message_id(message_id)
        self.send(:define_method, :message_id) { message_id }
      end

      def message_string(msg)
        msg = msg.join if msg.is_a?(Array)
        size = (0.to_network_int32.size + msg.size).to_network_int32
        "#{(message_id || '').to_byte}#{size}#{msg}"
      end
    end

    class BackendMessage < Message
      MessageIdMap = {}

      attr_reader :size

      def self.factory(type, stream, size)
        if klass = MessageIdMap[type]
          klass.new stream, size
        else
          Messages::Unknown.new type
        end
      end

      def self.message_id(message_id)
        super
        MessageIdMap[message_id] = self
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
