module Vertica
  module Messages
    class Terminate < FrontendMessage
      message_id ?X

      def to_bytes(stream)
        size = LENGTH_SIZE
        stream.write_byte(message_id)
        stream.write_network_int32(size)
      end

    end
  end
end
