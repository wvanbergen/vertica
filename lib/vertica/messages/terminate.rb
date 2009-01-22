module Vertica
  module Messages
    class Terminate < FrontendMessage
      message_id ?X

      def to_bytes(stream)
        size = LENGTH_SIZE
        size += 1 # ending zero
        stream.write_byte(message_id)
        stream.write_byte(0)
      end

    end
  end
end
