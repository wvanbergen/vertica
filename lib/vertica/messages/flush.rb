module Vertica
  module Messages
    class Flush < FrontendMessage
      message_id ?H

      def to_bytes(stream)
        size = LENGTH_SIZE
        
        stream.write_byte(message_id)
        stream.write_network_int32(size) # size
      end

    end
  end
end
