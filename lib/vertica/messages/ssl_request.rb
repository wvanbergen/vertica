module Vertica
  module Messages
    class SslRequest < FrontendMessage
      message_id nil

      def to_bytes(stream)
        size = LENGTH_SIZE + 4
        stream.write_network_int32(size) # size
        stream.write_network_int32(80877103) # size
      end

    end
  end
end
