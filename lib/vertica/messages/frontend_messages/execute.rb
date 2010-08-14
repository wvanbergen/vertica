module Vertica
  module Messages
    class Execute < FrontendMessage
      message_id ?E

      def initialize(portal_name, max_rows)
        @portal_name = portal_name
        @max_rows    = max_rows
      end

      def to_bytes(stream)
        size = LENGTH_SIZE
        size += @portal_name.length + 1
        size += 4
        
        stream.write_byte(message_id)
        stream.write_network_int32(size) # size
        stream.write_cstring(@portal_name)
        stream.write_network_int32(@max_rows)
      end

    end
  end
end
