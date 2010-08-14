module Vertica
  module Messages
    class Query < FrontendMessage
      message_id ?Q

      def initialize(query_string)
        @query_string = query_string
      end

      def to_bytes(stream)
        size = LENGTH_SIZE
        size += @query_string.length + 1
        stream.write_byte(message_id)
        stream.write_network_int32(size) # size
        stream.write_cstring(@query_string)
      end

    end
  end
end
