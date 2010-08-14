module Vertica
  module Messages
    class Query < FrontendMessage
      message_id ?Q

      def initialize(query_string)
        @query_string = query_string
      end

      def to_bytes
        size = LENGTH_SIZE
        size += @query_string.length + 1
        [ message_id.to_byte,
          size.to_network_int32,
          @query_string.to_cstring
        ].join
      end

    end
  end
end
