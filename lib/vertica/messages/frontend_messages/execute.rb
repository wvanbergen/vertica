module Vertica
  module Messages
    class Execute < FrontendMessage
      message_id ?E

      def initialize(portal_name, max_rows)
        @portal_name = portal_name
        @max_rows    = max_rows
      end

      def to_bytes
        size = LENGTH_SIZE
        size += @portal_name.length + 1
        size += 4

        [ message_id.to_byte,
          size.to_network_int32,
          @portal_name.to_cstring,
          @max_rows.to_network_int32
        ].join
      end

    end
  end
end
