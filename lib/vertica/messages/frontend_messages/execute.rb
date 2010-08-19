module Vertica
  module Messages
    class Execute < FrontendMessage
      message_id ?E

      def initialize(portal_name, max_rows)
        @portal_name = portal_name
        @max_rows    = max_rows
      end

      def to_bytes
        message_string([ 
          @portal_name.to_cstring,
          @max_rows.to_network_int32
        ])
      end

    end
  end
end
