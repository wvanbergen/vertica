module Vertica
  module Protocol
    class Execute < FrontendMessage
      message_id 'E'

      def initialize(portal_name, max_rows)
        @portal_name = portal_name
        @max_rows    = max_rows
      end

      def message_body
        [@portal_name, @max_rows].pack('Z*N')
      end
    end
  end
end
