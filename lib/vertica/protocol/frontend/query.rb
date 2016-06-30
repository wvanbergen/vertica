module Vertica
  module Protocol
    class Query < FrontendMessage
      message_id 'Q'

      def initialize(query_string)
        @query_string = query_string
      end

      def message_body
        [@query_string].pack('Z*')
      end
    end
  end
end
