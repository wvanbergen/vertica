module Vertica
  module Messages

    class Query < FrontendMessage
      message_id ?Q

      def initialize(query_string)
        @query_string = query_string
      end

      def to_bytes
        message_string @query_string.to_cstring
      end
    end

  end
end
