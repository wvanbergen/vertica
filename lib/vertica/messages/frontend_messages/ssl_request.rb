module Vertica
  module Messages
    class SslRequest < FrontendMessage
      message_id nil

      def to_bytes
        message_string 80877103.to_network_int32
      end

    end
  end
end
