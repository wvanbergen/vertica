module Vertica
  module Messages
    class SslRequest < FrontendMessage
      message_id nil

      def to_bytes
        [ (LENGTH_SIZE + 4).to_network_int32,
          80877103.to_network_int32
        ].join
      end

    end
  end
end
