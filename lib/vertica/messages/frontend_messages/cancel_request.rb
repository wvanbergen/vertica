module Vertica
  module Messages
    class CancelRequest < FrontendMessage
      message_id nil

      def initialize(backend_pid, backend_key)
        @backend_pid = backend_pid
        @backend_key = backend_key
      end

      def to_bytes
        size = LENGTH_SIZE
        size += 4
        size += 4
        size += 4

        [ size.to_network_int32,
          80877102.to_network_int32,
          @backend_pid.to_network_int32,
          @backend_key.to_network_int32
        ].join
      end

    end
  end
end
