module Vertica
  module Messages
    class CancelRequest < FrontendMessage
      message_id nil

      def initialize(backend_pid, backend_key)
        @backend_pid = backend_pid
        @backend_key = backend_key
      end

      def to_bytes(stream)
        size = LENGTH_SIZE
        size += 4
        size += 4
        size += 4

        stream.write_network_int32(size) # size
        stream.write_cstring(80877102)
        stream.write_network_int32(@backend_pid)
        stream.write_network_int16(@backend_key)
      end

    end
  end
end
