module Vertica
  module Messages
    class BackendKeyData < BackendMessage
      message_id ?K

      attr_reader :pid
      attr_reader :key

      def initialize(stream, size)
        super
        @pid = stream.read_network_int32
        @key = stream.read_network_int32
      end
    end
  end
end
