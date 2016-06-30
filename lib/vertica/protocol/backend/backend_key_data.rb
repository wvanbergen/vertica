module Vertica
  module Protocol
    class BackendKeyData < BackendMessage
      message_id 'K'

      attr_reader :pid, :key

      def initialize(data)
        @pid, @key = data.unpack('NN')
      end
    end
  end
end
