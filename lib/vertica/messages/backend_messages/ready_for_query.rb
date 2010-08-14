module Vertica
  module Messages
    class ReadyForQuery < BackendMessage
      message_id ?Z

      attr_reader :transaction_status

      def initialize(stream, size)
        super
        @transaction_status = stream.read_byte
      end
    end
  end
end
