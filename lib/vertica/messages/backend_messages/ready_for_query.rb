module Vertica
  module Messages
    class ReadyForQuery < BackendMessage
      message_id 'Z'

      attr_reader :transaction_status

      def initialize(data)
        @transaction_status = data.unpack('a').first
      end
    end
  end
end
