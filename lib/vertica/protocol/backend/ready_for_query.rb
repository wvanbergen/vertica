module Vertica
  module Protocol
    class ReadyForQuery < BackendMessage

      STATUSES = {
        'I' => :no_transaction,
        'T' => :in_transaction,
        'E' => :failed_transaction
      }

      message_id 'Z'

      attr_reader :transaction_status

      def initialize(data)
        @transaction_status = STATUSES[data.unpack('a').first]
      end
    end
  end
end
