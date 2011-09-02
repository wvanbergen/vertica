module Vertica
  module Messages
    class Unknown < BackendMessage
      attr_reader :message_id

      def initialize(message_id, data)
        @message_id, @data = message_id, data
      end
    end
  end
end
