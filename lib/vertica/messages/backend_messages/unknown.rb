module Vertica
  module Messages
    class Unknown < BackendMessage
      attr_reader :message_id

      def initialize(stream, size)
        @message_id = message_id
      end
    end
  end
end
