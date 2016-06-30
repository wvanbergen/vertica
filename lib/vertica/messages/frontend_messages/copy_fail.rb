module Vertica
  module Messages
    class CopyFail < FrontendMessage
      message_id 'f'

      def initialize(error_message)
        @error_message = error_message
      end

      def message_body
        [@error_message].pack('Z*')
      end
    end
  end
end
