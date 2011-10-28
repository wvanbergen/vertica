module Vertica
  module Messages
    class CopyFail < FrontendMessage
      message_id 'f'
      
      def initialize(error_message)
        @error_message = error_message
      end
      
      def to_bytes
        message_string [@error_message].pack('Z*')
      end
    end
  end
end
