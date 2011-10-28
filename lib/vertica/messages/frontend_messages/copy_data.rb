module Vertica
  module Messages
    class CopyData < FrontendMessage
      message_id 'd'
      
      def initialize(data)
        @data = data
      end
      
      def to_bytes
        message_string [@data].pack('a*')
      end      
    end
  end
end
