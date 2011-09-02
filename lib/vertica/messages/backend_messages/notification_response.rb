module Vertica
  module Messages
    class NotificationResponse < BackendMessage
      message_id 'A'
      
      attr_reader :pid, :condition, :addition_info
      
      def initialize(data)
        @pid, @condition, @addition_info = data.unpack('NZ*Z*')
      end
    end
  end
end
