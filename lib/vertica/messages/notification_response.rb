module Vertica
  module Messages
    class NotificationResponse < BackendMessage
      message_id ?A
      
      attr_reader :pid
      attr_reader :condition
      attr_reader :addition_info
      
      def initialize(stream, size)
        super
        @pid           = stream.read_network_int32
        @condition     = stream.read_cstring
        @addition_info = stream.read_cstring
      end
    end
  end
end
