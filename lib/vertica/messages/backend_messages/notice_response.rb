module Vertica
  module Messages
    class NoticeResponse < BackendMessage
      message_id ?N
      
      attr_reader :notices
      
      def initialize(stream, size)
        super
        @notices = []
        
        field_type = stream.read_byte
        while field_type != 0
          @notices << [field_type, stream.read_cstring]
          field_type = stream.read_byte
        end
      end

    end
  end
end
