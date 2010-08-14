module Vertica
  module Messages
    class NoticeResponse < BackendMessage
      message_id ?N

      attr_reader :notices

      def initialize(stream, size)
        super
        @notices, type = [], nil
        @notices << [type, stream.read_cstring] while (type = stream.read_byte) != 0
      end

    end
  end
end
