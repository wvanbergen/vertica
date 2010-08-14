module Vertica
  module Messages
    class CommandComplete < BackendMessage
      message_id ?C

      attr_reader :tag
      attr_reader :rows

      def initialize(stream, size)
        @tag = stream.read_cstring
        @rows = @tag.split[-1].to_i
      end

    end
  end
end
