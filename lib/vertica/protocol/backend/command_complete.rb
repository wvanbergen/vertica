module Vertica
  module Protocol
    class CommandComplete < BackendMessage
      message_id 'C'

      attr_reader :tag

      def initialize(data)
        @tag = data.unpack('Z*').first
      end
    end
  end
end
