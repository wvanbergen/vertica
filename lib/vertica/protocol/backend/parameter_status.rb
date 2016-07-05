module Vertica
  module Protocol
    class ParameterStatus < BackendMessage
      message_id 'S'

      attr_reader :name, :value

      def initialize(data)
        @name, @value = data.unpack('Z*Z*')
      end
    end
  end
end
