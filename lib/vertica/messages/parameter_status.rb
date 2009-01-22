module Vertica
  module Messages
    class ParameterStatus < BackendMessage
      message_id ?S
      
      attr_reader :name
      attr_reader :value
      
      def initialize(stream, size)
        super
        @name  = stream.read_cstring
        @value = stream.read_cstring
      end

    end
  end
end
