module Vertica
  module Messages
    class ParameterDescription < BackendMessage
      message_id ?t

      attr_reader :parameter_count
      attr_reader :parameter_types
      
      def initialize(stream, size)
        super
        @parameter_types = []
        @parameter_count = stream.read_network_int16
        @parameter_count.times do
          @parameter_types << Vertica::Column::DATA_TYPES[stream.read_network_int32]
        end
      end
    end
  end
end
