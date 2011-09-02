module Vertica
  module Messages
    class ParameterDescription < BackendMessage
      message_id 't'

      attr_reader :parameter_types
      
      def initialize(data)
        parameter_count    = data.unpack('n').first
        parameter_type_ids = data.unpack("@2N#{parameter_count}")
        @parameter_types   = parameter_type_ids.map { |id| Vertica::Column::DATA_TYPES[id] }
      end
    end
  end
end
