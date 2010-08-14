module Vertica
  module Messages
    class RowDescription < BackendMessage
      message_id ?T

      attr_reader :field_count
      attr_reader :fields

      def initialize(stream, size)
        super

        @fields = []

        @field_count = stream.read_network_int16
        @field_count.times do |field_index|
          @fields << {
            :name             => stream.read_cstring,
            :table_oid        => stream.read_network_int32,
            :attribute_number => stream.read_network_int16,
            :data_type_oid    => stream.read_network_int32,
            :data_type_size   => stream.read_network_int16,
            :type_modifier    => stream.read_network_int32,
            :format_code      => stream.read_network_int16
          }
        end
      end
    end
  end
end
