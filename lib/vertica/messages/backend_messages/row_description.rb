module Vertica
  module Messages
    class RowDescription < BackendMessage
      message_id 'T'

      attr_reader :fields

      def initialize(data)
        @fields = []
        field_count = data.unpack('n').first
        pos = 2
        field_count.times do |field_index|
          field_info = data.unpack("@#{pos}Z*NnNnNn")
          @fields << {
            :name             => field_info[0].force_encoding('UTF-8'),
            :table_oid        => field_info[1],
            :attribute_number => field_info[2],
            :data_type_oid    => field_info[3],
            :data_type_size   => field_info[4],
            :type_modifier    => field_info[5],
            :format_code      => field_info[6],
          }
          
          pos += 19 + field_info[0].size
        end
      end
    end
  end
end
