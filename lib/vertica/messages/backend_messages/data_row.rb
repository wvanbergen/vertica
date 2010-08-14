module Vertica
  module Messages
    class DataRow < BackendMessage
      message_id ?D

      attr_reader :field_count
      attr_reader :fields

      def initialize(stream, size)
        @fields = []

        @field_count = stream.read_network_int16
        @field_count.times do |field_index|
          size = stream.read_network_int32
          @fields << (size == -1 ? nil : stream.readn(size))
        end
      end
    end
  end
end
