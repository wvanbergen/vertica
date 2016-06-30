module Vertica
  module Protocol
    class DataRow < BackendMessage
      message_id 'D'

      attr_reader :values

      def initialize(data)
        @values = []
        field_count = data.unpack('n').first
        pos = 2
        field_count.times do |field_index|
          size = data.unpack("@#{pos}N").first
          size = -1 if size == 4294967295
          @values << (size == -1 ? nil : data.unpack("@#{pos + 4}a#{size}").first)
          pos += 4 + [size, 0].max
        end
      end
    end
  end
end
