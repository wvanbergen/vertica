module Vertica
  module Messages
    class NoticeResponse < BackendMessage
      message_id 'N'
      
      FIELDS = {
        'q' => [0, "Internal Query"],
        'S' => [1, "Severity"],
        'M' => [2, "Message"],
        'C' => [3, "Sqlstate"],
        'D' => [4, "Detail"],
        'H' => [5, "Hint"],
        'P' => [6, "Position"],
        'W' => [7, "Where"],
        'p' => [8, "Internal Position"],
        'R' => [10, "Routine"],
        'F' => [11, "File"],
        'L' => [12, "Line"],
      }
      
      attr_reader :values

      def initialize(data)
        @values, pos = {}, 0
        while pos < data.size - 1
          key, value = data.unpack("@#{pos}aZ*")
          @values[FIELDS[key][1]] = value
          pos += value.size + 2
        end
      end

      def error_message
        @values.map { |type, msg| [(FIELDS[type] || [FIELDS.size, type.to_s]), msg].flatten }.
                sort_by { |e| e.first }.
                map { |e| "#{e[1]}: #{e[2]}" }.
                join(', ')
      end
    end
  end
end
