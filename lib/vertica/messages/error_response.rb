module Vertica
  module Messages
    class ErrorResponse < BackendMessage
      message_id ?E
      
      def initialize(stream, size)
        super
        @errors = {}

        field_type = stream.read_byte
        while field_type != 0
          @errors[field_type] = stream.read_cstring
          field_type = stream.read_byte
        end
      end

      def error
        s = []
        @errors.each do |field_type, message|
          s << [convert_field_type_to_string(field_type), message].flatten
        end
        s.sort_by { |e| e.first }.map { |e| "#{e[1]}: #{e[2]}" }.join(', ')
      end
      
      protected
      
      def convert_field_type_to_string(field_type)
        case field_type
        when ?S
          [1, "Severity"]
        when ?C
          [3, "Sqlstate"]
        when ?M
          [2, "Message"]
        when ?D
          [4, "Detail"]
        when ?H
          [5, "Hint"]
        when ?P
          [6, "Position"]
        when ?p
          [8, "Internal Position"]
        when ?q
          [0, "Internal Query"]
        when ?W
          [7, "Where"]
        when ?F
          [11, "File"]
        when ?L
          [12, "Line"]
        when ?R
          [10, "Routine"]
        else
          [13, field_type.to_s]
        end
      end
    end
  end
end
