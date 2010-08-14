module Vertica
  module Messages
    class ErrorResponse < BackendMessage
      message_id ?E

      ERRORS = {
        ?q => [0, "Internal Query"],
        ?S => [1, "Severity"],
        ?M => [2, "Message"],
        ?C => [3, "Sqlstate"],
        ?D => [4, "Detail"],
        ?H => [5, "Hint"],
        ?P => [6, "Position"],
        ?W => [7, "Where"],
        ?p => [8, "Internal Position"],
        ?R => [10, "Routine"],
        ?F => [11, "File"],
        ?L => [12, "Line"],
      }

      def initialize(stream, size)
        super
        @errors, type = {}, nil
        @errors[type] = stream.read_cstring while (type = stream.read_byte) != 0
      end

      def error
        @errors.map { |type, msg| [(ERRORS[type] || [ERRORS.size, type.to_s]), msg].flatten }.
                sort_by { |e| e.first }.
                map { |e| "#{e[1]}: #{e[2]}" }.
                join(', ')
      end
    end
  end
end
