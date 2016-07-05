module Vertica
  module Protocol
    class Close < FrontendMessage
      message_id 'C'

      def initialize(close_type, close_name)
        @close_name = close_name
        @close_type = case close_type
          when :portal              then 'P'
          when :prepared_statement  then 'S'
          else raise ArgumentError.new("#{close_type} is not a valid close_type.  Must be either :portal or :prepared_statement.")
        end
      end

      def message_body
        [@close_type, @close_name].pack('AZ*')
      end
    end
  end
end
