module Vertica
  module Messages
    class Close < FrontendMessage
      message_id ?C

      def initialize(close_type, close_name)
        if close_type == :portal
          @close_type = ?P
        elsif close_type == :prepared_statement
          @close_type = ?S
        else
          raise ArgumentError.new("#{close_type} is not a valid close_type.  Must be either :portal or :prepared_statement.")
        end
        @close_name = close_name
      end

      def to_bytes(stream)
        size = LENGTH_SIZE
        size += 1
        size += @close_name.length + 1
        
        stream.write_byte(message_id)
        stream.write_network_int32(size) # size
        stream.write_byte(@close_type)
        stream.write_cstring(@close_name)
      end

    end
  end
end
