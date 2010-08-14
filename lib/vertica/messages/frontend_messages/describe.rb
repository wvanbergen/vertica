module Vertica
  module Messages
    class Describe < FrontendMessage
      message_id ?D

      def initialize(describe_type, describe_name)
        if describe_type == :portal
          @describe_type = ?P
        elsif describe_type == :prepared_statement
          @describe_type = ?S
        else
          raise ArgumentError.new("#{describe_type} is not a valid describe_type.  Must be either :portal or :prepared_statement.")
        end
        @describe_name = describe_name
      end

      def to_bytes(stream)
        size = LENGTH_SIZE
        size += 1
        size += @describe_name.length + 1
        
        stream.write_byte(message_id)
        stream.write_network_int32(size) # size
        stream.write_byte(@describe_type)
        stream.write_cstring(@describe_name)
      end

    end
  end
end
