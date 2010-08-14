module Vertica
  module Messages
    class Describe < FrontendMessage
      message_id ?D

      def initialize(describe_type, describe_name)
        @describe_name = describe_name
        @describe_type = case describe_type
        when :portal              then ?P
        when :prepared_statement  then ?S
        else raise ArgumentError.new("#{describe_type} is not a valid describe_type.  Must be either :portal or :prepared_statement.")
        end
      end

      def to_bytes
        size = LENGTH_SIZE
        size += 1
        size += @describe_name.length + 1

        [ message_id.to_byte,
          size.to_network_int32,
          @describe_type.to_byte,
          @describe_name.to_cstring
        ].join
      end

    end
  end
end
