module Vertica
  module Protocol
    class Describe < FrontendMessage
      message_id 'D'

      def initialize(describe_type, describe_name)
        @describe_name = describe_name
        @describe_type = case describe_type
          when :portal              then 'P'
          when :prepared_statement  then 'S'
          else raise ArgumentError.new("#{describe_type} is not a valid describe_type.  Must be either :portal or :prepared_statement.")
        end
      end

      def message_body
        [@describe_type, @describe_name].pack('AZ*')
      end
    end
  end
end
