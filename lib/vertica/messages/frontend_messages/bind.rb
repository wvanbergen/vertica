module Vertica
  module Messages
    class Bind < FrontendMessage
      message_id ?B

      def initialize(portal_name, prepared_statement_name, parameter_values)
        @portal_name = portal_name
        @prepared_statement_name = prepared_statement_name
        @parameter_values = parameter_values.map(&:to_s)
      end

      def to_bytes
        bytes = [@portal_name, @prepared_statement_name, 0, @parameter_values.length].pack('Z*Z*nn')
        bytes << @parameter_values.map { |val| [val.length, val].pack('Na*') }.join('') << [0].pack('n')
        message_string bytes
      end
    end
  end
end
