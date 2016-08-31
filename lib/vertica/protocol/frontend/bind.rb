module Vertica
  module Protocol
    class Bind < FrontendMessage
      message_id 'B'

      def initialize(portal_name, prepared_statement_name, parameter_types, parameter_values)
        @portal_name = portal_name
        @prepared_statement_name = prepared_statement_name
        @parameter_types = parameter_types
        @parameter_values = parameter_values
      end

      def message_body
        bytes = [@portal_name, @prepared_statement_name, @parameter_values.length].pack('Z*Z*n')
        bytes << @parameter_values.map{0}.pack('n*')
        bytes << [@parameter_types.length, *@parameter_types.map{|pt| pt.oid}].pack('nN*')
        bytes << @parameter_values.map { |val| val.nil? ? [-1].pack('N') : [val.to_s.length, val.to_s].pack('Na*') }.join('')
        bytes << [0].pack('n')
      end
    end
  end
end
