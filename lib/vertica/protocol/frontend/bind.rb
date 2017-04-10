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
        bytes << @parameter_values.each_with_index.map { |val, index| convert_to_string(val, @parameter_types[index]) }.map{ |val| val.nil? ? [-1].pack('N') : [val.length, val].pack('Na*') }.join('')
        bytes << [0].pack('n')
      end

      def convert_to_string(val, parameter_type)
        return nil if val.nil?
        return val.to_s if parameter_type.nil?
        return '1' if parameter_type.name == 'bool' and TrueClass === val
        return '0' if parameter_type.name == 'bool' and FalseClass === val

        return val.strftime('%Y-%m-%d %H:%M:%S.%6N %z') if Time === val

        val.to_s
      end
    end
  end
end
