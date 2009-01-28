module Vertica
  module Messages
    class Bind < FrontendMessage
      message_id ?B

      def initialize(portal_name, prepared_statement_name, parameter_values)
        @portal_name = portal_name
        @prepared_statement_name = prepared_statement_name
        @parameter_values = parameter_values.map { |pv| pv.to_s }
      end

      def to_bytes(stream)
        size = LENGTH_SIZE
        size += @portal_name.length + 1
        size += @prepared_statement_name.length + 1
        size += 2 # parameter format code (0)
        size += 2 # number of parameter values
        size += @parameter_values.inject(0) { |sum, e| sum += (e.length + 4) }
        size += 2

        stream.write_byte(message_id)
        stream.write_network_int32(size)                      # size
        stream.write_cstring(@portal_name)                    # portal name ("")
        stream.write_cstring(@prepared_statement_name)        # prep
        stream.write_network_int16(0)                         # format codes (0 - default text format)
        stream.write_network_int16(@parameter_values.length)  # number of parameters
        @parameter_values.each do |parameter_value|
          stream.write_network_int32(parameter_value.length)  # parameter value (which is represented as a string) length
          stream.write(parameter_value)               # parameter value written out in text representation
        end
        stream.write_network_int16(0)
      end

    end
  end
end
