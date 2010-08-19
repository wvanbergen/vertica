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
        bytes = [
          @portal_name.to_cstring,                    # portal name ("")
          @prepared_statement_name.to_cstring,        # prep
          0.to_network_int16,                         # format codes (0 - default text format)
          @parameter_values.length.to_network_int16,  # number of parameters
        ]
        @parameter_values.each do |parameter_value|
          bytes << parameter_value.length.to_network_int32  # parameter value (which is represented as a string) length
          bytes << parameter_value                          # parameter value written out in text representation
        end
        message_string bytes
      end

    end
  end
end
