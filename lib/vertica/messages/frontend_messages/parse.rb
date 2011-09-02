module Vertica
  module Messages
    class Parse < FrontendMessage
      message_id 'P'

      def initialize(name, query, param_types)
        @name         = name
        @query        = query
        @param_types  = param_types
      end

      def to_bytes
        message_string([@name, @query, @param_types.length, *@param_types].pack('Z*Z*nN*'))
      end
    end
  end
end
