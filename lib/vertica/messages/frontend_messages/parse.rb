module Vertica
  module Messages
    class Parse < FrontendMessage
      message_id ?P

      def initialize(name, query, param_types)
        @name         = name
        @query        = query
        @param_types  = param_types
      end

      def to_bytes
        message_string([ 
          @name.to_cstring,
          @query.to_cstring,
          @param_types.length.to_network_int16,
          @param_types.map { |type| type.to_network_int32 }
        ].flatten)
      end

    end
  end
end
