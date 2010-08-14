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
        size = LENGTH_SIZE
        size += @name.length + 1
        size += @query.length + 1
        size += 2
        size += (@param_types.length * 4)

        [ message_id.to_byte,
          size.to_network_int32,
          @name.to_cstring,
          @query.to_cstring,
          @param_types.length.to_network_int16,
          @param_types.map { |type| type.to_network_int32 }
        ].flatten.join
      end

    end
  end
end
