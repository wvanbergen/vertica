module Vertica
  module Messages
    class Parse < FrontendMessage
      message_id ?P

      def initialize(name, query, param_types)
        @name         = name
        @query        = query
        @param_types  = param_types
      end

      def to_bytes(stream)
        size = LENGTH_SIZE
        size += @name.length + 1
        size += @query.length + 1
        size += 2
        size += (@param_types.length * 4)
        
        stream.write_byte(message_id)
        stream.write_network_int32(size) # size
        stream.write_cstring(@name)
        stream.write_cstring(@query)
        stream.write_network_int16(@param_types.length)
        @param_types.each do |param_type|
          stream.write_network_int32(param_type)
        end
      end

    end
  end
end
