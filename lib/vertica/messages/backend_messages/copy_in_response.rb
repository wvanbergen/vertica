module Vertica
  module Messages
    class CopyInResponse < BackendMessage
      message_id 'G'
      
      def initialize(data)
        values = data.unpack('Cn*')
        @format = values[0]
        @column_formats = values.slice(2..-1)
      end
    end
  end
end
