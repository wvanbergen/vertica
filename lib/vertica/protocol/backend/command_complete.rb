module Vertica
  module Protocol
    class CommandComplete < BackendMessage
      message_id 'C'

      attr_reader :tag, :rows, :oid

      def initialize(data)
        case data = data.unpack('Z*').first
          when /^INSERT /
            @tag, oid, rows = data.split(' ', 3)
            @oid, @rows = oid.to_i, rows.to_i
          when /^DELETE /, /^UPDATE /, /^MOVE /, /^FETCH /, /^COPY /
            @tag, @rows = data.split(' ', 2)
            @rows = rows.to_i
          else
            @tag = data
        end
      end
    end
  end
end
