module Vertica
  module Messages

    class Startup < FrontendMessage
      message_id nil

      def initialize(user, database, options = nil)
        @user     = user
        @database = database
        @options  = options
      end

      def to_bytes
        bytes =   [Vertica::PROTOCOL_VERSION.to_network_int32]
        bytes +=  ['user'.to_cstring, @user.to_cstring] if @user
        bytes +=  ['database'.to_cstring, @database.to_cstring] if @database
        bytes +=  ['options'.to_cstring, @options.to_cstring] if @options
        bytes << 0

        message_string bytes
      end

    end
  end
end
