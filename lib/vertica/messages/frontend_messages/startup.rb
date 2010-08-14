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
        size = LENGTH_SIZE + 4 # length + protocol
        size += @user.length     + 4 + 2 if @user
        size += @database.length + 8 + 2 if @database
        size += @options.length  + 7 + 2 if @options
        size += 1 # ending zero

        bytes = [
          size.to_network_int32,
          Vertica::PROTOCOL_VERSION.to_network_int32
        ]
        bytes += ['user'.to_cstring, @user.to_cstring] if @user
        bytes += ['database'.to_cstring, @database.to_cstring] if @database
        bytes += ['options'.to_cstring, @options.to_cstring] if @options
        bytes << 0
        bytes.flatten.join
      end

    end
  end
end
