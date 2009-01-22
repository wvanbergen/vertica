module Vertica
  module Messages
    class Startup < FrontendMessage
      message_id nil

      def initialize(user, database, options = nil)
        @user     = user
        @database = database
        @options  = options
      end

      def to_bytes(stream)
        size = LENGTH_SIZE + 4 # length + protocol
        size += @user.length     + 4 + 2 if @user
        size += @database.length + 8 + 2 if @database
        size += @options.length  + 7 + 2 if @options
        size += 1 # ending zero

        stream.write_network_int32(size) # size
        stream.write_network_int32(Vertica::PROTOCOL_VERSION) # proto version
        if @user
          stream.write_cstring('user')
          stream.write_cstring(@user)
        end
        if @database
          stream.write_cstring('database')
          stream.write_cstring(@database)
        end
        if @options
          stream.write_cstring('options')
          stream.write_cstring(@options)
        end
        stream.write_byte(0)
      end

    end
  end
end
