module Vertica
  module Messages

    class Startup < FrontendMessage
      message_id nil

      def initialize(user, database, options = nil)
        @user     = user
        @database = database
        @options  = options
      end

      def message_body
        str =  [Vertica::PROTOCOL_VERSION].pack('N')
        str << ["user", @user].pack('Z*Z*')         if @user
        str << ["database", @database].pack('Z*Z*') if @database
        str << ["options", @options].pack('Z*Z*')   if @options
        str << [].pack('x')
      end
    end
  end
end
