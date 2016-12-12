module Vertica
  module Protocol

    class Startup < FrontendMessage
      message_id nil

      def initialize(user, database, options = nil)
        @user     = user
        @database = database
        @options  = options
        @type     = "vertica-rb"
        @pid      = Process.pid.to_s
        @platform = Gem::Platform.local.to_s
        @version  = Vertica::VERSION
        @label    = @type+"-"+@version+"-"+SecureRandom.hex(10) 
      end

      def message_body
        str =  [Vertica::PROTOCOL_VERSION].pack('N')
        str << ["user", @user].pack('Z*Z*')         if @user
        str << ["database", @database].pack('Z*Z*') if @database
        str << ["client_type", @type].pack('Z*Z*')
        str << ["client_pid", @pid].pack('Z*Z*')
        str << ["client_os", @platform].pack('Z*Z*')
        str << ["client_version", @version].pack('Z*Z*')
        str << ["client_label", @label].pack('Z*Z*')
        str << ["options", @options].pack('Z*Z*')   if @options
        str << [].pack('x')
      end
    end
  end
end
