module Vertica
  module Messages
    class Password < FrontendMessage
      message_id 'p'

      def initialize(password, auth_method: Vertica::Messages::Authentication::CLEARTEXT_PASSWORD, salt: nil, user: nil)
        @password = password
        @auth_method, @salt, @user = auth_method, salt, user
      end

      def encoded_password
        case @auth_method
        when Vertica::Messages::Authentication::CLEARTEXT_PASSWORD
          @password
        when Vertica::Messages::Authentication::CRYPT_PASSWORD
          @password.crypt(@salt)
        when Vertica::Messages::Authentication::MD5_PASSWORD
          require 'digest/md5'
          @password = Digest::MD5.hexdigest("#{@password}#{@user}")
          @password = Digest::MD5.hexdigest("#{@password}#{@salt}")
          @password = "md5#{@password}"
        else
          raise ArgumentError.new("unsupported authentication method: #{@auth_method}")
        end
      end

      def message_body
        [encoded_password].pack('Z*')
      end
    end
  end
end
