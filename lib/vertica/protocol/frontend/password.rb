module Vertica
  module Protocol
    class Password < FrontendMessage
      message_id 'p'

      def initialize(password, auth_method: Vertica::Protocol::Authentication::CLEARTEXT_PASSWORD, salt: nil, user: nil)
        @password = password
        @auth_method, @salt, @user = auth_method, salt, user
      end

      def encoded_password
        case @auth_method
        when Vertica::Protocol::Authentication::CLEARTEXT_PASSWORD
          @password
        when Vertica::Protocol::Authentication::CRYPT_PASSWORD
          @password.crypt(@salt)
        when Vertica::Protocol::Authentication::MD5_PASSWORD, \
             Vertica::Protocol::Authentication::HASH_MD5,     \
             Vertica::Protocol::Authentication::HASH,         \
             Vertica::Protocol::Authentication::HASH_SHA512
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
