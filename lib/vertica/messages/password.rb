require 'digest/md5'
          
module Vertica
  module Messages
    class Password < FrontendMessage
      message_id ?p

      def initialize(password, authentication_method = Messages::Authentication::CLEARTEXT_PASSWORD, options = {})
        case authentication_method
        when Messages::Authentication::CLEARTEXT_PASSWORD
          @password = password
        when Messages::Authentication::CRYPT_PASSWORD
          @password = password.crypt(options[:salt])
        when Messages::Authentication::MD5_PASSWORD
          @password = Digest::MD5.hexdigest(password + options[:user])
          @password = Digest::MD5.hexdigest(m + options[:salt])
          @password = 'md5' + @password
        else
          raise ArgumentError.new("unsupported authentication method: #{authentication_method}")
        end
      end

      def to_bytes(stream)
        size = LENGTH_SIZE
        size += @password.length + 1
        stream.write_byte(message_id)
        stream.write_network_int32(size) # size
        stream.write_cstring(@password)
      end

    end
  end
end
