module Vertica
  module Messages
    class Authentication < BackendMessage
      message_id ?R

      OK                  = 0
      KERBEROS_V5         = 2
      CLEARTEXT_PASSWORD  = 3
      CRYPT_PASSWORD      = 4
      MD5_PASSWORD        = 5
      SCM_CREDENTIAL      = 6
      GSS                 = 7
      GSS_CONTINUE        = 8
      SSPI                = 9

      attr_reader :code
      attr_reader :salt
      attr_reader :auth_data

      def initialize(stream, size)
        super
        @code = stream.read_network_int32
        if @code == CRYPT_PASSWORD
          @salt = stream.readn(2)
        elsif @code == MD5_PASSWORD
          @salt = stream.readn(4)
        elsif @code == GSS_CONTINUE
          @auth_data = stream.readn(size - 9)
        end
      end
    end
  end
end
