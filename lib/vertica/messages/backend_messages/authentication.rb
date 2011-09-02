module Vertica
  module Messages
    class Authentication < BackendMessage
      message_id 'R'

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

      def initialize(data)
        @code, other = data.unpack('Na*')
        case @code
          when CRYPT_PASSWORD, MD5_PASSWORD then @salt = other
          when GSS_CONTINUE then @auth_data = other
        end
      end
    end
  end
end
