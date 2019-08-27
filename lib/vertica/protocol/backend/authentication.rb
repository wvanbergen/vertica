module Vertica
  module Protocol
    class Authentication < BackendMessage
      message_id 'R'

      OK                  = 0
      KERBEROS_V4         = 1
      KERBEROS_V5         = 2
      CLEARTEXT_PASSWORD  = 3
      CRYPT_PASSWORD      = 4
      MD5_PASSWORD        = 5
      SCM_CREDENTIAL      = 6
      GSS                 = 7
      GSS_CONTINUE        = 8
      CHANGE_PASSWORD     = 9
      PASSWORD_CHANGED    = 10
      PASSWORD_GRACE      = 11
      HASH                = 65536
      HASH_MD5            = 65536+5
      HASH_SHA512         = 65536+512

      attr_reader :code
      attr_reader :salt
      attr_reader :userSalt
      attr_reader :auth_data

      def initialize(data)
        @code, other = data.unpack('Na*')
        puts "codes we received #{@code}, #{other}"
        case @code
          when CRYPT_PASSWORD then @salt = other
          when MD5_PASSWORD, HASH_MD5 then @salt = other[0..3]
          when GSS_CONTINUE then @auth_data = other
          when HASH, HASH_SHA512
            @salt =  other[0..3]
            @userSaltLen = other[4..7].unpack('n').first
            puts "user salt length is #{@userSaltLen}"
            if @userSaltLen != 16
              puts "user salt length isn't 16, raise error"
            end 
            @userSalt = other[8..other.size].unpack("!#{@userSaltLen}s")[0]
            puts "user salt is #{@userSalt}"
        end
      end
    end
  end
end
