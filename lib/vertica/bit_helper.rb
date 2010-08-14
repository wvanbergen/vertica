module Vertica
  module BitHelper

    def read_message
      type = read_byte
      size = read_network_int32

      raise Vertica::Error::MessageError.new("Bad message size: #{size}") unless size >= 4
      Message.factory type, self, size
    end

    def readn(n)
      s = read(n)
      raise "couldn't read #{n} characters" if s.nil? or s.size != n # TODO make into a Vertica Exception
      s
    end

    def read_byte
      readn(1).unpack('C').first
    end

    def read_network_int16
      readn(2).unpack('n').first
    end

    def read_network_int32
      handle_endian_flavor(readn(4)).unpack('l').first
    end

    def read_cstring
      readline("\000")[0..-2]
    end

    def handle_endian_flavor(s)
      little_endian? ? s.reverse : s
    end

    def little_endian?
      @little_endian ||= ([0x12345678].pack("L") == "\x12\x34\x56\x78" ? false : true)
    end
  end
end
