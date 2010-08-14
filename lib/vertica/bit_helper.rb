module Vertica
  module BitHelper

    def readn(n)
      s = read(n)
      raise "couldn't read #{n} characters" if s.nil? or s.size != n # TODO make into a Vertica Exception
      s
    end

    def read_byte
      readn(1).to_byte
    end

    def read_network_int16
      readn(2).to_network_int16
    end

    def read_network_int32
      handle_endian_flavor(readn(4)).to_network_int32
    end

    def read_cstring
      readline("\000").from_cstring
    end

    def handle_endian_flavor(s)
      little_endian? ? s.reverse : s
    end

    def little_endian?
      @little_endian ||= ([0x12345678].pack("L") == "\x12\x34\x56\x78" ? false : true)
    end
  end
end
