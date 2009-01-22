module Vertica
  module BitHelper

    def readn(n)
      s = read(n)
      raise "couldn't read #{n} characters" if s.nil? or s.size != n # TODO make into a Vertica Exception
      s
    end
    
    def write_byte(value)
      write [value].pack('C')
    end
    
    def read_byte
      readn(1).unpack('C').first
    end
    
    def read_network_int16
      readn(2).unpack('n').first
    end
    
    def write_network_int32(value)
      write [value].pack('N')
    end
    
    def read_network_int32
      readn(4).unpack('N').first
    end

    def write_cstring(value)
      raise ArgumentError, "Invalid cstring" if value.include?(0)
      write "#{value}\000"
    end

    def read_cstring
      readline("\000")[0..-2]
    end

  end
end
