module Vertica
  module SocketInstanceMethods

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
      readn(4).unpack('N').first
    end

    def read_cstring
      readline("\000")[0..-2]
    end

    def read_message
      type = [read_byte].pack('C')
      size = read_network_int32

      raise Vertica::Error::MessageError.new("Bad message size: #{size}") unless size >= 4
      Messages::BackendMessage.factory type, self, size
    end

    def write_message(message)
      raise ArgumentError, "invalid message: (#{message.inspect})" unless message.respond_to?(:to_bytes)
      write message.to_bytes
    end
  end
end
