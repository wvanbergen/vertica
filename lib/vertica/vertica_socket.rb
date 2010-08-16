module Vertica
  class VerticaSocket < TCPSocket
    include BitHelper

    def read_message
      type = read_byte
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
