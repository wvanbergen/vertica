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
      raise ArgumentError, "message must be a Message. was (#{message.class})" unless message.kind_of?(Messages::Message)
      write message.to_bytes
    end

  end
end
