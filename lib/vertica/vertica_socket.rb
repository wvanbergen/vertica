require 'socket'
require 'vertica/bit_helper'

module Vertica
  class VerticaSocket < TCPSocket
    include BitHelper
  end
end
