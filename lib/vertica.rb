module Vertica
  
  class Error < StandardError
    
    class ConnectionError < Error; end
    
    class MessageError < Error; end
    
  end
  
  PROTOCOL_VERSION = 3 << 16
  
end

require 'vertica/column'
require 'vertica/result'
require 'vertica/connection'
