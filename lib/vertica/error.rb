# Main class for exceptions relating to Vertica.
class Vertica::Error < StandardError
  
  class ConnectionError < Vertica::Error; end
  class MessageError < Vertica::Error; end
  class SynchronizeError < Vertica::Error; end
  class EmptyQueryError < Vertica::Error; end
  class ReadTimeout < Vertica::Error; end
    
  class QueryError < Vertica::Error
    
    attr_reader :error_response
    
    def initialize(error_response)
      @error_response = error_response
      super(error_response.error_message)
    end
    
    def self.from_error_response(error_response)
      klass = QUERY_ERROR_CLASSES[error_response.sqlstate] || self
      klass.new(error_response)
    end
  end
  
  QUERY_ERROR_CLASSES = {
    '55V03' => (LockFailure     = Class.new(Vertica::Error::QueryError)),
    '53200' => (OutOfMemory     = Class.new(Vertica::Error::QueryError)),
    '42601' => (SyntaxError     = Class.new(Vertica::Error::QueryError)),
    '42V01' => (MissingRelation = Class.new(Vertica::Error::QueryError)),
    '42703' => (MissingColumn   = Class.new(Vertica::Error::QueryError))
  }
end
