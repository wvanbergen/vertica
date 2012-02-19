require 'date'
require 'bigdecimal'

# Main module for this library. It contains the {.connect} method to return a
# {Vertica::Connection} instance, and methods to quote values ({.quote}) and 
# identifiers ({.quote_identifier}) to safely include those in SQL strings to
# prevent SQL injection.
module Vertica
  
  class Error < StandardError
    class ConnectionError < Error; end
    class MessageError < Error; end
    class QueryError < Error; end
    class SynchronizeError < Error; end
  end

  # The version number of this library.
  VERSION = File.read(File.join(File.dirname(__FILE__), *%w[.. VERSION])).strip

  # The protocol version (3.0.0) implemented in this library.
  PROTOCOL_VERSION = 3 << 16

  # Opens a new connection to a Vertica database.
  # @param (see Vertica::Connection#initialize)
  # @return [Vertica::Connection] The created connection to Vertica, ready for queries.
  def self.connect(options)
    Vertica::Connection.new(options)
  end
  
  # Properly quotes a value for safe usage in SQL queries.
  #
  # This method has quoting rules for common types. Any other object will be converted to
  # a string using +:to_s+ and then quoted as a string.
  #
  # @param [Object] value The value to quote.
  # @return [String] The quoted value that can be safely included in SQL queries.
  def self.quote(value)
    case value
      when nil        then 'NULL'
      when false      then 'FALSE'
      when true       then 'TRUE'
      when DateTime   then value.strftime("'%Y-%m-%d %H:%M:%S'::timestamp")
      when Time       then value.strftime("'%Y-%m-%d %H:%M:%S'::timestamp")
      when Date       then value.strftime("'%Y-%m-%d'::date")
      when String     then "'#{value.gsub(/'/, "''")}'"
      when BigDecimal then value.to_s('F')
      when Numeric    then value.to_s
      when Array      then value.map { |v| self.quote(v) }.join(', ')
      else self.quote(value.to_s)
    end
  end
  
  # Quotes an identifier for safe use within SQL queries, using double quotes.
  # @param [:to_s] identifier The identifier to quote.
  # @return [String] The quoted identifier that can be safely included in SQL queries.
  def self.quote_identifier(identifier)
    "\"#{identifier.to_s.gsub(/"/, '""')}\""
  end
end

require 'vertica/connection'
