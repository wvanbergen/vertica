require 'date'
require 'bigdecimal'

module Vertica
  
  class Error < StandardError
    class ConnectionError < Error; end
    class MessageError < Error; end
    class QueryError < Error; end
  end

  PROTOCOL_VERSION = 3 << 16
  VERSION = File.read(File.join(File.dirname(__FILE__), *%w[.. VERSION])).strip

  def self.connect(*args)
    Connection.new(*args)
  end
  
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
  
  def self.quote_identifier(identifier)
    "\"#{identifier.to_s.gsub(/"/, '""')}\""
  end
end

require 'vertica/connection'
