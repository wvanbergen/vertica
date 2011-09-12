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

%w[
  socket
  uri
  openssl/ssl
  bigdecimal
  bigdecimal/util
  date

  vertica/column
  vertica/result
  vertica/connection

  vertica/messages/message

  vertica/messages/backend_messages/authentication
  vertica/messages/backend_messages/backend_key_data
  vertica/messages/backend_messages/bind_complete
  vertica/messages/backend_messages/close_complete
  vertica/messages/backend_messages/command_complete
  vertica/messages/backend_messages/data_row
  vertica/messages/backend_messages/empty_query_response
  vertica/messages/backend_messages/notice_response
  vertica/messages/backend_messages/error_response
  vertica/messages/backend_messages/no_data
  vertica/messages/backend_messages/notification_response
  vertica/messages/backend_messages/parameter_description
  vertica/messages/backend_messages/parameter_status
  vertica/messages/backend_messages/parse_complete
  vertica/messages/backend_messages/portal_suspended
  vertica/messages/backend_messages/ready_for_query
  vertica/messages/backend_messages/row_description
  vertica/messages/backend_messages/unknown

  vertica/messages/frontend_messages/bind
  vertica/messages/frontend_messages/cancel_request
  vertica/messages/frontend_messages/close
  vertica/messages/frontend_messages/describe
  vertica/messages/frontend_messages/execute
  vertica/messages/frontend_messages/flush
  vertica/messages/frontend_messages/parse
  vertica/messages/frontend_messages/password
  vertica/messages/frontend_messages/query
  vertica/messages/frontend_messages/ssl_request
  vertica/messages/frontend_messages/startup
  vertica/messages/frontend_messages/sync
  vertica/messages/frontend_messages/terminate
].each { |r| require r }
