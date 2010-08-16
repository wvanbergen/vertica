module Vertica
  class Error < StandardError
    class ConnectionError < Error; end
    class MessageError < Error; end
  end

  PROTOCOL_VERSION = 3 << 16
  VERSION = File.read(File.join(File.dirname(__FILE__), *%w[.. VERSION])).strip

  def self.connect(*args)
    Connection.new(*args)
  end
end

%w[
  socket
  uri
  openssl/ssl
  bigdecimal
  bigdecimal/util
  date

  vertica/core_ext/numeric
  vertica/core_ext/string

  vertica/bit_helper
  vertica/vertica_socket

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
  vertica/messages/backend_messages/error_response
  vertica/messages/backend_messages/no_data
  vertica/messages/backend_messages/notice_response
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
