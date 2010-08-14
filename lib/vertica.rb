module Vertica

  class Error < StandardError

    class ConnectionError < Error; end

    class MessageError < Error; end

  end

  PROTOCOL_VERSION = 3 << 16

  VERSION = "0.7.3"

end

require 'socket'
require 'uri'
require 'stringio'
require 'openssl/ssl'
require 'bigdecimal'
require 'bigdecimal/util'

require 'vertica/bit_helper'
require 'vertica/vertica_socket'

require 'vertica/column'
require 'vertica/result'
require 'vertica/connection'

require 'vertica/messages/message'

require 'vertica/messages/backend_messages/authentication'
require 'vertica/messages/backend_messages/backend_key_data'
require 'vertica/messages/backend_messages/bind_complete'
require 'vertica/messages/backend_messages/close_complete'
require 'vertica/messages/backend_messages/command_complete'
require 'vertica/messages/backend_messages/data_row'
require 'vertica/messages/backend_messages/empty_query_response'
require 'vertica/messages/backend_messages/error_response'
require 'vertica/messages/backend_messages/no_data'
require 'vertica/messages/backend_messages/notice_response'
require 'vertica/messages/backend_messages/notification_response'
require 'vertica/messages/backend_messages/parameter_description'
require 'vertica/messages/backend_messages/parameter_status'
require 'vertica/messages/backend_messages/parse_complete'
require 'vertica/messages/backend_messages/portal_suspended'
require 'vertica/messages/backend_messages/ready_for_query'
require 'vertica/messages/backend_messages/row_description'
require 'vertica/messages/backend_messages/unknown'

require 'vertica/messages/frontend_messages/bind'
require 'vertica/messages/frontend_messages/cancel_request'
require 'vertica/messages/frontend_messages/close'
require 'vertica/messages/frontend_messages/describe'
require 'vertica/messages/frontend_messages/execute'
require 'vertica/messages/frontend_messages/flush'
require 'vertica/messages/frontend_messages/parse'
require 'vertica/messages/frontend_messages/password'
require 'vertica/messages/frontend_messages/query'
require 'vertica/messages/frontend_messages/ssl_request'
require 'vertica/messages/frontend_messages/startup'
require 'vertica/messages/frontend_messages/sync'
require 'vertica/messages/frontend_messages/terminate'
