module Vertica
  module Protocol
    class ErrorResponse < NoticeResponse
      message_id 'E'
    end
  end
end
