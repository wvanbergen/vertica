module Vertica
  module Messages
    class EmptyQueryResponse < BackendMessage
      message_id ?I
    end
  end
end
