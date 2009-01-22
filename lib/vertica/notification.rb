module Vertica
  class Notification
    attr_reader :process_pid
    attr_reader :condition
    attr_reader :additional_info
    
    def initialize(process_pid, condition, additional_info)
      @process_pid     = process_pid
      @condition       = condition
      @additional_info = additional_info
    end
  end
end
