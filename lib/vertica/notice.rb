module Vertica
  class Notice
    attr_reader :field_type
    attr_reader :field_value
    
    def initialize(field_type, field_value)
      @field_type  = field_type
      @field_value = field_value
    end
  end
end
