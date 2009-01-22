module Vertica
  class Result
    
    def initialize(field_descriptions, field_values)
      @field_descriptions = field_descriptions
      @field_values = field_values
    end
    
    def row_count
      @row_count ||= @field_values.length
    end
    
    def columns
      @columns ||= @field_descriptions.map { |fd| Column.new(fd[:type_modifier], fd[:format_code], fd[:table_oid], fd[:name], fd[:attribute_number], fd[:data_type_oid], fd[:data_type_size]) }
    end
    
    def rows
      @field_values.map { |fv| fv.map { |f| f[:value] }}
    end
  end
end
