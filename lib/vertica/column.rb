module Vertica
  class Column
    attr_reader :name
    attr_reader :table_oid
    attr_reader :type_modifier
    attr_reader :size
    attr_reader :data_type

    def initialize(type_modifier, format_code, table_oid, name, attribute_number, data_type_oid, size)
      @type_modifier = type_modifier
      @format = (format_code == 0 ? :text : :binary)
      @table_oid = table_oid
      @name = name
      @attribute_number = attribute_number
      @data_type = convert_data_type_to_sym(data_type_oid)
      @size = size
    end
    
    protected

    def convert_data_type_to_sym(data_type)
      case data_type
      when 0
        :unspecified      # Vertica: Unspecified
      when 1          
        :tuple            # Vertica: tuple
      when 2          
        :pos              # Vertica Position
      when 3          
        :record           # PG record
      when 4          
        :unknown          # T_String
      when 5          
        :bool
      when 6          
        :in
      when 7          
        :float
      when 8          
        :char
      when 9          
        :varchar
      when 10         
        :date
      when 11         
        :time
      when 12         
        :timestamp
      when 13         
        :timestamp_tz
      when 14         
        :interval
      when 15         
        :time_tz
      when 16         
        :numberic         # not yet supported
      when 17         
        :bytea            # not yet supported
      when 18         
        :rle_tuple        # Vertica: RLE_count/Tuple pairs
      else
        nil
      end
    end
    
  end
end
