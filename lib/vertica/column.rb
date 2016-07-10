module Vertica
  class Column
    attr_reader :name
    attr_reader :table_oid
    attr_reader :attribute_number
    attr_reader :data_type

    def initialize(name: nil, table_oid: nil, attribute_number: nil, data_format: 0, data_type_oid: nil, data_type_size: nil, data_type_modifier: nil)
      @name             = name
      @table_oid        = table_oid
      @attribute_number = attribute_number
      @data_type        = Vertica::DataType.build(oid: data_type_oid, size: data_type_size, modifier: data_type_modifier, format: data_format)
    end

    def eql?(other)
      self.class === other && other.name == name && other.data_type == data_type &&
        other.table_oid == table_oid && other.attribute_number == attribute_number
    end

    alias_method :==, :eql?

    def hash
      [name, data_type, table_oid, attribute_number].hash
    end
  end
end
