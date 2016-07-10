# Class representing a column in a result.
#
# @attr_reader name [String] The name of the column
# @attr_reader table_oid [Integer, nil] The OID of the table this column originates from. This can
#   be nil if the volumn was computed, and was not soruced from a table
# @attr_reader attribute_number [Integer, nil] The attribute index in the table this column originates from.
#   This can be nil if the volumn was computed, and was not soruced from a table
# @attr_reader data_type [Vertica::DataType] The type of the values in this column.
#
# @see Vertica::RowDescription
# @see Vertica::DataType
class Vertica::Column

  # Builds a new column instance based on the values provided by a {Vertica::Protocol::RowDescription} message.
  # @param name [String] The name of the column
  # @param table_oid [Integer, nil] The OID of the table this column originates from. This can
  #   be nil if the volumn was computed, and was not soruced from a table
  # @param attribute_number [Integer, nil] The attribute index in the table this column originates from.
  #   This can be nil if the volumn was computed, and was not soruced from a table
  # @param data_type_oid [Integer] The object ID of the type.
  # @param data_type_size [Integer] The size of the type.
  # @param data_type_modifier [Integer] A modifier of the type.
  # @param data_format [Integer] The serialization format of this type.
  # @return [Vertica::Column]
  def self.build(name: nil, table_oid: nil, attribute_number: nil, data_format: 0, data_type_oid: nil, data_type_size: nil, data_type_modifier: nil)
    data_type = Vertica::DataType.build(oid: data_type_oid, size: data_type_size, modifier: data_type_modifier, format: data_format)
    new(name: name, data_type: data_type, table_oid: table_oid, attribute_number: attribute_number)
  end

  attr_reader :name, :data_type, :table_oid, :attribute_number

  # Initializes a new Vertica::Column.
  # @see .build
  def initialize(name: nil, data_type: nil, table_oid: nil, attribute_number: nil)
    @name             = name
    @table_oid        = table_oid
    @attribute_number = attribute_number
    @data_type        = data_type
  end

  # @return [Boolean] Returns true iff this record is equal to the other provided object
  def eql?(other)
    self.class === other && other.name == name && other.data_type == data_type &&
      other.table_oid == table_oid && other.attribute_number == attribute_number
  end

  alias_method :==, :eql?

  # @return [Integer] Returns a hash digtest of this object.
  def hash
    [name, data_type, table_oid, attribute_number].hash
  end

  # @return [String] Returns a user-consumable string representation of this column.
  def inspect
    "#<#{self.class.name} name=#{name.inspect} data_type=#{data_type.inspect}>"
  end
end
