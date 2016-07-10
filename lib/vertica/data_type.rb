# Class that represents a data type of a column.
#
# This gem is only able to handle registered types. Types are registered using {.register}.
# If an unregistered type is encountered, the library will raise {Vertica::Error::UnknownTypeError}.
#
# @example Handling an unknown OID:
#   Vertica::DataType.register 12345, 'fancy_type', lambda { |bytes| ... }
#
# @attr_reader oid [Integer] The object ID of the type.
# @attr_reader name [String] The name of the type as it can be used in SQL.
# @attr_reader size [Integer] The size of the type.
# @attr_reader modifier [Integer] A modifier of the type.
# @attr_reader format [Integer] The serialization format of this type.
# @attr_reader deserializer [Proc] Proc that can deserialize values of this type coming from the database.
#
# @see Vertica::Column
class Vertica::DataType

  class << self
    # @return [Hash<Integer, Hash>] The Vertica types that are registered with this library, indexed by OID.
    # @see .register
    attr_accessor :registered_types

    # Registers a new type by OID.
    #
    # @param oid [Integer] The object ID of the type.
    # @param name [String] The name of the type as it can be used in SQL.
    # @param deserializer [Proc] Proc that can deserialize values of this type coming
    #    from the database.
    # @return [void]
    def register(oid, name, deserializer = self.default_deserializer)
      self.registered_types ||= {}
      self.registered_types[oid] = { oid: oid, name: name, deserializer: TYPE_DESERIALIZERS.fetch(deserializer) }
    end

    # Builds a new type instance based on an OID.
    # @param (see Vertica::DataType#initialize)
    # @return [Vertica::DataType]
    # @raise [Vertica::Error::UnknownTypeError] if the OID is not registered.
    def build(oid: nil, **kwargs)
      args = registered_types.fetch(oid) do |unknown_oid|
        raise Vertica::Error::UnknownTypeError, "Unknown type OID: #{unknown_oid}"
      end

      new(args.merge(kwargs))
    end

    # The name of the default deserializer proc.
    # @return [Symbol]
    def default_deserializer
      :generic
    end
  end

  attr_reader :oid, :name, :size, :modifier, :format, :deserializer

  # Instantiates a new DataType.
  #
  # @param oid [Integer] The object ID of the type.
  # @param name [String] The name of the type as it can be used in SQL.
  # @param size [Integer] The size of the type.
  # @param modifier [Integer] A modifier of the type.
  # @param format [Integer] The serialization format of this type.
  # @param deserializer [Proc] Proc that can deserialize values of this type coming
  #    from the database.
  # @see .build
  def initialize(oid: nil, name: nil, size: nil, modifier: nil, format: 0, deserializer: nil)
    @oid, @name, @size, @modifier, @format, @deserializer = oid, name, size, modifier, format, deserializer
  end

  # @return [Integer] Returns a hash digtest of this object.
  def hash
    [oid, size, modifier, format].hash
  end

  # @return [Boolean] Returns true iff this record is equal to the other provided object
  def eql?(other)
    other.kind_of?(Vertica::DataType) && oid == other.oid && size == other.size &&
      modifier == other.modifier && other.format == format
  end

  alias_method :==, :eql?

  # Deserializes a value of this type as returned by the server.
  # @param bytes [String, nil] The representation of the value returned by the server.
  # @return [Object] The Ruby-value taht repesents the value returned from the DB.
  # @see Vertica::Protocol::DataRow
  def deserialize(bytes)
    return nil if bytes.nil?
    deserializer.call(bytes)
  end

  # @return [String] Returns a user-consumable string representation of this type.
  def inspect
    "#<#{self.class.name}:#{oid} #{sql.inspect}>"
  end

  # Returns a SQL representation of this type.
  # @return [String]
  # @todo Take size and modifier into account.
  def sql
    name
  end

  TYPE_DESERIALIZERS = {
    generic: lambda { |bytes| bytes },
    bool: lambda { |bytes|
      case bytes
        when 't'; true
        when 'f'; false
        else raise ArgumentError, "Cannot convert #{bytes.inspect} to a boolean value"
      end
    },
    integer: lambda { |bytes| Integer(bytes) },
    float: lambda { |bytes|
      case bytes
        when 'Infinity'; Float::INFINITY
        when '-Infinity'; -Float::INFINITY
        when 'NaN'; Float::NAN
        else Float(bytes)
      end
    },
    bigdecimal: lambda { |bytes| BigDecimal(bytes) },
    unicode_string: lambda { |bytes| bytes.force_encoding(Encoding::UTF_8) },
    binary_string: lambda { |bytes| bytes.gsub(/\\([0-3][0-7][0-7])/) { $1.to_i(8).chr }.force_encoding(Encoding::BINARY) },
    date: lambda { |bytes| Date.parse(bytes) },
    timestamp: lambda { |bytes| Time.parse(bytes) },
  }.freeze

  private_constant :TYPE_DESERIALIZERS
end

Vertica::DataType.register 5, 'bool', :bool
Vertica::DataType.register 6, 'integer', :integer
Vertica::DataType.register 7, 'float', :float
Vertica::DataType.register 8, 'char', :unicode_string
Vertica::DataType.register 9, 'varchar', :unicode_string
Vertica::DataType.register 10, 'date', :date
Vertica::DataType.register 11, 'time'
Vertica::DataType.register 12, 'timestamp', :timestamp
Vertica::DataType.register 13, 'timestamp_tz', :timestamp
Vertica::DataType.register 14, 'time_tz'
Vertica::DataType.register 15, 'interval'
Vertica::DataType.register 16, 'numeric', :bigdecimal
Vertica::DataType.register 17, 'bytes', :binary_string
Vertica::DataType.register 115, 'long varchar', :unicode_string
