# Class that represents a data type of a column.
# @see Vertica::Column
class Vertica::DataType

  def self.register(oid, name, deserializer = :generic)
    TYPE_OIDS[oid] = { oid: oid, name: name, deserializer: TYPE_DESERIALIZERS.fetch(deserializer) }
  end

  def self.build(oid: nil, **kwargs)
    args = TYPE_OIDS.fetch(oid) do |unknown_oid|
      raise Vertica::Error::UnknownTypeError, "Unknown type OID: #{unknown_oid}"
    end

    new(args.merge(kwargs))
  end

  attr_reader :oid, :name, :size, :modifier, :format, :deserializer

  def initialize(oid: nil, name: nil, size: nil, modifier: nil, format: 0, deserializer: nil)
    @oid, @name, @size, @modifier, @format, @deserializer = oid, name, size, modifier, format, deserializer
  end

  def hash
    [oid, size, modifier, format].hash
  end

  def eql?(other)
    other.kind_of?(Vertica::DataType) && oid == other.oid && size == other.size &&
      modifier == other.modifier && other.format == format
  end

  alias_method :==, :eql?

  def deserialize(bytes)
    return nil if bytes.nil?
    deserializer.call(bytes)
  end

  TYPE_OIDS = {}
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

  private_constant :TYPE_OIDS, :TYPE_DESERIALIZERS
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
