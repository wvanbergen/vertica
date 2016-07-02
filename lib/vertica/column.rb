module Vertica
  class Column
    attr_reader :name
    attr_reader :table_oid
    attr_reader :attribute_number
    attr_reader :format
    attr_reader :data_type
    attr_reader :data_type_size
    attr_reader :data_type_modifier

    STRING_CONVERTER = lambda { |s| s.force_encoding('utf-8') }

    FLOAT_CONVERTER = lambda do |s|
      case s
      when 'Infinity'
        Float::INFINITY
      when '-Infinity'
        -Float::INFINITY
      when 'NaN'
        Float::NAN
      else
        s.to_f
      end
    end

    DATA_TYPE_CONVERSIONS = {
      0   => [:unspecified,  nil],
      1   => [:tuple,        nil],
      2   => [:pos,          nil],
      3   => [:record,       nil],
      4   => [:unknown,      nil],
      5   => [:bool,         lambda { |s| s == 't' }],
      6   => [:integer,      lambda { |s| s.to_i }],
      7   => [:float,        FLOAT_CONVERTER],
      8   => [:char,         STRING_CONVERTER],
      9   => [:varchar,      STRING_CONVERTER],
      10  => [:date,         lambda { |s| Date.new(*s.split("-").map{|x| x.to_i}) }],
      11  => [:time,         nil],
      12  => [:timestamp,    lambda { |s| DateTime.parse(s, true) }],
      13  => [:timestamp_tz, lambda { |s| DateTime.parse(s, true) }],
      14  => [:interval,     nil],
      15  => [:time_tz,      nil],
      16  => [:numeric,      lambda { |s| BigDecimal.new(s) }],
      17  => [:bytea,        lambda { |s| s.gsub(/\\([0-3][0-7][0-7])/) { $1.to_i(8).chr }} ],
      18  => [:rle_tuple,    nil],
      115 => [:long_varchar, STRING_CONVERTER],
    }

    DATA_TYPES = DATA_TYPE_CONVERSIONS.values.map { |t| t[0] }

    def initialize(name: nil, table_oid: nil, attribute_number: nil, format_code: 0, data_type_oid: nil, data_type_size: nil, data_type_modifier: nil)
      @name             = name
      @table_oid        = table_oid
      @attribute_number = attribute_number

      @format                = format_code == 0 ? :text : :binary
      @data_type_size        = data_type_size
      @data_type_modifier    = data_type_modifier
      @data_type, @converter = column_type_from_oid(data_type_oid)
    end

    def eql?(other)
      self.class === other &&
        other.name == name &&
        other.format == format &&
        other.data_type == data_type &&
        other.data_type_size == data_type_size &&
        other.data_type_modifier == data_type_modifier &&
        other.table_oid == table_oid &&
        other.attribute_number == attribute_number
    end

    alias_method :==, :eql?

    def hash
      [name, format, data_type, data_type_size, data_type_modifier, table_oid, attribute_number].hash
    end

    def convert(s)
      return unless s
      @converter ? @converter.call(s) : s
    end

    private

    def column_type_from_oid(oid)
      DATA_TYPE_CONVERSIONS.fetch(oid) do |unknown_oid|
        raise Vertica::Error::UnknownTypeError, "Unknown type OID: #{unknown_oid}"
      end
    end
  end
end
