module Vertica
  class Column
    attr_reader :name
    attr_reader :table_oid
    attr_reader :type_modifier
    attr_reader :size
    attr_reader :data_type

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

    DATA_TYPE_CONVERSIONS = [
      [:unspecified,  nil],
      [:tuple,        nil],
      [:pos,          nil],
      [:record,       nil],
      [:unknown,      nil],
      [:bool,         lambda { |s| s == 't' }],
      [:integer,      lambda { |s| s.to_i }],
      [:float,        FLOAT_CONVERTER],
      [:char,         STRING_CONVERTER],
      [:varchar,      STRING_CONVERTER],
      [:date,         lambda { |s| Date.new(*s.split("-").map{|x| x.to_i}) }],
      [:time,         nil],
      [:timestamp,    lambda { |s| DateTime.parse(s, true) }],
      [:timestamp_tz, lambda { |s| DateTime.parse(s, true) }],
      [:interval,     nil],
      [:time_tz,      nil],
      [:numeric,      lambda { |s| BigDecimal.new(s) }],
      [:bytea,        lambda { |s| s.gsub(/\\([0-3][0-7][0-7])/) { $1.to_i(8).chr }} ],
      [:rle_tuple,    nil]
    ]

    DATA_TYPES = DATA_TYPE_CONVERSIONS.map { |t| t[0] }

    def initialize(col)
      @type_modifier    = col.fetch(:type_modifier)
      @format           = col.fetch(:format_code) == 0 ? :text : :binary
      @table_oid        = col.fetch(:table_oid)
      @name             = col.fetch(:name).to_sym
      @attribute_number = col.fetch(:attribute_number)
      @size             = col.fetch(:data_type_size)

      @data_type, @converter = column_type_from_oid(col.fetch(:data_type_oid))
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
