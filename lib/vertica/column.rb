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
      [:bytea,        lambda { |s| s.gsub(/(\\[0-3][0-7][0-7]|.)/) { |e| e.length == 4 ? [e[1..3].oct].pack('C') : e } }],
      [:rle_tuple,    nil]
    ]

    DATA_TYPES = DATA_TYPE_CONVERSIONS.map { |t| t[0] }

    def initialize(col)
      @type_modifier    = col[:type_modifier]
      @format           = col[:format_code] == 0 ? :text : :binary
      @table_oid        = col[:table_oid]
      @name             = col[:name].to_sym
      @attribute_number = col[:attribute_number]
      @data_type        = DATA_TYPE_CONVERSIONS[col[:data_type_oid]][0]
      @converter        = DATA_TYPE_CONVERSIONS[col[:data_type_oid]][1]
      @size             = col[:data_type_size]
    end

    def convert(s)
      return unless s
      @converter ? @converter.call(s) : s
    end
  end
end
