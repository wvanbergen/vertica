class Vertica::Result
  include Enumerable
  
  attr_reader :columns
  attr_reader :rows
  attr_accessor :tag, :notice

  def initialize(row_style = :hash)
    @row_style = row_style
    @rows = []
  end

  def descriptions=(message)
    @columns = message.fields.map { |fd| Vertica::Column.new(fd) }
  end

  def format_row_as_hash(row_data)
    row = {}
    row_data.values.each_with_index do |value, idx|
      col = columns[idx]
      row[col.name] = col.convert(value)
    end
    row
  end
  
  def format_row(row_data)
    send("format_row_as_#{@row_style}", row_data)
  end
  
  def format_row_as_array(row_data)
    row = []
    row_data.values.each_with_index do |value, idx|
      row << columns[idx].convert(value)
    end
    row
  end

  def add_row(row)
    @rows << row
  end

  def each_row(&block)
    @rows.each(&block)
  end
  
  def empty?
    @rows.empty?
  end
  
  def the_value
    if empty?
      nil
    else
      @row_style == :array ? rows[0][0] : rows[0][columns[0].name]
    end
  end
  
  alias_method :each, :each_row

  def row_count
    @rows.size
  end

  alias_method :size, :row_count
  alias_method :length, :row_count
end
