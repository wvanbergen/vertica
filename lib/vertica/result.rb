module Vertica
  class Result
    include Enumerable

    attr_reader :columns

    def initialize#(stream)
      # @stream = stream
      @rows = []
    end

    def descriptions=(message)
      @columns = message.fields.map { |fd| Column.new(fd) }
    end

    def format_row(row_data)
      row = {}
      row_data.fields.each_with_index do |field, idx|
        col = columns[idx]
        row[col.name] = col.convert(field)
      end
      row
    end

    def add_row(row_data)
      @rows << format_row(row_data)
    end

    def each(&block)
      @rows.each(&block)
    end

    def all
      map { |r| r }
    end

  end
end
