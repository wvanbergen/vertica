module Vertica
  module Messages
    class NoticeResponse < BackendMessage
      message_id 'N'
      
      FIELDS_DEFINITIONS = [
        { :type => 'q', :name => "Internal Query" },
        { :type => 'S', :name => "Severity" },
        { :type => 'M', :name => "Message" },
        { :type => 'C', :name => "Sqlstate" },
        { :type => 'D', :name => "Detail" },
        { :type => 'H', :name => "Hint" },
        { :type => 'P', :name => "Position" },
        { :type => 'W', :name => "Where" },
        { :type => 'p', :name => "Internal Position" },
        { :type => 'R', :name => "Routine" },
        { :type => 'F', :name => "File" },
        { :type => 'L', :name => "Line" }
      ]
      
      FIELDS = Hash[*FIELDS_DEFINITIONS.map { |f| [f[:type], f[:name]] }.flatten]
      
      attr_reader :values

      def initialize(data)
        @values, pos = {}, 0
        while pos < data.size - 1
          key, value = data.unpack("@#{pos}aZ*")
          @values[FIELDS[key]] = value
          pos += value.size + 2
        end
      end

      def error_message
        ordered_values = FIELDS_DEFINITIONS.map do |field| 
          "#{field[:name]}: #{@values[field[:name]]}" if @values[field[:name]]
        end
        ordered_values.compact.join(', ')
      end
    end
  end
end
