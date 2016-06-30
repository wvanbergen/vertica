module Vertica
  module Protocol
    class NoticeResponse < BackendMessage
      message_id 'N'

      FIELDS_DEFINITIONS = [
        { :type => 'q', :name => "Internal Query", :method => :internal_query },
        { :type => 'S', :name => "Severity", :method => :severity },
        { :type => 'M', :name => "Message", :method => :message },
        { :type => 'C', :name => "Sqlstate", :method => :sqlstate },
        { :type => 'D', :name => "Detail", :method => :detail },
        { :type => 'H', :name => "Hint", :method => :hint },
        { :type => 'P', :name => "Position", :method => :position },
        { :type => 'W', :name => "Where", :method => :where },
        { :type => 'p', :name => "Internal Position", :method => :internal_position },
        { :type => 'R', :name => "Routine", :method => :routine },
        { :type => 'F', :name => "File", :method => :file },
        { :type => 'L', :name => "Line", :method => :line }
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

      FIELDS_DEFINITIONS.each do |field_def|
        define_method(field_def[:method]) do
          @values[field_def[:name]]
        end
      end
    end
  end
end
