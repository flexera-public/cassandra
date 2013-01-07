class Cassandra
  module Helpers
    # TODO Aqua remove this helper and its callers when thrift 0.9.1
    # is released with bugfixes about BINARY strings
    def self.convert_to_string_with_encoding(object)
      case object
      when SimpleUUID::UUID
        # Due to a bug in Thrift 0.9.0, we must pretend like
        # UUID byte-vectors (BINARY encoded Strings) are
        # UTF-8 encoded, otherwise thrift tries to transcode
        # to UTF-8 which is not kosher.
        object.to_s.force_encoding(Encoding::UTF_8)
      when Cassandra::Long
        # Cassandra#Long.to_s returns a string with BINARY encoding;
        # pretend UTF-8 encoding to work around Thrift bug.
        object.to_s.force_encoding(Encoding::UTF_8)
      else
        # All other strings can be passed with natural encoding
        # (UTF-8, or any encoding that can be transcoded to it).
        object.to_s
      end
    end

    def extract_and_validate_params(column_family, keys, args, options)
      options = options.dup
      column_family = column_family.to_s
      # Keys
      [keys].flatten.each do |key|
        raise ArgumentError, "Key #{key.inspect} must be a String for #{caller[2].inspect}." unless key.is_a?(String)
      end

      # Options
      if args.last.is_a?(Hash)
        extras = args.last.keys - options.keys
        raise ArgumentError, "Invalid options #{extras.inspect[1..-2]} for #{caller[1]}" if extras.any?
        options.merge!(args.pop)      
      end

      # Ranges
      column, sub_column = args[0], args[1]
      raise ArgumentError, "Invalid arguments: subcolumns specified for a non-supercolumn family" if sub_column && !is_super(column_family)      
      klass, sub_klass = column_name_class(column_family), sub_column_name_class(column_family)
      range_class = column ? sub_klass : klass

      [:start, :finish].each do |opt|
        options[opt] = options[opt] ? range_class.new(options[opt]).to_s : ''
      end

      [column_family, s_map(column, klass), s_map(sub_column, sub_klass), options]
    end

    # Convert stuff to strings.
    def s_map(el, klass)
      case el
      when Array then el.map { |i| s_map(i, klass) }
      when NilClass then nil
      else
        Cassandra::Helpers.convert_to_string_with_encoding(klass.new(el))
      end
    end
  end
end
