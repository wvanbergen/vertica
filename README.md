# Vertica

Vertica is a pure Ruby library for connecting to Vertica databases. You can learn more
about Vertica at http://www.vertica.com.  

This library currently supports simple queries. Prepared statements are currently not supported
but are being worked on. The gem is tested against Vertica version 5.0 and should work in both 
Ruby 1.8 and Ruby 1.9.

# Install

    $ gem install vertica

# Source

Vertica's git repo is available on GitHub, which can be browsed at:

    http://github.com/sprsquish/vertica

and cloned from:

    git://github.com/sprsquish/vertica.git

# Usage

## Connecting

    connection = Vertica.connect({
      :host     => 'db_server',
      :user     => 'user',
      :password => 'password',
      # :ssl         => true,  # use SSL for the connection
      # :port        => 5433,  # default 5433
      # :database    => 'db',  # there is only one database
      # :role        => '...', # the (additional) role(s) to enable for the user.
      # :search_path => '...'  # default: <user>,public,v_catalog
    })

## Buffered result

All rows will first be fetched and buffered into a result object. Probably shouldn't use
this for large result sets.

    result = connection.query("SELECT id, name FROM my_table")
    result.each_row |row|
      puts row # => {:id => 123, :name => "Jim Bob"}
    end

    result.rows # => [{:id => 123, :name => "Jim Bob"}, {:id => 456, :name => "Joe Jack"}]
    result.row_count # => 2

    connection.close

## Unbuffered result

The vertica gem will not buffer incoming results. The gem will read a result off the
socket and pass it to the provided block.

    connection.query("SELECT id, name FROM my_table") do |row|
      puts row # => {:id => 123, :name => "Jim Bob"}
    end
    connection.close

Rows can also be returned as arrays by providing a row_style:

    connection.query("SELECT id, name FROM my_table", :row_style => :array) do |row|
      puts row # => [123, "Jim Bob"]
    end
    
By adding <code>:row_style => :array</code> to the connection hash, all results will be 
returned as array.


# TODO

 * Prepared statements
 * More tests

# Authors

 * [Matt Bauer](http://github.com/mattbauer) all the hard work
 * [Jeff Smick](http://github.com/sprsquish) current maintainer
 * [Willem van Bergen](http://github.com/wvanbergen) contributor
