# Vertica

Vertica is a pure Ruby library for connecting to Vertica databases. You can learn more
about Vertica at http://www.vertica.com.

This library currently supports connecting, executing SQL queries, and transferring data
for a "COPY table FROM STDIN" statement. The gem is tested against Vertica version 4.1, 
5.0, and 5.1, and Ruby version 1.8 and 1.9.

# Install

    $ gem install vertica

# Source

Vertica's git repo is available on GitHub, which can be browsed at:

    http://github.com/sprsquish/vertica

and cloned from:

    git://github.com/sprsquish/vertica.git

# Usage

## Connecting

The <code>Vertica.connect</code> methods takes a connection parameter hash and returns a 
connection object. For most options, the gem will use a default value if no value is provided.

    connection = Vertica.connect({
      :host     => 'db_server',
      :user     => 'user',
      :password => 'password',
      # :ssl         => false, # use SSL for the connection
      # :port        => 5433,  # default Vertica port: 5433
      # :database    => 'db',  # there is only one database
      # :role        => nil,   # the (additional) role(s) to enable for the user.
      # :search_path => nil,   # default: <user>,public,v_catalog
      # :row_style   => :hash  # can also be :array (see below)
    })
    
To close the connection when you're done with it, run <code>connection.close</code>.

## Querying

You can run simple queries using the <code>query</code> method, either in buffered and 
unbuffered mode. For large result sets, you probably do not want to use buffered results.

### Unbuffered result

Get all the result rows without buffering by providing a block:

    connection.query("SELECT id, name FROM my_table") do |row|
      puts row # => {:id => 123, :name => "Jim Bob"}
    end
    
    connection.close

Note: you can only use the connection for one query at the time. If you try to run another 
query when the connection is still busy delivering the results of a previous query, a
`Vertica::Error::SynchronizeError` will be raised. Use buffered resultsets to prevent this
problem.

### Buffered result

Store the result of the query method as a variable to get a buffered resultset:

    result = connection.query("SELECT id, name FROM my_table")
    connection.close
    
    result.rows # => [{:id => 123, :name => "Jim Bob"}, {:id => 456, :name => "Joe Jack"}]
    result.row_count # => 2
    
    result.each do |row|
      puts row # => {:id => 123, :name => "Jim Bob"}
    end

### Row format

By default, rows are returned as hashes, using symbols for the column names. Rows can also 
be returned as arrays by providing a row_style:

    connection.query("SELECT id, name FROM my_table", :row_style => :array) do |row|
      puts row # => [123, "Jim Bob"]
    end
    
By adding <code>:row_style => :array</code> to the connection hash, all results will be 
returned as array.

# About

This package is MIT licensed. See the LICENSE file for more information.

## Development

This project comes with a test suite. The unit tests in <tt>/test/unit</tt> do not need a database
connection to run, the functional tests in <tt>/test/functional</tt> do need a working
database connection. You can specify the connection parameters by copying the file
<tt>/test/connection.yml.example</tt> to <tt>/test/connection.yml</tt> and filling out the 
necessary fields. 

Note that the test suite requires write access to the default schema of the provided connection, 
although it tries to be as little invasive as possible: all tables it creates (and drops) are 
prefixed with <tt>test_ruby_vertica_</tt>.

### TODO

 * Asynchronous / EventMachine version

## Authors

 * [Matt Bauer](http://github.com/mattbauer) all the hard work
 * [Jeff Smick](http://github.com/sprsquish) current maintainer
 * [Willem van Bergen](http://github.com/wvanbergen) contributor
 * [Camilo Lopez](http://github.com/camilo) contributor
