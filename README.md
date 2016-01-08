# Vertica [![Build Status](https://travis-ci.org/wvanbergen/vertica.png?branch=travis)](https://travis-ci.org/wvanbergen/vertica)

Vertica is a pure Ruby library for connecting to Vertica databases. You can learn more
about Vertica at http://www.vertica.com.

- Connecting, including over SSL.
- Executing queries, with results as streaming rows or buffered resultsets.
- `COPY table FROM STDIN` statement to load data from your application.
- Confirmed to work with Ruby 1.9, 2.0, and 2.1; JRuby 1.7.23 and 9.0.4.0; and
  with Vertica version 6.x, and 7.x.
- The library is thread-safe as of version 0.11. However, you can only run one
  statement at the time per connection, because the protocol is stateful.


## Installation

    $ gem install vertica

Or add it to your Gemfile:

    gem 'vertica'
    # gem 'vertica', git: 'git://github.com/wvanbergen/vertica.git' # HEAD version

### Compatiblity

- Ruby 1.8 is no longer supported, but version 0.9.x should still support it.
- Vertica versions 4.x, and 5.x worked with at some point with this gem, but
  compatibility is no longer tested. It probably still works as the protocol hasn't
  changed as far as I am aware.


## Usage

### Connecting

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

You can pass `OpenSSL::SSL::SSLContext` in `:ssl` to customize SSL connection options.

### Querying with unbuffered result as streaming rows

You can run simple queries using the <code>query</code> method, either in buffered and
unbuffered mode. For large result sets, you probably do not want to use buffered results.

Get all the result rows without buffering by providing a block:

    connection.query("SELECT id, name FROM my_table") do |row|
      puts row # => {:id => 123, :name => "Jim Bob"}
    end

Note: you can only use the connection for one query at the time. If you try to run another
query when the connection is still busy delivering the results of a previous query, a
`Vertica::Error::SynchronizeError` will be raised. Use buffered resultsets to prevent this
problem.

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

### Loading data into Vertica using COPY

Using the COPY statement, you can load arbitrary data from your ruby script to the database.

    connection.copy("COPY table FROM STDIN ...") do |stdin|
      File.open('data.tsv', 'r') do |f|
        begin
          stdin << f.gets
        end until f.eof?
      end
    end

You can also provide a filename or an IO object:

    connection.copy("COPY table FROM STDIN ...", "data.csv")
    connection.copy("COPY table FROM STDIN ...", io)


## About

This package is MIT licensed. See the LICENSE file for more information.

### Development

This project comes with a test suite. The unit tests in <tt>/test/unit</tt> do not need a database
connection to run, the functional tests in <tt>/test/functional</tt> do need a working
database connection. You can specify the connection parameters by copying the file
<tt>/test/connection.yml.example</tt> to <tt>/test/connection.yml</tt> and filling out the
necessary fields.

Note that the test suite requires write access to the default schema of the provided connection,
although it tries to be as little invasive as possible: all tables it creates (and drops) are
prefixed with <tt>test_ruby_vertica_</tt>.

The test suite is also run by Travis CI againast Vertica 7.0.1, and Ruby 1.9.3, 2.0.0, and 2.1.1.

### Authors

 * [Matt Bauer](https://github.com/mattbauer) & [Jeff Smick](https://github.com/sprsquish) all the hard work
 * [Willem van Bergen](https://github.com/wvanbergen) current maintainer
 * [Camilo Lopez](https://github.com/camilo) contributor
 * [Erik Selin](https://github.com/tyro89) contributor

### See also

* [Website](http://vanbergen.org/vertica)
* [API Documentation](http://www.rubydoc.info/gems/vertica/frames)
* [sequel-vertica](https://github.com/camilo/sequel-vertica): Sequel integration
* [newrelic-vertica](https://github.com/wvanbergen/newrelic-vertica): NewRelic monitoring of queries
* [node-vertica](https://github.com/wvanbergen/node-vertica): node.js Vertica driver
