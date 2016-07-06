# Vertica [![Build Status](https://travis-ci.org/wvanbergen/vertica.png?branch=travis)](https://travis-ci.org/wvanbergen/vertica)

Vertica is a pure Ruby library for connecting to Vertica databases. You can learn more
about Vertica at http://www.vertica.com.

- Connecting, including over SSL.
- Executing queries, with results as streaming rows or buffered resultsets.
- `COPY table FROM STDIN` statement to load data from your application.
- The library is thread-safe as of version 0.11. However, you can only run one
  statement at the time per connection, because the protocol is stateful. In a
  multi-threaded environment, you may want to tthink about setting up a
  connection pool.


## Installation

Add it to the Gemfile of your project:

    gem 'vertica', '~> 1.0'
    # gem 'vertica', git: 'git://github.com/wvanbergen/vertica.git' # HEAD version

Now, run `bundle install`.

### Compatiblity

- Ruby 2.0 or higher is required.
- Compatibility is tested with Vertica 7.x community edition. Vertica versions 4.x, 5.x,
  and 6.x worked with at some point with this gem, but compatibility is no longer tested.

## Usage

### Connecting

The `Vertica.connect` methods takes keyword arguments and returns a connection
instance. For most options, the gem will use a default value if no value is provided.

``` ruby
connection = Vertica.connect(
  host:     'db_server',
  username: 'user',
  password: 'password',

  # ssl:           false, # use SSL for the connection
  # port:          5433,  # default Vertica port: 5433
  # database:      nil,   # there is only one database
  # role:          nil,   # the (additional) role(s) to enable for the user.
  # search_path:   nil,   # default: <user>,public,v_catalog
  # timezone:      nil,   # the timezone for the connection to convert timestamps
  # autocommit:    false, # automatically commit INSERT/UPDATE/DELETE queries
  # interruptable: false, # set to true to allow sessions to be interrupted.
  # read_timeout:  600,   # timeout in seconds when reading data
  # debug:         false, # print all the messages back and forth to STDOUT.
)
```

- To close the connection when you're done with it, run `connection.close`.
- You can pass `OpenSSL::SSL::SSLContext` in `:ssl` to customize SSL connection options,
  or `true` to use the default (`OpenSSL::SSL::SSLContext.new`).

### Running queries

You can run queries using the <code>query</code> method, either in buffered and
unbuffered mode. For large result sets, you probably do not want to use buffered results,
because buffering the entire result may require a lot of memory.

Get all the result rows without buffering by providing a block:

``` ruby
connection.query("SELECT id, name FROM my_table") do |row|
  puts row['id']   # => 123
  puts row['name'] # => 'Unicorn'
end
```

Note: you can only use the connection for one query at the time. If you try to run another
query when the connection is still busy delivering the results of a previous query, a
`Vertica::Error::SynchronizeError` will be raised. Use buffered resultsets to prevent this
problem.

Store the result of the query method as a variable to get a buffered resultset:

``` ruby
result = connection.query("SELECT id, name FROM my_table")
connection.close # buffered result will be available even after closing the connection.

result.size # => 2

result.each do |row|
  puts "Hello #{row['name']}, your ID is #{row['id']}."
end
```

Rows are provided as `Vertica::Row` instances. You can access the individial fields by
referring to their name as String or Symbol,  or the index of the field in the result.

``` ruby
result.each do |row|
  p row # => Vertica::Row[123, "Unicorn"]>

  puts row['id'], row[:id], row[0]     # Three times 123
  puts row['name'], row[:name], row[1] # Three times 'Unicorn'
end
```

### Loading data into Vertica using COPY statements

Using the `COPY FROM STDIN` statement, you can load arbitrary data from your ruby script to the database.

``` ruby
connection.copy("COPY table FROM STDIN ...") do |stdin|
  File.open('data.tsv', 'r') do |f|
    begin
      stdin << f.gets
    end until f.eof?
  end
end
```

You can also provide a filename or an IO object:

``` ruby
connection.copy("COPY table FROM STDIN ...", "data.csv")
File.open('file.csv') do |io|
  connection.copy("COPY table FROM STDIN ...", io)
end
```

For more information, see [the Vertica documentation](https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/SQLReferenceManual/Statements/COPY/COPY.htm)

### Interrupting sessions

``` ruby
connection = Vertica.connect(...)

Thread.new do
  sleep(60)
  connection.interrupt
end

begin
  result = connection.query('SELECT complicated_query FROM huge_table')
rescue Vertica::Error::ConnectionError
  # ...
end
```

## About

This package is MIT licensed. See the LICENSE file for more information.

### Development

This project comes with a test suite. The unit tests in `/test/unit` do not need a database
connection to run, the functional tests in `/test/functional` do need a working
database connection. You can specify the connection parameters by copying the file
`/test/connection.yml.example` to `/test/connection.yml` and filling out the
necessary fields.

The `/vagrant` folder contains a Vagrantfile and a setup script to help you set up a development
database that you can run the functional test suite against. The full test suite is also run by
Travis CI against Vertica 7 CE, and against several Ruby versions.

### Authors

 * [Matt Bauer](https://github.com/mattbauer) & [Jeff Smick](https://github.com/sprsquish) all the hard work
 * [Willem van Bergen](https://github.com/wvanbergen) current maintainer
 * [Camilo Lopez](https://github.com/camilo) contributor
 * [Erik Selin](https://github.com/tyro89) contributor

### See also

* [Website](http://vanbergen.org/vertica)
* [API Documentation](http://www.rubydoc.info/gems/vertica/frames)
* [Vertica documentation](https://my.vertica.com/docs/7.1.x/HTML/index.htm)
* [sequel-vertica](https://github.com/camilo/sequel-vertica): Sequel integration
* [newrelic-vertica](https://github.com/wvanbergen/newrelic-vertica): NewRelic monitoring of queries
* [node-vertica](https://github.com/wvanbergen/node-vertica): node.js Vertica driver
