# Vertica

## Description

Vertica is a pure Ruby library for connecting to Vertica databases.  You can learn more
about Vertica at http://www.vertica.com.  This library currently supports queries. Prepared
statements still need a bit of work.

# Install

    $ gem install vertica

# Source

Vertica's git repo is available on GitHub, which can be browsed at:

    http://github.com/sprsquish/vertica

and cloned from:

    git://github.com/sprsquish/vertica.git

# Usage

## Example Query

### Connecting

    c = Vertica.connect({
      :user     => 'user',
      :password => 'password',
      :host     => 'db_server',
      :port     => '5433',
      :database => 'db
    })

### Buffered Rows

All rows will first be fetched and buffered into a result object. Probably shouldn't use
this for large result sets.

    r = c.query("SELECT id, name FROM my_table")
    r.each_row |row|
      puts row # {:id => 123, :name => "Jim Bob"}
    end

    r.rows => [{:id => 123, :name => "Jim Bob"}, {:id => 456, :name => "Joe Jack"}]

    c.close

### Unbuffered Rows

The vertica gem will not buffer incoming results. The gem will read a result off the
socket and pass it to the provided block.

    c.query("SELECT id, name FROM my_table") do |row
      puts row # {:id => 123, :name => "Jim Bob"}
    end
    c.close

### Example Prepared Statement

This is flaky at best right now and needs some work. This will probably fail and destroy
your connection. You'll need to throw the current connection away and start over.

    c.prepare("my_prepared_statement", "SELECT * FROM my_table WHERE id = ?", 1)
    r = c.execute_prepared("my_prepared_statement", 13)
    r.each_rows |row|
      puts row # {:id => 123, :name => "Jim Bob"}
    end
    r.rows => [{:id => 123, :name => "Jim Bob"}, {:id => 456, :name => "Joe Jack"}]
    c.close

# Todo

 * Tests.
 * Lots of tests.

# Authors

 * [Matt Bauer](http://github.com/mattbauer) all the hard work
 * [Jeff Smick](http://github.com/sprsquish) current maintainer
