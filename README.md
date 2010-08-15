# Vertica

## Description

Vertica is a pure Ruby library for connecting to Vertica databases.  You can learn more
about Vertica at http://www.vertica.com.  This library currently supports queries. Prepared
statements still need a bit of work

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

    c = Vertica::Connection.new({
      :user     => 'user',
      :password => 'password',
      :host     => 'db_server',
      :port     => '5433',
      :database => 'db
    })

### Buffered Rows

    r = c.query("SELECT id, name FROM my_table")
    r.each |row|
      puts row # {:id => 123, :name => "Jim Bob"}
    end
    c.close

### Unbuffered Rows

    c.query("SELECT id, name FROM my_table") do |row
      puts row # {:id => 123, :name => "Jim Bob"}
    end
    c.close

### Example Prepared Statement

This is flaky at best right now and needs some work.

    c.prepare("my_prepared_statement", "SELECT * FROM my_table WHERE id = ?", 1)
    r = c.execute_prepared("my_prepared_statement", 13)
    r.each |row|
      puts row # {:id => 123, :name => "Jim Bob"}
    end
    r.all => [{:id => 123, :name => "Jim Bob"}, {:id => 456, :name => "Joe Jack"}]
    c.close

# Todo

Tests. Lots of tests.

# Authors

 * [Matt Bauer](http://github.com/mattbauer) did all the hard work
 * [Jeff Smick](http://github.com/sprsquish) current maintainer
