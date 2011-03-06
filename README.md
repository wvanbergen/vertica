# Vertica

**WARNING:** This is not well tested software (yet) use at your own risk.

## Description

Vertica is a pure Ruby library for connecting to Vertica databases.  You can learn more
about Vertica at http://www.vertica.com.  This library currently supports queries. Prepared
statements still need a bit of work.

You should probably be using sprsquish's version of this, I just forked it to add some 1.9 compatibility which I think will get added back into his branch soon

# Install

    $ gem install vertica

# Source

Vertica's git repo is available on GitHub, which can be browsed at:

    http://github.com/dangcalvert/vertica

and cloned from:

    git://github.com/dangcalvert/vertica.git

# Usage

## Example Query

### Connecting

    vertica = Vertica.connect({
      :user     => 'user',
      :password => 'password',
      :host     => 'db_server',
      :port     => '5433',
      :database => 'db
    })

### Buffered Rows

All rows will first be fetched and buffered into a result object. Probably shouldn't use
this for large result sets.

    result = vertica.query("SELECT id, name FROM my_table")
    result.each_row |row|
      puts row # => {:id => 123, :name => "Jim Bob"}
    end

    result.rows # => [{:id => 123, :name => "Jim Bob"}, {:id => 456, :name => "Joe Jack"}]
    result.row_count # => 2

    vertica.close

### Unbuffered Rows

The vertica gem will not buffer incoming results. The gem will read a result off the
socket and pass it to the provided block.

    vertica.query("SELECT id, name FROM my_table") do |row|
      puts row # => {:id => 123, :name => "Jim Bob"}
    end
    vertica.close

### Example Prepared Statement

This is flaky at best right now and needs some work. This will probably fail and destroy
your connection. You'll need to throw the connection away and start over.

    vertica.prepare("my_prepared_statement", "SELECT * FROM my_table WHERE id = ?", 1)
    result = vertica.execute_prepared("my_prepared_statement", 13)
    result.each_rows |row|
      puts row # => {:id => 123, :name => "Jim Bob"}
    end
    result.rows # => [{:id => 123, :name => "Jim Bob"}, {:id => 456, :name => "Joe Jack"}]
    vertica.close

# TODO

 * Tests.
 * Lots of tests.
 * Make sure all messages are compatible with ruby 1.8 and 1.9 (so far I've tried connect, authenticate w/ password, query, close, and the associated responses.)

# Authors

 * [Matt Bauer](http://github.com/mattbauer) all the hard work
 * [Jeff Smick](http://github.com/sprsquish) current maintainer of the upstream project, which you should be using
 * [Dan Calvert](http://github.com/dangcalvert) a few tweaks for ruby 1.9