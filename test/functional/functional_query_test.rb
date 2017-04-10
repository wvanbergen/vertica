require 'test_helper'
require 'zlib'

class FunctionalQueryTest < Minitest::Test
  def setup
    @connection = Vertica::Connection.new(TEST_CONNECTION_HASH)
    @connection.query("DROP TABLE IF EXISTS test_ruby_vertica_table CASCADE;")
    @connection.query("CREATE TABLE test_ruby_vertica_table (id int, name varchar(100))")
    @connection.query("CREATE PROJECTION IF NOT EXISTS test_ruby_vertica_table_p (id, name) AS SELECT * FROM test_ruby_vertica_table SEGMENTED BY HASH(id) ALL NODES OFFSET 1")
    @connection.query("INSERT INTO test_ruby_vertica_table VALUES (1, 'matt')")
    @connection.query("COMMIT")
  end

  def teardown
    if @connection.ready_for_query?
      @connection.close
    elsif @connection.interruptable?
      @connection.interrupt
    end
  end

  def test_select_query_with_buffered_results
    r = @connection.query("SELECT * FROM test_ruby_vertica_table")
    assert_equal 1, r.size
    assert_equal 2, r.row_description.length
    assert_equal 'integer', r.columns[0].data_type.name
    assert_equal 'id', r.columns[0].name
    assert_equal 'varchar', r.columns[1].data_type.name
    assert_equal 'name', r.columns[1].name

    assert_equal 1, r[0][:id]
    assert_equal 'matt', r[0][:name]
  end

  def test_select_query_with_streaming_results
    rows = []
    result = @connection.query("SELECT 1 AS a, 2 AS b UNION ALL SELECT 3, 4") do |row|
      assert_kind_of Vertica::Row, row
      assert_kind_of Vertica::RowDescription, row.row_description
      rows << row
    end

    # For unbuffered result, we return the kind of command that was executed
    assert_equal "SELECT", result

    assert_equal 2, rows.length
    assert_equal 1, rows[0].fetch(:a)
    assert_equal 2, rows[0].fetch(:b)
    assert_equal 3, rows[1].fetch(:a)
    assert_equal 4, rows[1].fetch(:b)
  end

  def test_select_query_with_zero_streaming_results
    rows = []
    result = @connection.query("SELECT 'impossible' WHERE 1=2") do |row|
      rows << row
    end

    assert_equal "SELECT", result
    assert_equal 0, rows.length
  end

  def test_select_query_with_no_results
    r = @connection.query("SELECT * FROM test_ruby_vertica_table WHERE 1 != 1")
    assert_equal "SELECT", r.tag
    assert_equal 0, r.size
    assert_equal 2, r.columns.length
    assert_equal 'integer', r.columns[0].data_type.name
    assert_equal 'id', r.columns[0].name
    assert_equal 'varchar', r.columns[1].data_type.name
    assert_equal 'name', r.columns[1].name
    assert_empty r
  end

  def test_select_query_with_multibyte_column_names_results
    r = @connection.query("SELECT 1 as \"イチ\", 2 as \"ニ\"")
    assert_equal 1, r.size
    assert_equal 2, r.row_description.length

    assert_equal 'イチ', r.row_description[0].name
    assert_equal 'ニ', r.row_description[1].name
  end

  # Query with multiple statements is not officailly supported
  def test_query_with_multiple_statements
    # Results of only the last statement are obtained
    r = @connection.query("SET SEARCH_PATH TO default; SELECT 1 as one; SELECT 2 as two")
    assert_equal 1, r.size
    assert_equal 1, r.row_description.length
    assert_equal 'two', r.row_description[0].name

    # Error of the former statement is raised
    assert_raises(Vertica::Error::MissingColumn) do
      @connection.query("SELECT missing FROM test_ruby_vertica_table; SELECT 1 FROM missing")
    end
  end

  def test_insert
    r = @connection.query("INSERT INTO test_ruby_vertica_table VALUES (2, 'stefanie')")
    assert_equal "INSERT", r.tag
    assert_equal 1, r.size
    assert_equal 1, r.columns.length
    assert_equal 'integer', r.columns[0].data_type.name
    assert_equal 'OUTPUT', r.columns[0].name
    assert_equal 1, r.value
  end


  def test_delete_of_no_rows
    r = @connection.query("DELETE FROM test_ruby_vertica_table WHERE 1 != 1")
    assert_equal "DELETE", r.tag
    assert_equal 1, r.size
    assert_equal 1, r.columns.length
    assert_equal 'integer', r.columns[0].data_type.name
    assert_equal 'OUTPUT', r.columns[0].name
    assert_equal 0, r.value
  end

  def test_delete_of_a_row
    r = @connection.query("DELETE FROM test_ruby_vertica_table WHERE id = 1")
    assert_equal "DELETE", r.tag
    assert_equal 1, r.size
    assert_equal 1, r.columns.length
    assert_equal 'integer', r.columns[0].data_type.name
    assert_equal 'OUTPUT', r.columns[0].name
    assert_equal 1, r.value
  end

  def test_empty_query
    assert_raises Vertica::Error::EmptyQueryError do
      @connection.query("")
    end
    assert_raises Vertica::Error::EmptyQueryError do
      @connection.query(nil)
    end
    assert_raises Vertica::Error::EmptyQueryError do
      @connection.query("-- just a SQL comment")
    end
  end

  def test_cleanup_after_select
    3.times do
      r = @connection.query("SELECT * FROM test_ruby_vertica_table")
      assert_equal 1, r.size
      assert_equal 2, r.columns.length
      assert_equal 'integer', r.columns[0].data_type.name
      assert_equal 'id', r.columns[0].name
      assert_equal 'varchar', r.columns[1].data_type.name
      assert_equal 'name', r.columns[1].name
      assert_equal [{'id' => 1, 'name' => "matt"}], r.map(&:to_h)
    end
  end

  def test_read_timeout
    assert_raises(Vertica::Error::TimedOutError) do
      @connection.options[:read_timeout] = 0.0001
      @connection.query("SELECT * FROM test_ruby_vertica_table")
    end
  end

  def test_sql_error
    assert_raises(Vertica::Error::MissingColumn) do
      @connection.query("SELECT missing FROM test_ruby_vertica_table")
    end
    assert_raises(Vertica::Error::MissingRelation) do
      @connection.query("SELECT * FROM nonexisting_dsfdfsdfsdfs")
    end
    assert_raises(Vertica::Error::SyntaxError) do
      @connection.query("BLAH")
    end
  end

  def test_copy_in_alot_of_data_with_customer_handler
    @connection.copy("COPY test_ruby_vertica_table FROM STDIN") do |data|
      data.write "11|#{"a" * 1_000_000}\n"
    end

    result = @connection.query("SELECT id FROM test_ruby_vertica_table ORDER BY id")
    assert_equal 2, result.length
  end

  def test_copy_in_with_customer_handler
    copy_result = @connection.copy("COPY test_ruby_vertica_table FROM STDIN") do |data|
      data.write "11|Stuff\r\n"
      data << "12|More stuff\n13|Fin" << "al stuff\n"
    end

    assert_equal "COPY", copy_result

    result = @connection.query("SELECT * FROM test_ruby_vertica_table ORDER BY id")
    assert_equal 4, result.length
    assert_equal [[1, "matt"], [11, "Stuff"], [12, "More stuff"], [13, "Final stuff"]], result.map(&:to_a)
  end

  def test_copy_in_with_gzip
    @connection.copy("COPY test_ruby_vertica_table FROM STDIN GZIP") do |data|
      gz = Zlib::GzipWriter.new(data)
      gz << "11|Stuff\n12|More stuff\n13|Final stuff\n"
      gz.close
    end

    result = @connection.query("SELECT * FROM test_ruby_vertica_table ORDER BY id")
    assert_equal 4, result.length
    assert_equal [[1, "matt"], [11, "Stuff"], [12, "More stuff"], [13, "Final stuff"]], result.map(&:to_a)
  end

  def test_copy_with_ruby_exception
    2.times do
      begin
        @connection.copy("COPY test_ruby_vertica_table FROM STDIN") do |data|
          data.write "11|#{"a" * 10}\n"
          raise "some error"
        end
      rescue Vertica::Error::CopyFromStdinFailed
      end

      result = @connection.query("SELECT id FROM test_ruby_vertica_table ORDER BY id")
      assert_equal 1, result.length
    end
  end

  def test_copy_with_backend_exception
    2.times do
      begin
        @connection.copy("COPY test_ruby_vertica_table FROM STDIN ABORT ON ERROR") do |data|
          data.write "11|#{"a" * 10}|11\n" # write invalid data
        end
      rescue Vertica::Error::CopyRejected
      end

      result = @connection.query("SELECT id FROM test_ruby_vertica_table ORDER BY id")
      assert_equal 1, result.length
    end
  end

  def test_copy_in_with_file
    filename = File.expand_path('../../resources/test_ruby_vertica_table.csv', __FILE__)
    @connection.copy("COPY test_ruby_vertica_table FROM STDIN", source: filename)
    result = @connection.query("SELECT * FROM test_ruby_vertica_table ORDER BY id")
    assert_equal 4, result.length
    assert_equal [[1, "matt"], [11, "Stuff"], [12, "More stuff"], [13, "Final stuff"]], result.map(&:to_a)
  end

  def test_copy_in_with_io
    io = StringIO.new("11|Stuff\r\n12|More stuff\n13|Final stuff\n")
    @connection.copy("COPY test_ruby_vertica_table FROM STDIN", source: io)
    result = @connection.query("SELECT * FROM test_ruby_vertica_table ORDER BY id")
    assert_equal 4, result.length
    assert_equal [[1, "matt"], [11, "Stuff"], [12, "More stuff"], [13, "Final stuff"]], result.map(&:to_a)
  end

  def test_notice_handler
    notice_received = false
    @connection.on_notice { |notice| notice_received = true }
    @connection.query('COMMIT')
    assert notice_received
  end

  def test_query_mutex
    mutex = Mutex.new
    values = []
    t1 = Thread.new do
      mutex.synchronize do
        3.times { values << @connection.query("SELECT 1").the_value }
      end
    end
    t2 = Thread.new do
      mutex.synchronize do
        3.times { values << @connection.query("SELECT 2").the_value }
      end
    end
    t3 = Thread.new do
      mutex.synchronize do
        3.times { values << @connection.query("SELECT 3").the_value }
      end
    end

    t1.join
    t2.join
    t3.join

    assert_equal values.sort, [1,1,1,2,2,2,3,3,3]
  end

  def test_raise_when_connection_is_in_use
    assert_raises(Vertica::Error::SynchronizeError) do
      @connection.query("SELECT 1 UNION SELECT 2") do |record|
        @connection.query("SELECT 3")
      end
    end
  end

  def test_sql_with_non_unicode_characters
    assert_raises(Vertica::Error::MissingColumn) do
      sql = "select あ".encode('EUC-JP').force_encoding('BINARY')
      @connection.query(sql)
    end

    sql = "select 'あ'".encode('EUC-JP')
    result = @connection.query(sql)
    assert_equal "\xA4\xA2", result.the_value
    assert_equal Encoding::UTF_8, result.the_value.encoding
  end

  def test_interrupting_connections
    before = @connection.query("SELECT COUNT(1) FROM test_ruby_vertica_table").the_value
    interruptable = Vertica::Connection.new(TEST_CONNECTION_HASH.merge(:interruptable => true))
    assert interruptable.interruptable?
    t = Thread.new do
      Thread.current[:error_occurred] = false
      begin
        10.times { |n| interruptable.query("INSERT INTO test_ruby_vertica_table VALUES (#{n}, 'interrupt test')") }
        interruptable.query("COMMIT")
      rescue Vertica::Error::ConnectionError
        Thread.current[:error_occurred] = true
      end
    end

    interruptable.interrupt
    t.join

    assert t[:error_occurred], "Expected an exception to occur"
    assert_equal before, @connection.query("SELECT COUNT(1) FROM test_ruby_vertica_table").the_value
  end
end
