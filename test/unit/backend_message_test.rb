require 'test_helper'

class BackendMessageTest < Minitest::Test

  def test_cleartext_authentication_message
    msg = Vertica::Protocol::Authentication.new("\x00\x00\x00\x03")
    assert_equal Vertica::Protocol::Authentication::CLEARTEXT_PASSWORD , msg.code
    assert_nil msg.salt
    assert_nil msg.auth_data
  end

  def test_parameter_status_message
    msg = Vertica::Protocol::ParameterStatus.new("standard_conforming_strings\x00on\x00")
    assert_equal "standard_conforming_strings", msg.name
    assert_equal "on", msg.value
  end

  def test_backend_key_data_message
    msg = Vertica::Protocol::BackendKeyData.new("\x00\x01\xED\xD8\x02\xC4\"\t")
    assert_equal 126424, msg.pid
    assert_equal 46408201, msg.key
  end

  def test_ready_for_query_message
    msg = Vertica::Protocol::ReadyForQuery.new("I")
    assert_equal :no_transaction, msg.transaction_status
  end

  def test_error_response_message
    data = "SFATAL\x00C3D000\x00Mdatabase \"nonexistant_db\" does not exist\x00F/scratch_a/release/vbuild/vertica/Basics/ClientAuthentication.cpp\x00L1496\x00RClientAuthentication\x00\x00"
    msg = Vertica::Protocol::ErrorResponse.new(data)
    assert_equal msg.values, {
      "Severity" => "FATAL",
      "Sqlstate" => "3D000",
      "Message"  => "database \"nonexistant_db\" does not exist",
      "File"     => "/scratch_a/release/vbuild/vertica/Basics/ClientAuthentication.cpp",
      "Line"     => "1496",
      "Routine"  => "ClientAuthentication"
    }

    assert_equal "Severity: FATAL, Message: database \"nonexistant_db\" does not exist, Sqlstate: 3D000, Routine: ClientAuthentication, File: /scratch_a/release/vbuild/vertica/Basics/ClientAuthentication.cpp, Line: 1496", msg.error_message
  end

  def test_error_response_fields
    data = "SFATAL\x00C3D000\x00Mdatabase \"nonexistant_db\" does not exist\x00F/scratch_a/release/vbuild/vertica/Basics/ClientAuthentication.cpp\x00L1496\x00RClientAuthentication\x00\x00"
    msg = Vertica::Protocol::ErrorResponse.new(data)

    assert_equal "FATAL", msg.severity
    assert_equal "ClientAuthentication", msg.routine
  end


  def test_notice_response_values
    data = "SINFO\x00C00000\x00Mcannot commit; no transaction in progress\x00F/scratch_a/release/vbuild/vertica/Commands/PGCall.cpp\x00L3502\x00Rprocess_vertica_transaction\x00\x00"
    msg = Vertica::Protocol::NoticeResponse.new(data)

    assert_equal msg.values, {
      "Severity" => "INFO",
      "Sqlstate" => "00000",
      "Message"  => "cannot commit; no transaction in progress",
      "File"     => "/scratch_a/release/vbuild/vertica/Commands/PGCall.cpp",
      "Line"     => "3502",
      "Routine"  => "process_vertica_transaction"
    }
  end

  def test_row_description_message
    msg = Vertica::Protocol::RowDescription.new("\x00\x01OUTPUT\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x06\x00\b\x00\x00\x00\b\x00\x00")
    assert_equal 1, msg.fields.size
    assert_equal msg.fields[0], {
      :name               => "OUTPUT",
      :table_oid          => 0,
      :attribute_number   => 0,
      :data_type_oid      => 6,
      :data_type_size     => 8,
      :data_type_modifier => 8,
      :format_code        => 0
    }

    msg = Vertica::Protocol::RowDescription.new("\x00\x02id\x00\x00\np8\x00\x01\x00\x00\x00\x06\x00\b\xFF\xFF\xFF\xFF\x00\x00name\x00\x00\np8\x00\x02\x00\x00\x00\t\xFF\xFF\x00\x00\x00h\x00\x00")
    assert_equal msg.fields[0], {
      :name               => "id",
      :table_oid          => 684088,
      :attribute_number   => 1,
      :data_type_oid      => 6,
      :data_type_size     => 8,
      :data_type_modifier => 4294967295,
      :format_code        => 0
    }
    assert_equal msg.fields[1], {
      :name               => "name",
      :table_oid          => 684088,
      :attribute_number   => 2,
      :data_type_oid      => 9,
      :data_type_size     => 65535,
      :data_type_modifier => 104,
      :format_code        => 0
    }
  end

  def test_data_row_message
    msg = Vertica::Protocol::DataRow.new("\x00\x01\x00\x00\x00\x011")
    assert_equal ['1'], msg.values

    msg = Vertica::Protocol::DataRow.new("\x00\x02\x00\x00\x00\x011\x00\x00\x00\x04matt")
    assert_equal ['1', 'matt'], msg.values

    msg = Vertica::Protocol::DataRow.new("\x00\a\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")
    assert_equal [nil,nil,nil,nil,nil,nil,nil], msg.values
  end

  def test_command_complete_message
    msg = Vertica::Protocol::CommandComplete.new("CREATE TABLE\x00")
    assert_equal "CREATE TABLE", msg.tag
    assert_nil msg.rows
    assert_nil msg.oid
  end
end
