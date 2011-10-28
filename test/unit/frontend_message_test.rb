require 'test_helper'

class FrontendMessageTest < Test::Unit::TestCase

  def test_copy_done_message
    message = Vertica::Messages::CopyDone.new
    assert_equal message.to_bytes, ['c', 0, 0, 0, 4].pack('AC4')
  end
  
  def test_copy_fail_message
    message = Vertica::Messages::CopyFail.new('sad panda')
    assert_equal message.to_bytes, ['f', 0, 0, 0, 14, "sad panda"].pack('AC4Z*')
  end
  
  def test_copy_data_message
    message = Vertica::Messages::CopyData.new("foo|bar\n")
    assert_equal message.to_bytes, ['d', 0, 0, 0, 12, "foo|bar\n"].pack('AC4A*')
  end
end
