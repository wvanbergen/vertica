require 'test_helper'

class DataTypeTest < Minitest::Test
  def test_equality
    type = Vertica::DataType.new(oid: 6, size: 4)

    assert_equal type, Vertica::DataType.new(oid: 6, size: 4)
    assert_equal type, Vertica::DataType.new(oid: 6, size: 4, modifier: nil)

    refute_equal type, Vertica::DataType.new(oid: 6, size: 10)
    refute_equal type, Vertica::DataType.new(oid: 6, size: 4, modifier: 'unsigned')
    refute_equal type, Vertica::DataType.new(oid: 6, size: 1)
    refute_equal type, Vertica::DataType.new(oid: 7, size: 4)
  end

  def test_inspect
    type = Vertica::DataType.new(oid: 6, name: "integer", size: 4)
    assert_equal '#<Vertica::DataType:6 "integer">', type.inspect
  end

  def test_deserialize_bool
    type = Vertica::DataType.build(oid: 5)

    assert_nil type.deserialize(nil)
    assert_equal true, type.deserialize('t')
    assert_equal false, type.deserialize('f')
    assert_raises(ArgumentError) { type.deserialize('foo') }
  end

  def test_deserialize_integer
    type = Vertica::DataType.build(oid: 6)

    assert_nil type.deserialize(nil)
    assert_equal 0, type.deserialize('0')
    assert_equal -123, type.deserialize('-123')
    assert_raises(ArgumentError) { type.deserialize('foo') }
  end

  def test_deserialize_float
    type = Vertica::DataType.build(oid: 7)

    assert_nil type.deserialize(nil)
    assert_equal Float::INFINITY, type.deserialize('Infinity')
    assert_equal -Float::INFINITY, type.deserialize('-Infinity')
    assert type.deserialize('NaN').equal?(Float::NAN)
    assert_equal 1.1, type.deserialize('1.1')
    assert_equal -1.1, type.deserialize('-1.1')
    assert_raises(ArgumentError) { type.deserialize('foo') }
  end

  def test_deserialize_unicode_string
    type = Vertica::DataType.build(oid: 115)

    assert_nil type.deserialize(nil)
    converted = type.deserialize("foo\x00".force_encoding(Encoding::BINARY))
    assert_equal "foo\x00".force_encoding(Encoding::UTF_8), converted
  end

  def test_dersialize_binary_string
    type = Vertica::DataType.build(oid: 17)

    assert_nil type.deserialize(nil)
    converted = type.deserialize("\\231\\237".force_encoding(Encoding::BINARY))
    assert_equal "\x99\x9F".force_encoding(Encoding::BINARY), converted
  end

  def test_deserialize_bigdecimal
    type = Vertica::DataType.build(oid: 16)
    assert_nil type.deserialize(nil)
    assert_equal BigDecimal('1.1'), type.deserialize('1.1')
  end
end
