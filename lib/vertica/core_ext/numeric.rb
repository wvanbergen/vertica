class Numeric
  def to_network_int16
    [self].pack('n')
  end

  def to_network_int32
    [self].pack('N')
  end

  def to_byte
    [self].pack('C')
  end
end
