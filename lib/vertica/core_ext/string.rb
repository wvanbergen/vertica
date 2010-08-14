class String
  def to_cstring
    raise ArgumentError, "Invalid cstring" if self.include?("\000")
    "#{self}\000"
  end

  def from_cstring
    self[0..-2]
  end

  def to_byte
    unpack('C').first
  end

  def to_network_int16
    unpack('n').first
  end

  def to_network_int32
    unpack('l').first
  end
end
