class String
  def to_cstring
    raise ArgumentError, "Invalid cstring" if self.include?("\000")
    "#{value}\000"
  end
end
