# Changelog

## Version 1.0.1

- Fix an issue that made connecting over SSL not work.

## Version 1.0.0

- Version 1.0 is a complete rewrite of the internals, but should be mostly API compatible
  for the common use cases. However, many internals that were exposed before are now private,
  and cannot be accessed anymore.
- Rows our now returned as `Vertica::Row` instances, instead of a Hash or Array. You can access
  fields in a `Vertica::Row` using the field index as integer, or field name as String or
  Symbol. The `row_style` option has been removed.
- The library now uses keyword arguments instead of option hashes. This means that unknown
  options now raise an exception instead of being silently ignored.
- Properly handle timezones: `timestamp` and `timestamptz` values are now returned as `Time`
  instances, with the timezone set to the connection's timezone.
- `Time` and `DateTime` values now include microseconds and timezone when passed to
  `Vertica.quote`.
- Add support for setting the timezone (`timezone: 'America/Toronto'`) and enabling autocommit
  (`autocommit: true`) when initializing a connection.
- To set the username to use when connecting, set the `username` keyword argument; `user` is
  deprecated.
- Full support for `long varchar`, and `varbinary`/`bytea` data types.
- Made the API to support new types much easier to use (see `Vertica::DataType`), and raise
  a more useful exception when encountering a data type that cannot be handled.
- [Full API documentation](http://www.rubydoc.info/gems/vertica/frames) using yard.
- Much better unit test and functional test coverage.
- Support for Ruby 1.9 is dropped.
