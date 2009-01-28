VERTICA_SPEC = Gem::Specification.new do |s|
  s.platform  =   Gem::Platform::RUBY
  s.required_ruby_version = '>=1.8.4'
  s.name      =   "vertica"
  s.version   =   "0.7.3"
  s.author    =   "Matt Bauer"
  s.email     =   "bauer@pedalbrain.com"
  s.summary   =   "A Ruby interface to Vertica"
  s.files     =   ['vertica.gemspec',
                   'LICENSE',
                   'README.textile',
                   'Rakefile',
                   'lib/vertica.rb',
                   'lib/vertica/notice.rb',
                   'lib/vertica/bit_helper.rb',
                   'lib/vertica/column.rb',
                   'lib/vertica/connection.rb',
                   'lib/vertica/messages/',
                   'lib/vertica/notification.rb',
                   'lib/vertica/result.rb',
                   'lib/vertica/vertica_socket.rb',
                   'lib/vertica/messages/authentication.rb',
                   'lib/vertica/messages/backend_key_data.rb',
                   'lib/vertica/messages/bind.rb',
                   'lib/vertica/messages/bind_complete.rb',
                   'lib/vertica/messages/cancel_request.rb',
                   'lib/vertica/messages/close.rb',
                   'lib/vertica/messages/close_complete.rb',
                   'lib/vertica/messages/command_complete.rb',
                   'lib/vertica/messages/data_row.rb',
                   'lib/vertica/messages/describe.rb',
                   'lib/vertica/messages/empty_query_response.rb',
                   'lib/vertica/messages/error_response.rb',
                   'lib/vertica/messages/execute.rb',
                   'lib/vertica/messages/flush.rb',
                   'lib/vertica/messages/message.rb',
                   'lib/vertica/messages/no_data.rb',
                   'lib/vertica/messages/notice_response.rb',
                   'lib/vertica/messages/notification_response.rb',
                   'lib/vertica/messages/parameter_description.rb',
                   'lib/vertica/messages/parameter_status.rb',
                   'lib/vertica/messages/parse.rb',
                   'lib/vertica/messages/parse_complete.rb',
                   'lib/vertica/messages/password.rb',
                   'lib/vertica/messages/portal_suspended.rb',
                   'lib/vertica/messages/query.rb',
                   'lib/vertica/messages/ready_for_query.rb',
                   'lib/vertica/messages/row_description.rb',
                   'lib/vertica/messages/ssl_request.rb',
                   'lib/vertica/messages/startup.rb',
                   'lib/vertica/messages/sync.rb',
                   'lib/vertica/messages/terminate.rb',
                   'lib/vertica/messages/unknown.rb',
                   'test/connection_test.rb',
                   'test/create_schema.sql',
                   'test/test_helper.rb']
                   
  s.test_files =  ['test/test_helper.rb']

  s.homepage = "http://github.com/mattbauer/vertica"

  s.require_paths = ["lib"]
  s.has_rdoc      = true
end
