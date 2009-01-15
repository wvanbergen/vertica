VERTICA_SPEC = Gem::Specification.new do |s|
  s.platform  =   Gem::Platform::RUBY
  s.required_ruby_version = '>=1.8.4'
  s.name      =   "bdb"
  s.version   =   "0.0.1"
  s.author    =   "Matt Bauer"
  s.email     =   "bauer@pedalbrain.com"
  s.summary   =   "A Ruby interface to Vertica"
  s.files     =   ['vertica.gemspec',
                   'ext/extconf.rb',
                   'LICENSE',
                   'README.textile',
                   'Rakefile']
  s.test_files =  ['test/test_helper.rb']
  s.extensions = ["ext/extconf.rb"]

  s.homepage = "http://www.vertica.com"

  s.require_paths = ["lib", "ext"]
  s.has_rdoc      = true
end

