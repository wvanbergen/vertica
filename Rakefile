require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = 'vertica'
    gem.summary = 'Pure ruby library for interacting with Vertica'
    gem.description = 'Query Vertica with ruby'

    gem.email = 'sprsquish@gmail.com'
    gem.homepage = 'http://github.com/sprsquish/vertica'
    gem.authors = ['Jeff Smick', 'Matt Bauer']

    gem.files = FileList["[A-Z]*", 'lib/**/*.rb'].to_a

    gem.test_files = FileList['test/**/*.rb']

    # gem is a Gem::Specification... see http://www.rubygems.org/read/chapter/20 for additional settings
  end

  Jeweler::GemcutterTasks.new
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: sudo gem install jeweler"
end
#
# require 'rake/testtask'
# Rake::TestTask.new(:test) do |test|
#   test.libs << 'lib' << 'spec'
#   test.pattern = 'spec/**/*_spec.rb'
#   test.verbose = true
# end
#
# begin
#   require 'rcov/rcovtask'
#   Rcov::RcovTask.new do |test|
#     test.libs << 'spec'
#     test.pattern = 'spec/**/*_spec.rb'
#     test.rcov_opts += ['--exclude \/Library\/Ruby,spec\/', '--xrefs']
#     test.verbose = true
#   end
# rescue LoadError
#   task :rcov do
#     abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
#   end
# end


begin
  require 'yard'
  YARD::Rake::YardocTask.new do |t|
    t.options = ['--no-private', '-m', 'markdown', '-o', './doc']
  end
rescue LoadError
  task :yardoc do
    abort "YARD is not available. In order to run yardoc, you must: sudo gem install yard"
  end
end

desc 'Generate documentation'
task :doc => :yard
task :default => :test
