require "bundler/gem_tasks"
require "rake/testtask"
require "yard"

Rake::TestTask.new(:test) do |test|
  test.libs << 'test' << 'lib'
  test.pattern = 'test/**/*_test.rb'
  test.verbose = true
end

YARD::Rake::YardocTask.new(:doc) do |t|
  t.options = ['--no-private', '-m', 'markdown', '-o', './doc']
end

task :default => :test
