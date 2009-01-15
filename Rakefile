require 'rubygems'
require 'rake/gempackagetask'
require 'rake/rdoctask'
require 'rake/testtask'

load 'vertica.gemspec'
 
Rake::GemPackageTask.new(VERTICA_SPEC) do |pkg|
    pkg.need_tar = true
end
 
task :default => "test"

desc "Clean"
task :clean do
  include FileUtils
  Dir.chdir('ext') do
    rm(Dir.glob('*') - ['extconf.rb'])
  end
  rm_rf 'pkg'
end
 
desc "Run tests"
Rake::TestTask.new("test") do |t|
  t.libs << ["test", "ext"]
  t.pattern = 'test/*_test.rb'
  t.verbose = true
  t.warning = true
end
 
task :doc => [:rdoc]
namespace :doc do
  Rake::RDocTask.new do |rdoc|
    files = ["README", "lib/**/*.rb"]
    rdoc.rdoc_files.add(files)
    rdoc.main = "README.textile"
    rdoc.title = "Vertica Docs"
    rdoc.rdoc_dir = "doc"
    rdoc.options << "--line-numbers" << "--inline-source"
  end
end

