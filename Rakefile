require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

task :default => :spec

desc "Generate protobuf files"
task :proto do
  proto_dir = "lib/pulsar/internal/proto"

  Dir.glob("#{proto_dir}/*.proto").each do |proto_file|
    sh "protoc -I=#{proto_dir} --ruby_out=#{proto_dir} #{proto_file}"
  end

  puts "Protobuf files generated successfully in #{proto_dir}!"
end