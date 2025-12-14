require_relative 'lib/pulsar/client/version'

Gem::Specification.new do |spec|
  spec.name          = "pulsar-client"
  spec.version       = Pulsar::VERSION
  spec.authors       = ["Thomas Li"]
  spec.email         = ["thomas.li@shoplineapp.com"]

  spec.summary       = "Apache Pulsar Client for Ruby"
  spec.description   = "An Apache Pulsar client library for Ruby, developed to be independent of C++ bindings."
  spec.license       = "MIT"
  spec.required_ruby_version = Gem::Requirement.new(">= 2.3.0")

  spec.add_runtime_dependency 'google-protobuf', '~> 3.25'
  spec.add_runtime_dependency "digest-crc", "~> 0.6"
end
