# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = 'crabdb-prolly'
  spec.version = '0.1.0'
  spec.summary = 'Ruby bindings for CrabDB prolly-map'
  spec.authors = ['CrabDB Contributors']
  spec.email = ['opensource@crab.build']
  spec.license = 'MIT OR Apache-2.0'
  spec.required_ruby_version = '>= 2.6'

  spec.files = Dir['lib/**/*.rb'] + Dir['lib/**/*.md'] + ['README.md']
  spec.require_paths = ['lib']

  spec.add_runtime_dependency 'ffi', '>= 1.15', '< 1.17'
end
