# frozen_string_literal: true
Gem::Specification.new do |spec|
  spec.name = 'crabbuild-prolly-store-spanner'
  spec.version = '0.1.0'
  spec.summary = 'Cloud Spanner remote-store adapter for Prolly'
  spec.authors = ['Crabbuild Contributors']
  spec.license = 'MIT OR Apache-2.0'
  spec.required_ruby_version = '>= 3.2'
  spec.files = Dir['lib/**/*.rb'] + ['README.md']
  spec.require_paths = ['lib']
  spec.add_runtime_dependency 'crabbuild-prolly', '= 0.1.0'
  spec.add_runtime_dependency 'google-cloud-spanner', '= 2.36.0'
  spec.add_runtime_dependency 'mutex_m', '= 0.3.0'
end
