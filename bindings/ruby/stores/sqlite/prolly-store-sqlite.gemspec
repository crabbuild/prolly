# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = 'trail-prolly-store-sqlite'
  spec.version = '0.1.0'
  spec.summary = 'SQLite async remote-store adapter for Prolly'
  spec.authors = ['Trail Contributors']
  spec.license = 'MIT OR Apache-2.0'
  spec.required_ruby_version = '>= 3.2'
  spec.files = Dir['lib/**/*.rb'] + ['README.md']
  spec.require_paths = ['lib']
  spec.add_runtime_dependency 'trail-prolly', '= 0.1.0'
  spec.add_runtime_dependency 'sqlite3', '= 2.9.5'
end
