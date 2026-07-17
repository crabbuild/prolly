# frozen_string_literal: true

require 'prolly/store/sqlite'
require 'tmpdir'

Dir.mktmpdir('prolly-ruby-sqlite') do |directory|
  database = SQLite3::Database.new(File.join(directory, 'store.sqlite3'))
  store = Prolly::SqliteRemoteStore.new(database)
  store.initialize_schema
  engine = Prolly.open_remote_prolly_engine(store, Prolly.default_config)
  tree = engine.put(engine.create, 'key'.b, 'value'.b)
  raise 'Rust/Ruby SQLite round trip failed' unless engine.get(tree, 'key'.b) == 'value'.b

  name = 'main'.b
  missing = Prolly::OptionalBytesRecord.new(present: false, value: ''.b)
  results = 32.times.map do |index|
    Thread.new do
      replacement = Prolly::OptionalBytesRecord.new(present: true, value: [index].pack('C'))
      store.compare_and_swap_root_manifest(name, missing, replacement)
    end
  end.map(&:value)
  raise 'SQLite CAS did not have exactly one winner' unless results.count(&:applied) == 1

  store.close
  raise 'adapter closed caller-owned database' unless database.get_first_value('SELECT 1') == 1
  database.close
end

puts 'Ruby SQLite remote store passed'
