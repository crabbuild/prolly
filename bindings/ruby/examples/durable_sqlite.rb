# frozen_string_literal: true

require 'tmpdir'

$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))
require 'prolly'

def upsert(key, value)
  Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: key.b, value: value.b)
end

def delete(key)
  Prolly::MutationRecord.new(kind: Prolly::MutationKind::DELETE, key: key.b, value: nil)
end

def assert_equal(expected, actual, label = 'value')
  return if expected == actual

  raise "#{label}: expected #{expected.inspect}, got #{actual.inspect}"
end

def durable_sqlite
  Dir.mktmpdir('prolly-ruby-') do |dir|
    engine = Prolly::ProllyEngine.sqlite(File.join(dir, 'app.prolly.sqlite'), Prolly.default_config)
    tree = engine.batch(engine.create, [upsert('user/1', 'Ada'), upsert('user/2', 'Grace')])
    engine.publish_named_root('users/main'.b, tree)
    loaded = engine.load_named_root('users/main'.b)

    assert_equal tree, loaded, 'loaded SQLite root'
    assert_equal 'Ada'.b, engine.get(loaded, 'user/1'.b), 'sqlite user'
  end
  puts 'durable_sqlite: named root survived through SQLite store API'
end

durable_sqlite
