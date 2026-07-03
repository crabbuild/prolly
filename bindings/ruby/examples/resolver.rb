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

def resolver
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  base = engine.put(engine.create, 'settings/theme'.b, 'light'.b)
  left_delete = engine.delete(base, 'settings/theme'.b)
  right_update = engine.put(base, 'settings/theme'.b, 'dark'.b)

  update_wins = engine.merge(base, left_delete, right_update, 'update_wins')
  delete_wins = engine.merge(base, left_delete, right_update, 'delete_wins')

  assert_equal 'dark'.b, engine.get(update_wins, 'settings/theme'.b), 'update-wins setting'
  assert_equal nil, engine.get(delete_wins, 'settings/theme'.b), 'delete-wins setting'

  puts 'resolver: demonstrated update-wins and delete-wins policies'
end

resolver
