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

def local_first_state
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  main = 'app/demo/root/main'.b
  base = engine.batch(
    engine.create,
    [
      upsert('entity/user/001', 'Ada'),
      Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'index/user/name/Ada/001'.b, value: ''.b)
    ]
  )
  engine.publish_named_root(main, base)

  device = engine.batch(
    base,
    [
      upsert('entity/task/900', 'offline draft'),
      Prolly::MutationRecord.new(kind: Prolly::MutationKind::UPSERT, key: 'index/task/status/open/900'.b, value: ''.b)
    ]
  )
  canonical = engine.put(base, 'entity/user/002'.b, 'Grace'.b)
  engine.publish_named_root(main, canonical)

  current = engine.load_named_root(main)
  merged = engine.merge(base, current, device, 'prefer_right')
  update = engine.compare_and_swap_named_root(main, current, merged)

  assert_equal true, update.applied, 'CAS applied'
  assert_equal 'Grace'.b, engine.get(merged, 'entity/user/002'.b), 'canonical user'
  assert_equal 'offline draft'.b, engine.get(merged, 'entity/task/900'.b), 'device task'

  puts 'local_first_state: merged offline branch into main'
end

local_first_state
