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

def conversation_memory
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  main = 'conversation/c42/root/main'.b
  attempt_name = 'conversation/c42/attempt/extractor/a1'.b

  base = engine.put(engine.create, 'conversation/c42/memory/m001'.b, 'user|likes terse summaries|0.91'.b)
  engine.publish_named_root(main, base)
  attempt = engine.put(base, 'conversation/c42/memory/m002'.b, 'user|uses Ruby|0.87'.b)
  engine.publish_named_root(attempt_name, attempt)
  canonical = engine.put(base, 'conversation/c42/memory/m003'.b, 'user|prefers local-first apps|0.82'.b)
  engine.publish_named_root(main, canonical)

  merged = engine.merge(base, engine.load_named_root(main), engine.load_named_root(attempt_name), 'prefer_right')
  update = engine.compare_and_swap_named_root(main, canonical, merged)
  rows = engine.range(merged, 'conversation/c42/memory/'.b, 'conversation/c42/memory0'.b)

  assert_equal true, update.applied, 'CAS applied'
  assert_equal 3, rows.length, 'memory count'

  puts 'conversation_memory: accepted extractor attempt into canonical memory'
end

conversation_memory
