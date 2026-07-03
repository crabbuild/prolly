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

def crdt_merge
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  base_value = Prolly.timestamped_value_to_bytes(Prolly::TimestampedValueRecord.new(value: 'base'.b, timestamp: 1))
  left_value = Prolly.timestamped_value_to_bytes(Prolly::TimestampedValueRecord.new(value: 'left'.b, timestamp: 2))
  right_value = Prolly.timestamped_value_to_bytes(Prolly::TimestampedValueRecord.new(value: 'right'.b, timestamp: 3))

  base = engine.put(engine.create, 'counter/global'.b, base_value)
  left = engine.put(base, 'counter/global'.b, left_value)
  right = engine.put(base, 'counter/global'.b, right_value)
  merged = engine.crdt_merge(base, left, right, Prolly.crdt_config_lww(Prolly::CrdtDeletePolicyKind::UPDATE_WINS))
  decoded = Prolly.timestamped_value_from_bytes(engine.get(merged, 'counter/global'.b))
  merged_set = Prolly.multi_value_set_merge(['candidate-b'.b], ['candidate-a'.b, 'candidate-b'.b])

  assert_equal 'right'.b, decoded.value, 'CRDT value'
  assert_equal 3, decoded.timestamp, 'CRDT timestamp'
  assert_equal ['candidate-a'.b, 'candidate-b'.b], merged_set, 'multi-value set'

  puts 'crdt_merge: last-writer-wins and multi-value helpers passed'
end

crdt_merge
