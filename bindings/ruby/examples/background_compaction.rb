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

def background_compaction
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  events = engine.batch(
    engine.create,
    (1..6).map { |idx| upsert(format('event/%04d', idx), "raw-event-#{idx}") }
  )
  engine.publish_named_root('compaction/run/r7/root/events/0001'.b, events)
  compacted = engine.batch(
    events,
    [
      delete('event/0001'),
      delete('event/0002'),
      delete('event/0003'),
      delete('event/0004'),
      upsert('event/0004-summary', 'summary of events 1..4')
    ]
  )
  engine.publish_named_root('compaction/run/r7/root/events/current'.b, compacted)

  plan = engine.plan_store_gc([events, compacted])
  remaining = engine.range(compacted, 'event/'.b, 'event0'.b)

  assert_equal 3, remaining.length, 'remaining records'
  raise 'invalid GC plan' if plan.reclaimable_nodes.negative?

  puts "background_compaction: compacted log to #{remaining.length} records"
end

background_compaction
