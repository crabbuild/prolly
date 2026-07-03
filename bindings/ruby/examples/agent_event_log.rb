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

def agent_event_log
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  root = 'agent-log/run-7/root/events/current'.b
  tree = engine.batch(
    engine.create,
    [
      upsert('agent-log/run-7/event/1783036805000/0001', 'user|Summarize the plan'),
      upsert('agent-log/run-7/event/1783036805000/0002', 'tool-call|search-docs'),
      upsert('agent-log/run-7/event/1783036806000/0003', 'assistant|Plan ready')
    ]
  )
  engine.publish_named_root(root, tree)

  page = engine.range_page(engine.load_named_root(root), nil, nil, 2)
  assert_equal 2, page.entries.length, 'page length'
  raise 'missing next cursor' unless page.next_cursor

  puts "agent_event_log: first page has #{page.entries.length} events"
end

agent_event_log
