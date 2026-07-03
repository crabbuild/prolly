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

def batch_build
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  entries = (1..64).to_a.reverse.map do |idx|
    Prolly::EntryRecord.new(key: format('event/%04d', idx).b, value: "payload-#{idx}".b)
  end
  tree = engine.build_from_entries(entries)
  rows = engine.range(tree, 'event/'.b, 'event0'.b)
  stats = engine.collect_stats_json(tree).json

  assert_equal 64, rows.length, 'row count'
  assert_equal 'event/0001'.b, rows.first.key, 'first key'
  raise 'stats missing num_nodes' unless stats.include?('num_nodes')

  puts "batch_build: imported #{rows.length} events"
end

batch_build
