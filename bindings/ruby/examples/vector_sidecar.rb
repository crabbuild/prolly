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

def vector_sidecar
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  sidecar = { 'vec-1' => [0.9, 0.1], 'vec-2' => [0.8, 0.2], 'vec-stale' => [1.0, 0.0] }
  tree = engine.batch(
    engine.create,
    [
      upsert('vector-sidecar/corpus/docs/chunk/doc-1/0001', 'vec-1|doc-1|parser-v1'),
      upsert('vector-sidecar/corpus/docs/chunk/doc-2/0001', 'vec-2|doc-2|parser-v1')
    ]
  )
  allowed = engine
            .range(tree, 'vector-sidecar/corpus/docs/chunk/'.b, 'vector-sidecar/corpus/docs/chunk0'.b)
            .map { |entry| entry.value.split('|'.b, 2).first }
  hits = sidecar.keys.sort.select { |vector_id| allowed.include?(vector_id.b) }

  assert_equal %w[vec-1 vec-2], hits, 'sidecar hits'

  puts "vector_sidecar: filtered sidecar hits to #{hits.length} snapshot vectors"
end

vector_sidecar
