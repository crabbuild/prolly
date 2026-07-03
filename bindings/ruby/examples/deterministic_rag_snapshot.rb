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

def deterministic_rag_snapshot
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  index_root = 'rag/corpus/docs/root/index/current'.b
  index_v1 = engine.batch(
    engine.create,
    [
      upsert('rag/corpus/docs/chunk/doc-1/0001', 'vector:v1|CrabDB stores deterministic roots'),
      upsert('rag/corpus/docs/chunk/doc-2/0001', 'vector:v2|Prolly trees diff by key')
    ]
  )
  engine.publish_named_root(index_root, index_v1)
  answers = engine.put(engine.create, 'rag/answer/q1'.b, "query:q1|snapshot:#{index_v1.root.unpack1('H*')}|citation:doc-1/0001".b)
  engine.publish_named_root('rag/corpus/docs/root/answers'.b, answers)

  index_v2 = engine.put(index_v1, 'rag/corpus/docs/chunk/doc-3/0001'.b, 'vector:v3|New content'.b)
  engine.publish_named_root(index_root, index_v2)

  assert_equal 2, engine.range(index_v1, 'rag/corpus/docs/chunk/'.b, 'rag/corpus/docs/chunk0'.b).length, 'replay rows'
  assert_equal 3, engine.range(engine.load_named_root(index_root), 'rag/corpus/docs/chunk/'.b, 'rag/corpus/docs/chunk0'.b).length, 'current rows'

  puts 'deterministic_rag_snapshot: replay kept original index root'
end

deterministic_rag_snapshot
