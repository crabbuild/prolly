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

def document_chunk_index
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  blob_store = Prolly::ProllyBlobStore.memory
  text_key = 'doc-index/corpus/text/parser-v1/doc-1/chunk-0001'.b
  metadata_key = 'doc-index/corpus/parser/parser-v1/document/doc-1/chunk/000000'.b

  tree = engine.put_large_value(
    blob_store,
    engine.create,
    text_key,
    ('CrabDB stores large chunk text outside prolly leaves.' * 8).b,
    Prolly::LargeValueConfigRecord.new(inline_threshold: 32)
  )
  tree = engine.put(tree, metadata_key, 'doc-1|chunk-0001|0|384|vector-0001'.b)

  metadata = engine.range(tree, 'doc-index/corpus/parser/'.b, 'doc-index/corpus/parser0'.b)
  loaded_text = engine.get_large_value(blob_store, tree, text_key)

  assert_equal 1, metadata.length, 'metadata count'
  raise 'missing chunk text' unless loaded_text.start_with?('CrabDB stores'.b)

  puts 'document_chunk_index: metadata and blob-backed chunk text are linked'
end

document_chunk_index
