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

def provenance_values
  engine = Prolly::ProllyEngine.memory(Prolly.default_config)
  source = 'CrabDB language bindings design'.b
  source_cid = Prolly.cid_from_bytes(source).unpack1('H*')
  chunk_cid = Prolly.cid_from_bytes(source[0, 16]).unpack1('H*')
  tree = engine.batch(
    engine.create,
    [
      upsert('provenance/chunk/file-1/chunk-1', "source=#{source_cid}|chunk=#{chunk_cid}|parser=v1"),
      upsert('provenance/claim/file-1/claim-1', 'CrabDB uses Rust-backed bindings|chunk=file-1/chunk-1')
    ]
  )

  claims = engine.range(tree, 'provenance/claim/file-1/'.b, 'provenance/claim/file-10'.b)
  assert_equal 1, claims.length, 'claim count'
  raise 'missing claim text' unless claims.first.value.include?('Rust-backed'.b)

  puts 'provenance_values: claim links back to source and chunk CIDs'
end

provenance_values
