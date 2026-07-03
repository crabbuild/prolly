# frozen_string_literal: true

require 'prolly'

engine = Prolly::ProllyEngine.memory(Prolly.default_config)
blob_store = Prolly::ProllyBlobStore.memory
policy = Prolly::LargeValueConfigRecord.new(inline_threshold: 8)

tree = engine.create
tree = engine.put_large_value(blob_store, tree, 'doc/body'.b, Array.new(64, 7).pack('C*').b, policy)
raise 'expected blob value ref' unless engine.get_value_ref(tree, 'doc/body'.b).kind == Prolly::ValueRefKind::BLOB

updated = engine.put_large_value(blob_store, tree, 'doc/body'.b, Array.new(64, 9).pack('C*').b, policy)
raise 'large value mismatch' unless engine.get_large_value(blob_store, updated, 'doc/body'.b) == Array.new(64, 9).pack('C*').b

plan = engine.plan_blob_store_gc(blob_store, [updated])
raise 'expected one reclaimable blob' unless plan.reclaimable_blob_count == 1
sweep = engine.sweep_blob_store_gc(blob_store, [updated])
raise 'expected one deleted blob' unless sweep.deleted_blobs == 1

puts "file_blob_store: reclaimed #{sweep.deleted_blob_bytes} bytes"
