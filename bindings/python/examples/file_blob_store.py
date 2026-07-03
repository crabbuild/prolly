from __future__ import annotations

import prolly


engine = prolly.ProllyEngine.memory(prolly.default_config())
blob_store = prolly.ProllyBlobStore.memory()
policy = prolly.LargeValueConfigRecord(inline_threshold=8)

tree = engine.create()
tree = engine.put_large_value(blob_store, tree, b"doc/body", bytes([7]) * 64, policy)
assert engine.get_value_ref(tree, b"doc/body").kind == prolly.ValueRefKind.BLOB

updated = engine.put_large_value(blob_store, tree, b"doc/body", bytes([9]) * 64, policy)
assert engine.get_large_value(blob_store, updated, b"doc/body") == bytes([9]) * 64

plan = engine.plan_blob_store_gc(blob_store, [updated])
assert plan.reclaimable_blob_count == 1
sweep = engine.sweep_blob_store_gc(blob_store, [updated])
assert sweep.deleted_blobs == 1

print(f"file_blob_store: reclaimed {sweep.deleted_blob_bytes} bytes")
