# Native Proximity Map

`ProximityMap` is a persistent approximate nearest-neighbor index for finite
fixed-dimensional `f32` vectors. It combines an exact ordered directory with a
deterministic hierarchy of nearest representatives.

## Data model

Each logical record has a stable unique byte key, a fixed-dimensional vector,
and an opaque byte value. The key is identity. Equal vectors are legal and
remain distinct records. Exact `get` and `contains_key` use the ordered
directory and never use approximate routing.

One `PRXI` descriptor CID commits:

```text
PRXI descriptor
  existing ordered Tree: key -> PRVR(vector, value)
  PRXN proximity root: representative key/vector -> child CID
```

Persist the descriptor CID as the application-visible index version.

## Building and reopening

```rust
use prolly::{
    DistanceMetric, MemStore, ProximityConfig, ProximityMap, ProximityRecord,
};
use std::sync::Arc;

let store = Arc::new(MemStore::new());
let config = ProximityConfig {
    dimensions: 2,
    metric: DistanceMetric::L2Squared,
    log_chunk_size: 8,
    level_hash_seed: 42,
    max_node_bytes: 256 * 1024,
};
let map = ProximityMap::build(
    store.clone(),
    config,
    [ProximityRecord {
        key: b"doc-a".to_vec(),
        vector: vec![0.0, 1.0],
        value: b"first".to_vec(),
    }],
)?;
let descriptor = map.tree().descriptor.clone();
let reopened = ProximityMap::load(store, descriptor)?;
assert!(reopened.contains_key(b"doc-a")?);
# Ok::<(), prolly::Error>(())
```

Build validates before publishing the descriptor, sorts by key, rejects
duplicate keys, canonicalizes negative zero, and rejects NaN/infinity. Input
order does not affect any root CID.

## Exact lookup and ANN search

```rust,no_run
# use prolly::{ProximityMap, SearchOptions, Store};
# fn example<S>(map: &ProximityMap<S>) -> Result<(), prolly::Error>
# where S: Store + Clone + Send + Sync, S::Error: Send + Sync {
let exact = map.get(b"doc-a")?;
let result = map.search(
    &[0.1, 0.9],
    SearchOptions {
        k: 10,
        beam_width: 64,
        max_nodes: Some(1_000),
        max_distance_evaluations: Some(100_000),
    },
)?;
for neighbor in result.neighbors {
    println!("{:?} {}", neighbor.key, neighbor.distance);
}
# Ok(()) }
```

`k` is the result count. `beam_width` is the maximum candidate frontier per
level and must be at least `k`. A larger beam generally improves recall at the
cost of reads and distance evaluations. Results use total order `(distance,
key)`. Budget exhaustion returns deterministic partial results and sets
`stats.budget_exhausted`.

The cache is bounded and typed separately from ordered nodes. `nodes_read`
counts logical visits; `bytes_read` reports physical bytes and therefore drops
on warm-cache searches.

## Mutation

`mutate_batch` routes old and new vectors independently, rebuilds a child
cluster when its representative set can change, recursively rewrites affected
PRXN nodes, and reuses untouched CIDs. A root representative change triggers a
documented full PRXN rebuild. `rebuild_batch` is the clean-build oracle; all
three construction paths produce the same descriptor for the same records.

Value-only mutation reuses the complete PRXN root. The descriptor is always
written last, after its directory and PRXN descendants.

The existing ordered batch writer does not guarantee the same physical root
after every long mutation history, so the exact directory is canonically
bulk-rebuilt on each proximity mutation. This is O(n) directory work even when
PRXN work is localized. It does not weaken logical or CID correctness.

## Verification and limits

`verify` audits CIDs, codecs, sizes, level transitions, cycles, promotion
levels, nearest-parent routing, subtree counts, and exact-directory/leaf
identity equality. Normal `load` performs local descriptor/root validation.

`log_chunk_size = 8` gives expected mean fanout near 256. `max_node_bytes` is a
hard limit: the builder returns `Error::ProximityNodeTooLarge` instead of using
an unproven split rule.

Use an external ANN engine when the primary requirement is HNSW-style online
graph mutation, product quantization, GPU search, or metric plugins. This map is
optimized for deterministic versioned snapshots and structural sharing.

## Provenance and compatibility

The hierarchy, level-by-level nearest-representative routing, and localized
copy-on-write mutation behavior are inspired by Dolt's Apache-2.0 proximity-map
design. This is an independent Rust implementation with its own PRVR/PRXN/PRXI
wire format; it does not copy Dolt source or claim byte compatibility with
Dolt's storage objects. Existing CRAB v1 ordered-tree bytes remain unchanged.

## Initial benchmark characterization

The 2026-07-13 release-build smoke benchmark used deterministic synthetic data
on the development machine. These numbers validate the harness and mutation
reuse counters; they are not cross-machine performance promises.

| Records | Dimensions | Build | Search | Recall@10 | PRXN writes | PRXN roots reused |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 8 | 1.458 ms | 424.292 µs | 1.0000 | 2 | 0 |
| 256 | 128 | 2.228 ms | 1.144 ms | 1.0000 | 2 | 0 |
| 4,096 | 8 | 3.599 ms | 2.498 ms | 1.0000 | 3 | 11 |

The small run's exhaustive effective frontier explains its perfect recall and
is not representative of large-scale approximate recall. Run the full matrix
before selecting a production beam width:

```sh
PROLLY_PROXIMITY_BENCH_RECORDS=10000 \
PROLLY_PROXIMITY_BENCH_DIMENSIONS=8,128,768,1536 \
cargo bench --bench proximity_bench
```
