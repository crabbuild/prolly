# Proximity Map

`ProximityMap` is a deterministic, content-addressed vector index for finite,
fixed-dimensional `f32` vectors. It combines an authoritative ordered directory
with a persistent nearest-representative hierarchy. The design favors immutable
versions, reproducible builds, structural sharing, verifiable search, and
Dolt-style localized copy-on-write mutation.

This proximity map uses the current persisted format. Older proximity objects are
rejected and must be rebuilt from logical records. Existing ordered `CRAB` nodes
and ordered-tree APIs are unchanged.

## Model and persisted objects

Each record has a unique byte key, a vector, and an opaque value. Equal vectors
are legal; the key is identity. `get` and `contains_key` always use the exact
directory.

```text
PRXI descriptor
├── ordered CRAB tree: key -> PRVR(vector, value)
└── PRXN hierarchy
    ├── overflow PRXN directories/pages
    ├── PRXV external vectors
    └── optional PQS8 node-local routing data

PQPQ product-quantization sidecar ──bound to PRXI CID
HNSW graph sidecar                ──bound to PRXI CID
```

The application-visible immutable version is the PRXI descriptor CID. PQ and
HNSW are derived, independently retained sidecars and never replace exact
directory values.

## Build, reopen, and exact lookup

```rust
use prolly::{
    DistanceMetric, MemStore, ProximityConfig, ProximityMap, ProximityRecord,
    ScalarQuantizationConfig,
};
use std::sync::Arc;

let store = Arc::new(MemStore::new());
let mut config = ProximityConfig::new(3);
config.metric = DistanceMetric::Cosine;
config.hierarchy.level_hash_seed = 42;
config.scalar_quantization = Some(ScalarQuantizationConfig { group_size: 3 });

let map = ProximityMap::build(store.clone(), config, [
    ProximityRecord {
        key: b"doc/a".to_vec(),
        vector: vec![0.0, 1.0, 0.0],
        value: b"alpha".to_vec(),
    },
    ProximityRecord {
        key: b"doc/b".to_vec(),
        vector: vec![1.0, 0.0, 0.0],
        value: b"beta".to_vec(),
    },
])?;

let descriptor = map.tree().descriptor.clone();
assert_eq!(map.get(b"doc/a")?.unwrap().1, b"alpha");
let reopened = ProximityMap::load(store, descriptor)?;
reopened.verify()?;
# Ok::<(), prolly::Error>(())
```

Build input is key-sorted internally. Duplicate keys, wrong dimensions,
non-finite components, and zero cosine vectors are rejected. L2 uses squared
Euclidean distance, cosine uses normalized vectors and `1 - dot`, and inner
product uses negated dot product so lower scores are always better.

`build_with_parallelism` accepts `BuildParallelism`. Worker counts change
runtime only: persisted bytes, CIDs, logical statistics, and canonical error
selection remain identical.

## Search, filters, policies, and kernels

```rust
use prolly::{
    AdaptiveQuality, ProximityFilter, QueryKernel, SearchPolicy, SearchRequest,
};

# fn run<S>(map: &prolly::ProximityMap<S>) -> Result<(), prolly::Error>
# where S: prolly::Store + Clone + Send + Sync, S::Error: Send + Sync {
let query = [0.1, 0.9, 0.0];
let exact = map.search(SearchRequest::exact(&query, 10))?;

let mut filtered = SearchRequest::exact(&query, 10);
filtered.policy = SearchPolicy::Adaptive(AdaptiveQuality::HighRecall);
filtered.filter = ProximityFilter::Prefix(b"doc/");
filtered.kernel = QueryKernel::AutoDeterministic;
let approximate = map.search(filtered)?;
println!("{:?}", approximate.completion);
# let _ = exact;
# Ok(()) }
```

Filters are `All`, half-open `KeyRange`, `Prefix`, sorted-unique
`EligibleKeys`, or `SecondaryEligible` bound to the exact source directory.
Stale secondary candidates are rejected.

- `Exact` proves exact L2 termination from conservative lower bounds. Cosine
  and inner-product exact searches exhaust eligible leaves.
- `FixedBudget` reports `BudgetExhausted` when a configured resource limit is
  reached.
- `Adaptive(Fast|Balanced|HighRecall)` uses deterministic structural signals
  and reports `ApproximatePolicySatisfied`.

Results are ordered by `(score, key)`. Scalar, SIMD, and auto query kernels are
bit-identical; construction and mutation always use canonical scalar math.

With the `async-store` feature, `AsyncProximityMap` performs the same logical
best-first search over `AsyncStore`. `AsyncSearchControl` bounds in-flight
reads, prefetch width, and speculative bytes and supports cancellation and
deadlines. Read completion order cannot change committed visitation or results.

## Localized copy-on-write mutation

```rust
# use prolly::{ProximityMap, ProximityMutation, Store};
# fn update<S>(map: &ProximityMap<S>) -> Result<(), prolly::Error>
# where S: Store + Clone + Send + Sync, S::Error: Send + Sync {
let (next, stats) = map.mutate_batch([ProximityMutation {
    key: b"doc/b".to_vec(),
    value: Some((vec![0.2, 0.8, 0.0], b"updated".to_vec())),
}])?;
assert!(!stats.full_proximity_rebuild || stats.records_rebuilt > 0);
next.verify()?;
# Ok(()) }
```

The exact directory uses a canonical boundary-resynchronizing splice writer.
PRXN mutation routes old and new vectors independently and rewrites only
affected clusters, summaries, overflow pages, and ancestors. Value-only edits
reuse the complete PRXN root. A representative change at the root may require
a clean PRXN rebuild. `rebuild_batch` remains the canonical oracle; equal
logical records always produce the same descriptor CID.

Mutation statistics separate directory scans/rebuilds/reuse from PRXN
reads/writes/reuse and explicitly report full fallback.

## Overflow and vector storage

`OverflowConfig` defines deterministic minimum, target, and maximum page sizes
plus a splitter seed. Oversized logical nodes become content-defined overflow
pages and recursive directories. `VectorStorageConfig` deterministically moves
large vectors to PRXV objects. Immediate-child summaries commit key ranges,
subtree counts, representatives, and conservatively rounded L2 radii.

## SQ8, product quantization, and HNSW

Node-local SQ8 is enabled in `ProximityConfig`. It influences approximate
routing only; leaf candidates are resolved and reranked from full vectors.

```rust,no_run
# use prolly::*;
# fn sidecars<S>(map: &ProximityMap<S>) -> Result<(), Error>
# where S: Store + Clone + Send + Sync, S::Error: Send + Sync {
let (pq, _) = ProductQuantizer::build(
    map,
    ProductQuantizationConfig {
        subquantizers: 4,
        centroids_per_subquantizer: 16,
        training_iterations: 8,
        rerank_multiplier: 8,
        seed: 7,
    },
    BuildParallelism::new(4)?,
)?;
let (hnsw, _) = HnswIndex::build(map, HnswConfig::default())?;

let query = vec![0.0; map.tree().config.dimensions as usize];
let mut pq_request = SearchRequest::exact(&query, 10);
pq_request.policy = SearchPolicy::FixedBudget;
pq_request.backend = SearchBackend::ProductQuantized;
let _ = pq.search(map, pq_request)?;

let mut hnsw_request = SearchRequest::exact(&query, 10);
hnsw_request.policy = SearchPolicy::FixedBudget;
hnsw_request.backend = SearchBackend::Hnsw;
let _ = hnsw.search(map, hnsw_request)?;
# Ok(()) }
```

PQ training and clean HNSW construction are key-ordered and deterministic.
Both manifests bind source descriptor, dimensions, metric, and configuration.
Explicit stale/corrupt sidecars fail. `Auto` may fall back to native search;
neither PQ nor HNSW can satisfy `Exact`.

## Named publication, replication, and GC

```rust
use prolly::{
    copy_and_publish_content_graph, ContentGraphLimits, ContentRootManifest,
    MemStore, TypedContentRoot,
};
use std::collections::BTreeMap;

# fn publish(source: &MemStore, descriptor: prolly::Cid) -> Result<(), prolly::Error> {
let destination = MemStore::new();
let root = TypedContentRoot::proximity_descriptor(descriptor);
let manifest = ContentRootManifest {
    root,
    logical_version: 1,
    created_at_millis: 0,
    metadata: BTreeMap::new(),
};
copy_and_publish_content_graph(
    source,
    &destination,
    b"indexes/main",
    manifest,
    &ContentGraphLimits::default(),
)?;
# Ok(()) }
```

Typed walking verifies CIDs before decoding, carries codec context, suppresses
sharing, detects conflicting references, and enforces object/depth/byte/fanout
limits. Replication writes verified descendants before parents, rehashes
destination reuse, validates the complete destination closure, then publishes
the content-root manifest. Interrupted copies remain unreachable.

Use `compare_and_swap_named_content_root` for concurrent heads.
`plan_content_gc`/`sweep_content_gc_with_invalidator` mark any number of ordered,
PRXI, snapshot, PQ, or HNSW roots and preserve shared objects. Candidate sets
are explicit; the invalidator lets applications evict swept process caches.

## Proofs

```rust
# use prolly::{ContentGraphLimits, ProximityMap, SearchRequest, Store};
# fn prove<S>(map: &ProximityMap<S>) -> Result<(), prolly::Error>
# where S: Store + Clone + Send + Sync, S::Error: Send + Sync {
let membership = map.prove_membership(b"doc/a")?;
membership.verify_for(&map.tree().descriptor)?;

let limits = ContentGraphLimits::default();
map.prove_structure(&limits)?
    .verify_for(&map.tree().descriptor, &limits)?;

let query = vec![0.0; map.tree().config.dimensions as usize];
map.prove_search(SearchRequest::exact(&query, 5), &limits)?
    .verify_for_source(&map.tree().descriptor, &limits)?;
# Ok(()) }
```

Membership proofs bind PRXI bytes, the ordered path, and exact PRVR bytes.
Structural proofs carry the exact typed closure and replay every summary,
radius, routing, vector, and directory invariant in an isolated store. Native
search proofs commit request/filter/budgets/kernel and record frontier, visited
objects, candidates, and completion. PQ and HNSW proofs authenticate sidecar
closures and replay execution. Only exact native L2 returns
`ExactL2Optimal`; other modes return `HonestExecution`.

## Verification and operational limits

`load` validates descriptor and root locality. `verify` performs the expensive
whole-index audit. Keep traversal limits appropriate for untrusted proofs or
replicas, use explicit search budgets for tenant isolation, and retain every
root needed by snapshots before sweeping.

The node cache is bounded and process-local. Call `clear_content_cache`, or
wire it to the GC invalidator, after external deletion.

## Migration and compatibility

- There is no legacy reader, compatibility alias, or in-place migration.
- Export logical `(key, vector, value)` records with the old binary and rebuild
  a current proximity index with the same records.
- Publish the new descriptor only after `verify` and application checks pass.
- Existing ordered CRAB trees do not need rewriting unless they are replaced
  by the new proximity directory produced during rebuild.

The hierarchy and localized COW approach are inspired by Dolt's Apache-2.0
proximity map. This Rust implementation has independent codecs and does not
claim Dolt byte compatibility.

## Benchmarking

The harness covers dimensions 8/128/768/1536, build worker counts, localized
mutation, exact/adaptive/SQ8 search, scalar/SIMD, PQ/HNSW, overflow, content
graph copy/GC, and proofs. Async parity is exercised by the all-feature test
suite and benchmark compilation.

```sh
PROLLY_PROXIMITY_BENCH_RECORDS=10000 \
PROLLY_PROXIMITY_BENCH_DIMENSIONS=8,128,768,1536 \
cargo bench --all-features --bench proximity_bench
```

Benchmark rows are machine-specific evidence, not performance guarantees. See
[`proximity-map-completion-audit.md`](proximity-map-completion-audit.md)
for the release evidence matrix.
