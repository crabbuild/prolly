# Prolly performance guide

This guide explains how `prolly-map` gets good performance today, how to benchmark it, and where future optimization work should focus. It replaces the old root-level performance note, so this file is the canonical performance document for the crate.

`prolly-map` is not a raw key-value store. It is an immutable, content-addressed ordered map. Performance depends on keeping tree rewrites local, batching store input/output (I/O), reusing unchanged content-addressed nodes, and choosing storage backends that match the workload.

## Performance goals

Use these goals when you evaluate a change:

- Preserve immutable snapshot semantics
- Keep single-key updates bounded to the affected leaf and ancestor path
- Keep append workloads on the right edge of the tree
- Reuse content identifiers (CIDs) for unchanged subtrees
- Skip identical subtrees during diff, merge, sync, and garbage collection (GC)
- Use ordered batch reads and batch writes whenever stores support them
- Keep hints and caches correctness-optional
- Keep large payloads out of hot tree nodes
- Support both sync embedded stores and async remote stores
- Make every benchmark report enough verification data to catch incorrect fast paths

The key tradeoff is deliberate. Prolly trees add content-addressed metadata, but they buy cheap snapshots, structural diff, merge, sync, retention, and reproducible roots.

## Mental model

Think about cost in three layers:

```text
logical operation -> tree work -> store work
```

Tree work includes path traversal, node decoding, chunk boundary checks, rebalancing, CID computation, and serialization. Store work includes reads, writes, ordered batch reads, transactions, durable sync, and remote latency.

The fastest path usually:

1. touches the fewest leaves
2. rewrites the fewest ancestor nodes
3. batches reads and writes
4. reuses existing CIDs
5. avoids loading large values during metadata operations

## Implemented optimizations

The crate already includes several performance features. They should remain correctness-neutral unless the API explicitly states otherwise.

### Content-defined locality

Content-defined chunking keeps tree shape stable across replicas and rebuilds. A small edit usually rewrites one leaf and its ancestors, while unchanged subtrees keep the same CIDs.

Relevant configuration:

- `min_chunk_size`: avoids undersized nodes
- `max_chunk_size`: caps node size and rewrite size
- `chunking_factor`: controls average node size
- `hash_seed`: makes boundary placement deterministic for a dataset

### Packed node encoding

Newly written nodes use a compact deterministic encoding. Legacy CBOR node reads remain supported by tests, which protects existing persisted data while reducing the size of newly written nodes.

Why it matters:

- smaller node bytes reduce store I/O
- fewer bytes improve cache density
- stable bytes preserve CID identity
- deterministic encoding enables future cross-language fixtures

### Append fast path

`append_batch` detects sorted append-only mutation batches and updates the right edge directly. This avoids rediscovering the rightmost path from the root for every append batch.

Implemented pieces:

- in-process rightmost-path cache
- persisted rightmost-path hints for hint-capable stores
- ordered batch reads to hydrate a hinted path in a fresh manager
- fallback to normal traversal when hints are absent, stale, or unsupported

Use this path for:

- event logs
- document chunk ingestion
- monotonic timestamp indexes
- append-heavy agent traces
- staged imports sorted by key

### Batch mutation planning

`Prolly::batch` sorts and coalesces mutations, routes them to affected leaves, applies each touched leaf once, rebuilds affected ancestors, and flushes rewritten nodes in a batch.

This reduces repeated path rewrites compared with applying many `put` or `delete` calls individually.

Batch paths also support:

- prefetching when stores prefer batch reads
- append-heavy right-edge optimization
- bottom-up rebuild strategies through `BatchWriterConfig`
- deferred mutation planning for large mutation sets

### Bulk builders

`BatchBuilder` and `SortedBatchBuilder` build full trees from large input sets. Use them for first imports, fixture generation, index rebuilds, and materialized view rebuilds.

Use `SortedBatchBuilder` when input is already ordered by key. That avoids sorting and matches database export or log-compaction workflows.

### Ordered batch reads

The `Store` trait supports `batch_get_ordered` and `batch_get_ordered_unique`. Stores with native multi-get, request coalescing, transactions, or remote parallelism should override these methods.

Tree operations use ordered reads in:

- batch mutation route planning
- diff subtree hydration
- async traversal frontiers
- rightmost-path hint hydration
- missing-node copy

Ordered reads are important because callers often need results in the same order as requested child CIDs.

### Batch writes and transactions

Stores can override `batch_put` and `batch_put_with_hint`. SQLite uses transactions for batched writes. Hint-capable stores can atomically write nodes plus the performance hint that points to them.

Good store behavior:

- node writes are idempotent
- batch writes are atomic when the backend supports it
- hints never make correctness depend on local metadata
- root manifest writes use compare-and-swap when multiple writers may race

### Node cache and pinning

Each `Prolly` or `AsyncProlly` manager has a decoded-node cache. `Config` can bound it by node count and serialized-byte budget.

Cache features:

- hit, miss, and eviction counters
- optional hot-root or hot-path pinning
- explicit cache clearing
- correctness fallback to the store on every miss

Use bounded caches in long-running services. Use pinning only for hot snapshots that you know will be queried repeatedly.

### Diff and merge pruning

Diff, range diff, merge, and sync compare CIDs before descending. Equal CIDs mean the whole subtree is equal.

Implemented pruning:

- equal root fast path
- equal child-CID pruning
- range-disjoint child span pruning
- ordered batch hydration of changed child frontiers
- streaming diff for large comparisons
- range-limited merge for partitioned keyspaces

This is the main reason content-addressing pays for itself in versioned applications.

### Large value offload

`put_large_value` stores large payload bytes in a `BlobStore` and keeps only a `ValueRef` in the tree. That keeps leaf nodes small and preserves fast metadata diff.

Use large value offload for:

- document bodies
- transcript chunks
- tool outputs
- generated artifacts
- binary attachments

### Async store concurrency

`AsyncStore` supports async point reads, ordered batch reads, and bounded default read parallelism. `AsyncProlly` uses those APIs for async reads, writes, ranges, diff, merge, stats, and batch mutation.

Async does not make CPU-bound work faster by itself. It helps when node reads wait on object stores, browser APIs, remote peers, or network caches.

See [Async store support](async-store.md) for the async design track.

## Workload guidance

Use this section as a tuning checklist before changing code.

### Append-heavy event logs

Best APIs:

- `append_batch`
- `Prolly::batch` with sorted append-only mutations
- `BatchBuilder` for initial log import

Key guidance:

- Put timestamp and sequence segments near the end of a stable run prefix
- Keep event values small and versioned
- Offload large tool outputs to blobs
- Persist named roots after meaningful checkpoints
- Use retention-aware GC after compaction

Expected behavior:

- closed left-side leaves should keep their CIDs
- fresh managers should use rightmost-path hints when the store supports hints
- appends should avoid full-tree traversal chains

### Random point updates

Best APIs:

- `put` for a few changes
- `batch` for many changes
- `get_many` when reading multiple known keys

Key guidance:

- Batch related mutations
- Keep chunk sizes large enough to avoid excessive tree height
- Bound caches for broad update workloads
- Use store batch reads when point updates touch many leaves

Expected behavior:

- each changed key rewrites its leaf and ancestors
- unrelated subtrees keep existing CIDs
- the number of touched leaves matters more than total tree size

### Range scans

Best APIs:

- `range`
- `range_page`
- `RangeCursor`

Key guidance:

- Design keys around scan prefixes
- Use `prefix_range`
- Use pages for API responses and background jobs
- Avoid scanning large values if metadata is enough

Expected behavior:

- range scans load only relevant leaf spans
- internal nodes route directly to matching child ranges
- page cursors let long scans resume without restarting

### Diff, sync, and derived indexes

Best APIs:

- `diff`
- `stream_diff`
- `range_diff`
- `plan_missing_nodes`
- `copy_missing_nodes`

Key guidance:

- Diff roots rather than decoded application objects
- Keep derived indexes in separate trees
- Apply source diffs to secondary indexes in a batch
- Use range-limited diff for tenants, documents, or partitions

Expected behavior:

- equal CIDs skip unchanged subtrees
- appended suffixes should avoid comparing every earlier key
- missing-node sync should copy only unknown CIDs

### Merge-heavy collaboration

Best APIs:

- `merge`
- `merge_range`
- `merge_explain`
- `stream_conflicts`
- `MergePolicyRegistry`

Key guidance:

- Route conflicts by key prefix
- Keep resolvers deterministic
- Return `Resolution::Unresolved` for policy-sensitive data
- Use `merge_explain` when debugging fallback or resolver behavior

Expected behavior:

- disjoint changes merge without value decoding
- conflicting keys call the resolver with delete-aware optional values
- resolved deletes fall back when rebalancing requires the batch path

### Durable embedded storage

Best APIs and features:

- `sqlite`
- `rocksdb`
- `slatedb`
- `pglite`
- named roots
- store-native GC

Key guidance:

- Use named roots for every durable head
- Use CAS for multi-writer root publication
- Use write-ahead log (WAL) mode and a non-zero busy timeout for SQLite
- Run GC from explicit retained roots
- Benchmark with realistic payload sizes and cache settings

Expected behavior:

- batch writes should use backend transactions when available
- ordered batch reads should reduce read amplification
- root manifests and hints should live outside content-node namespaces

### Remote or object storage

Best APIs:

- `AsyncStore`
- `AsyncProlly`
- `AsyncBlobStore`
- `plan_missing_nodes`
- `copy_missing_nodes`

Key guidance:

- Make node writes idempotent
- Use conditional writes for root manifests
- Override ordered batch reads when the backend has native multi-get
- Set bounded read parallelism when it only has point reads
- Cache hot internal nodes near the application

Expected behavior:

- concurrent child-node reads hide network latency
- content-addressed nodes can be uploaded before root publication
- GC, not ordinary mutation, should delete object-store nodes

## Configuration guide

Defaults are good for development. Production workloads should benchmark with data that resembles the application.

| Setting | Effect | Tune when |
| --- | --- | --- |
| `min_chunk_size` | Avoids tiny nodes | Trees have too many sparse nodes |
| `max_chunk_size` | Caps node size and rewrite size | Leaves become too large or tree height grows |
| `chunking_factor` | Controls average boundary frequency | You need fewer nodes or finer sharing |
| `hash_seed` | Makes boundaries deterministic | You need dataset-specific stable shape |
| `node_cache_max_nodes` | Bounds decoded node count | Services run for a long time |
| `node_cache_max_bytes` | Bounds approximate cache bytes | Values or nodes vary widely in size |
| `Encoding` | Records value encoding metadata | You need JSON, CBOR, raw, or custom markers |

Rules of thumb:

- Larger chunks reduce tree height and store calls, but each edit rewrites more bytes
- Smaller chunks improve fine-grained sharing, but increase metadata and traversal work
- Stable `hash_seed` is part of reproducible tree shape
- Cache limits should be explicit in long-running processes
- Large values should move to blob stores before tuning chunk sizes

## Store backend guidance

| Backend | Best fit | Performance notes |
| --- | --- | --- |
| `MemStore` | Tests, examples, benchmarks | Fastest local baseline, not durable |
| `FileNodeStore` | Object-layout local testing | Verifies node bytes against CIDs, useful for sync layout |
| `SqliteStore` | Desktop apps, local-first apps, agents | Use transactions, WAL, batch reads, and named roots |
| `RocksDBStore` | Embedded write-heavy services | Tune RocksDB separately for write buffers and compaction |
| `SlateDbStore` | Object-store style storage | Use async/object-store patterns and benchmark latency |
| `PgliteStore` | Postgres-like embedded experiments | Keep sidecar test gates explicit |
| Custom `Store` | Local sync backend | Implement ordered batch reads and batch writes |
| Custom `AsyncStore` | Remote, browser, object store | Implement native multi-get or bounded read parallelism |

Conformance tests should cover both correctness and performance hooks:

- absent reads
- present reads
- overwrite
- delete
- duplicate batch reads
- ordered batch reads
- batch writes
- hints
- node scans
- manifests
- async parity if applicable

## Benchmark harnesses

Run benchmarks from the workspace root.

### Core operation benchmark

`prolly_bench` covers core operations:

- incremental inserts
- bulk build
- point gets
- point updates
- deletes
- range scans
- batch mutations
- append batches
- diff and stream diff
- range diff
- merge
- SQLite persistence when `sqlite` is enabled

Command:

```sh
PROLLY_BENCH_SCALE=5000 \
cargo bench -p prolly-map --bench prolly_bench --features sqlite
```

### AI and local-first workload benchmark

`ai_workloads_bench` covers application-shaped workloads:

- conversation event appends
- document chunk ingestion
- metadata updates across many prefixes
- agent memory branch and merge
- sync-style missing-node exchange

Command:

```sh
PROLLY_AI_BENCH_SCALE=10000 \
PROLLY_AI_BENCH_BATCH=256 \
cargo bench -p prolly-map --bench ai_workloads_bench
```

### Store diff and merge benchmark

`store_diff_merge_bench` compares stores on branch, diff, and merge workloads.

Command:

```sh
PROLLY_DIFF_MERGE_STAGES=10000,100000 \
PROLLY_DIFF_MERGE_CHANGES=1000 \
cargo bench -p prolly-map --bench store_diff_merge_bench --features sqlite
```

Enable optional stores with their feature flags:

```sh
cargo bench -p prolly-map --bench store_diff_merge_bench \
  --features "sqlite pglite slatedb"
```

### SQLite scale benchmark

`sqlite_scale_bench` appends staged record counts to a SQLite store and verifies reopen reads.

Command:

```sh
PROLLY_SQLITE_SCALE_STAGES=1000000,10000000,100000000 \
PROLLY_SQLITE_SCALE_BATCH=100000 \
PROLLY_SQLITE_SCALE_MAX_SECONDS=900 \
PROLLY_SQLITE_SCALE_MAX_DB_GB=70 \
cargo bench -p prolly-map --bench sqlite_scale_bench --features sqlite
```

### Optional backend scale benchmarks

The crate also includes:

- `pglite_scale_bench`
- `slatedb_scale_bench`
- `slatedb_ops_bench`
- `slatedb_workload_bench`

Run them only when their external dependencies and feature flags are configured.

### SlateDB production workload benchmark

`slatedb_workload_bench` is the recommended harness for pushing a SlateDB-backed
prolly map toward production-scale object-store workloads. It runs staged ingest,
hot and random reads, range scans, sparse updates, deletes, suffix appends,
streaming diff, disjoint three-way merge, named-root publication, object-store
size sampling, and final reopen verification.

Create the local RustFS bucket used by the default workload profile:

```sh
AWS_ACCESS_KEY_ID=crab \
AWS_SECRET_ACCESS_KEY=crab \
AWS_DEFAULT_REGION=us-east-1 \
aws --endpoint-url http://localhost:9000 \
  s3api create-bucket --bucket prolly
```

Small local verification:

```sh
PROLLY_SLATEDB_BUCKET=prolly \
PROLLY_SLATEDB_ACCESS_KEY_ID=crab \
PROLLY_SLATEDB_SECRET_ACCESS_KEY=crab \
PROLLY_SLATEDB_WORKLOAD_STAGES=10k \
PROLLY_SLATEDB_WORKLOAD_BATCH=5k \
PROLLY_SLATEDB_WORKLOAD_OPS=500 \
cargo bench -p prolly-map --bench slatedb_workload_bench --features slatedb
```

Large staged run:

```sh
PROLLY_SLATEDB_BUCKET=prolly \
PROLLY_SLATEDB_ACCESS_KEY_ID=crab \
PROLLY_SLATEDB_SECRET_ACCESS_KEY=crab \
PROLLY_SLATEDB_WORKLOAD_STAGES=10M,1B,10B \
PROLLY_SLATEDB_WORKLOAD_BATCH=100k \
PROLLY_SLATEDB_WORKLOAD_OPS=100k \
PROLLY_SLATEDB_WORKLOAD_CYCLES=1 \
PROLLY_SLATEDB_WORKLOAD_VALUE_BYTES=128 \
PROLLY_SLATEDB_WORKLOAD_MAX_SECONDS=86400 \
PROLLY_SLATEDB_WORKLOAD_MAX_OBJECT_GB=20000 \
PROLLY_SLATEDB_WORKLOAD_STATS_MAX_RECORDS=1M \
PROLLY_SLATEDB_FLUSH_AFTER_WRITE=false \
PROLLY_SLATEDB_FLUSH_INTERVAL_MS=1000 \
PROLLY_SLATEDB_L0_SST_MB=256 \
PROLLY_SLATEDB_MAX_UNFLUSHED_MB=4096 \
PROLLY_SLATEDB_L0_MAX_SSTS=256 \
PROLLY_SLATEDB_L0_MAX_SSTS_PER_KEY=32 \
PROLLY_SLATEDB_L0_FLUSH_PARALLELISM=16 \
PROLLY_SLATEDB_READ_PARALLELISM=512 \
PROLLY_SLATEDB_COMPACTION_CONCURRENCY=16 \
PROLLY_SLATEDB_COMPACTION_SUBCOMPACTIONS=8 \
cargo bench -p prolly-map --bench slatedb_workload_bench --features slatedb
```

Important controls:

- `PROLLY_SLATEDB_WORKLOAD_STAGES` accepts suffixes such as `10M`, `1B`, and
  `10B`.
- `PROLLY_SLATEDB_WORKLOAD_SOAK_SECONDS` keeps running mixed operation cycles
  after the initial stages finish. Use it for long-duration load tests.
- `PROLLY_SLATEDB_WORKLOAD_MAX_SOAK_CYCLES` is an additional safety cap for
  soak runs.
- `PROLLY_SLATEDB_WORKLOAD_OBJECT_SAMPLE_CYCLES` controls how often the harness
  lists the object-store prefix to refresh footprint metrics.
- `PROLLY_SLATEDB_WORKLOAD_MAX_SECONDS` and
  `PROLLY_SLATEDB_WORKLOAD_MAX_OBJECT_GB` are safety stops.
- `PROLLY_SLATEDB_WORKLOAD_KEEP_DB=true` preserves the object-store prefix for
  later inspection.
- `PROLLY_SLATEDB_FLUSH_AFTER_WRITE=false` measures sustained ingest through
  SlateDB's WAL and flush interval. Set it to `true` when measuring
  synchronous durability after every prolly batch.
- Increase `PROLLY_SLATEDB_WORKLOAD_STATS_MAX_RECORDS` only when a full
  tree-stats traversal is acceptable at that scale.

For serious production use, treat this as a capacity harness rather than a
guarantee. Size the object store and network for the configured value size and
write amplification, keep durable heads in named roots, isolate independent
writer shards into separate SlateDB paths, keep large payloads in blob storage
instead of leaf values, and tune L0/compaction settings until read amplification
and write backpressure stay flat through the largest stage.

### 100M initial version plus soak test

Use this profile when you want to build a 100M-record initial version, publish
it as `main`, then keep simulating production-like changes over time:

```sh
PROLLY_SLATEDB_BUCKET=prolly \
PROLLY_SLATEDB_ACCESS_KEY_ID=crab \
PROLLY_SLATEDB_SECRET_ACCESS_KEY=crab \
PROLLY_SLATEDB_WORKLOAD_STAGES=100M \
PROLLY_SLATEDB_WORKLOAD_BATCH=100k \
PROLLY_SLATEDB_WORKLOAD_OPS=100k \
PROLLY_SLATEDB_WORKLOAD_CYCLES=0 \
PROLLY_SLATEDB_WORKLOAD_SOAK_SECONDS=21600 \
PROLLY_SLATEDB_WORKLOAD_MAX_SOAK_CYCLES=10000 \
PROLLY_SLATEDB_WORKLOAD_OBJECT_SAMPLE_CYCLES=10 \
PROLLY_SLATEDB_WORKLOAD_VALUE_BYTES=128 \
PROLLY_SLATEDB_WORKLOAD_MAX_SECONDS=25200 \
PROLLY_SLATEDB_WORKLOAD_MAX_OBJECT_GB=200 \
PROLLY_SLATEDB_WORKLOAD_STATS_MAX_RECORDS=1M \
PROLLY_SLATEDB_WORKLOAD_KEEP_DB=true \
PROLLY_SLATEDB_FLUSH_AFTER_WRITE=false \
PROLLY_SLATEDB_FLUSH_INTERVAL_MS=1000 \
PROLLY_SLATEDB_L0_SST_MB=256 \
PROLLY_SLATEDB_MAX_UNFLUSHED_MB=4096 \
PROLLY_SLATEDB_L0_MAX_SSTS=256 \
PROLLY_SLATEDB_L0_MAX_SSTS_PER_KEY=32 \
PROLLY_SLATEDB_L0_FLUSH_PARALLELISM=16 \
PROLLY_SLATEDB_READ_PARALLELISM=512 \
PROLLY_SLATEDB_COMPACTION_CONCURRENCY=16 \
PROLLY_SLATEDB_COMPACTION_SUBCOMPACTIONS=8 \
cargo bench -p prolly-map --bench slatedb_workload_bench --features slatedb
```

This command requires real capacity. With 128-byte values, the append-only 1M
local measurement was roughly 268 bytes per record in object storage before
long-lived history, so 100M needs about 27 GB before soak-cycle write
amplification. Mixed updates and retained roots can raise this materially; use
the object GB cap as a guardrail and keep the database prefix for post-run
inspection only when the backing store has enough space.

The generated records use deterministic pseudo-random payload bytes with
ordered primary keys so the initial import represents a sorted bulk load. The
over-time workload chooses random records for reads, updates, deletes, diffs,
and merges. For truly random primary keys, sort the source records before the
initial build or run a separate random-insert profile; otherwise the benchmark
will measure random insert amplification rather than bulk-load capacity.

## Reading benchmark output

Most benchmark rows include:

- operation name
- record count
- change count
- total time
- items per second or nanoseconds per item
- verification flag
- backend-specific size or status fields

Trust rows where `verified` is true. A fast row without verification is only a timing probe.

Compare results by workload, not only by raw throughput. A point-get benchmark, append benchmark, sparse update benchmark, and full rebuild benchmark answer different questions.

## Current evidence

The following figures are local measurements from the existing benchmark notes. Treat them as directionally useful, not universal claims. Re-run the harnesses on your target hardware and backend before setting production limits.

### SQLite disk results at scale 5000

| Bench | ns/item | Approx items/sec |
| --- | ---: | ---: |
| `sqlite_disk_batch_builder_persist` | 1146 | 872k |
| `sqlite_disk_batch_mutations` | 9686 | 103k |
| `sqlite_disk_append_batch_persist` | 1677 | 596k |
| `sqlite_disk_append_batch_chain_persist` | 3727 | 268k |
| `sqlite_disk_append_batch_chain_new_manager_persist` | 4493 | 223k |
| `sqlite_disk_diff_empty_to_full` | 342 | 2.92M |

### SQLite staged append results

| Target records | Total records | Total time ms | DB bytes | Bytes/record | Stage records/sec | Verified |
| ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| 1,000,000 | 1,000,000 | 1554 | 135,456,400 | 135.46 | 643k | yes |
| 10,000,000 | 10,000,000 | 22,405 | 1,195,430,128 | 119.54 | 432k | yes |
| 100,000,000 | 100,000,000 | 606,434 | 11,721,597,960 | 117.22 | 154k | yes |

At the measured 100M footprint, a 1B-record SQLite database would require roughly 117 GB before write-ahead log, filesystem, and safety headroom.

## Performance playbooks

### Import a dataset

1. Sort records by key if the source can do it cheaply
2. Use `SortedBatchBuilder` for sorted input
3. Use `BatchBuilder` for unsorted input
4. Publish the resulting root by name
5. Record `collect_stats` output with the import job

### Maintain a secondary index

1. Keep source records in one tree
2. Keep the index in another tree
3. Diff source roots
4. Translate source diffs into index mutations
5. Apply mutations with `batch`
6. Store source and index roots in a manifest value

### Sync a remote peer

1. Compare root CIDs
2. Plan missing CIDs against the destination store
3. Copy missing nodes
4. Verify copied bytes against CIDs
5. Publish the received root after all nodes exist

### Serve RAG metadata

1. Store chunk metadata in the tree
2. Store chunk text in blobs when it is large
3. Store vectors in a sidecar vector engine
4. Save the prolly root in each answer record
5. Replay old answers against the saved root

### Run in an async application

1. Use `AsyncStore` for remote or browser storage
2. Use `TokioBlockingStore` only when wrapping blocking sync stores in Tokio
3. Override ordered batch reads for native multi-get
4. Limit read parallelism for remote stores
5. Keep root publication conditional

## Known limits

Prolly trees are not the fastest choice for every problem.

Use another structure when:

- you only need an append-only log with no diff, merge, or sync
- you never need immutable snapshots
- you need analytical scans over every row for every query
- values are huge and cannot be separated into blobs
- storage format stability matters more than pre-`1.0` API cleanup

Current technical limits:

- node encoding compatibility still needs fixture files before language ports can claim structural compatibility
- remote object-store backends are design patterns, not first-party implementations yet
- benchmark gates are local scripts rather than a published performance dashboard
- some root-level docs still describe broader CrabDB performance rather than `prolly-map` alone

## Future enhancements

This section tracks likely performance work after the current hardening.

### P0: release confidence

- Add a reproducible benchmark smoke suite to CI
- Add a `cargo bench` result parser for threshold checks
- Add fixture-backed performance tests for append, diff, and merge fast paths
- Add docs for interpreting `collect_stats`, `stats_diff`, and manager metrics
- Add compatibility fixtures before making format-level performance promises

### P1: store and cache improvements

- Add shared cache options for multiple managers using the same store
- Add request coalescing for repeated CID reads
- Add cache admission policies for broad scans
- Add prepared-statement reuse experiments for long SQLite read batches
- Add backend-specific tuning docs for SQLite, RocksDB, SlateDB, and PGlite
- Add store-size and reclaimable-byte reports to inspection tooling
- Add async prefetch strategies for wide diff and range traversals

### P1: mutation and merge improvements

- Add a direct subtree-splice builder for large middle-range insertions where neighboring boundaries are known
- Add more direct structural delete paths when rebalancing is provably unnecessary
- Add range-partitioned batch planning for large mutation sets
- Add benchmark cases for large delete ranges
- Add merge benchmarks for long-lived divergent branches
- Add policy-registry benchmark coverage for many key-family rules

### P1: remote and object-store improvements

- Add first-party object-store node backend
- Add async manifest store with conditional root writes
- Add bounded background prefetch for hot internal nodes
- Add retry and backoff guidance for remote stores
- Add sync progress metrics
- Add object-store GC dry-run reports
- Add remote blob store examples

### P2: encoding and layout research

- Evaluate prefix compression inside nodes
- Evaluate optional node compression for durable stores
- Evaluate zero-copy node decode views for read-heavy workloads
- Evaluate adaptive chunking parameters by workload
- Evaluate Bloom-filter or summary sidecars for negative lookups
- Evaluate content-defined chunking variants from current prolly-tree research

Any encoding or layout change must preserve a migration path. Once cross-language fixtures exist, performance work that changes node bytes must update those fixtures intentionally.

## Further reading

- [Async store support](async-store.md)
- [Architecture](architecture.md)
- [Design spec](design-spec.md)
- [Implementation notes](implementation.md)
- [Cookbook](cookbook.md)
- [CrabDB performance and scale benchmarks](../../../docs/guides/performance-and-scale-benchmarks.md)
- [Accelerating Prolly Trees: Simplified Chunking for Rapid Updates](https://ceur-ws.org/Vol-3791/paper8.pdf)
