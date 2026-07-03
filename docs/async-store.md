# Async store support

This document expands the async-store roadmap item into a concrete design track.
The goal is to support remote, browser, object-store, and background-agent
workloads without forcing async dependencies or runtime assumptions onto the
current embedded `Store` API.

## Why Async Store Support Matters

The current `Store` trait is synchronous, which is a good fit for in-process
and embedded backends such as memory, SQLite, and RocksDB. AI-native and
local-first applications also need storage patterns where blocking APIs become
awkward or inefficient:

- S3, R2, GCS, and other object stores.
- Remote peer sync over HTTP, WebSocket, or custom transports.
- Browser and WASM storage APIs.
- Network caches and edge storage.
- Background agents that should overlap many node reads.
- Long-running diff, sync, and indexing tasks that need backpressure.

Prolly trees are especially well suited to these environments because immutable
content-addressed nodes can be fetched, cached, exchanged, and verified
independently.

## Design Principles

- Keep the sync `Store` trait stable and useful.
- Put async support behind an optional feature such as `async-store`.
- Avoid a hard dependency on Tokio in the core async API.
- Preserve ordered batch-read semantics; many tree algorithms depend on result
  order matching request order.
- Preserve correctness if stores ignore hints, batching, or prefetching.
- Make concurrency explicit and bounded.
- Support object stores where immutable node writes and mutable root updates
  have different consistency models.
- Leave room for browser/WASM stores that may not be `Send`.

## Sync vs Async Tradeoffs

Async storage does not make CPU-bound tree work faster by itself. It helps when
the workload spends meaningful time waiting on storage, network, browser APIs,
or remote object stores. In those cases, async lets the tree overlap multiple
node reads and keep the application runtime responsive.

Sync `Store` benefits:

- Smallest dependency surface and easiest API for embedded use.
- Excellent fit for in-process memory, SQLite, and RocksDB.
- Simpler error paths and easier debugging.
- Lower overhead for CPU-bound or already-local operations.
- Works well in CLI tools, benchmarks, and synchronous host applications.

Sync `Store` costs:

- Blocking calls can stall async runtime worker threads if used directly.
- Harder to overlap many remote node reads without application-level threading.
- Awkward for browser/WASM, HTTP, WebSocket, and object-store APIs.

Async `Store` benefits:

- Natural fit for S3/R2/GCS, remote sync, network caches, and browser storage.
- Can overlap independent node fetches with bounded concurrency.
- Keeps async applications responsive during storage waits.
- Enables background agents and sync tasks to share a runtime cleanly.

Async `Store` costs:

- More API surface and more complex test matrix.
- Slight overhead for futures and scheduling.
- Runtime-specific adapters must be handled carefully.
- Blocking embedded stores still need an adapter such as `TokioBlockingStore`
  when used from async applications.

Default guidance: keep Tokio optional. The core async trait should stay
runtime-neutral, while the `tokio` feature provides the convenient
`TokioBlockingStore<S>` adapter for applications that already use Tokio.

## Proposed API Shape

The async API should be parallel to `Store`, not a replacement for it.

```rust
pub trait AsyncStore {
    type Error: std::error::Error + 'static;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error>;

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error>;

    async fn batch_get_ordered(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error>;

    async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error>;

    fn prefers_batch_reads(&self) -> bool {
        false
    }

    fn read_parallelism(&self) -> usize {
        1
    }

    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error>;

    async fn get_hint(
        &self,
        namespace: &[u8],
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Self::Error>;

    async fn put_hint(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error>;

    async fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error>;
}
```

The exact Rust representation needs a small prototype before stabilization.
Options:

- Use `async fn` in the public trait for the cleanest source API.
- Use associated future types if the crate needs more control over `Send`
  bounds and allocations.
- Add a boxed dynamic adapter later if applications need
  `Arc<dyn DynAsyncStore<...>>`.

The first implementation uses `async fn` in the public trait and keeps the base
trait free of `Send`/`Sync` bounds so browser and WASM stores can implement it.
Native managers or backends can add stronger bounds when they need cross-thread
execution.

## Async Prolly Manager

Add a separate manager instead of making `Prolly<S>` dual-mode:

```rust
pub struct AsyncProlly<S> {
    store: S,
    config: Config,
    // async-aware cache and hint state
}
```

Current methods:

- `create`
- `get`
- `get_many`
- `range`
- `range_after`
- `range_from_cursor`
- `range_page`
- `diff`
- `range_diff`
- `stream_diff`
- `structural_diff_page`
- `merge`
- `crdt_merge`
- `put`
- `delete`
- route-planned coalesced `batch`
- `collect_stats`
- `stats_diff`
- `cache_len`
- `cache_bytes_len`
- `cache_pinned_len`
- `cache_pinned_bytes_len`
- `clear_cache`
- `pin_tree_root`
- `pin_tree_path`
- `unpin_all_cache_nodes`

The implementation should share as much non-I/O logic as possible with the
sync engine. Any pure operations such as node search, mutation preprocessing,
conflict resolution, boundary detection, and serialization should remain
runtime-agnostic.

## Adapters

Adapters make adoption less abrupt.

### Sync Store as Async Store

Wrap an existing `Store` and expose it as `AsyncStore`.

Use cases:

- tests;
- migration;
- apps that already run inside async tasks but use SQLite or memory stores;
- validating that async algorithms match sync algorithms.

This adapter should not spawn blocking work by default. A runtime-specific
adapter can be added later for `spawn_blocking` behavior.

### Tokio Blocking Store Adapter

The optional `tokio` feature provides `TokioBlockingStore<S>`, which adapts a
blocking `Store` to `AsyncStore` by running each sync store call through
`tokio::task::spawn_blocking`.

Use this when an async application wants to use an embedded blocking backend
such as SQLite, RocksDB, or an in-process test store without blocking runtime
worker threads.

The default crate remains runtime-free; Tokio is opt-in.

### Async Store as Sync Store

This is less desirable and should be optional, if it exists at all. Blocking on
async storage can deadlock or accidentally tie the crate to a runtime. Prefer
documenting this as an application-level adapter rather than a core feature.

### Arc Support

Mirror the sync implementation:

```rust
impl<T: AsyncStore> AsyncStore for Arc<T> { ... }
```

## Batch Reads and Concurrency

The current sync `Store` has `batch_get_ordered`, `batch_get_ordered_unique`,
and `prefers_batch_reads`. Async support should preserve those semantics and
add explicit concurrency limits.

Important behaviors:

- deduplicate repeated CIDs before fetch;
- preserve caller order after fetch;
- cap in-flight requests;
- allow stores to override with native multi-get;
- expose read parallelism as a store preference;
- avoid unbounded fanout during broad diff or range scans.

Potential API:

```rust
pub struct AsyncReadConfig {
    pub max_in_flight_reads: usize,
    pub prefetch_child_frontiers: bool,
}
```

Default behavior should be conservative. Object stores and remote sync stores
can opt into higher concurrency.

## Object Store Backend Pattern

Object stores fit prolly nodes well because nodes are immutable and addressed by
CID.

Recommended layout:

```text
nodes/sha256/ab/cd/<cid-bytes>
hints/<namespace>/<key>
manifests/<name>
blobs/sha256/ab/cd/<blob-cid>
```

Design notes:

- Node writes are idempotent.
- Reads can verify CID by hashing bytes after fetch.
- Node writes can be massively parallel.
- Mutable root updates belong in a manifest layer, not in node storage.
- Garbage collection should mark from manifest roots before deleting objects.
- Hints are optional and can be stale.

Open question: whether the first object-store backend should depend on the
`object_store` crate or provide a smaller trait for application-owned clients.

## Async Blob Store Pattern

Large-value blobs use a separate async abstraction from nodes. `AsyncBlobStore`
mirrors `BlobStore` for content-addressed payload bytes, while keeping the
runtime-neutral `async-store` feature:

- native object stores can implement async point reads/writes directly;
- `get_blobs_ordered` deduplicates repeated `BlobRef`s, preserves caller order,
  and can overlap point reads using `read_parallelism`;
- `SyncBlobStoreAsAsync` adapts embedded blob stores without spawning;
- `TokioBlockingBlobStore` is available behind the `tokio` feature for
  blocking blob backends inside Tokio applications;
- `AsyncProlly` resolves and writes large values through async blob stores and
  can run blob mark/plan/sweep against async blob candidates.

Blob GC remains candidate-driven for the same reason node GC does: object-store
listing, application blob indexes, and shared blob namespaces have different
ownership rules.

## Browser and WASM Storage

Browser storage needs slightly different constraints:

- APIs may be async but not `Send`.
- IndexedDB transactions have their own lifetime rules.
- storage quotas and eviction are real concerns.
- large values should probably be offloaded or chunked.

The async design should avoid requiring Tokio or `Send` futures at the lowest
trait layer. If native multi-threaded async needs `Send`, expose that as an
additional bound on the manager or helper methods rather than on every store.

## Remote Sync Use Cases

Async store support should pair naturally with sync primitives:

1. Compare root CIDs.
2. Traverse changed subtrees.
3. Request missing node CIDs concurrently.
4. Verify fetched nodes by CID.
5. Write missing nodes locally.
6. Merge roots through standard or CRDT policies.
7. Update a manifest root with compare-and-swap where supported.

This gives AI applications an efficient way to sync agent memory, document
indexes, conversation history, and derived metadata.

## Implementation Phases

### Phase 1: Trait and Test Harness

- [x] Add an `async-store` feature.
- [x] Add `AsyncStore` trait.
- [x] Add async equivalents for ordered batch-read planning.
- [x] Add `Arc<T>` support.
- [x] Add a sync-store-to-async adapter.
- [x] Add optional Tokio `spawn_blocking` adapter for blocking stores.
- [x] Add initial conformance tests that compare async behavior with `Store`.
- [x] Add `AsyncBlobStore` for native async large-value/object-store backends.
- [x] Add `SyncBlobStoreAsAsync` and optional `TokioBlockingBlobStore` adapters.
- [x] Add async large-value and blob-GC helpers on `AsyncProlly`.

### Phase 2: Async Reads

- [x] Add `AsyncProlly<S>`.
- [x] Implement `create`, `get`, and `get_many`.
- [x] Implement range scan with `next().await` and `into_stream()`.
- [x] Implement resumable async range cursors and bounded range pages.
- [x] Implement `collect_stats` and `stats_diff`.
- [x] Add cache metrics for async reads.

### Phase 3: Async Writes

- [x] Implement `put` and `delete`.
- [x] Implement initial sequential async batch mutation application.
- [x] Add async batch write collector.
- [x] Replace sequential async batch with a single-flush full-rebuild batch.
- [x] Port route-planned coalesced async batch mutation for large trees.
- [x] Preserve one-call batch write semantics where the backend supports it.
- [x] Add append-heavy rightmost-path support with async hints.

### Phase 4: Async Diff and Merge

- [x] Implement async eager structural diff.
- [x] Implement async streaming diff.
- [x] Implement async range diff.
- [x] Implement async three-way merge with the existing resolver model.
- [x] Implement async CRDT merge.
- [x] Implement async node reachability and missing-node copy helpers.
- [x] Add bounded child-frontier prefetch for broad async traversals.

### Phase 5: Backends

- [x] Add generic store-to-store missing-node sync primitives.
- [x] Add local object-layout node backend pattern (`FileNodeStore`).
- [ ] Add object-store backend prototype.
- [ ] Add HTTP or remote peer store prototype.
- [ ] Add browser/WASM storage prototype or example.
- [ ] Document runtime-specific adapters for Tokio and WASM.

## Acceptance Criteria

- Existing sync APIs continue to compile unchanged.
- Async support is absent unless the feature is enabled.
- Async tests verify behavior parity with sync `MemStore`.
- Ordered batch reads preserve duplicate positions and result ordering.
- Broad diffs and range scans respect bounded concurrency.
- Object-store node reads verify content by CID.
- Hints remain optional and never affect correctness.
- The async API does not require Tokio unless a Tokio-specific backend is used.

## Risks and Open Questions

- Avoiding algorithm duplication between `Prolly` and `AsyncProlly`.
- Choosing between `async fn` in traits, associated future types, or boxed
  futures.
- Supporting non-`Send` browser futures without weakening native concurrency.
- Defining root manifest consistency for eventually consistent object stores.
- Avoiding accidental unbounded task creation during diff and merge.
- Deciding whether async support should live in this crate or a companion crate.
