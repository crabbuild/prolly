# Async-first Prolly engine design

**Date:** 2026-07-18

**Status:** Implemented and locally verified

**Audience:** Prolly maintainers, storage-adapter authors, binding maintainers, and performance reviewers

**Goal:** Make async the canonical storage execution model while preserving canonical roots, public behavior, and native sync performance

## TL;DR

Prolly will have one internal async-first tree engine named `ProllyEngine<S: AsyncStore>`. `AsyncProlly<S>` will call it directly. `Prolly<S: Store>` will use the same engine through a ready `SyncStoreAsAsync<S>` adapter, without Tokio, threads, or a blocking executor.

`ProllyEngine` will own Prolly-tree algorithms and validated node I/O. It will not become a god object. Transactions, named snapshots, versioned maps, content graphs, blob coordination, secondary indexes, remote synchronization, and proximity will each have one async-first service implementation that composes the engine or the relevant async capability traits. Their sync APIs will be ready-adapter facades over the same service logic.

Correctness is release-blocking. Every loaded node will be checked against its requested CID and tree format. Every sync and async mutation must produce the same root as a clean canonical rebuild. Concurrency, caches, hints, speculation, cancellation, and backend preferences may change cost only, never results. The migration will not start until the public API inventory and pre-migration correctness and performance baselines are frozen.

## Context

Prolly currently has two storage-facing managers: `Prolly<S: Store>` and `AsyncProlly<S: AsyncStore>`. Large parts of their behavior are implemented independently. The synchronous writer in `src/prolly/write.rs` is the authoritative canonical mutation implementation, while most non-append async mutations collect the complete logical tree into a `BTreeMap` and rebuild it.

That async fallback preserved canonical roots when configurable formats landed, but it makes every non-append point mutation O(N) in tree entries. Repeated random or clustered updates become O(N times mutations). Fresh local measurements at 50,000 records showed Turso async random and clustered point updates around 140 to 169 times slower than SQLite sync. Partial 500,000-record measurements showed roughly 727 to 831 times higher total latency. Append was only about 2.7 to 4.6 times slower. This shape identifies the duplicated async mutation algorithm, not local Turso configuration, as the dominant cause.

Duplication is also a correctness risk. A change to chunking, hard byte caps, CID resynchronization, node validation, caching, diff ordering, conflict behavior, or failure handling can land in one manager without reaching the other. Differential tests detect known divergence but do not remove its source.

Async is the more general storage execution model. Native async stores can suspend and overlap I/O. A synchronous store can be represented by async methods whose futures complete immediately. Prolly will invert the current relationship: async orchestration is canonical, sync is a ready calling convention over it, and pure deterministic work remains synchronous Rust.

## Decision

Introduce an internal `ProllyEngine<S: AsyncStore>` as the only implementation of storage-backed Prolly-tree algorithms. It owns validated node access, tree caches, operation metrics, building, mutation, reads, cursors, diff, merge, proofs, statistics, and tree reachability.

Introduce one async-first internal service per higher-level storage domain. Services compose `ProllyEngine`, an async overlay, or separate async capability traits. They retain their own state, lifetimes, atomicity, and authorization rules. Public sync services use ready adapters and the same service implementation.

`AsyncProlly<S>` forwards to `ProllyEngine<S>`. `Prolly<S: Store>` owns `ProllyEngine<SyncStoreAsAsync<S>>`. The sync facade evaluates a complete engine operation with a ready-only inline runner. It never creates a runtime, parks an operating-system thread, dispatches to a worker, or calls Tokio.

Pure work stays synchronous. Encoding, decoding, validation, boundary evaluation, mutation normalization, canonical emission, cursor transitions, proof verification, and conflict resolution become ordinary reusable state machines or functions. Await points exist only at storage or explicitly asynchronous coordination boundaries.

The migration is vertical. Both public calling conventions switch to the same implementation for an API family, then the superseded production bodies are removed before that slice is complete. A test-only oracle may remain temporarily for differential checks, but no release contains two selectable canonical production algorithms.

## Goals

- Make async the canonical execution design, not an optional parallel implementation.
- Preserve source compatibility and observable behavior for existing public sync and async APIs unless an exception is inventoried and approved.
- Guarantee byte-identical canonical roots for every supported format and mutation history.
- Make sync runtime-free, reentrant, WASM-safe, and measurably close to the current native sync path.
- Let native async stores use ordered batches and bounded concurrent point reads without runtime-specific code.
- Support single-threaded, non-`Send` browser and WASM stores.
- Keep one storage-backed algorithm per domain.
- Keep caches, hints, concurrency, and speculation outside the correctness model.
- Replace full logical-map mutation rebuilds with localized canonical replay and a streaming fallback.
- Make existing-key async point updates proportional to tree height rather than entry count.
- Bound in-flight I/O and streaming state, and make eager memory costs explicit.
- Validate native SQLite sync and native Turso async locally through 2 million records.

## Non-goals

- Changing canonical encodings, chunking rules, CIDs, tree formats, or logical data semantics.
- Requiring Tokio, an ambient executor, worker threads, timers, or a reactor in the core.
- Turning pure CPU helpers into futures.
- Guaranteeing executor fairness during a pure CPU phase. Automatic yielding or offload needs an explicit scheduler policy and is not hidden in this migration.
- Automatically offloading synchronous stores. `SyncStoreAsAsync` intentionally calls the original store on the caller thread.
- Putting every storage domain inside `ProllyEngine`.
- Retaining two production canonical algorithms behind a long-lived feature.
- Making experimental Turso MVCC a production recommendation.
- Promising that generic async operations are `Send` solely because `S: AsyncStore`.
- Promising rollback when an async future is dropped. Cancellation and transactional atomicity are separate contracts.

## Required invariants

| Invariant | Required behavior |
| --- | --- |
| Canonical identity | Equal base root, `Config.format`, and normalized mutations produce the same root and persisted node bytes through every facade and execution policy. Runtime settings cannot affect identity. |
| Content integrity | Raw bytes are hashed and decoded against the requested CID and active tree format before use or caching. |
| One algorithm per domain | Sync and async calling conventions drive one implementation. Facades contain no alternate routing, traversal, or persistence policy. |
| Immutable base | A failed, cancelled, or panicking operation cannot mutate an existing tree. |
| Visibility | A new root is returned or named only after every node it references is available. |
| Atomic mutable state | Root compare-and-swap and strict transactions use their capability contract. Immutable node publication alone is not a commit. |
| Deterministic errors | Concurrent completion order cannot change the first logical error or error category. |
| Performance-only state | While the storage contract holds, cache contents, hints, speculation, and read scheduling cannot change values, roots, proofs, conflicts, or success. |
| Bounded orchestration | In-flight reads, batch widths, frontiers, streaming buffers, and speculative state respect explicit engine limits. |
| Ready sync | Any engine operation over a sealed ready adapter completes without an external wake. A public sync call enters `run_ready` at most once and never once per store call. |
| Suspension safety | No lock guard, mutable engine borrow, or borrowed callback value crosses `.await`. |
| Reentrancy | Synchronous resolvers and visitors may reenter the same manager unless the existing API explicitly forbids it. |

## Layered architecture

```text
Public API
  AsyncProlly and async domain types       Prolly and sync domain types
                  |                                      |
                  |                              ready adapters + run_ready
                  +----------------------+---------------+
                                         |
                         Async-first domain services
             roots, transactions, versions, blobs, GC, copy,
              content graph, secondary index, proximity, remote
                                         |
                              ProllyEngine<S>
                 canonical tree algorithms + validated node I/O
                                         |
                  AsyncStore and async capability traits
                                         |
                       native async or ready sync adapters
```

### Ownership boundaries

| Layer | Owns | Does not own |
| --- | --- | --- |
| Pure components | Codecs, boundary state, emitters, mutation normalization, cursor transitions, proof verification, distance math | Storage access, caches, publication, runtime policy |
| `ProllyEngine<S>` | Validated Prolly-node I/O, per-manager tree cache, tree build and mutation, reads, ranges, diff, merge, proof construction, statistics, debug traversal, tree reachability | Named-root policy, transaction lifetime, store-wide deletion authority, blobs, content-graph kinds, secondary-index coordination, proximity graph policy |
| Domain services | One async-first implementation for named roots and snapshots, transactions, versioned maps, GC sweep, copy and remote sync, blobs and large values, content graphs, secondary indexes, and proximity | Alternate Prolly-tree algorithms |
| Facades | Public names, constructors, lifetime and return-shape adaptation, `Future` or stream to sync result or iterator adaptation | Tree routing, validation, encoding, fallback, store-call selection |
| Adapters | Backend capability implementation, connection and transaction scope, ordered batch semantics, cancellation contract | Canonical tree decisions |

This boundary replaces the earlier proposal that made `ProllyEngine` the sole owner of all storage domains. That earlier scope would have coupled unrelated capability traits and made transaction, blob, content-graph, and proximity lifetimes harder to reason about.

### Engine shape

The conceptual engine is:

```rust,ignore
pub(crate) struct ProllyEngine<S: AsyncStore> {
    store: S,
    config: Config,
    limits: EngineLimits,
    node_cache: RwLock<NodeCache>,
    route_cache: RwLock<RouteCache>,
    metrics: ProllyMetrics,
}
```

`Config.format` remains canonical tree configuration. The existing `Config.runtime` fields continue to seed cache and read settings for source compatibility, even though `Config` is also carried in manifests today. New caps are not added as public fields to `RuntimeConfig`, because that would break downstream struct literals and alter serialized manifests. Internal `EngineLimits` is derived at manager construction and is never encoded into a tree or manifest.

If callers need to set the new caps, add an additive, non-exhaustive `ExecutionConfig` with private fields, accessors, and a builder, plus `new_with_execution_config` constructors. Existing `new(store, config)` behavior remains valid and maps legacy runtime fields exactly. `ExecutionConfig` values override only non-canonical execution policy. The API inventory must approve its final public names before implementation.

The concrete engine fields may be split into internal components, but a public manager has one tree cache and one metric source.

Engine operations retain the current `S::Error: Send + Sync` bound needed by `Error::Store(Box<dyn Error + Send + Sync>)`. Neither `AsyncStore` nor its futures receive a blanket `Send` or `Sync` bound. Supporting non-`Send` store error objects requires a separate public error design and is outside this migration.

An input `Tree` is authoritative for its own persisted format. The manager's constructor config is used by `create` and manager-owned builders, not as a second restriction on existing tree handles. Each operation creates a validated `TreeContext` from the input tree. Multi-tree reads keep one context per tree; structural CID reuse requires compatible validated content. Merge output follows the base tree's format. The current async-only rejection of a tree solely because its format differs from the manager default is removed as an explicit compatibility correction and receives a fixture and inventory row.

`ProllyEngine` stays internal. `CoreStore` is not used because the type is an algorithm engine, not a storage adapter.

### Async-first services

Each higher-level domain gets an internal service core only when it has storage-backed logic. Examples include `TransactionService`, `NamedRootService`, `VersionedMapService`, `GcService`, `CopyService`, `LargeValueService`, `SecondaryIndexService`, and the existing proximity search/build engines after consolidation.

Services may borrow an engine, own an engine over an overlay store, or call capability traits directly. They share pure components and validated engine primitives where their data is made of Prolly nodes. Content-graph records and proximity-specific nodes use their own codecs and validators rather than pretending to be Prolly nodes.

The requirement is one algorithm per domain, not one struct for the repository.

### Public facades

Async tree methods forward directly:

```rust,ignore
impl<S: AsyncStore> AsyncProlly<S> {
    pub async fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.engine.get(tree, key).await
    }
}
```

Sync tree methods enter the same future once:

```rust,ignore
impl<S: Store> Prolly<S> {
    pub fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        run_ready(self.engine.get(tree, key))
    }
}
```

`Prolly::store()` reaches the original store through `SyncStoreAsAsync::inner()`. Constructors, cache controls, limits, and metrics forward to shared state. Facades may adapt `Future` and stream shapes but must not choose an algorithm or I/O route.

## Execution model

### Native async execution

The engine is executor-agnostic. It creates no tasks, timers, reactors, or worker pools. The caller polls it on Tokio, async-std, smol, a browser executor, a local executor, or a manual test harness.

Bounded concurrency uses polled child futures, not spawned tasks. Dropping the parent drops outstanding child futures. Backend work may still complete after cancellation according to the adapter contract.

### Ready synchronous execution

`SyncStoreAsAsync<S>` and corresponding manifest, transaction, scan, and blob adapters implement a sealed internal `ReadyAsync` marker. Every adapter method invokes its synchronous counterpart during polling and never depends on an external wake.

`run_ready` is an inline poller used only with this sealed contract. A storage-requiring public call enters it at most once, while a pure fast path may enter it zero times. The valid path returns `Ready` during that top-level poll even when the future crosses many ready `.await` points. It uses no runtime, thread parking, thread-local executor, global lock, or worker.

An unexpected `Pending` is a Prolly implementation bug, not backend backpressure. `run_ready` panics with a stable internal-invariant message instead of spinning or deadlocking. This policy is necessary because some existing sync APIs do not return `Result`, and translating this programmer error into a public store error would produce inconsistent signatures. Tests assert first-poll completion for every sync API family and for all internal combinators used by a ready store.

Shared engine code cannot insert cooperative yields or choose a combinator that turns ready child futures into an externally woken `Pending`. Ready adapters override async default methods whenever the default could change native method selection or readiness. Capability conformance tests cover every trait method, including ordered reads, shared reads, batches, hints, manifests, scans, transactions, and blobs.

The runner is stateless and reentrant. Calling a sync Prolly API inside Tokio, from a sync resolver, or from a visitor that reenters the same manager does not nest or block a runtime. No engine lock may be held while invoking user code.

### WASM behavior

The core and async facade must compile for `wasm32-unknown-unknown` without Tokio. Non-`Send` browser stores remain supported. The sync facade also compiles for WASM when its synchronous store does, because `run_ready` uses no operating-system parking or thread API.

The release command is:

```bash
cargo check --manifest-path bindings/wasm/Cargo.toml --target wasm32-unknown-unknown
```

A browser test uses a deliberately non-`Send` store and exercises point read, range traversal, mutation, and cancellation.

### Future `Send` contract

The base `AsyncStore` contract permits non-`Send` futures. Rust therefore cannot prove that a generic `AsyncProlly<S>` operation is `Send` from `S: AsyncStore` alone. This is an intentional public limitation of the current trait shape.

Concrete native operations may still infer `Send`. Compile tests must prove that the production Turso operations used with `tokio::spawn` are `Send`, while a deliberately non-`Send` store works on a local executor. A compile-fail fixture documents that a generic helper cannot promise `Send` without a stronger future-bearing trait.

This migration does not add boxed futures or duplicate hot-path methods to manufacture a generic `Send` guarantee. If downstream inventory finds that generic spawning is required, a separately reviewed associated-future trait can be added without changing the engine algorithms.

### CPU work and callbacks

Pure CPU work runs deterministically in the polling task. Existing Rayon paths may remain behind their explicit policy. Sequential and parallel policies must emit identical bytes and roots. The engine does not call `spawn_blocking`.

Large pure phases use incremental state where that bounds memory, but they do not return `Pending` solely for fairness. Applications that require isolation for a large eager sort, proof verification, or CPU-heavy proximity build choose the task or thread on which they poll it. A future scheduler abstraction must preserve the ready sync contract and requires a separate design.

Resolvers, readers, and visitors remain synchronous callbacks unless the existing public API is explicitly async. A borrowed entry cannot cross `.await`. The engine releases all locks and mutable borrows before calling user code. Panic propagation and existing unwind behavior remain unchanged.

## Validated engine I/O

### Storage-adapter contract

Every native and ready adapter must satisfy these rules:

- A node key is the raw CID of its exact bytes. Writing equal bytes under the same CID is idempotent. Writing different bytes under an existing CID is a contract violation and must fail.
- Completion of `put` or a successful batch establishes read-after-write visibility in the consistency scope used by the returned `Tree`. An eventually consistent backend must provide an adapter-side confirmation barrier.
- Ordered batch reads return exactly one slot per requested key in request order. Unique variants receive unique keys. Missing values use `None` rather than changing cardinality.
- Native batch atomicity is reported accurately. The base engine assumes a failed generic content batch may have written a strict prefix.
- Hint methods are advisory and cannot be the only route to data.
- Root CAS and strict transaction methods have one documented linearization point and the atomicity stated by their capability traits.
- Dropping a future may leave backend work running. The adapter documents whether completion can be discovered and preserves immutable, CAS, or transaction safety even when the caller does not receive a result.
- Content-addressed nodes are immutable for their durable lifetime. Deletion occurs only through explicit GC authority. Out-of-band overwrite is unsupported.
- A component that deletes nodes outside the owning manager, including another process or explicit replica synchronization, must coordinate the same retention authority and cache invalidation policy. Every input `Tree` must remain retained and durable for the operation. The engine may reuse unchanged subtrees from that valid base without rereading them. Previously validated cached bytes remain authentic but cannot rehabilitate a base tree after its retention has ended.

Adapter conformance tests run these rules against memory, SQLite, Turso, the generic remote adapter, and every provider that can run through a local mock or emulator. Credentialed live-provider validation remains outside this local-only migration gate and is never reported as completed by it.

### One loader protocol

Every owned, shared, point, batch, cached, prefetched, proof, diff, merge, mutation, and reachability load goes through one validated loader. No caller decodes raw store bytes directly.

Before the first load, `TreeContext` validates the persisted format identifier, chunking constraints, node-layout registration, and hard limits. An invalid tree configuration fails deterministically before storage I/O. Empty trees still validate their format when an operation depends on it.

For each physical CID, the loader:

1. Checks a validated cache entry.
2. Reads raw bytes through the selected ordered store route on a miss.
3. Hashes the exact bytes and compares the result with the requested CID.
4. Decodes the requested representation.
5. Validates node structure, level, persisted format, hard limits, and tree context.
6. Inserts the node into the cache only after all checks pass.
7. Re-expands duplicate logical requests in original order.

Batch results must have the exact requested cardinality. A missing node, malformed node, wrong level, wrong format, wrong count, or CID mismatch is a deterministic corruption or store-protocol error. Existing public error categories are preserved unless the compatibility inventory approves a new variant.

The current sync owned-node path and current async ordered-node path decode without always verifying the requested CID. Migration Slice 1 fixes those paths before any algorithm is moved.

### Ordered concurrency

For each node frontier, the engine:

1. Preserves logical request order and records the first position of each CID.
2. Deduplicates physical CIDs.
3. Serves validated cache hits.
4. Uses `batch_get_shared_ordered_unique` when the adapter prefers native batches.
5. Otherwise polls point reads up to `min(max(store.read_parallelism(), 1), limits.max_in_flight_reads)`.
6. Chunks native batches by `limits.max_batch_read_keys` and `limits.max_batch_read_bytes`.
7. Validates every result before publishing it to callers or caches.
8. Chooses the error at the lowest logical request position, not the first future to complete.

A native batch method that returns one backend error has one store-level failure. Point-read concurrency records indexed results and does not expose completion-order nondeterminism. Reads later than a known earliest error may be dropped once every earlier logical position is resolved.

### Cache rules

- Cache keys include the CID and representation or tree-format context needed to prevent type confusion.
- Cache insert occurs only after CID and format validation.
- Under the immutable storage contract, cache state can affect I/O count only. Cache hits contain previously validated authentic bytes.
- Root-specific path and hint entries include the root CID and config identity.
- Mutation publication invalidates or replaces affected route entries only after success.
- A poisoned cache lock is bypassed and increments `cache_bypass_poisoned`; it does not change operation success or return a storage error.
- The initial migration deduplicates within one operation but does not add cross-operation single-flight. Duplicate concurrent reads are allowed. Single-flight requires separate cancellation and ownership measurements before adoption.
- Configured cache limits are enforced after every insert. Pinned entries count toward those limits and cannot bypass them.

No lock guard crosses `.await` or a user callback.

The current `RuntimeConfig` uses `None` for an unbounded cache, including its legacy default. This migration does not silently reinterpret that public value. Existing constructors preserve it. The new production `ExecutionConfig` profile selects finite node and byte limits, and production adapter examples use that profile. Explicit legacy unbounded mode remains documented as an operator memory tradeoff. Changing the default for existing constructors requires a separate compatibility decision.

### Resource limits and backpressure

`EngineLimits` is non-canonical runtime configuration. It includes at least:

- Maximum in-flight point reads.
- Maximum keys and bytes per native read and write batch.
- Maximum logical frontier width.
- Maximum speculative nodes and bytes.
- Maximum in-memory staged write nodes and bytes.
- Maximum streaming diff, merge, cursor, and copy buffer sizes.

Zero values are rejected or normalized at construction, never interpreted inconsistently by an algorithm. Store hints can lower effective concurrency but cannot exceed engine caps.

Streaming APIs keep O(tree height plus configured frontier and page limits) orchestration state. Eager APIs such as `diff -> Vec<Diff>` remain O(output) by contract. Unsorted bulk building remains O(input). No API may add an unadvertised O(total tree entries) copy.

Finalized immutable nodes may be flushed in bounded batches once an algorithm has committed to a canonical route. They are still unreachable until a root is returned or named. Speculative fast-path output is never flushed before its proof succeeds. A stager deduplicates CIDs and rejects the impossible case where one CID is paired with different bytes. Strict transaction staging follows the transaction service rules and either uses backend staging or enforces its explicit memory limit.

### Metrics and operation statistics

Each public operation creates an `OperationContext` with local counters. Child futures return indexed observations to the parent rather than mutating shared context across `.await`. Returned mutation, diff, merge, copy, GC, and traversal statistics come from that context. They are never computed by subtracting global before and after snapshots, which is racy under concurrent operations.

Global manager metrics use atomic counters and record completed physical events independently of operation success. Metrics distinguish:

- Attempted, completed, failed, and indeterminate store operations.
- Logical versus physical reads.
- Bytes requested, returned, validated, and written.
- Cache hit, miss, eviction, and poisoned bypass.
- Native batches, point-read frontiers, and peak in-flight work.
- Speculative nodes built and discarded.
- Finalized nodes staged, published, reused, or left unreachable after failure.
- Route selected, resynchronization distance, and fallback reason.
- Root or transaction publication attempts and applied outcomes.

Publication-success metrics advance only after the relevant visibility operation succeeds. A generic failed batch may have written a strict prefix, so it records attempted nodes and an indeterminate committed count rather than inventing an exact value. Cancellation drops local return statistics but completed global events remain observable.

The existing public `ProllyMetricsSnapshot` fields and meanings remain unchanged. Adding public fields would break downstream struct literals and exhaustive patterns. Detailed attempted, failed, indeterminate, route, frontier, and speculation counters use a new additive `ProllyDiagnosticsSnapshot` with private fields and accessors, or an internal versioned observation sink for benchmarks. Existing public operation-stat structs are extended only when the API inventory proves the change source-compatible; otherwise new diagnostic return methods carry the additional data.

Existing `metrics()` snapshots remain non-transactional views of independent atomic counters. `reset_metrics()` is intended for quiescent managers; concurrent completions may appear on either side of the reset, as today. Returned operation-local statistics remain exact even when another thread snapshots or resets global diagnostics.

## Canonical build and mutation

### One canonical implementation

The engine reuses the deterministic concepts already proven by `src/prolly/write.rs`: last-write-wins normalization, canonical level emitters, right-edge append, direct key-stable updates, guarded structural islands, CID resynchronization, and streaming sequential fallback.

For the same base root, tree configuration, and normalized mutation stream, the async facade, sync facade, configured CPU policies, and a clean bulk rebuild must produce the same root CID and persisted bytes. A fast path is a proof of that canonical root, not an alternate tree layout.

### Route order

After validating the tree and mutations, the engine attempts:

1. Empty-base canonical build.
2. Right-edge append.
3. Direct existing-key value updates, including coalesced key-stable batches.
4. Direct canonical deletion routes whose proofs apply.
5. Guarded localized structural islands.
6. Sequential streaming canonical replay.

An inapplicable proof discards private output and tries the next route. Speculative nodes never enter the store, write cache, returned tree, hint, named root, or publication metrics.

### Resynchronization

Structural replay begins with the canonical predecessor context required by hard byte caps and boundary state. It merges old entries with normalized mutations through the canonical emitter. An existing suffix becomes reusable only when emitter state is empty at a persisted boundary and emitted content matches the corresponding existing CID. A guarded island must prove resynchronization before its protected boundary. Otherwise islands coalesce or the engine falls back.

The fallback streams entries and mutations. It may retain compact summaries and bounded staged output, but it must not materialize the complete logical key/value map.

### Staging and publication

Publication has two phases:

1. Make finalized immutable nodes available. Small operations use one batch. Large builders, streaming fallbacks, and copies may use bounded batches. A failure can leave only unreachable content-addressed nodes.
2. Establish visibility by returning the `Tree` to the caller or by a separate named-root or transaction operation.

A tree operation returns no new `Tree` until all referenced nodes have completed and validated publication. Newly emitted cache entries appear only after their node write succeeds. Loaded base nodes may enter the read cache earlier.

Hints remain advisory. If the existing API surfaces a `batch_put_with_hint` error, a hint failure after node publication still returns an error and leaves unreachable or retryable nodes. It never invalidates an existing tree. Changing hint failure into best-effort success requires a separately inventoried compatibility decision.

## Builders, cursors, reads, diff, and merge

### Bulk builders

Bulk building uses pure emitter state plus engine publication.

- `BatchBuilder<S: Store>` remains source-compatible. It retains unsorted input, sorts it, applies last-write-wins, builds deterministically, and publishes through the ready engine.
- Add `AsyncBatchBuilder<S: AsyncStore>` with synchronous `add` and asynchronous `build`.
- `SortedBatchBuilder<S: Store>` remains source-compatible and memory-bounded. Its pure streaming emitter yields finalized node batches. `add` publishes a yielded batch through `run_ready`; `build` publishes the tail and returns the root.
- Add `AsyncSortedBatchBuilder<S: AsyncStore>`. Its `add` is async because an emission can require storage. `build` is async.

The pure state and emission rules are shared. Sync and async builds from equal logical input must produce equal roots and node bytes. Incremental builder writes are unreachable until `build` returns a tree. Dropping or failing a builder may leave unreachable nodes and never a visible partial root.

### Cursors, iterators, and streams

The shared traversal is a pull state machine. A pure step returns one of `Yield`, `NeedNode`, `Done`, or `Error`. Async drivers await `NeedNode`. Sync iterators satisfy it through the ready adapter. This avoids a second traversal algorithm and preserves lazy sync behavior.

By default, a cursor buffers only its path and entries already present in the loaded leaf. It does not prefetch the next leaf merely to fill an arbitrary page. Therefore:

- Error timing remains after all earlier loaded entries have been yielded.
- Dropping an iterator causes no further I/O.
- Resume tokens advance only after a value is yielded.
- Forward, reverse, prefix, bound, exact-key, gap, and empty-tree semantics remain unchanged.
- Metrics count actual node loads, not logical `next` calls.
- A single oversized persisted node remains governed by canonical hard limits.

Explicit page and prefetch APIs may load bounded frontiers, but page size cannot affect ordering, cursor tokens, proofs, or errors for entries before the page boundary. Eager `collect` remains O(output).

### Reads and proofs

Point reads, shared reads, batched reads, rank, select, pages, prefix operations, cardinality, and proof construction use the engine loader and traversal components. Batch reads preserve input order and duplicate logical results while deduplicating physical CIDs.

Owned APIs copy values only where their current signatures require ownership. Shared-read and visitor APIs retain backend-owned `Arc<[u8]>` or validated packed-node storage and invoke borrowed callbacks after required I/O, without an extra owned-node round trip. The migration must not turn current zero-copy read paths into decode-and-clone paths.

Proof verification remains pure and storage-free. Proof construction uses only validated nodes. A cache hit can never allow a proof that a cold read would reject.

### Diff and merge

Structural diff, range diff, streaming and resumable diff, three-way merge, merge explanation, conflict streaming, and CRDT merge share traversal frontiers and deterministic ordering.

Conflict resolvers remain synchronous callbacks and cannot perform hidden engine I/O. The engine holds no lock while calling them. Reentrant reads are supported. Sync and async facades must produce identical conflicts, explanations, reuse decisions, statistics, and roots.

Streaming variants are bounded. Existing eager variants remain O(output). Completion-order concurrency cannot reorder diffs or conflicts.

## Higher-level service semantics

### Named roots, snapshots, and versioned maps

Named roots use `AsyncManifestStore` through one `NamedRootService`. A Prolly `Tree` return is not a durable branch commit. `put_root`, delete, and compare-and-swap are separate visibility operations.

Snapshot and versioned-map services compose named-root policy with the engine. Their existing public sync and async types, lifetimes, timestamps, retention ordering, conflict results, and proof behavior remain source-compatible. They do not duplicate engine traversal.

### Strict transactions

Transactions remain a service over transaction overlays and `AsyncTransactionalStore`. An overlay owns staged nodes, root conditions, and root writes. Tree work inside the overlay uses `ProllyEngine<OverlayStore>`.

`commit_transaction` must atomically validate every root condition, publish required nodes, and apply all root writes. It returns either `Applied` or `Conflict`; no partial named-root state is permitted. A cancelled commit may have applied or not applied, but the backend must preserve all-or-nothing atomicity. The caller can resolve an unknown outcome by reading the affected roots. Adapters must document their linearization point.

Sync transaction types use ready transaction adapters. Drop and explicit rollback preserve current semantics. Transaction callbacks may reenter the outer manager, but not mutate the same overlay through aliased mutable access.

Strict transaction staging is bounded. A backend transaction can own staged data, or the service enforces `max_transaction_staged_nodes` and `max_transaction_staged_bytes` before memory grows without limit. Any new public resource-limit error requires inventory approval and a compatibility fixture.

### GC, copy, content graphs, and remote sync

Tree reachability is an engine traversal. Store scanning, retention selection, deletion authority, and sweep progress belong to `GcService`.

GC planning is read-only and deterministic. Sweep is idempotent per CID but may be partially complete when cancelled or when a delete fails. Every deleted CID must have been unreachable from the retained root set captured under the API's existing concurrency authority. If a backend cannot snapshot roots and candidates together, the caller must exclude concurrent publication during sweep. Retry recomputes or resumes from durable candidates and cannot delete a retained node.

Copy and remote synchronization validate source bytes through the source domain codec before writing the destination. They may leave a validated prefix in the destination after failure or cancellation. Content-addressed writes are idempotent. A destination root or manifest is published only after the complete graph is present.

Content graphs remain a separate service because they include typed records beyond Prolly nodes. Their walk, validation, GC, and copy logic becomes async-first independently and uses ready sync adapters.

### Blobs and large values

Large-value coordination composes the engine with `AsyncBlobStore`. A blob is content-addressed and validated against its reference on read. Publication writes the blob before it returns or names a tree containing that reference.

Failure between blob and node publication may leave an orphan blob. Failure after node publication but before a named root may leave unreachable nodes and blobs. Neither case corrupts an existing tree, and blob GC can reclaim both.

There is no implied atomic transaction across separate node, blob, and manifest stores. An adapter that needs atomic visibility must provide one explicit coordinator. Otherwise the service uses ordered publication and orphan cleanup. This limitation is documented in public large-value and transaction APIs.

### Secondary indexes and proximity

Secondary-index coordination keeps one async-first snapshot and publication service. It may compose multiple Prolly engines but must preserve its existing cross-root consistency contract. It is not folded into the base engine.

Proximity storage, search, accelerator construction, catalog updates, and proofs keep domain-specific engines and codecs. Sync and async search/build drivers consolidate around one async-first I/O interface. Pure distance, quantization, graph, and proof calculations remain synchronous. Every separate storage traversal is either consolidated or listed as an approved exception in the API inventory.

## Error, failure, and cancellation model

Dropping an async future is not rollback. An adapter may complete an operation after its future is dropped. The repository guarantees storage safety by separating immutable content from mutable visibility and by requiring atomic capability methods where visibility changes.

| Operation class | Failure or cancellation effect | Retry rule |
| --- | --- | --- |
| Validated read | No logical state change. Backend accounting may complete. | Retry is safe. |
| Immutable node or blob write | A strict prefix or full write may remain unreachable. | Retry by content address is idempotent. |
| Tree mutation returning `Tree` | No tree is returned until every referenced node is available. Existing trees stay valid. | Repeat against the same base and mutations. The canonical root must match. |
| Advisory hint | May be absent, stale, or written without a returned tree. It cannot be required for traversal. | Ignore, overwrite, or regenerate. Preserve current surfaced-error behavior. |
| Named-root put or delete | The individual operation has one linearization point. A cancelled caller may not know the outcome. | Read the name, then decide. |
| Named-root CAS | Applied or not applied atomically. Cancellation can hide the returned outcome. | Read current state. Retry only with a newly validated expectation. |
| Strict transaction | All root conditions, nodes, and root writes apply atomically or none do. | Read affected roots to resolve an unknown cancelled outcome. |
| GC sweep | A proven subset may be deleted before failure or cancellation. Retained content must never be deleted. | Replan or resume under the same publication-exclusion rule. |
| Copy or remote sync | A validated destination prefix may remain; no destination root is published early. | Resume by missing CID. |
| Large-value publication | Orphan blobs or nodes may remain; a visible root cannot reference a missing required blob. | Retry ordered publication, then reclaim orphans. |

Store adapters must document method linearization, ordering, batch cardinality, atomicity, and cancellation behavior. The engine does not assume that dropping a write future cancels backend work.

## Cargo and compatibility model

Async foundations compile unconditionally:

- `AsyncStore`, async capability traits, ready adapters, and `AsyncProlly` are no longer gated by `cfg(feature = "async-store")`.
- `futures-util` becomes a normal dependency for streams and bounded orchestration.
- `async-store` remains an accepted no-op compatibility feature.
- If cutover ships in 0.4.0, `async-store` remains accepted through all 0.5.x releases and can be removed no earlier than 0.6.0 after a documented deprecation. A different cutover version uses the same two-minor-release rule.
- `tokio` remains optional and covers only adapters that explicitly need Tokio or blocking offload.
- The root package retains Rust 1.81 MSRV. Adapters with a higher existing MSRV retain their own declaration.
- Default and `--no-default-features` builds both include the async engine and do not pull Tokio.

`prolly-store-turso` keeps its existing adapter policy. The default is the native local async store. Its optional `sync` feature continues to mean explicit Turso Cloud `push()` and `pull()`, not a synchronous Prolly facade. Local correctness and performance tests do not enable that feature or contact the network. The release matrix compiles and locally tests both adapter feature configurations, while credentialed cloud synchronization remains a separate explicit integration result.

Public names and signatures remain compatible unless Rust makes the change impossible. Availability of async names without a feature is additive. Public struct fields and layout are not promised. Any changed bound, lifetime, iterator timing, error, callback rule, or feature behavior must appear in the compatibility inventory and receive approval before implementation.

### Public API inventory gate

Before implementation, generate `docs/superpowers/specs/2026-07-18-async-first-api-inventory.md` from rustdoc JSON plus a manual storage-domain classifier. Every public storage-dependent item has one row with:

- Fully qualified public name and current signature.
- Current feature gate and MSRV.
- Current sync and async equivalents.
- Target owner: pure component, `ProllyEngine`, domain service, facade, or approved exception.
- Observable behavior to preserve, including laziness, error timing, callback and cancellation rules.
- Existing public auto-trait behavior such as `Send`, `Sync`, and `Unpin` where downstream code can rely on it.
- Migration slice and required equivalence test.
- Source-compatibility status and approved exception link.

The inventory covers batch, blob, boundary, builder, chunking, CID, config, content graph, CRDT, cursor, debug, diff, encoding, error, format, GC, key, manifest, merge, node, parallel, patch, policy, proof, proximity, range, range delete, read, remote, secondary index, snapshot, splice, statistics, store traits and adapters, streaming, sync and copy, tombstone, traits, transactions, tree, utilities, value, versioned maps, write, write sessions, every crate-root re-export, and every binding export. Pure modules are classified explicitly rather than omitted.

No unclassified row is allowed when migration begins or completes. `binding_api_inventory.py`, downstream compile fixtures, rustdoc JSON, and `cargo-semver-checks` or an equivalent pinned public-API diff freeze the baseline. Tool versions and the baseline revision and dirty-patch digest are recorded.

## Migration and rollout

### Slice 0: inventory and baselines

- Generate and review the complete public API inventory.
- Freeze root CIDs, node bytes, values, ordering, errors, store-call traces, cache behavior, and callback behavior for representative fixtures.
- Capture a controlled pre-migration sync microbenchmark and the current SQLite/Turso local matrix with revision, dirty digest, compiler, target, hardware, filesystem, and durability settings. The known O(N) Turso path gets a fixed per-cell time and disk cap; larger timed-out cells are preserved as censored baseline results rather than delaying the migration indefinitely.
- Add the explicit multi-manifest verification script.
- Do not change algorithms in this slice.

### Slice 1: validated foundation

1. Add raw-byte CID and format validation shared by owned and shared reads.
2. Add deterministic ordered frontier loading and operation-local metrics.
3. Add `ProllyEngine`, `EngineLimits`, shared caches, and sealed ready adapters.
4. Add `run_ready` and first-poll, reentrancy, nested-runtime, and WASM tests.
5. Put both facades over the engine for create, configuration, point load, and publication primitives.

Every checkpoint is independently green and reviewable. No higher algorithm migrates before all load paths use validation.

### Slice 2: canonical build and mutation

- Move pure builder and emitter state behind engine orchestration.
- Add async builder counterparts and preserve sync builder signatures.
- Port canonical mutation routes to native async node acquisition.
- Switch put, delete, batch, append, and range delete for both facades.
- Remove the async full-tree `BTreeMap` rebuild, retired planner, and superseded sync mutation entry points.
- Replace global-snapshot-derived mutation statistics with operation-local statistics.

### Slice 3: reads, cursors, and proofs

- Move point and batch reads, shared reads, rank, select, ranges, reverse and prefix traversal, pages, and proof construction.
- Use the shared pull cursor state machine for sync iterators and async streams.
- Preserve lazy I/O and error timing.
- Remove duplicate read and range bodies.

### Slice 4: diff and merge

- Move structural, range, streaming, and resumable diff.
- Move three-way merge, explanation, conflict streams, and CRDT resolution.
- Preserve resolver reentrancy and deterministic conflict order.
- Remove duplicate traversal and merge bodies.

### Slice 5: async-first domain services

- Consolidate named roots, snapshots, versioned maps, transactions, large values, GC, copy and remote sync, content graphs, secondary indexes, and proximity.
- Use engine traversal only for Prolly nodes and retain domain-specific validators elsewhere.
- Preserve atomicity, authorization, lifetimes, and binding behavior.
- Remove each domain's duplicate sync or async production driver before its sub-slice completes.

### Slice 6: hard cutover

- Delete stale algorithm bodies, dead-code allowances, and obsolete feature gates.
- Make facade modules mechanically thin and source-guarded.
- Update documentation, examples, feature descriptions, architecture diagrams, and bindings.
- Run every correctness, compatibility, memory, and performance release gate.

### Rollout rules

- Each sub-slice is one bisectable behavior change with tests and benchmarks attached.
- Main remains green after every commit.
- A test-only old implementation may act as an oracle during a slice. It is unavailable in normal builds and removed when the slice closes.
- A failing slice is reverted as a unit. No persisted format migration is needed because canonical bytes do not change.
- Performance cannot waive correctness. A root or behavior divergence stops the slice before optimization continues.
- Mutation is the first performance priority, but the full objective remains open until every inventoried domain is migrated or explicitly excepted.

## Correctness verification

### Facade and oracle equivalence

For every API family, equal fixtures and inputs run through native async, ready sync, and the pre-migration test oracle while it exists. Tests compare:

- Values and borrowed or owned shapes.
- Entry, diff, conflict, and root ordering.
- Cursor and resume behavior.
- Proofs and verification results.
- Errors, panic boundaries, and error timing.
- Callback count and reentrancy.
- Operation statistics and applicable global metric deltas in isolated tests.
- Store-call method, logical order, and batch shape.
- Root CIDs and persisted node bytes.

Every mutation also compares an independent clean canonical rebuild after each step. Logical equality and invariants supplement root identity and never replace it.

### Format and history matrix

The matrix includes every built-in chunking policy and node layout, hard byte caps, boundary-sensitive sizes, empty through multi-level trees, duplicate mutations, and malformed or oversized input.

Each canonical case also runs with different cache, concurrency, batch, staging, and CPU policies. Equal `Config.format` must produce equal roots and node bytes even when `Config.runtime` or `ExecutionConfig` differs.

Histories cover append, prepend, interior insert, key-stable update, boundary-changing update, present and missing delete, range delete, delete-to-empty, sparse and dense batches, clustered and random changes, mixed operations, and structural islands that resynchronize, coalesce, or fall back.

CI runs at least 16 fixed seeds with 500 operations per built-in format. Nightly runs 128 seeds with 5,000 operations. A release candidate runs 256 seeds with 10,000 operations and records the seed list and root transcript as an artifact.

### Store-contract and suspension tests

A yielding async store returns controlled `Pending` from every I/O method. Tests verify resume behavior, concurrency limits, deterministic error selection, cancellation at every await boundary, and absence of locks or borrows across suspension.

Limit tests set every execution cap to 1 or another small value, force multi-level traversals and staged writes, and assert that counters never exceed the cap. A limit failure occurs before unbounded allocation, leaves existing roots valid, and is deterministic across facades. Legacy unbounded cache mode is tested separately from bounded orchestration.

A malicious store returns wrong counts, reordered values, missing data, malformed bytes, valid bytes under the wrong CID, wrong formats, duplicate aliases, late lower-index errors, strict-prefix batch writes, hint-only failures, and indeterminate cancelled writes. No invalid data enters a cache or algorithm.

A counting ready store verifies direct method selection, native batch use, zero or one `run_ready` entry per public sync call, first-poll completion whenever entered, callback reentrancy, and no point-call degradation.

### Property, fuzz, and concurrency tools

- Proptest covers mutation normalization, cursor state transitions, frontier deduplication and expansion, and sync/async root equivalence.
- Fuzz targets cover node decoding and CID validation, malformed batch results, mutation histories, proof decoding, and cursor resume tokens.
- Pull requests run deterministic property suites. Nightly fuzzing runs each target for at least 10 minutes. Release fuzzing runs each target for at least 30 minutes and stores crashing inputs.
- Miri covers pure unsafe-sensitive codecs, ready-runner wake handling, and cursor state tests that it supports.
- Loom is required only for custom shared concurrency state such as future single-flight. Standard cache locks are tested with stress and poison injection.
- Supported Linux sanitizer jobs run AddressSanitizer and LeakSanitizer for core test fixtures before release. Miri covers undefined-behavior-sensitive Rust paths that it supports.

### Duplication guards

Source and API checks enforce:

- Facades contain forwarding and shape adaptation only.
- No production mutation path collects a full logical tree into a `BTreeMap`.
- No migrated domain has separate sync and async routing bodies.
- No raw node bytes returned by storage are decoded outside approved validators. Pure codec and proof-decoding entry points remain allowed and have their own validation tests.
- No lock guard type is present in a future state across `.await`.
- Hints and caches always have cold correctness paths.
- Every API inventory row has a final owner and test.

## Compatibility and release verification

The repository root is not a Cargo workspace. Add `scripts/verify-async-first.sh` that invokes every supported manifest explicitly and writes a machine-readable result. At minimum it covers:

- Root crate with Rust 1.81: `--no-default-features`, default, legacy `async-store`, `tokio`, and `--all-features`.
- Root tests, examples, benches, strict Clippy, formatting, and rustdoc with warnings denied.
- `bindings/node/native/Cargo.toml`, `bindings/uniffi/Cargo.toml`, and `bindings/wasm/Cargo.toml`.
- `stores/prolly-store-cosmosdb/Cargo.toml`, `stores/prolly-store-dynamodb/Cargo.toml`, `stores/prolly-store-mysql/Cargo.toml`, `stores/prolly-store-pglite/Cargo.toml`, `stores/prolly-store-postgres/Cargo.toml`, `stores/prolly-store-redis/Cargo.toml`, `stores/prolly-store-rocksdb/Cargo.toml`, `stores/prolly-store-slatedb/Cargo.toml`, `stores/prolly-store-spanner/Cargo.toml`, `stores/prolly-store-sqlite/Cargo.toml`, `stores/prolly-store-test/Cargo.toml`, and `stores/prolly-store-turso/Cargo.toml`.
- `benchmarks/sqlite-turso-local/Cargo.toml` and `benchmarks/postgres-scale/Cargo.toml` as compile checks.
- `dolt/integration-tests/mysql-client-tests/rust/Cargo.toml` under its existing integration prerequisites.
- Existing language-store scripts and local mock or emulator conformance tests. Credentialed service scripts are inventoried but are not invoked by this migration's local-only gate.

The script performs no network access. It reports credentialed external-service tests as outside the local-only gate rather than as passes. Release evidence distinguishes compile-only and local integration results and does not imply remote-provider validation. The repository currently has no root GitHub Actions workflow, so the script is the canonical local matrix and must be invoked by whatever release CI owns this repository.

Downstream compile fixtures cover existing constructors, trait implementations, manager and cursor auto traits, builders, iterators, transactions, versioned maps, callbacks, feature combinations, and bindings. The supported WASM check is mandatory. Feature-unification tests include a downstream crate that enables no Prolly feature and another that still enables `async-store`.

## Performance verification

Correctness validation runs before measurements. Any root, value, count, or behavior divergence invalidates the performance result.

### Sync facade overhead

Capture pre-migration native sync baselines for in-memory and SQLite operations. Measure ready-runner entry, point reads, puts, batches, builders, iteration, diff, merge, proof construction, statistics, and representative service operations.

Separate allocation benchmarks cover owned, shared, and visitor reads. Shared and visitor paths cannot add a value-sized allocation or clone relative to baseline.

For each microbenchmark:

- Use five warm-up samples followed by 30 paired measured samples.
- Alternate baseline and candidate order within each pair.
- Record compiler, profile, target, CPU governor, load, and dirty-patch digest.
- Require candidate-to-baseline median latency at or below 1.05.
- Require candidate-to-baseline p95 latency at or below 1.10.
- Require the upper bound of a 95 percent paired bootstrap confidence interval for the median ratio at or below 1.08.

A failing API slice is profiled and fixed or explicitly approved. Iterator store counters must show no additional I/O or prefetch beyond the shared cursor contract.

A minimal sync-only downstream fixture also records clean compile time, release binary size, and `cargo tree -e features`. Tokio must be absent. Relative to the frozen baseline, release binary size must stay within 5 percent and clean compile time within 15 percent, or the dependency and code-size cause must receive explicit approval.

### Async algorithm complexity

Metrics record node and byte reads and writes, cache hits, frontier widths, peak in-flight work, batch calls, streamed entries, resynchronization distance, reused nodes, and canonical route.

Hard gates include:

- A key-stable existing-key point update performs at most `4 * tree_height + 16` physical node reads and at most `4 * tree_height + 16` node writes at p99 in the deterministic matrix.
- No point-update route reads a number of entries proportional to total tree size.
- Sparse batches read touched routes plus bounded canonical context.
- Append remains right-edge-local.
- A normal point mutation uses at most one immutable-node publication batch. Large builds and streaming routes use bounded batches and report them.
- Streaming fallback never materializes all logical entries.
- In-flight work never exceeds the effective store and engine limits.
- Instrumented frontier, cursor, speculative, and staging bytes never exceed their configured caps, apart from one already-persisted node whose canonical hard limit is tested separately.

For fixed-size key-stable updates, Turso p50 latency at 2 million records must be no more than 3 times its 50,000-record p50, and p95 must be no more than 4 times its 50,000-record p95. The instrumented node-work gate remains authoritative if host noise affects latency.

### SQLite and Turso local matrix

Run SQLite native sync and Turso native async locally at 10K, 50K, 100K, 500K, 1M, and 2M records. Use append, deterministic uniform random, and deterministic contiguous-cluster patterns across point put, batch mutation, diff, and merge.

The existing workload definition remains stable: changes are 1 percent of records clamped to 100 through 10,000; merge uses two disjoint change sets; seeds, key encoding, value encoding, and expected counts are recorded. Any workload change increments the benchmark schema and invalidates direct comparison with the earlier schema.

The release matrix uses five measured repetitions and alternates adapter order. It reports total time, operations per second, p50, p95, p99, maximum latency, logical operations, node and byte I/O, store calls, route counts, database size, peak resident memory, and environment provenance.

SQLite explicitly uses WAL and `synchronous=NORMAL`. Turso uses its native local engine with cloud sync disabled. The harness queries and records Turso's effective journal and durability settings when the engine exposes them, and applies supported equivalent local settings before measurement. It does not label durability as equivalent unless the effective values prove it. Both adapters use local files on the same volume. Manager-cache state is reported as cold-manager or warm-manager. The harness does not claim a cold operating-system page cache unless the host can prove it.

Mandatory engine CID and format validation remains enabled in every measured path. A result cannot improve by disabling integrity checks. If adapter-level validation duplicates the engine hash, profiling may justify a trusted internal handoff only when direct adapter use retains its existing validation guarantee and malicious-store tests still terminate at the engine boundary.

The harness increments its result schema, writes resumable cell state, validates every persisted result, and can resume without mixing revisions or schemas. It preflights free space for at least three times the estimated largest live fixture plus 10 GiB, keeps only one adapter fixture live unless a test explicitly requires otherwise, and deletes each fixture immediately after its cell unless retention is requested.

Cutover gates are:

- At least a 10 times Turso throughput improvement over the current implementation for 50K random and clustered point updates.
- No linear point-update node work or latency growth through 2 million records.
- No more than 5 percent median throughput regression or 10 percent p95 latency regression for append, batch, diff, or merge within the same adapter, unless explicitly approved.
- Turso local random and clustered point-update p50 within 10 times SQLite sync as the production target.

If algorithm gates pass but Turso misses the final production target, profile adapter costs before tuning. Evidence-backed local candidates include page-cache sizing, validated connection reuse, transaction scope, prepared statement reuse, and explicit single-process locking. SQLite WAL plus `synchronous=NORMAL`, or a measured Turso durability equivalent, remains the comparison baseline. Experimental MVCC is not a production gate.

## Documentation requirements

Cutover updates or replaces `docs/async-store.md`, which currently describes async as optional and separate. It also updates the root README, `src/prolly/README.md`, Cargo feature descriptions, rustdoc examples, adapter READMEs, binding documentation, and architecture diagrams.

Documentation must state:

- Async is foundational and runtime-neutral.
- Sync uses the same engine with a ready adapter.
- `async-store` is a compatibility no-op during its deprecation window.
- Base generic futures are not promised `Send`.
- Cancellation is not rollback.
- Tree return, named-root publication, and strict transaction commit are distinct visibility levels.
- Large values across separate stores can leave reclaimable orphans.
- Streaming and eager memory behavior differ.

## Alternatives rejected

### One god-object engine

Putting transactions, manifests, content graphs, blobs, secondary indexes, and proximity inside `ProllyEngine` would centralize storage but blur capability, lifetime, atomicity, and authorization boundaries. Layered async-first services preserve one algorithm without creating that coupling.

### Storage-neutral state machine with separate sync and async drivers

This removes deterministic duplication but leaves two storage orchestration drivers that can diverge. An async-first implementation plus ready adapters makes sync execute the same operation.

### Generic blocking executor for sync

Parking on arbitrary pending futures permits accidental runtime dependencies, complicates WASM, can deadlock reentrant use, and adds overhead. Sync stores already provide an immediately ready contract. A fail-fast ready runner makes violations visible.

### Keep sync canonical and call `spawn_blocking`

This requires a runtime worker pool, blocks native async concurrency, and does not work for browser stores.

### Maintain separate sync and async algorithms

This is the current source of drift and the point-update regression. Tests cannot make two evolving implementations equivalent by construction.

### Tune Turso while retaining full rebuild

Configuration improves constants but cannot correct O(N times mutations). Append results already show that the backend is not uniformly slow.

## Acceptance criteria

The async-first objective is complete only when current evidence proves every item below:

- The public API inventory has no unclassified storage-dependent item or unapproved compatibility exception.
- `ProllyEngine<S: AsyncStore>` is the only production owner of Prolly-tree algorithms.
- Every higher-level storage domain has one async-first service or an explicitly approved exception.
- `AsyncProlly` and `Prolly` are thin facades, with sync using sealed ready adapters and at most one `run_ready` entry per public call, never one per store call.
- Ready sync is first-poll, runtime-free, reentrant, nested-runtime-safe, and WASM-safe.
- Async foundations compile without Tokio, non-`Send` local stores work, and concrete Turso futures used in production are `Send`.
- Every raw Prolly-node path verifies requested CID, format, structure, and batch protocol before use or caching.
- Operation statistics are concurrency-safe and do not use global snapshot subtraction.
- Sync and async builders share emitter state and produce identical roots and node bytes.
- Sync and async facade equivalence passes for values, ordering, cursors, proofs, conflicts, errors, callbacks, statistics, store traces, and roots.
- Every mutation root matches an independent clean canonical rebuild across the full format and history matrix.
- Yielding, cancellation, malformed-store, cache-poison, publication-failure, GC-resume, copy-resume, and transaction-atomicity tests pass.
- No lock guard or mutable engine borrow crosses `.await` or user code.
- Streaming state and in-flight I/O are bounded; eager O(input) or O(output) costs and explicit legacy unbounded-cache mode are documented.
- Existing-key updates meet the instrumented O(height) gate.
- The full-tree async logical-map rebuild, retired planner, and every migrated duplicate production body are removed.
- The exact multi-manifest compatibility, MSRV, bindings, adapters, docs, Clippy, formatting, fuzz, sanitizer, and WASM gates pass.
- Sync facade benchmarks meet the paired overhead gates.
- The complete validated SQLite/Turso local matrix meets the algorithmic and production performance gates.
- Remaining performance differences are explained by measured engine or adapter evidence.
- `docs/async-store.md` and all public documentation describe the final architecture accurately.

## Approval gate

No architectural question remains open in this revision. Implementation begins only after this revised design is approved and Slice 0 produces a reviewed API inventory and frozen baseline artifacts. Any compatibility exception discovered in Slice 0 returns for explicit approval before code changes that public behavior.
