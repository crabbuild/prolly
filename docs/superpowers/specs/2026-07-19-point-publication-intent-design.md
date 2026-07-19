# Improve every store with universal node publication context

**Date:** 2026-07-19

**Status:** Conversation-approved; written-spec review pending before planning

**Content type:** Conceptual design specification

**Audience:** Prolly maintainers, storage-adapter authors, binding maintainers, and performance reviewers

**Goal:** Let every synchronous and asynchronous store optimize immutable node publication without changing correctness, durability, or canonical results

**Content plan:** Define the universal publication contract, route it through both engine paths, constrain adapter optimization, and specify correctness and performance gates

**Open questions:** None

## TL;DR

Add one allocation-free `NodePublication` request to `Store`, `AsyncStore`, and `RemoteStoreBackend`. The request contains immutable node entries, an optional performance hint, and a store-neutral `PublicationOrigin`. Every existing adapter receives a default implementation that preserves its current batch behavior.

Both engine calling conventions emit the same semantic request. The synchronous ready writer uses an internal publication-aware store facade, while the native async engine publishes after canonical replay. Transparent wrappers preserve the request, transaction overlays absorb it, and remote adapters verify every content identifier (CID) before forwarding it.

Adapter-specific fast paths require measurements. The deferred Turso point-publication transaction is the first proven candidate, not the scope or purpose of the architecture. Memory, file, SQLite, RocksDB, SlateDB, PGlite, remote databases, and foreign adapters all receive the contract and conformance coverage. They retain their current implementation until evidence proves a correctness-equivalent optimization.

## Why the contract must be universal

The previous design added point intent only to `AsyncStore` and `RemoteStoreBackend` so Turso could defer writer-lock acquisition. That boundary omitted the native synchronous path. `Prolly<S: Store>` uses `ReadyWriteManager` and writes directly through `Store`, while `AsyncProlly<S: AsyncStore>` discovers canonical writes through replay and publishes them afterward.

The repository also contains several storage families:

- Built-in memory and file stores
- Local SQLite, RocksDB, SlateDB, PGlite, and Turso adapters
- PostgreSQL, MySQL, Redis, DynamoDB, Cosmos DB, and Spanner backends
- Ready-sync, Tokio-blocking, remote, transaction, search, reference, and foreign-language wrappers

A Turso-only method would preserve this architectural split and force later adapters to invent another signal. A universal request lets every adapter observe the same workload semantics while keeping canonical decisions inside the engine.

## Measured motivation

The async-first engine removed the previous O(N) mutation rebuild and sequential replay discovery. Native Turso point puts still cost more than synchronous SQLite point puts at 10,000 records. Five clean local repetitions at revision `a2f4e7a3` produced these medians:

| Pattern | Turso async total | SQLite sync total | Turso/SQLite latency | Turso p50 per put |
| --- | ---: | ---: | ---: | ---: |
| Append | 87.264 ms | 11.815 ms | 7.39x | 0.847 ms |
| Clustered | 80.110 ms | 8.322 ms | 9.63x | 0.755 ms |
| Random | 102.172 ms | 17.407 ms | 5.87x | 0.952 ms |

Temporary tagged instrumentation measured 100 point puts. Each put published two nodes in one transaction. Connection acquisition consumed 0.6 to 0.7 ms across the complete run. `BEGIN IMMEDIATE` consumed 37 to 42 ms, SQL writes consumed 3 to 4 ms, and commits consumed 5 to 6 ms.

An adapter-wide deferred-transaction prototype reduced Turso point-put totals by 55.9% to 70.8%:

| Pattern | Immediate transaction | Deferred prototype | Change |
| --- | ---: | ---: | ---: |
| Append | 87.26 ms | 28.48 ms | -67.4% |
| Clustered | 80.11 ms | 23.36 ms | -70.8% |
| Random | 102.17 ms | 45.08 ms | -55.9% |

The same adapter-wide prototype regressed small Turso diff cells. Append diff rose 12.7%, and clustered diff rose 12.1%. Autocommit improved point puts by about 56% to 73%, but it regressed cold diff cells by about 9% to 14% and removed one explicit atomic publication boundary. Direct one-key reads and SQL caching also regressed protected cells.

These results support two requirements. The engine must communicate workload origin explicitly, and each adapter optimization must pass its own performance gates.

These exploratory numbers justify the design direction; they are not release evidence. The implementation must reproduce the baseline and candidate through the frozen, provenance-recorded gates below before making a production performance claim.

## Chosen architecture

Use a structured `NodePublication` request across every store contract. This approach was selected over two alternatives:

| Approach | Advantage | Rejection reason |
| --- | --- | --- |
| Mirrored point/general enum | Small API change | Hides builds, merges, range deletes, replication, and maintenance behind one opaque general bucket |
| Structured publication request | One extensible contract with explicit origin and optional hint | Chosen because it serves sync, async, local, remote, and future measured optimizations |
| Capability negotiation | Lets the engine plan around backend limits and concurrency | Deferred until measurements justify allowing backend capabilities to influence engine scheduling |

`NodePublication` describes a completed canonical publication. It does not let a backend choose canonical algorithms, change batch contents, or weaken correctness.

## Goals

The architecture must achieve these outcomes:

- Give `Store`, `AsyncStore`, and `RemoteStoreBackend` the same publication semantics
- Preserve one canonical algorithm for sync and async callers
- Let adapters select correctness-equivalent native fast paths from explicit workload origin
- Preserve byte-identical roots and reachable node bytes
- Preserve CID and tree-format verification
- Preserve node-plus-hint atomicity when the backend supports it
- Preserve current durability, acknowledgment, visibility, and error behavior
- Preserve runtime neutrality and ready-sync completion
- Add no heap allocation to the request itself
- Keep default adapter behavior unchanged
- Require evidence before any adapter-specific override lands
- Improve throughput or scalability without accepting correctness exceptions

## Non-goals

This design does not introduce these behaviors:

- Backend-driven canonical tree planning or canonical tree chunking
- Capability negotiation, adaptive batch sizing, or automatic concurrency selection
- Caller-selectable durability, locking, or transaction modes
- Inferring `PublicationOrigin` or correctness semantics from node count, key shape, hint presence, or batch length
- Changes to node encoding, CIDs, manifests, hints, or wire formats
- Changes to named-root compare-and-swap or strict transaction semantics
- Speculative fast paths for credentialed cloud adapters without measurements
- Experimental multiversion concurrency control
- A requirement that every adapter override the safe default
- Network performance claims from local-only measurements
- A long-lived compatibility shim for the superseded intent-only contract; first-party Rust adapters and language bindings migrate in one coordinated change
- Test-driven development ordering; implementation slices add and run regression tests before each commit, but production code may be written first

## Universal publication types

Define the origin beside `Store` and `AsyncStore`:

```rust,ignore
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum PublicationOrigin {
    #[default]
    General,
    PointUpsert,
    PointDelete,
    BatchMutation,
    TreeBuild,
    Merge,
    RangeDelete,
    Replication,
    Maintenance,
}
```

Define an optional performance hint without changing its persisted representation:

```rust,ignore
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodePublicationHint<'a> {
    namespace: &'a [u8],
    key: &'a [u8],
    value: &'a [u8],
}
```

Define one borrowed, allocation-free publication request:

```rust,ignore
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodePublication<'a> {
    entries: &'a [(&'a [u8], &'a [u8])],
    hint: Option<NodePublicationHint<'a>>,
    origin: PublicationOrigin,
}
```

Fields remain private. `NodePublicationHint::new(namespace, key, value)` constructs a hint. `NodePublication::new(entries, origin)` constructs an unhinted request, and `NodePublication::with_hint(entries, hint, origin)` constructs a combined request. Public `const` accessors expose `entries`, `hint`, and `origin`. Private fields allow additive metadata later without exposing correctness knobs or requiring exhaustive struct literals.

`PublicationOrigin` is advisory and non-exhaustive. Adapters outside the core crate must map unknown future variants to their general path.

## Store contract

Add one method to synchronous `Store`:

```rust,ignore
fn publish_nodes(
    &self,
    publication: NodePublication<'_>,
) -> Result<(), Self::Error> {
    match publication.hint() {
        Some(hint) => self.batch_put_with_hint(
            publication.entries(),
            hint.namespace(),
            hint.key(),
            hint.value(),
        ),
        None => self.batch_put(publication.entries()),
    }
}
```

Add the same method shape to `AsyncStore` with an async body. Add `publish_nodes` to `RemoteStoreBackend`, where its default delegates to `batch_put_nodes` or `batch_put_nodes_with_hint`.

The default dispatches to exactly one existing virtual entry point: either `batch_put` or the combined `batch_put_with_hint`. Existing adapter overrides therefore retain their atomicity, batching, and error mapping. Existing `batch_put` methods do not call `publish_nodes`, which prevents recursion and keeps direct store callers on `General` behavior.

## Publication-origin mapping

The engine assigns origin at the public or internal operation boundary, never in the adapter:

| Origin | Engine operations |
| --- | --- |
| `General` | Direct trait calls and any path without a stronger reviewed classification |
| `PointUpsert` | Sync and async `put`, including tree publication from `put_large_value` |
| `PointDelete` | Sync and async single-key `delete` |
| `BatchMutation` | Public batch APIs, including a one-mutation batch |
| `TreeBuild` | Entry builds, sorted builds, and batch builders |
| `Merge` | Structural merge and every fallback merge output path |
| `RangeDelete` | Half-open range deletion publication |
| `Replication` | Snapshot import, node copy, and remote synchronization publication |
| `Maintenance` | Content-addressed derived-index and proximity maintenance publication |

The mapping describes logical origin, not data layout or performance expectation. A one-item public batch remains `BatchMutation`; adapters cannot infer `PointUpsert` from entry count.

Read-only diff emits no publication. Benchmark preparation that constructs a changed tree retains the origin of the API used to construct it.

## Required invariants

Every implementation and override must satisfy these invariants:

| Invariant | Required behavior |
| --- | --- |
| Canonical identity | Equal base state, format, and logical operation produce equal node bytes and roots for every origin and adapter path. |
| Content integrity | Canonical writers derive each CID from the exact generated bytes. Loaded or externally supplied content is validated against its CID and tree format before trusted use. Remote adapters validate before backend invocation. |
| Advisory origin | Origin may select a measured, native publication plan. It cannot change entries, values, roots, hints, atomicity, durability, visibility, or success semantics. |
| Atomic publication | An adapter that currently publishes a node batch atomically retains that boundary. Node-plus-hint overrides retain their combined boundary. |
| Durability | A successful optimized call provides the same durability and acknowledgment as the adapter's general path. |
| Visibility | The engine returns a tree only after every referenced node has been acknowledged by the store. |
| Failure behavior | A publication error returns no usable new tree. Existing immutable trees and named roots remain unchanged. |
| Runtime-only context | Origin is never hashed, persisted, included in canonical wire formats, or synchronized with database contents. Language bindings may marshal it only for the duration of a store callback. |
| Default stability | An adapter without an override executes its existing batch or combined hinted publication. |
| Ready sync | Synchronous publication remains runtime-free and completes without an external wake. |
| Bounded overhead | The request owns no heap state. Native and ready wrappers add no task, worker, lock, or node copy. Worker and language boundaries may retain their existing ownership copy, but origin adds no payload copy. |

## Synchronous engine path

The synchronous ready writer currently exposes the underlying `Arc<S>` to canonical write code. Nested `BatchBuilder` calls therefore write directly through `Store::batch_put`.

Introduce an internal `PublicationStore<'a, S>` that carries `&S` and one `PublicationOrigin`. It implements `Store` by forwarding every read, capability, delete, and generic batch method. Its `put`, `batch_put`, and `batch_put_with_hint` methods construct `NodePublication` and call `S::publish_nodes`; `put` uses a one-entry stack array and does not allocate.

Canonical node publication must use `put`, `batch_put`, or `batch_put_with_hint`. It must not use generic `batch` or `delete`, because those methods can represent non-publication mutations and therefore forward without origin. A conformance test guards this boundary so a future canonical-writer refactor cannot silently erase publication context.

`ReadyWriteManager` owns this facade and returns it from `write_store`. This captures direct canonical batch calls and nested builder calls without changing the canonical writer or duplicating algorithms.

Add origin-specific ready entry points only where public operation identity differs:

- Point upsert and point delete use dedicated ready entries
- Public batch and configured batch use `BatchMutation`
- Build APIs use `TreeBuild`
- Structural and fallback merge use `Merge`
- Range deletion uses `RangeDelete`

The facade forwards optimized shared reads, ordered reads, hints, and capability queries explicitly. It cannot fall back to trait defaults that would erase a native store optimization.

## Asynchronous engine path

The async canonical writer continues using `ReplayStore` for deterministic discovery. Replay staging receives no backend policy. After replay resolves every missing node and validates every output node, final publication constructs one `NodePublication` and calls `AsyncStore::publish_nodes`.

Async builders, merge fallbacks, replication, and maintenance paths pass their reviewed origin to the same final publication boundary. Empty publications remain no-ops and make no store call.

`SyncStoreAsAsync<S>` forwards `NodePublication` to `S::publish_nodes` inline. An async caller over a synchronous store therefore receives the same native optimization as `Prolly<S>`. `TokioBlockingStore<S>` copies borrowed entries and the optional hint once, then invokes `S::publish_nodes` on its existing blocking worker.

## Direct publication sites

Not every valid publication starts in the mutation writer. Direct engine utilities must call `publish_nodes` with reviewed origin instead of bypassing the contract:

| Direct site | Required origin |
| --- | --- |
| Sorted and unsorted builder flushes | `TreeBuild`, unless nested inside a stronger parent operation such as `PointUpsert` |
| Large-value tree construction | The parent operation's origin |
| Snapshot import and missing-node copy | `Replication` |
| Secondary-index and proximity node maintenance | `Maintenance` |
| Transaction staging | The caller's origin inside the overlay; the overlay absorbs it before commit |
| Low-level public store calls with no engine operation | `General` through existing trait methods |

Builder publication helpers accept origin as an explicit argument so nested work does not get misclassified as an independent tree build. Empty direct publications remain no-ops. A repository-wide conformance audit requires every content-addressed write under `src/prolly` either to call `publish_nodes` explicitly or to pass through `PublicationStore`.

## Wrapper and transaction behavior

Every wrapper has an explicit classification:

| Layer | Required handling |
| --- | --- |
| `Arc<T>: Store` and `&T: Store` | Forward the request unchanged to `T`. |
| `Arc<T>: AsyncStore` | Forward the request unchanged to `T`. |
| `SearchIo<S>` | Forward sync and async publication unchanged. |
| Transparent counting and instrumentation stores | Record the same publication metrics they record today, then forward the request unchanged. |
| `ReplayStore` | Stage and discover generated nodes only; never invoke a backend. The engine attaches origin at final publication. |
| `SyncStoreAsAsync<S>` | Forward inline to synchronous `publish_nodes`. |
| `TokioBlockingStore<S>` | Preserve origin and hint while owning data across the worker boundary. |
| `RemoteProllyStore<B>` | Verify every node CID, then forward unchanged to `B`; publication verification is not bypassed by the legacy verification toggle. |
| `Arc<T>: RemoteStoreBackend` | Forward unchanged to `T`. |
| Sync and async transaction overlays | Stage nodes through existing overlay storage, keep hints disabled, and absorb origin before final commit. |
| Foreign backends | Marshal origin, entries, and the optional hint through the binding callback; language implementations may use the default behavior or a separately measured override. |

Transaction overlays are semantic boundaries. A point mutation can later commit with deletes, root conditions, root writes, and other nodes. Final `commit_transaction` therefore retains its current general transaction behavior.

## Language-binding representation

Bindings use an extensible record instead of a closed foreign enum:

```rust,ignore
#[derive(Clone, Copy, Debug, Eq, PartialEq, uniffi::Record)]
pub struct PublicationOriginRecord {
    pub code: u32,
}
```

Generated binding packages expose named constants for the current codes:

| Code | Name |
| ---: | --- |
| 0 | `General` |
| 1 | `PointUpsert` |
| 2 | `PointDelete` |
| 3 | `BatchMutation` |
| 4 | `TreeBuild` |
| 5 | `Merge` |
| 6 | `RangeDelete` |
| 7 | `Replication` |
| 8 | `Maintenance` |

A shared `NodePublicationRecord` owns `Vec<NodeEntryRecord>`, an optional owned `NodePublicationHintRecord`, and `PublicationOriginRecord`. Both synchronous `HostStoreCallback::publish_nodes` and asynchronous `ForeignRemoteStore::publish_nodes` receive that record. Their Rust adapters override the native publication method and forward the callback result through the existing error type. Existing ownership limits, node-size validation, batch-size validation, and error mapping remain in force.

Every language implementation switches on `code` with a default branch that executes its general batch path, including combined node-plus-hint behavior where supported. Adding a Rust origin assigns a new code and does not change the record shape. Because the callbacks are coordinated source-breaking additions, increment the remote `STORE_PROTOCOL_MAJOR` and regenerate every first-party binding and fixture in the same release.

The origin exists only for the callback duration. It is not written into binding persistence fixtures, manifests, database rows, or synchronization payloads.

## Adapter policy

Every first-party adapter implements or inherits the universal method and passes conformance tests. A custom override is optional.

| Adapter family | Initial handling |
| --- | --- |
| Memory | Default path; current batch already acquires one write lock. |
| File | Default path; profile validation, file creation, and optional parallel publication before considering an override. |
| SQLite | Default path; ordinary transactions already begin deferred. Profile before changing SQL or transaction behavior. |
| RocksDB | Default path; current `WriteBatch` is the native atomic primitive. |
| SlateDB | Default path; current write batch and configured flush behavior remain unchanged. |
| PGlite | Default path; profile sidecar round trips before extending its protocol. |
| Turso | Measured `PointUpsert` override using deferred transaction acquisition. |
| PostgreSQL and MySQL | Default transaction path until service-specific measurements justify an override. |
| Redis | Default atomic pipeline until measurements justify a distinct command plan. |
| DynamoDB, Cosmos DB, and Spanner | Default native batch or transaction behavior until credentialed measurements justify an override. |
| Foreign-language backends | Add a publication callback and stable origin record so non-Rust adapters receive the same semantic request. Default language implementations delegate to their existing batch operations. |

Default handling is beneficial architecturally: every adapter receives stable semantic context, no behavior change, and a measured extension point. Performance claims require an actual measured override.

## Evidence-backed fast-path rule

An adapter override may land only when all correctness tests pass and evidence meets these gates:

- At least five alternating baseline and candidate repetitions; directional gates use the median of the within-pair baseline-to-candidate percentage changes so machine drift cannot break the pairing
- Identical workload bytes, seeds, durability settings, and cache policy
- No protected median latency increase above 5% and no protected median throughput decrease above 5%
- No protected p95 latency regression above 10%
- At least 5% improvement in the target median, or a proven scalability reduction in round trips, lock acquisitions, write amplification, or memory growth
- No new correctness exception, retry semantic, hidden background work, or durability weakening

If a candidate misses the gain threshold, the adapter retains the default method. If a candidate improves one workload but regresses another beyond a gate, reject or narrow it by explicit origin and remeasure the complete protected matrix.

## Initial Turso override

Turso maps publication origin to transaction behavior:

| Publication | Transaction behavior |
| --- | --- |
| `PointUpsert` without or with hint | `Deferred` |
| Every other current or future origin | `Immediate` |
| Existing `batch_nodes` | `Immediate` |
| Root compare-and-swap | `Immediate` |
| Strict transaction commit | `Immediate` |

The point transaction performs no reads. Its first database statement writes immutable nodes, followed by an optional hint write, then commit. Deferred mode changes lock-acquisition timing only. It does not change statements, transaction boundaries, commit acknowledgment, durability, error mapping, or cancellation semantics.

No other adapter receives a custom path in the initial slice without passing the evidence-backed rule.

## Errors, cancellation, and concurrency

Default publication returns the same error type and category as the existing batch methods. Wrappers preserve errors without translating origin into a new failure category.

A failed statement or commit returns no usable tree. The existing immutable-node contract permits failed or cancelled work to leave unreachable nodes, but it cannot expose a partially updated named root. This design does not promise rollback solely because a future is dropped.

Concurrent optimized publications either commit with the adapter's existing atomicity or return its existing busy, conflict, timeout, or service error. The contract adds no retry loop. An adapter override that needs different retry behavior requires a separate design.

## Correctness and conformance tests

Implementation adds post-implementation regression tests, following the approved non-TDD workflow. Tests cover these contracts:

1. Sync and async APIs emit the exact origin in the mapping table.
2. One-item batch remains `BatchMutation`.
3. Point, batch, build, merge, range-delete, replication, and maintenance paths produce byte-identical roots and reachable-node maps against clean rebuilds.
4. `NodePublication` construction and default dispatch allocate no request-owned heap state.
5. Each default sync, async, and remote publication dispatches to exactly one existing virtual batch or combined hinted entry point.
6. Every transparent wrapper preserves origin, entries, and the optional hint exactly once.
7. Ready-sync publication completes on its first poll through `SyncStoreAsAsync`.
8. Borrowed and owned transaction overlays stage nodes, report hints unsupported, and never publish origin to the base store before commit.
9. `RemoteProllyStore` rejects a CID mismatch before backend invocation.
10. Foreign binding callbacks receive the same origin and optional hint as native Rust adapters.
11. Turso maps only `PointUpsert` to deferred mode.
12. Forced Turso hint failure rolls back its combined node-plus-hint transaction.
13. Local Turso point publication persists after close and reopen.
14. Concurrent Turso point publications either succeed or return documented busy errors without corruption.
15. Unknown future origin simulation uses the general adapter path where external matching permits a wildcard.
16. Canonical sync writers publish through `put`, `batch_put`, or `batch_put_with_hint`, never through the facade's generic `batch` or `delete` forwarding paths.
17. Every direct builder, replication, and maintenance publication carries its mapped origin; an audit fails on an unclassified content-addressed write site.
18. Remote publication rejects a CID mismatch even when the legacy verification toggle is disabled.

Existing canonical-root, malicious-store, transaction, remote-provider, builder, merge, range-delete, snapshot, copy, and platform suites remain mandatory.

## Adapter-wide verification

Every adapter crate receives compilation and conformance coverage appropriate to its environment:

- Run full tests for built-in memory and file stores
- Run local tests and performance screens for SQLite, RocksDB, SlateDB, PGlite, and Turso when their documented local prerequisites exist
- Run default and cloud-feature Turso compilation without network synchronization
- Compile and run mock or local conformance tests for PostgreSQL, MySQL, Redis, DynamoDB, Cosmos DB, and Spanner
- Compile and test publication-origin forwarding through UniFFI, Node, Go, Java, Kotlin, Python, Ruby, Swift, WebAssembly, and the versioned C ABI wherever they expose store callbacks
- Require no network credentials for the release's local performance claims

An unavailable optional local prerequisite must be recorded as an environment limitation. It cannot be reported as a passing performance result.

## Performance verification

Correctness runs before measurement. Any root, byte, value, count, atomicity, or reopen failure invalidates the performance result.

### Universal-dispatch overhead

Run paired in-memory sync, ready-sync, and native-async microbenchmarks. Measure point upsert, point delete, batch, build, merge, range delete, and request forwarding. Allow no more than a 5% median-latency increase, a 5% median-throughput decrease, or a 10% p95-latency increase. Require no added publication calls and no request-owned allocation.

### Local adapter screens

Run each locally available adapter before and after universal plumbing with the same workloads. Memory, file, SQLite, RocksDB, SlateDB, PGlite, and Turso screens cover point, batch, build, diff preparation, merge, and reopen behavior. An adapter without a custom override must remain within the no-regression gates.

Measure each adapter through its preferred production contract. Memory, file, SQLite, RocksDB, SlateDB, and PGlite use native synchronous `Store`; Turso uses native asynchronous `RemoteProllyStore<TursoBackend>`. Adapter bridges appear only in the separate universal-dispatch overhead screen. Cross-adapter reports label these paths explicitly and never compare an adapted compatibility path with a native production path as if they were equivalent.

Profile an adapter-specific candidate only after the universal screen passes. Retain an override only when it meets the evidence-backed fast-path rule.

### Focused SQLite and Turso gate

Build baseline revision `a2f4e7a3` and the candidate in separate target directories. Run five alternating process pairs for the frozen 10,000-record, 100-change matrix. Cover both adapters, append, deterministic random, clustered patterns, and put, batch, diff, and merge.

Require at least 40% lower paired Turso point-put median total latency for every pattern. Require no paired Turso point p50 or p95 regression. Reject any protected SQLite or Turso paired median regression above 5%. Keep the independent baseline and candidate medians as descriptive values, but calculate acceptance from within-pair percentage changes. If a protected cell crosses 5%, run five additional alternating pairs and evaluate the combined ten.

### Full SQLite and Turso matrix

After focused acceptance, run three alternating baseline and candidate pairs at 10K, 50K, 100K, 500K, 1M, and 2M records. Use both adapters, all three patterns, put, batch, diff, and merge. Keep fixed inputs, the 1% change count clamped to 100 through 10,000, cold-manager fixture policy, local durability settings, and exact validation.

All 432 candidate rows must validate with no skip. No protected median may regress above 5%. Turso point puts must improve at every size and pattern, publication must remain at most one node batch per point operation, and node work must remain bounded by tree height.

The run never enables cloud synchronization, reads credentials, calls `push`, or calls `pull`.

## Documentation requirements

Update store trait rustdoc to define publication origin as advisory. Update adapter documentation only when an adapter overrides the default.

The Turso README must state that explicit point upserts use deferred local transactions while generic publications, roots, and strict commits retain immediate transactions. The SQLite/Turso report must record before and after throughput, total latency, percentiles, transaction policy, machine provenance, and the local-only limitation.

The pull request must describe the universal architecture before the Turso result. It must not present `NodePublication` as a Turso feature.

## Implementation inventory

The implementation plan will cover these responsibility groups:

- `src/prolly/store/mod.rs` and `src/lib.rs`: public types, store methods, and wrapper forwarding
- `src/prolly/engine/write.rs`: publication-aware ready store and async final publication
- `src/prolly/mod.rs`, `src/prolly/diff.rs`, builders, range delete, snapshot, copy, secondary index, and proximity call sites: exact origin assignment
- `src/prolly/remote.rs`: validation and remote forwarding
- `src/prolly/transaction.rs`: origin absorption at transaction overlays
- Binding schemas and callback adapters: runtime-only origin forwarding with a general fallback for unknown values
- Every first-party adapter crate: compilation and default conformance
- `stores/prolly-store-turso`: the first evidence-backed override
- Core, adapter, binding, and benchmark suites: correctness and performance evidence
- Store and performance documentation: universal contract and measured overrides

The plan must split these changes into reviewable commits without introducing an alternate canonical algorithm.

## Rollout and rollback

Land the universal contract and default delegation before any custom fast path. Verify all default adapters and bindings. Then land Turso's override as a separate commit with focused evidence.

If universal plumbing regresses a protected path, correct or revert the plumbing before adapter tuning. If one adapter override fails correctness or performance gates, revert only that override and retain its default path. Do not infer origin from batch shape as a fallback.

No feature flag is required. Default methods preserve current behavior, and custom overrides are isolated at adapter boundaries.

The coordinated contract migration may be source-breaking for generated foreign callback interfaces. Update every first-party binding and its fixtures in the same release; do not retain parallel intent-only and publication-aware APIs that could diverge.

## Resolved decisions

The approved design resolves these choices:

- Use a structured `NodePublication`, not a Turso-specific flag
- Apply the contract equally to `Store`, `AsyncStore`, and `RemoteStoreBackend`
- Carry explicit origins for point, batch, build, merge, range delete, replication, and maintenance
- Use one publication method with an optional hint instead of separate intent-aware hinted and unhinted methods
- Keep correctness requirements fixed rather than caller-configurable
- Use a publication-aware facade for the synchronous canonical writer
- Preserve async replay and attach origin only at final backend publication
- Give every adapter a safe default and conformance coverage
- Add custom adapter paths only after evidence proves a gain
- Keep Turso as the first measured override, not the architectural scope
- Preserve non-TDD implementation ordering while requiring tests before each commit

There are no open design questions.
