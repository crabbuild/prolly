# Universal Node Publication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give every synchronous and asynchronous Prolly store the same immutable-node publication context, preserve canonical correctness, and land only performance fast paths supported by local evidence.

**Architecture:** Add a borrowed `NodePublication` request with explicit `PublicationOrigin` to `Store`, `AsyncStore`, and `RemoteStoreBackend`. The ready-sync writer publishes through an origin-aware `PublicationStore` facade, the native async engine attaches origin after canonical replay, direct builders and utilities classify their own publications, transparent wrappers forward, semantic overlays absorb, and Turso initially uses the only measured override.

**Tech Stack:** Rust 2021, runtime-neutral async functions in traits, native synchronous stores, native Turso 0.7 async storage, UniFFI and checked-in language bindings, Tokio integration tests, shell/Python benchmark orchestration, and the existing SQLite/Turso local comparison harness.

## Global Constraints

- Follow the approved design in `docs/superpowers/specs/2026-07-19-point-publication-intent-design.md`.
- Execute inline in this session; the user already selected Inline Execution.
- Do not use test-driven development. Implement each production slice first, then add and run its regression tests before committing.
- Treat correctness as release-blocking. Canonical roots, reachable node bytes, CIDs, values, counts, atomicity, durability, visibility, reopen behavior, and error semantics admit no exception.
- Keep one canonical mutation algorithm. Do not fork sync and async tree planning.
- Keep `PublicationOrigin` advisory and runtime-only. Never hash, persist, synchronize, or include it in canonical wire formats.
- Preserve the current publication entries and optional hint byte-for-byte.
- Preserve ready-sync first-poll completion and keep the base async contract free of Tokio.
- Give every adapter a safe default. Add a custom adapter override only after it passes the evidence gate.
- Keep Turso Cloud synchronization disabled during all performance runs. Do not read credentials or call `push` or `pull`.
- Allow no protected median-latency increase above 5%, median-throughput decrease above 5%, or p95-latency increase above 10%.
- Make all implementation commits only in `/Users/haipingfu/CrabDB-worktrees/prolly-async-first`.
- Do not modify the dirty primary worktree at `/Users/haipingfu/CrabDB/prolly`.

---

## File Map

| File or directory | Responsibility |
| --- | --- |
| `src/prolly/store/mod.rs` | Public publication types, sync/async defaults, and core wrapper forwarding |
| `src/lib.rs` | Re-export the universal adapter contract |
| `src/prolly/engine/write.rs` | Ready-sync publication facade and final async replay publication |
| `src/prolly/mod.rs` | Public API origin assignment, builder publication, snapshot import, and node copy |
| `src/prolly/builder.rs` | Standalone tree-build publication |
| `src/prolly/diff.rs` | Structural and fallback merge origin |
| `src/prolly/range_delete.rs` | Range-delete instrumentation forwarding |
| `src/prolly/transaction.rs` | Sync and async overlay absorption |
| `src/prolly/proximity/search/runtime.rs` | Transparent sync/async search-store forwarding |
| `src/prolly/proximity/**/*.rs` | Content-addressed proximity maintenance publication |
| `src/prolly/secondary_index/**/*.rs` | Derived-index and catalog maintenance publication |
| `src/prolly/remote.rs` | Remote backend contract, unconditional publication CID verification, and conformance |
| `stores/prolly-store-test/src/lib.rs` | Shared synchronous adapter conformance |
| `stores/prolly-store-*/src/lib.rs` | Default compilation for every adapter and the Turso measured override |
| `bindings/uniffi/src/publication.rs` | FFI-safe publication records and stable origin codes |
| `bindings/uniffi/src/lib.rs` | Synchronous host-store publication callback |
| `bindings/uniffi/src/async_store.rs` | Asynchronous foreign-store publication callback and protocol version |
| `bindings/{python,go,node,kotlin,java,ruby,swift,wasm}/` | Generated or native callback forwarding and unknown-code fallback |
| `tests/node_publication.rs` | Cross-path origin, identity, wrapper, and direct-publication tests |
| `stores/prolly-store-turso/tests/turso_backend.rs` | Turso policy, rollback, persistence, and concurrency tests |
| `benches/async_first_foundation_bench.rs` | Universal dispatch overhead screen |
| `benchmarks/local-store-publication/` | Identical native-path workload screen for every local adapter |
| `scripts/run_node_publication_revision_gate.sh` | Alternating baseline/candidate orchestration |
| `scripts/run_local_store_publication_revision_gate.sh` | Alternating all-local-adapter orchestration |
| `scripts/summarize_node_publication_revision_gate.py` | Directional regression and improvement gates |
| `scripts/tests/test_summarize_node_publication_revision_gate.py` | Deterministic gate validation |
| `performance-results/node-publication-*/` | Compact local-only evidence |
| `docs/sqlite-turso-local-performance.md` | Native SQLite-sync versus Turso-async results |
| `stores/prolly-store-turso/README.md` | Turso local transaction policy |

## Task 1: Add the Universal Store Contract

This task introduces the public types and safe defaults without changing adapter behavior.

**Files:**

- Modify: `src/prolly/store/mod.rs`
- Modify: `src/lib.rs`
- Test: `src/prolly/store/mod.rs`

**Interfaces:**

- Consumes: existing `Store::batch_put`, `Store::batch_put_with_hint`, `AsyncStore::batch_put`, and `AsyncStore::batch_put_with_hint`.
- Produces: `PublicationOrigin`, `NodePublicationHint<'a>`, `NodePublication<'a>`, `Store::publish_nodes`, and `AsyncStore::publish_nodes`.

- [x] **Step 1: Implement the publication types**

Add these definitions immediately after `BatchOp`:

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodePublicationHint<'a> {
    namespace: &'a [u8],
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> NodePublicationHint<'a> {
    pub const fn new(namespace: &'a [u8], key: &'a [u8], value: &'a [u8]) -> Self {
        Self { namespace, key, value }
    }

    pub const fn namespace(self) -> &'a [u8] { self.namespace }
    pub const fn key(self) -> &'a [u8] { self.key }
    pub const fn value(self) -> &'a [u8] { self.value }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodePublication<'a> {
    entries: &'a [(&'a [u8], &'a [u8])],
    hint: Option<NodePublicationHint<'a>>,
    origin: PublicationOrigin,
}

impl<'a> NodePublication<'a> {
    pub const fn new(
        entries: &'a [(&'a [u8], &'a [u8])],
        origin: PublicationOrigin,
    ) -> Self {
        Self { entries, hint: None, origin }
    }

    pub const fn with_hint(
        entries: &'a [(&'a [u8], &'a [u8])],
        hint: NodePublicationHint<'a>,
        origin: PublicationOrigin,
    ) -> Self {
        Self { entries, hint: Some(hint), origin }
    }

    pub const fn entries(self) -> &'a [(&'a [u8], &'a [u8])] { self.entries }
    pub const fn hint(self) -> Option<NodePublicationHint<'a>> { self.hint }
    pub const fn origin(self) -> PublicationOrigin { self.origin }
}
```

Add rustdoc stating that origin is advisory, unknown variants use the general path, and the request cannot alter correctness or durability.

- [x] **Step 2: Implement the sync and async defaults**

Add this method after each trait's existing hinted batch method:

```rust,ignore
fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
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

Use the same body with `async fn` and `.await` in `AsyncStore`. Do not make `batch_put` call `publish_nodes`.

- [x] **Step 3: Forward through core reference and runtime adapters**

Add exact forwarding overrides to `Store for Arc<T>`, `Store for &T`, and `AsyncStore for Arc<T>`:

```rust,ignore
fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
    (**self).publish_nodes(publication)
}
```

The async `Arc<T>` override awaits the forwarded call. `SyncStoreAsAsync<S>` calls `self.inner.publish_nodes(publication)` inline.

For `TokioBlockingStore<S>`, own the borrowed data before entering the worker and rebuild the borrowed request inside the closure:

```rust,ignore
async fn publish_nodes(
    &self,
    publication: NodePublication<'_>,
) -> Result<(), Self::Error> {
    let entries = publication
        .entries()
        .iter()
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect::<Vec<_>>();
    let hint = publication.hint().map(|hint| {
        (
            hint.namespace().to_vec(),
            hint.key().to_vec(),
            hint.value().to_vec(),
        )
    });
    let origin = publication.origin();
    spawn_store_blocking(self.inner.clone(), move |store| {
        let entries = entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>();
        let publication = match hint.as_ref() {
            Some((namespace, key, value)) => NodePublication::with_hint(
                &entries,
                NodePublicationHint::new(namespace, key, value),
                origin,
            ),
            None => NodePublication::new(&entries, origin),
        };
        store.publish_nodes(publication)
    })
    .await
}
```

- [x] **Step 4: Re-export the contract**

Add `NodePublication`, `NodePublicationHint`, and `PublicationOrigin` to the existing `pub use prolly::store::{...}` list in `src/lib.rs`.

- [x] **Step 5: Add post-implementation contract tests**

Extend the store test module with a recording store and assert:

```rust,ignore
#[test]
fn publication_defaults_dispatch_once_and_preserve_hint() {
    let store = RecordingStore::default();
    let entries = [(b"node".as_slice(), b"bytes".as_slice())];
    let hint = NodePublicationHint::new(b"rightmost", b"root", b"path");

    store
        .publish_nodes(NodePublication::new(
            &entries,
            PublicationOrigin::PointUpsert,
        ))
        .unwrap();
    store
        .publish_nodes(NodePublication::with_hint(
            &entries,
            hint,
            PublicationOrigin::TreeBuild,
        ))
        .unwrap();

    assert_eq!(store.batch_calls(), 1);
    assert_eq!(store.hinted_batch_calls(), 1);
    assert_eq!(store.last_hint(), Some((b"rightmost".to_vec(), b"root".to_vec(), b"path".to_vec())));
}
```

Add first-poll tests for `SyncStoreAsAsync::publish_nodes` and forwarding tests for `Arc<T>` and `&T`. Use `std::mem::size_of::<NodePublication<'static>>()` only as a structural assertion; allocation behavior is verified later with the existing allocation-counting harness.

- [x] **Step 6: Run focused verification**

Run:

```sh
cargo fmt --all -- --check
cargo test -p prolly-map prolly::store::tests --all-features
cargo test -p prolly-map --test async_foundation_default
cargo check -p prolly-map --no-default-features
```

Expected: all commands exit `0`, ready tests return `Poll::Ready`, and the no-default-features build adds no Tokio dependency.

- [x] **Step 7: Commit the contract**

```sh
git add src/lib.rs src/prolly/store/mod.rs
git commit -m "feat: add universal node publication contract"
```

## Task 2: Route Origin Through Ready-Sync and Native-Async Writers

This task attaches semantic origin while preserving one canonical mutation implementation.

**Files:**

- Modify: `src/prolly/engine/write.rs`
- Modify: `src/prolly/mod.rs`
- Create: `tests/node_publication.rs`

**Interfaces:**

- Consumes: `NodePublication` and `PublicationOrigin` from Task 1.
- Produces: `PublicationStore<'a, S>`, origin-aware ready manager construction, origin-aware `execute_replay`, and private sync/async `put_with_origin` and `batch_with_origin` helpers for higher-level operations.

- [x] **Step 1: Implement `PublicationStore`**

Add an internal facade before `ReadyWriteManager`:

```rust,ignore
struct PublicationStore<'a, S: Store> {
    inner: &'a S,
    origin: PublicationOrigin,
}

impl<'a, S: Store> PublicationStore<'a, S> {
    const fn new(inner: &'a S, origin: PublicationOrigin) -> Self {
        Self { inner, origin }
    }
}
```

Implement every `Store` method explicitly. Forward reads, capabilities, `delete`, generic `batch`, and hint access to `inner`. Intercept only immutable publication methods:

```rust,ignore
fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
    let entries = [(key, value)];
    self.inner
        .publish_nodes(NodePublication::new(&entries, self.origin))
}

fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
    self.inner
        .publish_nodes(NodePublication::new(entries, self.origin))
}

fn batch_put_with_hint(
    &self,
    entries: &[(&[u8], &[u8])],
    namespace: &[u8],
    key: &[u8],
    value: &[u8],
) -> Result<(), Self::Error> {
    self.inner.publish_nodes(NodePublication::with_hint(
        entries,
        NodePublicationHint::new(namespace, key, value),
        self.origin,
    ))
}
```

Do not classify generic `batch` or `delete` as node publication.

- [x] **Step 2: Make `ReadyWriteManager` own the facade**

Change its store type and construction:

```rust,ignore
struct ReadyWriteManager<'a, S: Store> {
    engine: &'a ProllyEngine<SyncStoreAsAsync<Arc<S>>>,
    store: PublicationStore<'a, S>,
    config: &'a Config,
}

impl<'a, S: Store> ReadyWriteManager<'a, S> {
    fn new(
        engine: &'a ProllyEngine<SyncStoreAsAsync<Arc<S>>>,
        config: &'a Config,
        origin: PublicationOrigin,
    ) -> Self {
        Self {
            engine,
            store: PublicationStore::new(engine.store.inner().as_ref(), origin),
            config,
        }
    }
}
```

Set `CanonicalWriteManager::Store = PublicationStore<'a, S>` and return `&self.store`. Replace every literal manager construction with `ReadyWriteManager::new`.

- [x] **Step 3: Add origin to async final publication**

Change the signature to:

```rust,ignore
pub(crate) async fn execute_replay<T, F, U>(
    &self,
    tree: &Tree,
    publish_rightmost_hint: bool,
    origin: PublicationOrigin,
    operation: F,
    update_io: U,
) -> Result<(Tree, T), Error>
```

Replace final `batch_put` and `batch_put_with_hint` calls with one request per branch. Keep the encoded hint and its borrowed request in the same scope as the awaited call:

```rust,ignore
if publish_rightmost_hint && self.store.supports_hints() {
    let root = result.0.root.as_ref().ok_or(Error::InvalidNode)?;
    let path = replay_rightmost_path(&manager, root)?;
    let hint = encode_rightmost_path_hint(&path)?;
    self.store
        .publish_nodes(NodePublication::with_hint(
            &entries,
            NodePublicationHint::new(
                RIGHTMOST_PATH_HINT_NAMESPACE,
                root.as_bytes(),
                &hint,
            ),
            origin,
        ))
        .await
        .map_err(|error| Error::Store(Box::new(error)))?;
} else {
    self.store
        .publish_nodes(NodePublication::new(&entries, origin))
        .await
        .map_err(|error| Error::Store(Box::new(error)))?;
}
```

Keep replay discovery, validation, cache insertion, metrics, and empty-write behavior unchanged. Keep the hint bytes alive through the awaited call by placing publication and the call in the hint branch when necessary.

- [x] **Step 4: Assign public point, batch, and range origins**

Add `origin: PublicationOrigin` to the internal canonical batch entry points. Define private helpers on both sync and async managers:

```rust,ignore
pub(crate) fn put_with_origin(
    &self,
    tree: &Tree,
    key: Vec<u8>,
    val: Vec<u8>,
    origin: PublicationOrigin,
) -> Result<Tree, Error> {
    self.batch_with_origin(tree, vec![Mutation::Upsert { key, val }], origin)
}

pub(crate) fn batch_with_origin(
    &self,
    tree: &Tree,
    mutations: Vec<Mutation>,
    origin: PublicationOrigin,
) -> Result<Tree, Error> {
    self.engine
        .canonical_batch_tree_ready(tree, mutations, origin)
}
```

Implement the async forms with `async fn`, `.await`, and the native async canonical entry. Route public APIs:

```rust,ignore
pub fn put(...) -> Result<Tree, Error> {
    self.put_with_origin(
        tree,
        key,
        val,
        PublicationOrigin::PointUpsert,
    )
}

pub fn delete(...) -> Result<Tree, Error> {
    self.batch_with_origin(
        tree,
        vec![Mutation::Delete { key: key.to_vec() }],
        PublicationOrigin::PointDelete,
    )
}

pub fn batch(...) -> Result<Tree, Error> {
    self.batch_with_origin(tree, mutations, PublicationOrigin::BatchMutation)
}
```

Implement the same routing for async methods. Keep one-item public batch as `BatchMutation`. Hardcode `RangeDelete` in sync and async range-delete engine entries. Pass `BatchMutation` from configured and stats-producing public batch APIs.

- [x] **Step 5: Add post-implementation sync/async routing tests**

In `tests/node_publication.rs`, implement `RecordingSyncStore` and `RecordingAsyncStore` over `MemStore`. Record owned entries, hint, and origin in each `publish_nodes` override before delegating.

Exercise changed operations and assert exact origins:

```rust,ignore
assert_eq!(sync_origins(&sync_put_store), vec![PublicationOrigin::PointUpsert]);
assert_eq!(sync_origins(&sync_delete_store), vec![PublicationOrigin::PointDelete]);
assert_eq!(sync_origins(&sync_batch_store), vec![PublicationOrigin::BatchMutation]);
assert_eq!(sync_origins(&sync_range_store), vec![PublicationOrigin::RangeDelete]);

assert_eq!(async_origins(&async_put_store), vec![PublicationOrigin::PointUpsert]);
assert_eq!(async_origins(&async_delete_store), vec![PublicationOrigin::PointDelete]);
assert_eq!(async_origins(&async_batch_store), vec![PublicationOrigin::BatchMutation]);
assert_eq!(async_origins(&async_range_store), vec![PublicationOrigin::RangeDelete]);
```

For every operation, compare the returned root and a recursively collected reachable-node map with an uninstrumented `MemStore` execution. Add an unchanged point operation assertion that emits no publication.

- [x] **Step 6: Run focused verification**

```sh
cargo fmt --all -- --check
cargo test -p prolly-map --test node_publication --all-features
cargo test -p prolly-map --test ready_sync --all-features
cargo test -p prolly-map --test canonical_range_delete --all-features
cargo test -p prolly-map --test canonical_roots --all-features
```

Expected: every command exits `0`; roots and reachable bytes are identical; sync publication remains runtime-free.

- [x] **Step 7: Commit core routing**

```sh
git add src/prolly/engine/write.rs src/prolly/mod.rs tests/node_publication.rs
git commit -m "feat: route publication origin through core writers"
```

## Task 3: Classify Builders, Replication, Transactions, and Transparent Wrappers

This task closes publication paths that do not originate in the main point/batch writer.

**Files:**

- Modify: `src/prolly/builder.rs`
- Modify: `src/prolly/mod.rs`
- Modify: `src/prolly/transaction.rs`
- Modify: `src/prolly/proximity/search/runtime.rs`
- Modify: `src/prolly/range_delete.rs`
- Modify: `stores/prolly-store-slatedb/benches/slatedb_ops_bench.rs`
- Modify: `stores/prolly-store-slatedb/benches/slatedb_workload_bench.rs`
- Modify: `tests/node_publication.rs`
- Modify: `tests/transactions.rs`
- Modify: `tests/snapshot_manager.rs`

**Interfaces:**

- Consumes: origin-aware core paths from Task 2.
- Produces: `TreeBuild` and `Replication` routing, explicit wrapper forwarding, and overlay absorption.

- [x] **Step 1: Publish standalone builder output as `TreeBuild`**

Change `builder::persist_nodes` and test-only serial builder publication from `batch_put` to:

```rust,ignore
store
    .publish_nodes(NodePublication::new(
        &entries,
        PublicationOrigin::TreeBuild,
    ))
    .map_err(|error| Error::Store(Box::new(error)))
```

Change async builder helpers to accept an origin:

```rust,ignore
pub(crate) async fn publish_builder_nodes(
    &self,
    nodes: &[builder::DeferredNode],
    origin: PublicationOrigin,
) -> Result<(), Error>
```

Pass `TreeBuild` from public sorted and unsorted build APIs. Pass the parent origin from any nested builder caller. When a sync builder receives `PublicationStore`, its outer facade origin intentionally replaces `TreeBuild`.

- [x] **Step 2: Classify snapshot import and missing-node copy as `Replication`**

Replace both async destination batches in `copy_missing_nodes` and `import_snapshot` with:

```rust,ignore
destination
    .publish_nodes(NodePublication::new(
        &entries,
        PublicationOrigin::Replication,
    ))
    .await
    .map_err(|error| Error::Store(Box::new(error)))?;
```

The synchronous APIs already enter through `SyncStoreAsAsync` and therefore forward the same request inline.

- [x] **Step 3: Forward through transparent wrappers**

Add sync and async `publish_nodes` overrides to `SearchIo<S>` that call `self.store.publish_nodes(publication)`. Add an override to the production range-delete counting store and the two SlateDB benchmark counting stores that records the same write count as `batch_put` and then forwards `publication` unchanged.

- [x] **Step 4: Absorb origin in transaction overlays**

Add `publish_nodes` to all four overlay stores:

```rust,ignore
fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
    self.batch_put(publication.entries())
}
```

Use the awaited form for async overlays. Do not forward origin or hints to the base store. Keep `supports_hints() == false`. Keep final strict `commit_transaction` unchanged and general.

- [x] **Step 5: Add post-implementation tests**

Extend `tests/node_publication.rs` with `TreeBuild` and `Replication` assertions for sync and async build, copy, and import. Extend `tests/transactions.rs` so borrowed and owned sync/async overlays receive a point publication, can read staged nodes, and leave the base recorder at zero publication calls before commit.

Add this wrapper assertion:

```rust,ignore
assert_eq!(
    recorder.take_publications(),
    vec![RecordedPublication {
        origin: PublicationOrigin::TreeBuild,
        hinted: true,
        entries: expected_entries,
    }]
);
```

- [x] **Step 6: Run focused verification**

```sh
cargo fmt --all -- --check
cargo test -p prolly-map --test node_publication --all-features
cargo test -p prolly-map --test transactions --all-features
cargo test -p prolly-map --test snapshot_manager --all-features
cargo test -p prolly-map builder --all-features
cargo check --manifest-path stores/prolly-store-slatedb/Cargo.toml --benches
```

Expected: all commands exit `0`; overlays stage but do not leak origin; copy/import validate exact bytes.

- [x] **Step 7: Commit direct-path routing**

```sh
git add src/prolly/builder.rs src/prolly/mod.rs src/prolly/transaction.rs \
  src/prolly/proximity/search/runtime.rs src/prolly/range_delete.rs \
  stores/prolly-store-slatedb/benches/slatedb_ops_bench.rs \
  stores/prolly-store-slatedb/benches/slatedb_workload_bench.rs \
  tests/node_publication.rs tests/transactions.rs tests/snapshot_manager.rs
git commit -m "feat: classify direct node publications"
```

## Task 4: Classify Merge and Maintenance Publications

This task gives structural/fallback merge and content-addressed derived data their reviewed origins.

**Files:**

- Modify: `src/prolly/diff.rs`
- Modify: `src/prolly/proximity/map.rs`
- Modify: `src/prolly/proximity/search/engine.rs`
- Modify: `src/prolly/proximity/search/async.rs`
- Modify: `src/prolly/proximity/accelerator/catalog.rs`
- Modify: `src/prolly/proximity/accelerator/composite.rs`
- Modify: `src/prolly/proximity/accelerator/hnsw/storage.rs`
- Modify: `src/prolly/proximity/accelerator/pq.rs`
- Modify: `src/prolly/secondary_index/coordinator.rs`
- Modify: `tests/node_publication.rs`
- Modify: `tests/diff_merge.rs`
- Modify: `tests/secondary_index.rs`
- Modify: `tests/proximity_mutation.rs`

**Interfaces:**

- Consumes: private sync/async `batch_with_origin` and `put_with_origin` helpers from Task 2.
- Produces: complete `Merge` and `Maintenance` coverage.

- [x] **Step 1: Route every merge writer as `Merge`**

Pass `PublicationOrigin::Merge` to structural `execute_replay`. Replace every sync and async merge fallback call to public `batch` with the private origin-aware helper:

```rust,ignore
prolly
    .batch_with_origin(left, mutations, PublicationOrigin::Merge)
    .await
```

Use the synchronous form without `.await` in sync merge code. Cover chunked fallback flushes, traced merges, borrowed resolver merges, range-limited merges, and CRDT merge fallbacks. Read-only diff emits no request.

- [x] **Step 2: Route direct content-addressed maintenance writes**

Replace direct CID-keyed `put` and `batch_put` in proximity modules with one-entry or batched publication:

```rust,ignore
let entries = [(cid.as_bytes(), bytes)];
store
    .publish_nodes(NodePublication::new(
        &entries,
        PublicationOrigin::Maintenance,
    ))
    .map_err(|error| Error::Store(Box::new(error)))?;
```

Use the awaited async form where the store is asynchronous. Retain CID derivation and existing validation before publication.

- [x] **Step 3: Route derived index and catalog tree writes**

Change coordinator-internal source candidates, hidden index trees, catalog trees, and control trees to call `put_with_origin` or `batch_with_origin` with `Maintenance`. Keep public standalone `Prolly::put` and `Prolly::batch` classifications unchanged.

Do not classify temporary in-memory bundle verification as maintenance; it remains a low-level `General` store call because it is not adapter-visible production publication.

- [x] **Step 4: Add post-implementation tests**

Add tests that force structural merge and fallback merge, then assert every emitted request is `Merge` and both routes have byte-identical reachable maps. Add one indexed-map mutation and one proximity mutation over a recorder, then assert all derived tree/descriptor requests are `Maintenance`.

Use these assertions:

```rust,ignore
assert!(merge_origins.iter().all(|origin| *origin == PublicationOrigin::Merge));
assert!(maintenance_origins
    .iter()
    .all(|origin| *origin == PublicationOrigin::Maintenance));
assert_eq!(reachable_bytes(&recorded_store, &tree), reachable_bytes(&control_store, &control_tree));
```

- [x] **Step 5: Audit direct source writes**

Run:

```sh
rg -n "\.(put|batch_put|batch_put_with_hint)\(" src/prolly -g '*.rs'
```

Classify every production content-addressed write as `PublicationStore` interception, explicit `publish_nodes`, transaction staging, replay staging, or documented low-level `General` behavior. Any unclassified production write blocks the commit.

- [x] **Step 6: Run focused verification**

```sh
cargo fmt --all -- --check
cargo test -p prolly-map --test node_publication --all-features
cargo test -p prolly-map --test diff_merge --all-features
cargo test -p prolly-map --test range_limited_merge --all-features
cargo test -p prolly-map --test secondary_index --all-features
cargo test -p prolly-map --test proximity_mutation --all-features
```

Expected: every command exits `0` and every changed publication has the reviewed origin.

- [x] **Step 7: Commit merge and maintenance routing**

```sh
git add src/prolly/diff.rs src/prolly/proximity src/prolly/secondary_index \
  tests/node_publication.rs tests/diff_merge.rs tests/secondary_index.rs \
  tests/proximity_mutation.rs
git commit -m "feat: classify merge and maintenance publications"
```

## Task 5: Extend Remote and Adapter Conformance

This task carries the same request through remote backends and proves every first-party adapter retains safe defaults.

**Files:**

- Modify: `src/prolly/remote.rs`
- Modify: `stores/prolly-store-test/src/lib.rs`
- Modify: `tests/common/mod.rs` (shared by `tests/store_conformance.rs`)
- Test: `stores/prolly-store-{postgres,mysql,redis,dynamodb,cosmosdb,spanner,turso}/tests/*.rs`

**Interfaces:**

- Consumes: `NodePublication` from Task 1.
- Produces: `RemoteStoreBackend::publish_nodes`, unconditional remote publication CID verification, and reusable adapter conformance.

- [x] **Step 1: Add the remote backend default and `Arc` forwarding**

Add:

```rust,ignore
async fn publish_nodes(
    &self,
    publication: NodePublication<'_>,
) -> Result<(), Self::Error> {
    match publication.hint() {
        Some(hint) => self
            .batch_put_nodes_with_hint(
                publication.entries(),
                hint.namespace(),
                hint.key(),
                hint.value(),
            )
            .await,
        None => self.batch_put_nodes(publication.entries()).await,
    }
}
```

The `Arc<T>` implementation forwards `(**self).publish_nodes(publication).await`.

- [x] **Step 2: Verify before forwarding from `RemoteProllyStore`**

Override `AsyncStore::publish_nodes`:

```rust,ignore
async fn publish_nodes(
    &self,
    publication: NodePublication<'_>,
) -> Result<(), Self::Error> {
    for (key, value) in publication.entries() {
        verify_node_cid::<B::Error>(key, value)?;
    }
    self.backend
        .publish_nodes(publication)
        .await
        .map_err(backend_error)
}
```

Call `verify_node_cid` directly so the legacy `verify_node_cids` toggle cannot bypass publication validation. Keep existing direct read/write configuration behavior unchanged.

- [x] **Step 3: Extend reusable conformance**

In remote conformance, publish valid CID/value pairs with every current origin and one hinted request. Assert the backend returns the exact bytes and hint. Add a recording backend test that asserts `RemoteProllyStore` preserves origin exactly once and rejects a mismatched CID before its backend counter increments. Repeat the mismatch assertion with `RemoteStoreConfig { verify_node_cids: false }` to prove the legacy toggle cannot disable publication validation.

In `prolly-store-test`, extend `assert_store_contract` with:

```rust,ignore
let bytes = b"published-node";
let cid = Cid::from_bytes(bytes);
store
    .publish_nodes(NodePublication::new(
        &[(cid.as_bytes(), bytes)],
        PublicationOrigin::PointUpsert,
    ))
    .unwrap();
assert_eq!(store.get(cid.as_bytes()).unwrap(), Some(bytes.to_vec()));
```

- [x] **Step 4: Compile every adapter on its default path**

Run:

```sh
cargo test -p prolly-store-test
cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml
cargo test --manifest-path stores/prolly-store-rocksdb/Cargo.toml
cargo test --manifest-path stores/prolly-store-slatedb/Cargo.toml
cargo test --manifest-path stores/prolly-store-pglite/Cargo.toml
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml
cargo check --manifest-path stores/prolly-store-postgres/Cargo.toml --all-features
cargo check --manifest-path stores/prolly-store-mysql/Cargo.toml --all-features
cargo check --manifest-path stores/prolly-store-redis/Cargo.toml --all-features
cargo check --manifest-path stores/prolly-store-dynamodb/Cargo.toml --all-features
cargo check --manifest-path stores/prolly-store-cosmosdb/Cargo.toml --all-features
cargo check --manifest-path stores/prolly-store-spanner/Cargo.toml --all-features
```

Expected: locally available tests pass and credentialed providers compile without requiring credentials. No adapter except Turso has a custom publication branch.

- [x] **Step 5: Commit remote and conformance changes**

```sh
git add src/prolly/remote.rs stores/prolly-store-test/src/lib.rs \
  tests/common/mod.rs
git commit -m "feat: forward node publication through remote stores"
```

## Task 6: Carry Publication Context Through Language Bindings

This task prevents foreign store adapters from silently losing origin.

**Files:**

- Create: `bindings/uniffi/src/publication.rs`
- Modify: `bindings/uniffi/src/lib.rs`
- Modify: `bindings/uniffi/src/async_store.rs`
- Modify: inline test modules in `bindings/uniffi/src/lib.rs` and `bindings/uniffi/src/async_store.rs`
- Regenerate: `bindings/python/prolly/uniffi/prolly.py`
- Regenerate: `bindings/kotlin/src/main/kotlin/build/crab/prolly/generated/prolly.kt`
- Regenerate: `bindings/ruby/lib/prolly/generated/prolly.rb`
- Regenerate: `bindings/swift/Sources/Prolly/prolly.swift`
- Regenerate: `bindings/swift/Sources/prollyFFI/include/prollyFFI.h`
- Modify native wrappers under: `bindings/go`, `bindings/node`, `bindings/java`, and `bindings/wasm` where store callbacks are exposed
- Modify provider implementations returned by:
  `rg -l "batch_put_nodes_with_hint|HostStoreCallback|ForeignRemoteStore" bindings --glob '!**/target/**'`
- Modify: `bindings/api/parity.json`
- Modify: `bindings/api/classification-audit.json`
- Modify: `bindings/api/application-gap-report.json`

**Interfaces:**

- Consumes: `PublicationOrigin` and `NodePublication`.
- Produces: stable FFI codes, `NodePublicationRecord`, `HostStoreCallback::publish_nodes`, `ForeignRemoteStore::publish_nodes`, protocol major `2`, and unknown-code general fallback.

- [ ] **Step 1: Implement focused FFI records**

Create `publication.rs`:

```rust,ignore
use prolly::{NodePublication, PublicationOrigin};

pub const GENERAL: u32 = 0;
pub const POINT_UPSERT: u32 = 1;
pub const POINT_DELETE: u32 = 2;
pub const BATCH_MUTATION: u32 = 3;
pub const TREE_BUILD: u32 = 4;
pub const MERGE: u32 = 5;
pub const RANGE_DELETE: u32 = 6;
pub const REPLICATION: u32 = 7;
pub const MAINTENANCE: u32 = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Record)]
pub struct PublicationOriginRecord {
    pub code: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NodeEntryRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NodePublicationHintRecord {
    pub namespace: Vec<u8>,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NodePublicationRecord {
    pub nodes: Vec<NodeEntryRecord>,
    pub hint: Option<NodePublicationHintRecord>,
    pub origin: PublicationOriginRecord,
}
```

Remove `NodeEntryRecord` from `async_store.rs`, define it once in `publication.rs` as shown, and import it into both callback modules. Implement `From<PublicationOrigin>` with a wildcard mapping to `GENERAL`, and implement an owned conversion from `NodePublication<'_>`.

- [ ] **Step 2: Add both callback methods**

Add:

```rust,ignore
fn publish_nodes(
    &self,
    publication: NodePublicationRecord,
) -> HostStoreUnitResultRecord;
```

to `HostStoreCallback`, and add the awaited counterpart returning `UnitResultRecord` to `ForeignRemoteStore`. Override `Store::publish_nodes` for `HostStore` and `RemoteStoreBackend::publish_nodes` for `ForeignRemoteBackend`, preserving size/count checks before callback invocation.

Change `STORE_PROTOCOL_MAJOR` from `1` to `2`.

- [ ] **Step 3: Implement language fallback behavior**

For every checked-in store callback implementation:

- Preserve the received code for application-defined backends.
- Make every first-party default implementation execute its existing general batch or combined node-plus-hint method for codes `0` through `8` and every unknown code.
- Expose an idiomatically named `normalize_publication_origin_code` helper that returns codes `0` through `8` unchanged and maps every other `u32` value to `GENERAL`.
- Preserve one callback invocation and existing error records.
- Keep origin out of persisted rows, manifests, fixtures, and provider synchronization payloads.

Do not add a language-native fast path in this slice; none has adapter-specific measurements yet.

Update every first-party `StoreDescriptorRecord.protocol_major` producer and the compatibility matrices to `2`. A descriptor that still reports `1` must fail construction with the existing invalid-descriptor error instead of running without publication context.

Add named constants with the exact numeric values above to each hand-written package facade.

- [ ] **Step 4: Add post-implementation binding tests**

Rust facade tests must pass `PointUpsert` with a hint through both callbacks, assert exact owned bytes, then pass `PublicationOriginRecord { code: 4_294_967_295 }` to a language/default dispatcher and assert it takes the general path.

Construct one remote descriptor with protocol major `1` and assert `ForeignRemoteBackend::new` rejects it. Construct the same descriptor with major `2` and assert publication reaches its callback.

Add one callback test in Python and one JVM or Swift generated-binding test:

```python
publication = captured_publications.pop()
assert publication.origin.code == POINT_UPSERT
assert publication.nodes[0].value == expected_bytes
assert publication.hint.namespace == b"rightmost"
assert normalize_publication_origin_code(0xFFFFFFFF) == GENERAL
```

- [ ] **Step 5: Regenerate and verify binding surfaces**

Run the provenance commands from `bindings/VERIFICATION.md`, then:

```sh
cargo test --manifest-path bindings/uniffi/Cargo.toml --target-dir target
python3 scripts/binding_api_inventory.py generate
python3 scripts/binding_api_inventory.py check
mvn -f bindings/pom.xml test
(cd bindings/go && go test ./...)
npm --prefix bindings/node test
cargo check --manifest-path bindings/wasm/Cargo.toml \
  --target wasm32-unknown-unknown --target-dir target
DYLD_LIBRARY_PATH="$PWD/target/debug" \
  swift run --package-path bindings/swift prolly-fixture-check
```

Run the Ruby and Python callback suites with the library path commands documented in `bindings/README.md`. Expected: all installed local binding prerequisites pass; unavailable optional prerequisites are recorded, never reported as passing.

- [ ] **Step 6: Audit generated artifacts**

Run:

```sh
find bindings -type d \( -name node_modules -o -name target -o -name .build -o -name __pycache__ -o -name pkg \) -prune -print
git status --short
```

Remove only newly generated untracked build artifacts, preserving checked-in generated source. Do not commit native binaries, dependency directories, or local lockfiles.

- [ ] **Step 7: Commit binding propagation**

```sh
git add bindings scripts/binding_api_inventory.py
git commit -m "feat: expose node publication to store bindings"
```

## Task 7: Add the Evidence-Backed Turso Fast Path

This task changes only local transaction lock-acquisition timing for explicit point upserts.

**Files:**

- Modify: `stores/prolly-store-turso/src/lib.rs`
- Modify: `stores/prolly-store-turso/tests/turso_backend.rs`
- Modify: `stores/prolly-store-turso/README.md`

**Interfaces:**

- Consumes: `RemoteStoreBackend::publish_nodes` and `PublicationOrigin`.
- Produces: `publication_transaction_behavior` and a shared Turso publication transaction helper.

- [ ] **Step 1: Implement an exhaustive-safe selector**

Add:

```rust,ignore
fn publication_transaction_behavior(origin: PublicationOrigin) -> TransactionBehavior {
    match origin {
        PublicationOrigin::PointUpsert => TransactionBehavior::Deferred,
        _ => TransactionBehavior::Immediate,
    }
}
```

The wildcard is mandatory because the enum is non-exhaustive and future origins must remain immediate.

- [ ] **Step 2: Consolidate publication transaction code**

Add:

```rust,ignore
async fn publish_node_entries(
    &self,
    publication: NodePublication<'_>,
    behavior: TransactionBehavior,
) -> Result<(), TursoStoreError> {
    if publication.entries().is_empty() {
        return Ok(());
    }
    let mut connection = self.connect().await?;
    let transaction = connection.transaction_with_behavior(behavior).await?;
    apply_node_entries(&transaction, publication.entries()).await?;
    if let Some(hint) = publication.hint() {
        transaction
            .execute(
                UPSERT_HINT_SQL,
                (hint.namespace(), hint.key(), hint.value()),
            )
            .await?;
    }
    transaction.commit().await?;
    Ok(())
}
```

Override backend `publish_nodes` and pass the selector result. Keep existing `batch_put_nodes`, `batch_put_nodes_with_hint`, `batch_nodes`, root CAS, and strict commit on `Immediate` by routing them through the helper with `General` or retaining their current transaction code.

- [ ] **Step 3: Add post-implementation Turso tests**

Add selector assertions for every current origin. Exercise hinted and unhinted `PointUpsert` publication through `TursoStore` with valid CID/value pairs. Reuse the existing rejecting hint trigger to prove the deferred node-plus-hint transaction rolls back both.

Add close/reopen verification:

```rust,ignore
drop(prolly);
drop(store);
drop(backend);

let reopened = TursoStore::new(TursoBackend::open(&path).await.unwrap());
let reopened = AsyncProlly::new(reopened, tree.config.clone());
assert_eq!(reopened.get(&tree, b"key").await.unwrap(), Some(b"value".to_vec()));
```

Add two concurrent point upserts. Accept success or the existing documented busy error, then reopen and verify every successful tree without retrying inside the adapter.

- [ ] **Step 4: Run Turso correctness gates**

```sh
cargo fmt --all -- --check
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml \
  --features turso-cloud-sync
cargo clippy --manifest-path stores/prolly-store-turso/Cargo.toml \
  --all-targets --all-features -- -D warnings
```

Expected: local tests pass without credentials; the cloud integration remains guarded; no test invokes synchronization.

- [ ] **Step 5: Commit the isolated override**

```sh
git add stores/prolly-store-turso/src/lib.rs \
  stores/prolly-store-turso/tests/turso_backend.rs \
  stores/prolly-store-turso/README.md
git commit -m "perf: defer Turso point publication locks"
```

## Task 8: Complete Correctness and Coverage Verification

This task runs the universal contract against all core paths before any performance claim.

**Files:**

- Modify: `tests/node_publication.rs`
- Modify: `tests/invariants.rs`
- Modify: `tests/performance_hints.rs`
- Modify: `tests/store_conformance.rs`
- Modify: `src/prolly/remote.rs` test module

**Interfaces:**

- Consumes: Tasks 1 through 7.
- Produces: final cross-origin identity, allocation, failure, and coverage evidence.

- [ ] **Step 1: Add the cross-origin identity matrix**

For each current origin, execute a representative changed operation through sync and async recording stores. Collect the returned root and every reachable `(Cid, bytes)` pair. Assert:

```rust,ignore
assert_eq!(sync_tree.root, async_tree.root);
assert_eq!(sync_reachable, async_reachable);
assert!(recorded
    .iter()
    .all(|publication| publication.origin == expected_origin));
assert!(recorded
    .iter()
    .all(|publication| publication.entries.iter().all(cid_matches_bytes)));
```

Cover hinted append publication and unhinted random/clustered publication. Assert node work stays bounded by tree height and point operations issue at most one final async publication batch.

Run every canonical sync writer over an inner store whose generic `batch` and `delete` methods panic while its `publish_nodes` override succeeds. This is the regression guard that canonical immutable publication never escapes through the facade's unclassified generic mutation methods.

- [ ] **Step 2: Add failure and allocation checks**

Use existing failing stores to make publication fail after canonical generation. Assert no usable tree is returned and the original tree remains readable. Use the repository allocation-counting helper around construction/default dispatch and assert the request itself adds zero allocations; exclude existing `batch_put` fallback allocation from that assertion.

Add an async acknowledgment store whose publication future remains pending until a test flag is released. Assert the engine future cannot return a tree before acknowledgment. Drop one pending publication future and assert no named root changes; unreachable immutable nodes are allowed. Resume a fresh operation and assert normal success, proving cancellation adds no hidden retry or poisoned adapter state.

- [ ] **Step 3: Run the complete local correctness gate**

```sh
cargo fmt --all -- --check
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo check -p prolly-map --no-default-features
cargo check --manifest-path bindings/wasm/Cargo.toml \
  --target wasm32-unknown-unknown
git diff --check
```

Expected: every non-environment-gated test passes; Clippy has no warning; WASM and no-default core remain runtime-neutral.

- [ ] **Step 4: Audit every implementation and publication call site**

Run:

```sh
rg -n '^impl.*\b(Store|AsyncStore|RemoteStoreBackend)\b.*\bfor\b' \
  src stores bindings -g '*.rs'
rg -n 'publish_nodes|batch_put|batch_put_with_hint' \
  src/prolly stores bindings/uniffi/src -g '*.rs'
```

Record each implementation in the task notes as default, transparent forwarding, semantic absorption, validation boundary, foreign boundary, or measured override. No unclassified production implementation may proceed to benchmarks.

- [ ] **Step 5: Commit the final correctness matrix**

```sh
git add tests src/prolly/remote.rs
git commit -m "test: verify universal publication invariants"
```

## Task 9: Build Reproducible Performance Gates

This task adds deterministic paired tooling before collecting acceptance results.

**Files:**

- Modify: `benches/async_first_foundation_bench.rs`
- Create: `scripts/run_node_publication_revision_gate.sh`
- Create: `scripts/summarize_node_publication_revision_gate.py`
- Create: `scripts/tests/test_summarize_node_publication_revision_gate.py`
- Create: `benchmarks/local-store-publication/Cargo.toml`
- Create: `benchmarks/local-store-publication/src/main.rs`
- Create: `benchmarks/local-store-publication/src/model.rs`
- Create: `benchmarks/local-store-publication/src/sync_runner.rs`
- Create: `benchmarks/local-store-publication/src/turso_runner.rs`
- Create: `scripts/run_local_store_publication_revision_gate.sh`

**Interfaces:**

- Consumes: baseline revision `a2f4e7a3066d2259783c8d815b45c0461c77b06b` and the verified candidate.
- Produces: alternating process pairs, merged revision-labelled rows, directional gate summaries, and local-only provenance.

- [ ] **Step 1: Extend the in-memory overhead screen**

Add point upsert, point delete, one-item batch, build, merge, range delete, and direct request forwarding to `async_first_foundation_bench`. Emit:

```text
revision,facade,api,records,items_per_sample,samples,median_ns,p95_ns,throughput_items_per_sec,publication_calls,request_allocations,root
```

Run sync-ready, async-over-sync, and native async recorder paths with identical seeds and assert equal roots before timing.

- [ ] **Step 2: Implement the all-local-adapter workload harness**

Create a standalone benchmark package with path dependencies on `prolly-map`, SQLite, RocksDB, SlateDB, PGlite, and Turso. The core crate supplies memory and file stores. Use native synchronous `Store` paths for memory, file, SQLite, RocksDB, SlateDB, and PGlite; use native `AsyncProlly<TursoStore>` for Turso.

The CLI accepts:

```text
--output PATH --records N --changes N --runs N
--adapters CSV --apis CSV --patterns CSV --revision TEXT
```

Run `put,batch,build,diff,merge,reopen` for `append,random,clustered` with identical keys, values, seeds, tree configuration, cold-manager policy, and local durability settings. Emit one row per adapter/API/pattern/run:

```text
revision,adapter,records,changes,api,pattern,run,total_ns,operations_per_sec,p50_ns,p95_ns,root,node_count,byte_count,value_valid,count_valid,root_valid,reopen_valid
```

Each runner must verify values, record count, root stability, reachable CIDs, and reopen behavior before writing `true` validation fields. The harness must skip no requested adapter silently: a missing PGlite sidecar or other prerequisite produces an explicit nonzero exit and a named environment-limitation record from the orchestration script.

- [ ] **Step 3: Implement alternating SQLite/Turso revision orchestration**

The shell script accepts:

```text
--suite foundation|sqlite-turso
--baseline-repo PATH
--candidate-repo PATH
--output PATH
--sizes CSV
--runs N
--changes N|auto
--apis CSV
--patterns CSV
--adapters CSV
```

Build each revision once with separate `CARGO_TARGET_DIR` values and `CARGO_INCREMENTAL=0`. The `foundation` suite runs `async_first_foundation_bench`; the `sqlite-turso` suite runs the existing local comparison binary. For repetition `1..N`, run baseline then candidate on odd repetitions and candidate then baseline on even repetitions. Each invocation uses a fresh output directory and one internal repetition. Concatenate validated raw rows with added `revision_role` and `pair` columns. Refuse a dirty baseline and refuse any dependency graph containing `turso-cloud-sync`.

- [ ] **Step 4: Implement alternating all-local-adapter orchestration**

`run_local_store_publication_revision_gate.sh` accepts the same baseline, candidate, output, and run arguments plus the local-adapter list. Build the harness once per revision in separate target directories. Alternate revision order per repetition, give every cell a fresh database directory on the same volume, concatenate revision-labelled rows, and write `environment-limitations.csv` for prerequisites that are genuinely unavailable.

Use this required adapter list unless a recorded prerequisite prevents execution:

```text
memory-sync,file-sync,sqlite-sync,rocksdb-sync,slatedb-sync,pglite-sync,turso-async
```

- [ ] **Step 5: Implement directional summarization**

The Python summarizer groups by suite, adapter or facade, records, API, pattern, and revision role. It calculates median total latency, throughput, p50, p95, and percent change. Exit nonzero when:

- candidate median latency rises more than 5%;
- candidate median throughput falls more than 5%;
- candidate p95 rises more than 10%;
- focused Turso point-put latency improves less than 40% for any pattern;
- any row fails value, count, root, reopen, or fixture validation.

Write `summary.csv`, `gate.csv`, and `report.md` with machine and revision provenance.

- [ ] **Step 6: Add post-implementation script tests**

Create synthetic CSV fixtures covering pass, latency regression, throughput regression, p95 regression, missing pair, validation failure, environment limitation, and Turso target miss. Assert exact exit codes and gate reasons:

```python
self.assertEqual(result.returncode, 2)
self.assertIn("median_latency_regression", result.stderr)
self.assertIn("turso_point_target_miss", result.stderr)
```

- [ ] **Step 7: Verify the tooling with smoke runs**

```sh
if [[ ! -d /private/tmp/prolly-node-publication-baseline ]]; then
  git worktree add --detach /private/tmp/prolly-node-publication-baseline \
    a2f4e7a3066d2259783c8d815b45c0461c77b06b
fi
python3 -m unittest scripts.tests.test_summarize_node_publication_revision_gate -v
bash -n scripts/run_node_publication_revision_gate.sh
bash -n scripts/run_local_store_publication_revision_gate.sh
if command -v shellcheck >/dev/null 2>&1; then
  shellcheck scripts/run_node_publication_revision_gate.sh \
    scripts/run_local_store_publication_revision_gate.sh
fi
scripts/run_node_publication_revision_gate.sh \
  --suite sqlite-turso \
  --baseline-repo /private/tmp/prolly-node-publication-baseline \
  --candidate-repo /Users/haipingfu/CrabDB-worktrees/prolly-async-first \
  --output /tmp/node-publication-smoke \
  --sizes 10000 --runs 1 --changes 100 \
  --apis put,batch,diff,merge \
  --patterns append,random,clustered \
  --adapters sqlite-sync,turso-async
scripts/run_local_store_publication_revision_gate.sh \
  --baseline-repo /private/tmp/prolly-node-publication-baseline \
  --candidate-repo /Users/haipingfu/CrabDB-worktrees/prolly-async-first \
  --output /tmp/node-publication-local-adapters-smoke \
  --records 10000 --changes 100 --runs 1 \
  --apis put,batch,build,diff,merge,reopen \
  --patterns append,random,clustered \
  --adapters memory-sync,file-sync,sqlite-sync,rocksdb-sync,slatedb-sync,pglite-sync,turso-async
```

Expected: script tests pass, smoke rows validate, and the smoke report is explicitly marked statistically insufficient.

- [ ] **Step 8: Commit reproducible tooling**

```sh
git add benches/async_first_foundation_bench.rs \
  benchmarks/local-store-publication scripts
git commit -m "perf: add universal publication regression gates"
```

## Task 10: Run Local Acceptance, Document Results, and Update PR 24

This task collects release evidence only after correctness and tooling pass.

**Files:**

- Create: `performance-results/node-publication-focused-2026-07-19/`
- Create: `performance-results/node-publication-full-2026-07-19/`
- Create: `performance-results/node-publication-local-adapters-2026-07-19/`
- Modify: `docs/sqlite-turso-local-performance.md`
- Modify: `stores/prolly-store-turso/README.md`
- Modify: `docs/superpowers/specs/2026-07-19-point-publication-intent-design.md`
- Modify: pull request `crabbuild/prolly#24`

**Interfaces:**

- Consumes: verified implementation and Task 9 tooling.
- Produces: focused and full local evidence, adapter screens, final documentation, pushed commits, and an updated pull request.

- [ ] **Step 1: Verify the clean baseline created by Task 9**

Verify:

```sh
git -C /private/tmp/prolly-node-publication-baseline rev-parse HEAD
git -C /private/tmp/prolly-node-publication-baseline status --porcelain
```

Expected: HEAD is `a2f4e7a3066d2259783c8d815b45c0461c77b06b` and status is empty. Stop rather than reusing the directory if either check differs. Build baseline and candidate into different target directories.

- [ ] **Step 2: Run five alternating focused pairs**

```sh
scripts/run_node_publication_revision_gate.sh \
  --suite sqlite-turso \
  --baseline-repo /private/tmp/prolly-node-publication-baseline \
  --candidate-repo /Users/haipingfu/CrabDB-worktrees/prolly-async-first \
  --output performance-results/node-publication-focused-2026-07-19 \
  --sizes 10000 --runs 5 --changes 100 \
  --apis put,batch,diff,merge \
  --patterns append,random,clustered \
  --adapters sqlite-sync,turso-async
```

Require at least 40% lower Turso point-put median latency for every pattern and all universal 5%/10% no-regression gates. If a protected cell crosses a gate, run five additional alternating pairs and evaluate the combined ten.

- [ ] **Step 3: Run in-memory and all-local-adapter screens**

Run five alternating baseline/candidate executions at 10,000 records:

```sh
scripts/run_node_publication_revision_gate.sh \
  --suite foundation \
  --baseline-repo /private/tmp/prolly-node-publication-baseline \
  --candidate-repo /Users/haipingfu/CrabDB-worktrees/prolly-async-first \
  --output performance-results/node-publication-local-adapters-2026-07-19/foundation \
  --sizes 10000 --runs 5 --changes 100 \
  --apis put,delete,batch,build,merge,range-delete,forward \
  --patterns random \
  --adapters memory-sync,memory-async-adapted
scripts/run_local_store_publication_revision_gate.sh \
  --baseline-repo /private/tmp/prolly-node-publication-baseline \
  --candidate-repo /Users/haipingfu/CrabDB-worktrees/prolly-async-first \
  --output performance-results/node-publication-local-adapters-2026-07-19/stores \
  --records 10000 --changes 100 --runs 5 \
  --apis put,batch,build,diff,merge,reopen \
  --patterns append,random,clustered \
  --adapters memory-sync,file-sync,sqlite-sync,rocksdb-sync,slatedb-sync,pglite-sync,turso-async
```

Record unavailable optional prerequisites in `environment-limitations.csv`. Default adapters must pass the universal no-regression gate; do not add a custom override based on noise.

- [ ] **Step 4: Run the full 432-row candidate matrix**

The requested record sizes are 10K, 50K, 100K, 500K, 1M, and 2M.

```sh
scripts/run_node_publication_revision_gate.sh \
  --suite sqlite-turso \
  --baseline-repo /private/tmp/prolly-node-publication-baseline \
  --candidate-repo /Users/haipingfu/CrabDB-worktrees/prolly-async-first \
  --output performance-results/node-publication-full-2026-07-19 \
  --sizes 10000,50000,100000,500000,1000000,2000000 \
  --runs 3 --changes auto \
  --apis put,batch,diff,merge \
  --patterns append,random,clustered \
  --adapters sqlite-sync,turso-async
```

Expected: all 432 candidate rows validate with no skip. Turso point put improves at every size/pattern, point work remains bounded by tree height, and no protected median or p95 gate fails.

- [ ] **Step 5: Update documentation with measured facts**

Document:

- native `Prolly<SqliteStore>` versus native `AsyncProlly<RemoteProllyStore<TursoBackend>>`;
- before/after throughput, total latency, p50, p95, p99, and percent change;
- `PointUpsert -> Deferred` and every other Turso path `-> Immediate`;
- exact revisions, compiler, machine, filesystem, features, cache policy, durability settings, and seeds;
- all unavailable adapter prerequisites;
- the local-only limitation and explicit absence of cloud synchronization.

Change the design status to implemented only after all correctness and performance gates pass.

- [ ] **Step 6: Run final repository verification**

```sh
cargo fmt --all -- --check
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
python3 scripts/binding_api_inventory.py check --release
git diff --check
git status --short
```

Expected: all checks pass and status contains only the intended reports and documentation before the final commit.

- [ ] **Step 7: Commit final evidence**

```sh
git add docs/sqlite-turso-local-performance.md \
  docs/superpowers/specs/2026-07-19-point-publication-intent-design.md \
  stores/prolly-store-turso/README.md \
  performance-results/node-publication-focused-2026-07-19 \
  performance-results/node-publication-full-2026-07-19 \
  performance-results/node-publication-local-adapters-2026-07-19
git commit -m "docs: record universal publication performance"
```

- [ ] **Step 8: Push and update pull request 24**

Before publishing, invoke `github:yeet` and `superpowers:verification-before-completion`. Push `codex/async-first-prolly-engine` to `origin`. Update `crabbuild/prolly#24` so its summary leads with the universal `NodePublication` architecture, lists correctness gates, identifies Turso as the first measured override, links the local result reports, and states that every performance claim is local-only.

Verify:

```sh
git status --short --branch
git log --oneline origin/codex/async-first-prolly-engine..HEAD
gh pr view 24 --repo crabbuild/prolly --json url,headRefName,statusCheckRollup
```

Expected: the worktree is clean, the local branch is not ahead of origin, PR 24 points at the pushed head, and checks are visible.
