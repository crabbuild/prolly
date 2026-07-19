# Point publication intent implementation plan

> **Superseded:** Do not execute this Turso-scoped plan. The approved [universal node-publication design](../specs/2026-07-19-point-publication-intent-design.md) now covers every synchronous and asynchronous store. A replacement plan will follow written-spec approval.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce native local Turso point-put latency by carrying explicit point-publication intent from `AsyncProlly::put` to the backend without changing canonical results or generic write behavior.

**Architecture:** Add a runtime-only `NodeWriteIntent` to the async node-publication contract. `ProllyEngine` marks only public point upserts, transparent wrappers preserve the intent, transaction overlays absorb it, and `TursoBackend` selects deferred transactions only for `PointUpsert`. All canonical planning, validation, publication atomicity, ready-sync execution, and general transaction paths remain unchanged.

**Tech stack:** Rust 2021, async functions in traits, the runtime-neutral Prolly async engine, native Turso 0.7, Tokio integration tests, and the existing local SQLite/Turso benchmark harness.

**Content type:** How-to implementation plan

**Audience:** Prolly maintainers implementing and reviewing pull request 24

**Content plan:** Define the contract, route intent, optimize Turso, verify correctness, measure local performance, and publish the evidence

**Open questions:** None

## Global constraints

- Follow the approved design in `docs/superpowers/specs/2026-07-19-point-publication-intent-design.md`
- Do not use test-driven development: implement each reviewed slice first, then add and run its regression tests before committing
- Treat correctness as release-blocking: canonical roots, node bytes, CIDs, values, counts, atomicity, and reopen checks admit no exception
- Use `NodeWriteIntent::{General, PointUpsert}` and keep the enum runtime-only and non-exhaustive
- Mark only `AsyncProlly::put` and its `put_large_value` tree publication as `PointUpsert`
- Keep one-item `AsyncProlly::batch`, delete, range delete, builders, diff preparation, merge preparation, root operations, and strict commits on `General`
- Preserve node-plus-hint atomicity and remote CID verification
- Preserve ready-sync and Tokio-blocking synchronous store selection
- Keep Turso Cloud synchronization disabled during every performance run
- Block release on any protected median latency regression above 5% or any p95 regression above 10%
- Make commits only in `/Users/haipingfu/CrabDB-worktrees/prolly-async-first`

---

## File map

This plan keeps the existing large modules because each change extends an established trait or engine boundary:

| File | Responsibility in this change |
| --- | --- |
| `src/prolly/store/mod.rs` | Define `NodeWriteIntent`, default delegation, and transparent async wrapper behavior |
| `src/lib.rs` | Re-export `NodeWriteIntent` for adapter crates |
| `src/prolly/proximity/search/runtime.rs` | Preserve intent through the transparent `SearchIo` write facade |
| `src/prolly/remote.rs` | Extend remote backend publication and retain CID validation |
| `src/prolly/engine/write.rs` | Carry intent only through final replay publication |
| `src/prolly/mod.rs` | Route `AsyncProlly::put` through a dedicated point-upsert engine entry |
| `src/prolly/transaction.rs` | Prove overlays absorb intent and keep hints disabled until general commit |
| `stores/prolly-store-turso/src/lib.rs` | Select and execute deferred point-publication transactions |
| `tests/async_store.rs` | Verify public API routing, canonical identity, hints, and sync stability |
| `stores/prolly-store-turso/tests/turso_backend.rs` | Verify local persistence, atomic rollback, and concurrency |
| `stores/prolly-store-turso/README.md` | Document the local transaction policy |
| `docs/sqlite-turso-local-performance.md` | Record focused and full comparison evidence |

## Task 1: Add the async node-write intent contract

This task introduces the adapter contract and preserves existing behavior for stores without an override.

**Files:**

- Modify: `src/prolly/store/mod.rs:343-480`
- Modify: `src/prolly/store/mod.rs:579-670`
- Modify: `src/prolly/store/mod.rs:791-955`
- Modify: `src/prolly/store/mod.rs:1179-1270`
- Modify: `src/prolly/proximity/search/runtime.rs:274-350`
- Modify: `src/lib.rs:394`
- Test: `src/prolly/store/mod.rs:1370-end`

**Interfaces:**

- Consumes: existing `AsyncStore::batch_put` and `AsyncStore::batch_put_with_hint`
- Produces: `NodeWriteIntent`, `AsyncStore::batch_put_with_intent`, and `AsyncStore::batch_put_with_hint_and_intent`

- [ ] **Step 1: Define and export `NodeWriteIntent`**

Add the runtime-only enum immediately before `AsyncStore`:

```rust,ignore
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum NodeWriteIntent {
    #[default]
    General,
    PointUpsert,
}
```

Change the root re-export to:

```rust,ignore
pub use prolly::store::{
    AsyncStore, NodeWriteIntent, SyncStoreAsAsync,
};
```

- [ ] **Step 2: Add default intent-aware publication methods**

Place the first method after `batch_put`. It must delegate to the existing virtual method so adapter overrides retain their behavior:

```rust,ignore
async fn batch_put_with_intent(
    &self,
    entries: &[(&[u8], &[u8])],
    intent: NodeWriteIntent,
) -> Result<(), Self::Error> {
    let _ = intent;
    self.batch_put(entries).await
}
```

Place the hinted method after `batch_put_with_hint`:

```rust,ignore
async fn batch_put_with_hint_and_intent(
    &self,
    entries: &[(&[u8], &[u8])],
    namespace: &[u8],
    key: &[u8],
    value: &[u8],
    intent: NodeWriteIntent,
) -> Result<(), Self::Error> {
    let _ = intent;
    self.batch_put_with_hint(entries, namespace, key, value).await
}
```

- [ ] **Step 3: Preserve or absorb intent in every core wrapper**

Import `NodeWriteIntent` beside `AsyncStore` in `SearchIo`. Add exact forwarding overrides to `Arc<T>` and `SearchIo<S>`:

```rust,ignore
async fn batch_put_with_intent(
    &self,
    entries: &[(&[u8], &[u8])],
    intent: NodeWriteIntent,
) -> Result<(), Self::Error> {
    self.store.batch_put_with_intent(entries, intent).await
}
```

Use `(**self)` instead of `self.store` in the `Arc<T>` implementation. Add the corresponding hinted override with all existing hint arguments followed by `intent`.

In `SyncStoreAsAsync<S>`, ignore the intent and call the synchronous store inline:

```rust,ignore
async fn batch_put_with_intent(
    &self,
    entries: &[(&[u8], &[u8])],
    _intent: NodeWriteIntent,
) -> Result<(), Self::Error> {
    self.inner.batch_put(entries)
}
```

The hinted override calls `self.inner.batch_put_with_hint` with `entries`, `namespace`, `key`, and `value`. `TokioBlockingStore<S>` copies owned arguments and invokes the same existing synchronous methods through `spawn_store_blocking`; it does not forward intent into `Store`.

- [ ] **Step 4: Add post-implementation contract tests**

Extend the store test module with an `IntentAwareAsyncStore` whose override records `(hinted, intent)` before delegating. Add these tests:

```rust,ignore
#[test]
fn async_store_intent_defaults_preserve_existing_batches() {
    block_on(async {
        let store = DefaultAsyncReadStore::with_entries(1, &[]);
        store.batch_put_with_intent(
            &[(b"a", b"1")],
            NodeWriteIntent::PointUpsert,
        ).await.unwrap();
        assert_eq!(store.get(b"a").await.unwrap(), Some(b"1".to_vec()));
    });
}
```

Add an `Arc<IntentAwareAsyncStore>` test that calls both new methods and asserts exactly one preserved `PointUpsert` record per call. Add ready-runner coverage that polls both new `SyncStoreAsAsync` methods once and requires `Poll::Ready`.

- [ ] **Step 5: Run the focused core tests**

Run:

```sh
cargo test -p prolly-map prolly::store::tests --all-features
cargo test -p prolly-map --test async_foundation_default
cargo check -p prolly-map --no-default-features
```

Expected: every command exits `0`; the no-default-features graph contains no new Tokio requirement.

- [ ] **Step 6: Commit the contract slice**

```sh
git add src/lib.rs src/prolly/store/mod.rs \
  src/prolly/proximity/search/runtime.rs
git commit -m "feat: classify async node publications"
```

## Task 2: Carry intent through the verified remote adapter

This task extends the provider boundary without letting intent bypass node validation.

**Files:**

- Modify: `src/prolly/remote.rs:19-25`
- Modify: `src/prolly/remote.rs:137-240`
- Modify: `src/prolly/remote.rs:274-355`
- Modify: `src/prolly/remote.rs:432-555`
- Test: `src/prolly/remote.rs:1080-end`

**Interfaces:**

- Consumes: `NodeWriteIntent` and both intent-aware `AsyncStore` methods from Task 1
- Produces: `RemoteStoreBackend::batch_put_nodes_with_intent` and `RemoteStoreBackend::batch_put_nodes_with_hint_and_intent`

- [ ] **Step 1: Add remote backend defaults**

Import `NodeWriteIntent` beside `AsyncStore` and add this method after `batch_put_nodes`:

```rust,ignore
async fn batch_put_nodes_with_intent(
    &self,
    entries: &[(&[u8], &[u8])],
    intent: NodeWriteIntent,
) -> Result<(), Self::Error> {
    let _ = intent;
    self.batch_put_nodes(entries).await
}
```

Add the hinted counterpart after `batch_put_nodes_with_hint`. Its default calls the existing combined hinted method, not separate node and hint methods.

- [ ] **Step 2: Forward backend intent through `Arc<T>`**

Implement both methods with direct forwarding:

```rust,ignore
async fn batch_put_nodes_with_intent(
    &self,
    entries: &[(&[u8], &[u8])],
    intent: NodeWriteIntent,
) -> Result<(), Self::Error> {
    (**self).batch_put_nodes_with_intent(entries, intent).await
}
```

The hinted forwarding method passes `entries`, `namespace`, `key`, `value`, and `intent` unchanged.

- [ ] **Step 3: Validate before forwarding from `RemoteProllyStore`**

Override both new `AsyncStore` methods. Validate every `(key, value)` with `self.verify_node` before the backend call:

```rust,ignore
async fn batch_put_with_intent(
    &self,
    entries: &[(&[u8], &[u8])],
    intent: NodeWriteIntent,
) -> Result<(), Self::Error> {
    for (key, value) in entries {
        self.verify_node(key, value)?;
    }
    self.backend
        .batch_put_nodes_with_intent(entries, intent)
        .await
        .map_err(backend_error)
}
```

Use the same validation loop before `batch_put_nodes_with_hint_and_intent`.

- [ ] **Step 4: Add post-implementation remote routing tests**

Extend the test `MemoryBackend` with `write_intents: Mutex<Vec<(bool, NodeWriteIntent)>>`. Override both backend intent methods, record once, then delegate to the existing publication methods.

Construct valid bytes for direct adapter tests:

```rust,ignore
let node = Node::builder()
    .keys(vec![b"a".to_vec()])
    .vals(vec![b"1".to_vec()])
    .leaf(true)
    .level(0)
    .build();
let bytes = node.to_bytes();
let cid = node.cid();
```

Call both intent-aware adapter methods through `Arc<MemoryBackend>` and assert exact `PointUpsert` records. Then pair `cid.as_bytes()` with different valid node bytes, assert `RemoteAdapterError::CidMismatch`, and assert that the backend record count did not change.

- [ ] **Step 5: Run remote conformance tests**

Run:

```sh
cargo test -p prolly-map prolly::remote::tests --all-features
cargo test -p prolly-map --test transactions --all-features
```

Expected: remote intent tests and all existing backend and transaction conformance tests pass.

- [ ] **Step 6: Commit the verified remote slice**

```sh
git add src/prolly/remote.rs
git commit -m "feat: forward verified node write intent"
```

## Task 3: Mark only async point upserts in the engine

This task changes call intent without changing canonical mutation work.

**Files:**

- Modify: `src/prolly/engine/write.rs:637-790`
- Modify: `src/prolly/mod.rs:4110-4185`
- Modify: `src/prolly/remote.rs:1360-end`
- Modify: `tests/async_store.rs:1-40`
- Test: `tests/async_store.rs`
- Test: `src/prolly/transaction.rs:1520-end`

**Interfaces:**

- Consumes: intent-aware async publication methods from Tasks 1 and 2
- Produces: `ProllyEngine::canonical_point_upsert` and intent-aware final replay publication

- [ ] **Step 1: Share canonical batch logic behind an intent parameter**

Import `NodeWriteIntent` beside `AsyncStore` in `engine/write.rs`. Keep `canonical_batch` as the general entry point:

```rust,ignore
pub(crate) async fn canonical_batch(
    &self,
    tree: &Tree,
    mutations: Vec<Mutation>,
) -> Result<(Tree, crate::prolly::write::WriteStats), Error> {
    self.canonical_batch_with_publication_intent(
        tree,
        mutations,
        NodeWriteIntent::General,
    ).await
}
```

Move the current body without algorithm changes into `canonical_batch_with_publication_intent`. Add the dedicated point entry:

```rust,ignore
pub(crate) async fn canonical_point_upsert(
    &self,
    tree: &Tree,
    key: Vec<u8>,
    val: Vec<u8>,
) -> Result<Tree, Error> {
    let mutations = vec![Mutation::Upsert { key, val }];
    Ok(self.canonical_batch_with_publication_intent(
        tree,
        mutations,
        NodeWriteIntent::PointUpsert,
    ).await?.0)
}
```

- [ ] **Step 2: Carry intent only to final replay publication**

Add `publication_intent: NodeWriteIntent` to `execute_replay`. Pass `General` from range delete and the explicit value from canonical batch.

Replace only the two final store calls:

```rust,ignore
self.store
    .batch_put_with_intent(&entries, publication_intent)
    .await
    .map_err(|error| Error::Store(Box::new(error)))?;
```

Use `batch_put_with_hint_and_intent` in the hint branch. Do not change replay discovery, decoding, metrics, caching, or empty-write handling.

- [ ] **Step 3: Route `AsyncProlly::put` directly to the point entry**

Replace the delegation to `self.batch`:

```rust,ignore
pub async fn put(
    &self,
    tree: &Tree,
    key: Vec<u8>,
    val: Vec<u8>,
) -> Result<Tree, Error> {
    self.canonical_point_upsert(tree, key, val).await
}
```

Keep delete and public batch unchanged. Update the rustdoc to reference intent-aware publication instead of only `AsyncStore::batch_put`.

- [ ] **Step 4: Add post-implementation public routing and identity tests**

In `tests/async_store.rs`, import `AsyncStore` and `NodeWriteIntent` without the Tokio feature gate. Add a native `IntentRecordingStore` that stores nodes in `MemStore`, stores hints in its existing mutex map, reports `supports_hints() == true`, and records `(hinted, intent)` in both new methods.

Add one test with these exact assertions. Record and clear calls between operations:

```rust,ignore
assert_eq!(after_create, vec![(true, NodeWriteIntent::PointUpsert)]);
assert_eq!(after_update, vec![(false, NodeWriteIntent::PointUpsert)]);
assert_eq!(after_batch, vec![(false, NodeWriteIntent::General)]);
assert_eq!(after_delete, vec![(false, NodeWriteIntent::General)]);
assert_eq!(point_tree.root, batch_tree.root);
```

Build equivalent base trees in separate recording stores. Apply the same logical upsert through point put and one-item batch. Walk each result's reachable CIDs into separate `BTreeMap<Cid, Vec<u8>>` values and assert both maps match. Build a clean canonical tree in a third store and assert the same root. Call `put_large_value` with an inline value and assert its tree publication also records `PointUpsert`.

- [ ] **Step 5: Prove transaction overlays absorb point intent**

Extend the async transaction tests with a base async store whose intent methods increment an atomic counter. Exercise both borrowed and owned overlays through `batch_put_with_intent`, then assert the counter remains zero and the staged node is readable from each overlay.

Also assert both overlays report `supports_hints() == false`. In the existing remote transaction test, inspect `MemoryBackend::write_intents` after commit and require it to remain empty because `commit_transaction`, not node-batch publication, writes staged nodes. Do not add hint fields to `TransactionState`.

- [ ] **Step 6: Run engine, canonical, and transaction tests**

Run:

```sh
cargo test -p prolly-map --test async_store --all-features
cargo test -p prolly-map --test canonical_roots --all-features
cargo test -p prolly-map prolly::transaction::tests --all-features
cargo test -p prolly-map --test basic_ops --all-features
```

Expected: all tests pass, point and batch roots match, and transaction tests observe no premature backend publication.

- [ ] **Step 7: Commit the engine routing slice**

```sh
git add src/prolly/engine/write.rs src/prolly/mod.rs \
  src/prolly/remote.rs src/prolly/transaction.rs tests/async_store.rs
git commit -m "perf: identify async point publications"
```

## Task 4: Select deferred transactions only in Turso point publication

This task applies the measured optimization at the final provider boundary.

**Files:**

- Modify: `stores/prolly-store-turso/src/lib.rs:5-15`
- Modify: `stores/prolly-store-turso/src/lib.rs:168-285`
- Test: `stores/prolly-store-turso/src/lib.rs`
- Test: `stores/prolly-store-turso/tests/turso_backend.rs`

**Interfaces:**

- Consumes: `NodeWriteIntent` and both remote backend methods from Tasks 1 and 2
- Produces: point-only `TransactionBehavior::Deferred` selection with shared transaction execution

- [ ] **Step 1: Add a private transaction behavior selector**

Import `NodeWriteIntent` from `prolly` and add:

```rust,ignore
fn node_write_behavior(intent: NodeWriteIntent) -> TransactionBehavior {
    match intent {
        NodeWriteIntent::PointUpsert => TransactionBehavior::Deferred,
        _ => TransactionBehavior::Immediate,
    }
}
```

The wildcard is mandatory because `NodeWriteIntent` is non-exhaustive outside the core crate.

- [ ] **Step 2: Share node-entry transaction execution**

Add one private helper on `TursoBackend`:

```rust,ignore
async fn write_node_entries(
    &self,
    entries: &[(&[u8], &[u8])],
    hint: Option<(&[u8], &[u8], &[u8])>,
    behavior: TransactionBehavior,
) -> Result<(), TursoStoreError> {
    let mut connection = self.connect().await?;
    let transaction = connection.transaction_with_behavior(behavior).await?;
    apply_node_entries(&transaction, entries).await?;
    if let Some((namespace, key, value)) = hint {
        transaction.execute(UPSERT_HINT_SQL, (namespace, key, value)).await?;
    }
    transaction.commit().await?;
    Ok(())
}
```

Use this helper from existing general node-entry methods with `Immediate`. Do not change `batch_nodes`, root compare-and-swap, or `commit_transaction`.

- [ ] **Step 3: Override the two remote intent methods**

Add:

```rust,ignore
async fn batch_put_nodes_with_intent(
    &self,
    entries: &[(&[u8], &[u8])],
    intent: NodeWriteIntent,
) -> Result<(), Self::Error> {
    self.write_node_entries(entries, None, node_write_behavior(intent))
        .await
}
```

The hinted override passes `Some((namespace, key, value))`. Existing methods pass `Immediate` directly so one-entry general batches cannot acquire deferred behavior.

- [ ] **Step 4: Add post-implementation selector and rollback tests**

Add unit assertions with `matches!`:

```rust,ignore
assert!(matches!(
    node_write_behavior(NodeWriteIntent::PointUpsert),
    TransactionBehavior::Deferred
));
assert!(matches!(
    node_write_behavior(NodeWriteIntent::General),
    TransactionBehavior::Immediate
));
```

Duplicate the existing hint-trigger rollback fixture but invoke the complete call below. Assert both the first node and hint remain absent after the forced hint statement failure:

```rust,ignore
backend.batch_put_nodes_with_hint_and_intent(
    &[(b"node".as_slice(), b"value".as_slice())],
    b"ns",
    b"key",
    b"hint",
    NodeWriteIntent::PointUpsert,
).await
```

- [ ] **Step 5: Add persistence and concurrency tests**

For persistence, publish two nodes with `PointUpsert`, drop the backend, and reopen the same local path:

```rust,ignore
#[tokio::test(flavor = "multi_thread")]
async fn point_intent_persists_after_reopen() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("point-persistence.db");
    {
        let backend = TursoBackend::open(&path).await.unwrap();
        backend.batch_put_nodes_with_intent(
            &[(b"a", b"1"), (b"b", b"2")],
            NodeWriteIntent::PointUpsert,
        ).await.unwrap();
    }
    let reopened = TursoBackend::open(&path).await.unwrap();
    assert_eq!(reopened.get_node(b"a").await.unwrap(), Some(b"1".to_vec()));
    assert_eq!(reopened.get_node(b"b").await.unwrap(), Some(b"2".to_vec()));
}
```

For concurrency, launch two cloned backends with `tokio::join!`. Record which calls succeeded, accept only busy errors for the others, drop every handle, reopen the database, and verify each acknowledged value:

```rust,ignore
let point = NodeWriteIntent::PointUpsert;
let left = backend.clone();
let right = backend.clone();
let (left_result, right_result) = tokio::join!(
    left.batch_put_nodes_with_intent(&[(b"left", b"1")], point),
    right.batch_put_nodes_with_intent(&[(b"right", b"2")], point),
);
for result in [&left_result, &right_result] {
    assert!(matches!(
        result,
        Ok(()) | Err(TursoStoreError::Turso(
            turso::Error::Busy(_) | turso::Error::BusySnapshot(_)
        ))
    ));
}
assert!(left_result.is_ok() || right_result.is_ok());
```

After reopening, assert `left` exists when `left_result.is_ok()` and `right` exists when `right_result.is_ok()`.

- [ ] **Step 6: Run both Turso feature sets**

Run:

```sh
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml \
  --features turso-cloud-sync
cargo clippy --manifest-path stores/prolly-store-turso/Cargo.toml \
  --all-targets --all-features -- -D warnings
```

Expected: local tests run without credentials, cloud integration remains skipped by its existing environment gate, and Clippy exits `0`.

- [ ] **Step 7: Commit the Turso policy slice**

```sh
git add stores/prolly-store-turso/src/lib.rs \
  stores/prolly-store-turso/tests/turso_backend.rs
git commit -m "perf: defer Turso point publication locks"
```

## Task 5: Complete correctness and platform verification

This task validates the complete architecture before measuring performance.

**Files:**

- Modify only if a verification failure identifies a defect in a file changed by Tasks 1 through 4
- Record command output in the implementation handoff

**Interfaces:**

- Consumes: the complete point-publication slice
- Produces: a clean, platform-checked candidate eligible for performance measurement

- [ ] **Step 1: Run formatting and compile checks**

Run:

```sh
cargo fmt --all -- --check
cargo check --workspace --all-features
cargo check --manifest-path bindings/wasm/Cargo.toml \
  --target wasm32-unknown-unknown
```

Expected: every command exits `0`; the WASM build adds no Tokio dependency through the base async contract.

- [ ] **Step 2: Run the full correctness suite**

Run:

```sh
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Expected: every non-ignored test and documentation test passes; Clippy reports no warning.

- [ ] **Step 3: Audit implementation coverage**

Run:

```sh
rg -n '^impl.*AsyncStore|^impl.*RemoteStoreBackend' \
  src stores bindings -g '*.rs'
rg -n 'batch_put_with_(hint_and_)?intent|NodeWriteIntent' \
  src stores bindings tests -g '*.rs'
```

Classify every implementation as transparent forwarding, semantic absorption, optimized handling, or default general handling. Confirm `ForeignRemoteBackend` uses the default general path and strict transaction commits contain no `PointUpsert` branch.

- [ ] **Step 4: Commit any verification-only corrections**

If Tasks 1 through 3 required a correction, stage only the affected production and test files. Use `git commit -m "fix: preserve point publication invariants"`. Skip this commit when verification required no changes.

## Task 6: Run focused performance acceptance

This task proves the optimization works and protects unrelated APIs before the full scale run.

**Files:**

- Create: `performance-results/turso-point-intent-focused-2026-07-19/`
- Compare: detached baseline revision `a2f4e7a3`
- Use: `scripts/run_sqlite_turso_local_comparison.sh`

**Interfaces:**

- Consumes: verified candidate and existing benchmark schema
- Produces: paired 10K latency and throughput evidence for every API and pattern

- [ ] **Step 1: Build isolated baseline and candidate binaries**

Reuse the clean detached baseline at `/private/tmp/prolly-turso-put-baseline.9PVDvG/repo` after verifying `git rev-parse HEAD` equals `a2f4e7a3066d2259783c8d815b45c0461c77b06b`. Build baseline and candidate into separate target directories with `CARGO_INCREMENTAL=0`.

- [ ] **Step 2: Run five alternating focused pairs**

Run the frozen 10K workload with 100 changes, five repetitions, both adapters, all APIs, and all patterns. Alternate which revision runs first for each repetition. Store raw rows, summaries, manifests, dependency features, and machine metadata in separate baseline and candidate subdirectories.

Use these workload arguments for both revisions:

```sh
--sizes 10000 --changes 100 --runs 5 \
--adapters sqlite-sync,turso-async \
--apis put,batch,diff,merge \
--patterns append,random,clustered
```

- [ ] **Step 3: Apply the focused gates**

Require at least 40% lower Turso point-put median total latency for append, random, and clustered patterns. Require no Turso point p50 or p95 regression. Reject any SQLite/Turso batch, diff, merge, or SQLite put median regression above 5%.

If a protected cell crosses 5%, run five more alternating pairs and evaluate the combined ten. Correct or revert every confirmed regression before proceeding.

- [ ] **Step 4: Re-run in-memory performance verification**

Run the same `prolly_bench` core-operation screen and `async_first_foundation_bench` comparison recorded in the async-first design. Use alternating baseline/candidate runs and the same 10,000-record fixtures. Require at most 5% median and 10% p95 regression for every protected operation.

- [ ] **Step 5: Commit focused evidence**

Add only durable summaries, manifests, and compact findings. Do not commit database fixtures, target directories, or per-command terminal logs:

```sh
git add performance-results/turso-point-intent-focused-2026-07-19
git commit -m "perf: verify Turso point publication intent"
```

## Task 7: Run the full local matrix and update documentation

This task verifies scale behavior through 2 million records and prepares the pull request evidence.

**Files:**

- Create: `performance-results/turso-point-intent-full-2026-07-19/`
- Modify: `docs/sqlite-turso-local-performance.md`
- Modify: `stores/prolly-store-turso/README.md`
- Modify: `docs/superpowers/specs/2026-07-19-point-publication-intent-design.md`

**Interfaces:**

- Consumes: focused acceptance from Task 6
- Produces: 432 validated candidate rows, scale comparison, documented transaction policy, and final PR evidence

- [ ] **Step 1: Run three alternating full-matrix pairs**

Run baseline and candidate at 10K, 50K, 100K, 500K, 1M, and 2M records. Use three repetitions, both adapters, all three patterns, and put, batch, diff, and merge. Keep the existing fixed keys, values, seeds, durability settings, cold-manager policy, and 1% change count clamped to 100 through 10,000.

Expected: all 432 candidate rows validate with no skip, timeout, incorrect value, incorrect count, or reopen failure.

- [ ] **Step 2: Apply full scale gates**

Reject any protected API/pattern/size median latency regression above 5%. Require Turso point puts to improve at every size and pattern. Confirm engine metrics show one publication batch and node work bounded by tree height.

- [ ] **Step 3: Update adapter and comparison documentation**

Add this policy statement to the Turso README, adjusted only for surrounding prose:

```markdown
`AsyncProlly::put` publishes immutable nodes in a deferred local transaction.
Generic batches, node deletes, root compare-and-swap, and strict transaction
commits retain immediate transactions. Both point publication paths commit
nodes and the optional rightmost-path hint atomically.
```

Update the comparison report with before/after throughput, total latency, p50, p95, p99, the transaction selector, full gate outcome, machine provenance, and local-only limitation. Append the final measured result and implementation status to the approved design.

- [ ] **Step 4: Run final documentation and repository checks**

Run:

```sh
cargo fmt --all -- --check
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
git diff --check
```

Expected: all commands exit `0`, all result links resolve, and the worktree contains no fixture database or target output.

- [ ] **Step 5: Commit final evidence and documentation**

```sh
git add docs/sqlite-turso-local-performance.md \
  docs/superpowers/specs/2026-07-19-point-publication-intent-design.md \
  stores/prolly-store-turso/README.md \
  performance-results/turso-point-intent-full-2026-07-19
git commit -m "docs: record Turso point publication results"
```

## Task 8: Publish and update pull request 24

This task publishes only the verified branch and records the review focus.

**Files:**

- No source changes expected
- Update: `https://github.com/crabbuild/prolly/pull/24`

**Interfaces:**

- Consumes: clean commits and passing correctness and performance gates
- Produces: updated remote branch and pull request description

- [ ] **Step 1: Verify publish scope**

Run:

```sh
git status --short --branch
git log --oneline origin/codex/async-first-prolly-engine..HEAD
git diff --stat origin/codex/async-first-prolly-engine...HEAD
```

Expected: the worktree is clean and every unpublished commit belongs to this design, implementation, verification, or documentation slice.

- [ ] **Step 2: Push the branch**

```sh
git push origin codex/async-first-prolly-engine
```

Expected: the remote branch advances without force push.

- [ ] **Step 3: Update pull request 24**

Add a concise section that states:

- `AsyncProlly::put` now carries explicit point-publication intent
- Canonical algorithms, bytes, roots, validation, and sync behavior remain unchanged
- Turso uses deferred transactions only for explicit point publication
- Generic batches, diff and merge preparation, roots, and strict commits retain immediate transactions
- Correctness suite and exact local-only performance gates passed
- Links point to the approved design and committed focused and full findings

Keep the pull request in draft until every full-matrix gate passes. Mark it ready only after remote checks also succeed.
