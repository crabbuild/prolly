# IndexedMap Secondary Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the complete hardened Rust-core `IndexedMap` v1 described by `docs/superpowers/specs/2026-07-13-indexed-map-secondary-index-design.md`, including strict non-unique indexes, all three projection modes, dynamic builds, fencing, exact snapshots, lifecycle repair/replacement/deactivation, retention, and verified current-snapshot transfer.

**Architecture:** Add a focused `secondary_index` module above the existing raw tree and `VersionedMap` layers. Hidden `VersionedMap`s store one catalog and one tree per definition generation; a deterministic control root fences every public raw managed-map mutation; `IndexCoordinator` stages source, index, catalog, and control roots in one `VersionedMapsTransaction`. Keep node encoding and rebalancing unchanged.

**Tech Stack:** Rust 2021, MSRV 1.81, existing `serde`/`serde_cbor`/`sha2` dependencies, existing `Prolly`, `VersionedMap`, transaction, builder, range, sync, GC, and manifest primitives.

## Global Constraints

- Preserve existing tree node bytes, CIDs, boundary behavior, and source `MapVersionId` calculation.
- Do not add dependencies unless an existing dependency cannot implement a required invariant.
- `KeysOnly` is the default; `Include` and `All` values are inline and bounded.
- Index extractors are deterministic, side-effect-free, and may be called again after optimistic conflicts.
- Every public raw sync, async, and multi-map head mutation observes the control root, including its absence.
- Unsupported indexed rollback, merge publication, migration, rebuild, restore-as-head, and raw pruning fail closed.
- The low-level tree API remains index-agnostic.
- Preserve unrelated user changes and stage only files named by the active task.

---

## File Structure

- Create `src/prolly/secondary_index/mod.rs`: stable public re-exports and `Prolly::indexed_map` constructor.
- Create `src/prolly/secondary_index/definition.rs`: runtime definitions, projection modes, limits, extractor adapter, and registry.
- Create `src/prolly/secondary_index/storage.rs`: canonical persisted records, fingerprints, hidden names, control/catalog keys, physical keys, and projected-value codec.
- Create `src/prolly/secondary_index/coordinator.rs`: startup validation, registration/build, atomic edit planning, retries, verification, repair, replacement, deactivation, and retention.
- Create `src/prolly/secondary_index/snapshot.rs`: exact snapshot resolution, query handles, match decoding, cursor identity, and batched source resolution.
- Create `src/prolly/secondary_index/bundle.rs`: verified current indexed-snapshot export/import.
- Create `tests/secondary_index.rs`: public behavior, lifecycle, failure, concurrency, and property-style deterministic rebuild tests.
- Create `benches/secondary_index_bench.rs`: build, update, projection, query, and retry benchmarks.
- Modify `src/prolly/error.rs`: structured index error variants and display text.
- Modify `src/prolly/key.rs`: internal/public segment-prefix encoding needed for term-prefix scans.
- Modify `src/prolly/transaction.rs`: overlay-aware ordered batch reads.
- Modify `src/prolly/versioned_map.rs`: centralized map write authority and control-root fencing.
- Modify `src/prolly/mod.rs`: module declaration and transaction/coordinator integration.
- Modify `src/lib.rs`: public re-exports.
- Modify `Cargo.toml`: register the benchmark only.
- Modify `src/bin/prolly-conformance.rs` and `conformance/prolly-fixtures.v1.json`: deterministic secondary-index fixtures.
- Modify `README.md`, `docs/versioned-map.md`, `docs/secondary-index-design.md`, and `examples/secondary_index.rs`: public workflow and compatibility guidance.

---

### Task 1: Runtime Definitions, Projection Modes, and Structured Errors

**Files:**
- Create: `src/prolly/secondary_index/mod.rs`
- Create: `src/prolly/secondary_index/definition.rs`
- Modify: `src/prolly/mod.rs`
- Modify: `src/prolly/error.rs`
- Modify: `src/lib.rs`
- Test: `tests/secondary_index.rs`

**Interfaces:**
- Produces: `IndexProjection`, `SecondaryIndexEntry`, `SecondaryIndexLimits`, `SecondaryIndexError`, `SecondaryIndex`, `SecondaryIndexBuilder`, `SecondaryIndexRegistry`.
- Produces: all structured `Error` variants consumed by later tasks.

- [ ] **Step 1: Write failing public-definition tests**

Add tests that construct the three modes, reject duplicate names/generations, enforce positive generations, and verify `KeysOnly` defaults:

```rust
#[test]
fn secondary_index_registry_validates_definitions() {
    let by_status = SecondaryIndex::non_unique(
        "by-status",
        1,
        "app.users.by-status/v1",
        |_, _| Ok(vec![b"active".to_vec()]),
    )
    .unwrap();
    assert_eq!(by_status.projection(), IndexProjection::KeysOnly);

    let registry = SecondaryIndexRegistry::new()
        .register(by_status.clone())
        .unwrap();
    assert!(registry.get(b"by-status").is_some());
    assert!(registry.register(by_status).is_err());
}

#[test]
fn include_entries_carry_projection_bytes() {
    let entry = SecondaryIndexEntry::included(b"active", b"Ada");
    assert_eq!(entry.term, b"active");
    assert_eq!(entry.projection, Some(b"Ada".to_vec()));
}
```

- [ ] **Step 2: Run the tests and confirm missing API failures**

Run: `cargo test --test secondary_index`

Expected: compilation fails because `SecondaryIndex` public types do not exist.

- [ ] **Step 3: Implement definitions and registry**

Implement these exact public shapes, with a closure adapter stored behind `Arc<dyn SecondaryIndexExtractor>`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexProjection { KeysOnly, Include, All }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexEntry {
    pub term: Vec<u8>,
    pub projection: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexLimits {
    pub max_term_bytes: usize,
    pub max_projection_bytes: usize,
    pub max_all_value_bytes: usize,
    pub max_terms_per_record: usize,
    pub max_projected_bytes_per_record: usize,
    pub max_derived_mutations_per_transaction: usize,
    pub max_projected_bytes_per_transaction: usize,
    pub max_indexes: usize,
    pub build_page_size: usize,
    pub max_temporary_sort_bytes: usize,
    pub max_bundle_nodes: usize,
    pub max_bundle_bytes: usize,
    pub max_verification_entries: usize,
    pub max_write_retries: usize,
    pub max_build_retries: usize,
}

pub trait SecondaryIndexExtractor: Send + Sync + 'static {
    fn extract(&self, primary_key: &[u8], source_value: &[u8])
        -> Result<Vec<SecondaryIndexEntry>, SecondaryIndexError>;
}
```

Make `SecondaryIndex::non_unique` adapt `Vec<Vec<u8>>` into `KeysOnly` entries. Make `SecondaryIndexBuilder::extract` accept entries for `Include`, and `extract_terms` accept terms for `All`. Validate projection presence against the selected mode before returning emissions.

- [ ] **Step 4: Add structured errors**

Add every spec error to `Error`, including map/name/generation/term fields, implement stable `Display`, and avoid putting callback types inside errors. Include `IndexProjectionMismatch` and `ConflictingIndexProjection`.

- [ ] **Step 5: Re-export and run tests**

Run: `cargo test --test secondary_index`

Expected: definition tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/prolly/secondary_index src/prolly/mod.rs src/prolly/error.rs src/lib.rs tests/secondary_index.rs
git commit -m "feat(index): add secondary index definitions"
```

---

### Task 2: Canonical Storage Records and Physical Layout

**Files:**
- Create: `src/prolly/secondary_index/storage.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Modify: `src/prolly/key.rs`
- Test: unit tests in `src/prolly/secondary_index/storage.rs`
- Test: public behavior in `tests/secondary_index.rs`

**Interfaces:**
- Consumes: definition types from Task 1.
- Produces: `SecondaryIndexDescriptor`, `IndexCheckpoint`, `IndexedHeadRecord`, `IndexControl`, `IndexValue`, `descriptor_fingerprint`, `catalog_map_id`, `index_map_id`, `control_root_name`, `physical_index_key`, `decode_physical_index_key`, `term_bounds`, and canonical codec functions.

- [ ] **Step 1: Write failing deterministic codec/layout tests**

Test canonical round trips, stable fingerprints, hidden-ID isolation, zero-byte terms, empty primary keys, exact/range/prefix bounds, all projection value modes, and rejection of trailing/oversized bytes. Include fixed expected hex for at least one descriptor, control record, key, and projected value.

```rust
#[test]
fn physical_keys_round_trip_arbitrary_bytes() {
    let key = physical_index_key(&[0, b'a'], &[b'u', 0, 0xff]).unwrap();
    let decoded = decode_physical_index_key(&key).unwrap();
    assert_eq!(decoded.term, vec![0, b'a']);
    assert_eq!(decoded.primary_key, vec![b'u', 0, 0xff]);
}

#[test]
fn projection_values_are_versioned_and_canonical() {
    let encoded = IndexValue::Included(b"Ada".to_vec()).to_bytes().unwrap();
    assert_eq!(IndexValue::from_bytes(&encoded).unwrap(), IndexValue::Included(b"Ada".to_vec()));
    assert_ne!(encoded, b"Ada");
}
```

- [ ] **Step 2: Verify failures**

Run: `cargo test --lib secondary_index::storage::tests`

Expected: compilation fails because storage APIs do not exist.

- [ ] **Step 3: Implement canonical records and fingerprints**

Prefix every record with a distinct ASCII magic and big-endian `u32` version, then serialize private fixed-position tuple wire types with `serde_cbor`; do not serialize Rust structs as CBOR maps whose key ordering another language could change. Do not serialize timestamps into catalog tree values. Hash the exact descriptor envelope with `Cid::from_bytes`.

- [ ] **Step 4: Implement physical key and term-prefix encoding**

Add `encode_segment_prefix` beside `encode_segment`. Encode physical keys as two completed segments. Decode exactly two segments and reject extras. Derive exact/prefix/range bounds without raw concatenation.

- [ ] **Step 5: Implement projected values**

Use empty bytes only for `KeysOnly`; use a versioned envelope with distinct `Included` and `FullSource` tags otherwise. Enforce limits before cloning payloads.

- [ ] **Step 6: Run focused and key-helper tests**

Run: `cargo test --test secondary_index && cargo test --test key_helpers`

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/prolly/secondary_index/storage.rs src/prolly/secondary_index/mod.rs src/prolly/key.rs tests/secondary_index.rs
git commit -m "feat(index): add canonical index storage layout"
```

---

### Task 3: Transaction Overlay Batch Reads and Universal Write Fence

**Files:**
- Modify: `src/prolly/transaction.rs`
- Modify: `src/prolly/versioned_map.rs`
- Modify: `src/prolly/secondary_index/storage.rs`
- Test: `tests/transactions.rs`
- Test: `tests/versioned_map.rs`
- Test: `tests/secondary_index.rs`

**Interfaces:**
- Consumes: `control_root_name` from Task 2.
- Produces: overlay-aware `Store::batch_get_ordered`, crate-private `MapWriteAuthority`, `IndexMaintenancePermit`, guarded `VersionedMapsTransaction::apply`, and permitted coordinator apply methods.

- [ ] **Step 1: Write failing overlay-read tests**

Stage node writes and deletes, call `batch_get_ordered` with duplicates, and assert staged values shadow the base while input order is preserved.

- [ ] **Step 2: Implement overlay batch reads**

For borrowed and owned overlays, partition requested keys into staged hits and base misses, issue one base ordered read for unique misses, and expand results back to original positions. Never hold the transaction mutex during base I/O.

- [ ] **Step 3: Write failing write-fence matrix tests**

Install a deterministic control root for a source map and assert failure for initialize, import, restore, apply, conditional apply, put, delete, edit, append, parallel apply, rebuild, rollback, merge publication, migration, pruning, sync `VersionedMapsTransaction::apply`, and async raw apply. Also start a raw transaction while control is absent, activate the control root, and assert commit conflict.

- [ ] **Step 4: Implement centralized authority checks**

Add:

```rust
pub(crate) enum MapWriteAuthority<'a> {
    Unmanaged,
    IndexMaintenance(&'a IndexMaintenancePermit),
}

pub(crate) struct IndexMaintenancePermit {
    map_id: Vec<u8>,
    control_fingerprint: Cid,
}
```

Every managed head/prune transaction calls one guard that loads the control root even when absent. `Unmanaged` rejects presence. A permit validates map ID and exact control fingerprint. Public multi-map apply uses `Unmanaged`; only crate-private coordinator methods accept a permit. Add the equivalent guard to async raw writes even though `AsyncIndexedMap` is deferred.

- [ ] **Step 5: Run regression matrix**

Run: `cargo test --test transactions && cargo test --test versioned_map && cargo test --features async-store --test secondary_index`

Expected: fence tests and all existing transaction/versioned-map tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/prolly/transaction.rs src/prolly/versioned_map.rs src/prolly/secondary_index/storage.rs tests/transactions.rs tests/versioned_map.rs tests/secondary_index.rs
git commit -m "feat(index): fence managed map writes"
```

---

### Task 4: Catalog Access, IndexedMap Open, and Startup Health

**Files:**
- Create: `src/prolly/secondary_index/coordinator.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Modify: `src/prolly/mod.rs`
- Modify: `src/lib.rs`
- Test: `tests/secondary_index.rs`

**Interfaces:**
- Consumes: definitions, storage records, and fence permit.
- Produces: `Prolly::indexed_map`, `IndexedMap`, `IndexedMapHealth`, cheap catalog/control validation, and active-definition resolution.

- [ ] **Step 1: Write failing open/health tests**

Cover an unindexed map with extra runtime definitions, an indexed map with matching definitions, missing runtime definition, fingerprint mismatch, missing version root, unsupported transactional store, and control/catalog disagreement.

- [ ] **Step 2: Implement IndexedMap handle and constructor**

Require `S: Store + ManifestStore + TransactionalStore` for construction. Derive catalog/control names from source ID, preserve the registry by value, and return `Result<IndexedMap<'_, S>, Error>`.

- [ ] **Step 3: Implement bounded startup validation**

Open only control, catalog head, `current`, referenced named version roots, and runtime descriptors. Do not scan source or index trees. Missing or mismatched state fails closed.

- [ ] **Step 4: Implement health output**

Return source ID/version, catalog version, sorted active index names/generations/fingerprints/index versions, and `supports_transactions`. Do not hide validation errors inside a boolean.

- [ ] **Step 5: Run tests and commit**

Run: `cargo test --test secondary_index indexed_map_open`

```bash
git add src/prolly/secondary_index src/prolly/mod.rs src/lib.rs tests/secondary_index.rs
git commit -m "feat(index): open and validate indexed maps"
```

---

### Task 5: Retryable Dynamic Registration and Shadow Builds

**Files:**
- Modify: `src/prolly/secondary_index/coordinator.rs`
- Modify: `src/prolly/secondary_index/storage.rs`
- Test: `tests/secondary_index.rs`

**Interfaces:**
- Produces: `IndexedMap::ensure_index`, `IndexBuildResult`, internal deterministic `build_index_tree`, and atomic activation.

- [ ] **Step 1: Write failing registration tests**

Cover uninitialized source, initialized empty source, populated source, sparse/multi-term emissions, all projections, idempotent ensure, missing registry entry, build limit failure, and source movement between build and activation.

- [ ] **Step 2: Implement deterministic full build**

Stream the pinned source snapshot, extract and validate entries, construct projected values, sort by physical key, reject conflicting projections, and use the existing sorted tree builder. Account for term count, projected bytes, total entries, and temporary memory.

- [ ] **Step 3: Implement atomic activation**

In one permitted transaction validate `Sbase`, prior control, and catalog; initialize an uninitialized source when needed; stage hidden index version/head, checkpoint, descriptor, catalog `current`, and control tree; commit or retry from a fresh source.

- [ ] **Step 4: Add deterministic race injection for tests**

Keep injection crate-private and test-only: invoke a hook after build and before activation so the test can move the source head without sleeps.

- [ ] **Step 5: Run tests and commit**

Run: `cargo test --test secondary_index ensure_index`

```bash
git add src/prolly/secondary_index tests/secondary_index.rs
git commit -m "feat(index): build and activate indexes"
```

---

### Task 6: Atomic Incremental Writes and Projection Delta Planning

**Files:**
- Modify: `src/prolly/secondary_index/coordinator.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Test: `tests/secondary_index.rs`

**Interfaces:**
- Produces: `IndexedVersion`, `IndexedMapUpdate`, `IndexedMapEditor`, `IndexedMapMetricsSnapshot`, `apply`, `apply_if`, `put`, `delete`, `edit`, and `metrics`.

- [ ] **Step 1: Write failing mutation tests**

Cover add/change/delete, last-write-wins duplicates, empty and multi-term emissions, non-indexed changes under `KeysOnly`, projection-only updates under `Include`, every-value updates under `All`, extraction failure, projection mismatch/conflict, no-op source edits, and retry after concurrent writer conflict.

- [ ] **Step 2: Implement normalized delta planner**

Normalize source mutations by key, batch-read old values through the overlay, derive final values, extract old/new physical `(key, value)` maps per index, and emit delete/upsert mutations. Deduplicate exact repeats and reject one physical key with different values.

- [ ] **Step 3: Implement one-commit publication**

Validate control, descriptors, source head, and catalog `current`; stage source, affected indexes, new checkpoints, and catalog current through permitted multi-map apply; skip unchanged trees; commit once and retry the complete extractor/planner cycle only on transaction conflict.

- [ ] **Step 4: Expose operation metrics**

Back `IndexedMap` with shared atomic counters and return a snapshot containing normalized source mutations, records extracted, terms emitted, projected bytes, physical upserts/deletes, unchanged emissions skipped, source/index/catalog nodes written, retries, build attempts, verification outcomes, and retained roots by generation. Add tests that reset a fresh handle, execute one known edit, and assert exact logical counters while treating node counts as nonzero implementation metrics.

- [ ] **Step 5: Run deterministic root oracle**

After every mutation in a generated deterministic sequence, call the internal full builder and assert its index tree equals the incrementally maintained tree for every projection mode.

- [ ] **Step 6: Run tests and commit**

Run: `cargo test --test secondary_index indexed_writes`

```bash
git add src/prolly/secondary_index tests/secondary_index.rs
git commit -m "feat(index): maintain indexes atomically"
```

---

### Task 7: Exact Snapshots, Queries, and Versioned Cursors

**Files:**
- Create: `src/prolly/secondary_index/snapshot.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Test: `tests/secondary_index.rs`

**Interfaces:**
- Produces: `IndexedSnapshot`, `IndexedSnapshotId`, `SecondaryIndexSnapshot`, `SecondaryIndexMatch`, `SecondaryIndexCursor`, exact/prefix/range pages, `primary_keys`, `projected`, and `records`.

- [ ] **Step 1: Write failing snapshot/query tests**

Verify catalog-first pinning during concurrent head changes, exact term order, arbitrary-byte prefix/range bounds, forward/reverse pages, projection decoding for all modes, ordered source record resolution, missing source records as checkpoint corruption, cursor snapshot mismatch, `snapshot_at`, and exact `snapshot_by_id`.

- [ ] **Step 2: Implement catalog-first resolution**

Open one catalog version, decode the selected source/index IDs, load immutable named version roots, validate ownership/fingerprints, and keep `MapSnapshot`s inside `IndexedSnapshot`.

- [ ] **Step 3: Implement query translation and match decoding**

Translate logical bounds through storage helpers, decode exactly two key segments, decode the projection value according to the descriptor, and return matches without reading source. `records` performs one ordered source `get_many` and preserves match order.

- [ ] **Step 4: Implement cursor envelope**

Use a canonical cursor containing source ID, catalog ID, index ID, fingerprint, direction, logical bounds, and underlying `RangeCursor` or `ReverseCursor`. Reject any mismatch before scanning.

- [ ] **Step 5: Run tests and commit**

Run: `cargo test --test secondary_index indexed_snapshot`

```bash
git add src/prolly/secondary_index tests/secondary_index.rs
git commit -m "feat(index): query exact indexed snapshots"
```

---

### Task 8: Verification, Repair, Replacement, and Deactivation

**Files:**
- Modify: `src/prolly/secondary_index/coordinator.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Test: `tests/secondary_index.rs`

**Interfaces:**
- Produces: `IndexVerification`, `verify_index`, `verify_all`, `repair_index`, `replace_index`, and `deactivate_index`.

- [ ] **Step 1: Write failing lifecycle tests**

Cover valid verification, logical drift, missing index nodes, repair to rebuild root, repair conflict, replacement requiring greater generation/different fingerprint, old pinned snapshots surviving replacement, one-index deactivation, final-index deactivation removing the fence, and retained historical queries.

- [ ] **Step 2: Implement read-only semantic verification**

Rebuild from the selected retained source version under limits and compare the full tree/`MapVersionId`. Return expected/actual IDs and entry counts without mutating roots.

- [ ] **Step 3: Implement repair**

Build the correct tree first, then condition on source/catalog/control and replace only the checkpoint/index named roots required for the selected source. Never repair during a query.

- [ ] **Step 4: Implement replacement/deactivation state transitions**

Replacement shadow-builds the new generation while the old remains active, atomically swaps current selection/control, and records the old descriptor under `retired`. Deactivation removes the selected checkpoint from `current`; removing the last active index deletes the control root but retains historical catalog records and hidden version roots.

- [ ] **Step 5: Run tests and commit**

Run: `cargo test --test secondary_index index_lifecycle`

```bash
git add src/prolly/secondary_index tests/secondary_index.rs
git commit -m "feat(index): verify and manage index lifecycle"
```

---

### Task 9: Reference-Aware Retention and GC Planning

**Files:**
- Modify: `src/prolly/secondary_index/coordinator.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Test: `tests/secondary_index.rs`
- Test: `tests/gc.rs`

**Interfaces:**
- Produces: `IndexedRetentionResult`, `IndexedMap::keep_last`, `plan_indexed_gc`, and safe root-closure computation.

- [ ] **Step 1: Write failing retention tests**

Create source versions where index roots are both shared and changed. Assert `keep_last(0)` retains current, kept source checkpoints retain exact index roots, retired generations remain while referenced, old catalog IDs become unavailable, raw prune is fenced, and global GC sees every remaining named root.

- [ ] **Step 2: Implement retained checkpoint closure**

Select newest source versions with current always included; retain checkpoint records and source/index immutable roots reachable from them; produce deterministic sorted root deletions for everything else in this source/catalog namespace.

- [ ] **Step 3: Commit retention atomically**

Condition on source/catalog/control, update the catalog tree, delete unreferenced version roots, and publish the new catalog head in one transaction. Keep physical node deletion separate and use the existing store-global named-root reachability planner.

Document and test the reader rule: removing named roots does not remove nodes; node sweeping must either honor registered live roots or be operationally serialized with readers. Do not claim that cache pinning alone is a GC lease.

- [ ] **Step 4: Run tests and commit**

Run: `cargo test --test secondary_index retention && cargo test --test gc`

```bash
git add src/prolly/secondary_index tests/secondary_index.rs tests/gc.rs
git commit -m "feat(index): retain indexed history safely"
```

---

### Task 10: Verified Current-Snapshot Export and Import

**Files:**
- Create: `src/prolly/secondary_index/bundle.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Test: `tests/secondary_index.rs`

**Interfaces:**
- Produces: `IndexedSnapshotBundle`, `IndexedSnapshotBundleSummary`, `IndexedSnapshotBundleVerification`, `export_current`, `inspect`, `verify`, and `import_current`.

- [ ] **Step 1: Write failing bundle tests**

Round-trip all projection modes, verify deterministic bytes, reject missing/duplicate/CID-mismatched nodes, wrong source map ID, descriptor mismatch, checkpoint mismatch, unsupported format, trailing bytes, stale expected source, and partial import. Assert failed imports publish no roots.

- [ ] **Step 2: Implement canonical bundle manifest**

Include source ID/version, catalog ID/version, control record, sorted descriptors/checkpoints, exact index IDs/versions, and deduplicated nodes sorted by CID. Keep timestamps outside canonical content.

- [ ] **Step 3: Implement verification and atomic import**

Verify all node CIDs and complete reachability for every tree before writes. Require same source map ID and exact runtime descriptors. Stage missing nodes, immutable roots, heads, catalog, and control in one transaction conditioned on `expected_source`.

- [ ] **Step 4: Run tests and commit**

Run: `cargo test --test secondary_index indexed_bundle`

```bash
git add src/prolly/secondary_index tests/secondary_index.rs
git commit -m "feat(index): export and import indexed snapshots"
```

---

### Task 11: Conformance Fixtures, Documentation, Example, and Benchmarks

**Files:**
- Modify: `src/bin/prolly-conformance.rs`
- Modify: `conformance/prolly-fixtures.v1.json`
- Modify: `conformance/README.md`
- Modify: `examples/secondary_index.rs`
- Create: `benches/secondary_index_bench.rs`
- Modify: `Cargo.toml`
- Modify: `README.md`
- Modify: `docs/versioned-map.md`
- Modify: `docs/secondary-index-design.md`
- Test: `tests/conformance_fixtures.rs`

**Interfaces:**
- Consumes: complete public v1.
- Produces: stable bytes/roots, runnable example, user docs, and repeatable performance rows.

- [ ] **Step 1: Add fixture generation and fixture assertions**

Generate fixed descriptor/control/checkpoint/index-value bytes, hidden IDs, physical keys, and source/index/catalog roots for all projection modes. Update fixture tests to parse and assert every field.

- [ ] **Step 2: Replace the manual example**

Demonstrate runtime registry, post-population `ensure_index`, strict edit, `KeysOnly`, `Include`, `All`, exact query, `.projected`, `.records`, verification, replacement, retention, and export/import. Keep records small enough to run quickly.

- [ ] **Step 3: Add benchmark executable**

Emit CSV-style rows for build, indexed/non-indexed updates, projection-only updates, full-value amplification, exact/prefix/range queries, batched records, verification, export/import, and two-writer conflicts. Accept scale/batch environment variables and verify every result.

- [ ] **Step 4: Update docs**

Document strict consistency, dynamic-build retry behavior, extractor retry safety, projection cost, raw write fencing, lifecycle limits, and GSI/LSI conceptual mapping without introducing GSI/LSI public types.

- [ ] **Step 5: Run complete verification**

Run:

```bash
cargo fmt --all -- --check
cargo test --all-targets
cargo test --features async-store
cargo test --doc
cargo run --example secondary_index
PROLLY_INDEX_BENCH_SCALE=1000 cargo bench --bench secondary_index_bench
```

Expected: every command exits 0; benchmark rows report `verified=true`.

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml src/bin/prolly-conformance.rs conformance README.md docs examples/secondary_index.rs benches/secondary_index_bench.rs tests/conformance_fixtures.rs
git commit -m "docs(index): finalize IndexedMap v1"
```

---

### Task 12: Final Safety Audit and Release Gate

**Files:**
- Modify only files required by failures found in this task.

**Interfaces:**
- Consumes: all previous tasks.
- Produces: release-ready hardened v1 evidence.

- [ ] **Step 1: Run feature and backend matrix**

Run core default/async/tokio tests and each transactional provider-store conformance suite supported in the checkout. Record unsupported backends as capability-gated, not passing.

- [ ] **Step 2: Run deterministic repetition**

Generate conformance fixtures twice in separate temporary directories and compare bytes. Run the property-style rebuild oracle with at least 100 deterministic seeds and all projection modes.

- [ ] **Step 3: Run fence audit**

Search every `publish_named_root_at_millis`, `delete_named_root`, and managed-map head write in `versioned_map.rs`; require either the centralized authority helper or a documented low-level raw API exemption. Add a regression test for every missed public route before fixing it.

- [ ] **Step 4: Run performance sanity checks**

Compare `KeysOnly` non-indexed-field updates against the same source workload without indexes, verify index work is skipped when emissions are unchanged, and confirm update cost grows with changed keys/emissions/projected bytes rather than total map size.

- [ ] **Step 5: Final verification and commit fixes**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
cargo test --doc --all-features
git diff --check
```

Expected: all commands exit 0 and the worktree contains only intentional feature changes.

If fixes were necessary:

```bash
git add Cargo.toml README.md src/prolly src/lib.rs tests benches examples docs conformance
git commit -m "fix(index): close IndexedMap release gaps"
```
