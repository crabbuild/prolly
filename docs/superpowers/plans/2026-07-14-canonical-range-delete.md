# Canonical Range Deletion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an atomic half-open range deletion whose localized height-2 path skips fully covered leaf payloads and closes the SQLite clustered-delete regression without weakening canonical convergence.

**Architecture:** A new range-specific canonical writer classifies leaf summaries using separator bounds, reads only predecessor/boundary/resynchronization leaves, rebuilds the affected level-1 window, and splices it into the root only after CID resynchronization. A full streaming rebuild is the correctness fallback. Sync, async, and maintained language bindings expose the same `[start, end)` contract.

**Tech Stack:** Rust 2024, content-addressed prolly nodes, SQLite/rusqlite workload harness, UniFFI 0.31, napi-rs, wasm-bindgen, Python, Go, JVM, Ruby, and Swift bindings.

## Global Constraints

- Bounds are half-open raw byte ranges: `start <= key < end`.
- `start >= end`, an empty tree, and a disjoint range return the original tree without writes.
- Configured chunking policies and built-in node layouts remain authoritative.
- Custom layouts and unsupported tree shapes use a correct fallback.
- The optimized path must prove leaf and internal CID resynchronization before subtree reuse.
- Existing `Mutation`, point-delete, batch, diff, merge, and transaction semantics do not change.
- No implemented source-code comments or identifiers reference the external comparison implementation.
- The 1M/10M SQLite gate uses five alternating-order runs under WAL+FULL and WAL+NORMAL.
- A material residual regression is reported honestly and blocks merge readiness.

---

## File Structure

- `src/prolly/canonical_range_delete.rs`: range-specific sync writer, localized height-2 splice, and streaming fallback.
- `src/prolly/mod.rs`: public sync and async methods plus module registration.
- `src/lib.rs`: public stats re-export remains sourced from `canonical`; no mutation-wire change.
- `tests/canonical_range_delete.rs`: public contract, canonical oracle, layouts/policies, no-op behavior, randomized cases, and bounded-I/O regression.
- `tests/async_store.rs`: async parity tests.
- `bindings/uniffi/src/lib.rs`: binding facade methods and facade test.
- `bindings/wasm/src/lib.rs`: direct WASM methods and tests.
- Generated UniFFI sources under `bindings/python`, `bindings/kotlin`, `bindings/ruby`, and `bindings/swift`; generated napi declarations under `bindings/node`.
- Handwritten convenience wrappers/tests under `bindings/go`, `bindings/java`, `bindings/kotlin`, `bindings/node`, `bindings/python`, `bindings/ruby`, and `bindings/swift`.
- `stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs`: current range call plus a benchmark-only extension-trait fallback that preserves the original revision's point-delete path.
- `scripts/run_sqlite_workload_report.sh`: unchanged shared-source provenance and alternating-order execution.
- `performance-results/sqlite-range-delete-2026-07-14/`: raw rows, report, and findings.

---

### Task 1: Public Sync Contract and Correct Streaming Fallback

**Files:**
- Create: `src/prolly/canonical_range_delete.rs`
- Create: `tests/canonical_range_delete.rs`
- Modify: `src/prolly/mod.rs:126-128`
- Modify: `src/prolly/mod.rs:1529-1545`

**Interfaces:**
- Produces: `canonical_range_delete::apply(manager, tree, start, end) -> Result<(Tree, CanonicalWriteStats), Error>`.
- Produces: `canonical_range_delete::apply_tree(manager, tree, start, end) -> Result<Tree, Error>`.
- Produces: `Prolly::delete_range` and `Prolly::delete_range_with_stats`.

- [ ] **Step 1: Write failing public-contract tests**

Add tests that build keys `a` through `f` and require:

```rust
#[test]
fn delete_range_is_half_open_and_immutable() {
    let (manager, base) = fixture([b"a", b"b", b"c", b"d", b"e", b"f"]);
    let deleted = manager.delete_range(&base, b"b", b"e").unwrap();
    assert_eq!(keys(&manager, &base), bytes(["a", "b", "c", "d", "e", "f"]));
    assert_eq!(keys(&manager, &deleted), bytes(["a", "e", "f"]));
}

#[test]
fn empty_reversed_and_disjoint_ranges_are_write_free_noops() {
    let (manager, base) = fixture([b"a", b"b", b"c"]);
    for (start, end) in [(b"b".as_slice(), b"b".as_slice()), (b"z", b"a"), (b"x", b"z")] {
        manager.reset_metrics();
        let deleted = manager.delete_range(&base, start, end).unwrap();
        assert_eq!(deleted.root, base.root);
        assert_eq!(manager.metrics().nodes_written, 0);
    }
}
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `cargo test --test canonical_range_delete -- --nocapture`

Expected: compilation fails because `Prolly::delete_range` does not exist.

- [ ] **Step 3: Add the module and public sync methods**

Register `pub(crate) mod canonical_range_delete;` and add:

```rust
pub fn delete_range(&self, tree: &Tree, start: &[u8], end: &[u8]) -> Result<Tree, Error> {
    canonical_range_delete::apply_tree(self, tree, start, end)
}

pub fn delete_range_with_stats(
    &self,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<(Tree, canonical::CanonicalWriteStats), Error> {
    canonical_range_delete::apply(self, tree, start, end)
}
```

- [ ] **Step 4: Implement the minimal streaming fallback**

In the new module, validate the format, return early for `start >= end` or no root, stream the original tree, and build only survivors:

```rust
pub(crate) fn apply<S: Store>(
    manager: &Prolly<S>, tree: &Tree, start: &[u8], end: &[u8],
) -> Result<(Tree, CanonicalWriteStats), Error> {
    if start >= end || tree.root.is_none() {
        return Ok((tree.clone(), CanonicalWriteStats::default()));
    }
    let mut saw_deleted = false;
    let mut builder = SortedBatchBuilder::new(manager.store(), tree.config.clone());
    for entry in manager.range(tree, &[], None)? {
        let (key, value) = entry?;
        if key.as_slice() >= start && key.as_slice() < end {
            saw_deleted = true;
        } else {
            builder.add(key, value)?;
        }
    }
    if !saw_deleted {
        return Ok((tree.clone(), CanonicalWriteStats::default()));
    }
    let written = builder.build()?;
    Ok((written, CanonicalWriteStats::default()))
}
```

Use the actual `SortedBatchBuilder` constructor/add signatures found in `src/prolly/builder.rs`; do not add a second builder abstraction.

- [ ] **Step 5: Verify GREEN and add full-range coverage**

Add assertions that `delete_range(b"", b"\xff")` returns an empty tree and that bounds before/after the keyspace work. Run:

`cargo test --test canonical_range_delete -- --nocapture`

Expected: all range-delete tests pass.

- [ ] **Step 6: Commit the fallback contract**

```sh
git add src/prolly/canonical_range_delete.rs src/prolly/mod.rs tests/canonical_range_delete.rs
git commit -m "feat: add canonical range deletion"
```

---

### Task 2: Proof-Gated Height-2 Splice

**Files:**
- Modify: `src/prolly/canonical_range_delete.rs`
- Modify: `src/prolly/canonical.rs` only to expose existing crate-private emitter helpers if reuse requires it
- Modify: `src/prolly/builder.rs` only to retain existing crate-private deferred level-building interfaces
- Modify: `tests/canonical_range_delete.rs`

**Interfaces:**
- Consumes: Task 1 `apply` and `apply_tree`.
- Produces: `try_localized_height_two(...) -> Result<Option<(Tree, CanonicalWriteStats)>, Error>`.
- Produces: stats whose `nodes_read` and `bytes_read` reflect actual localized work.

- [ ] **Step 1: Add the failing interior-leaf elision test**

Use a `BatchReadMemStore` wrapper and build 200,000 deterministic records. Delete the middle 2,000 records with one range operation. Clear cache and metrics before deletion. Require the canonical oracle and a strict read bound:

```rust
let start = key(99_000);
let end = key(101_000);
let (deleted, stats) = manager.delete_range_with_stats(&base, &start, &end).unwrap();
assert_eq!(deleted.root, rebuild_without_range(&store, &start, &end).root);
assert!(stats.nodes_read <= 12, "{stats:?}");
assert!(manager.metrics().store_batch_get_calls >= 1);
```

- [ ] **Step 2: Verify RED**

Run: `cargo test --test canonical_range_delete clustered_range_delete_skips_covered_leaf_payloads -- --exact --nocapture`

Expected: failure because the streaming fallback reads the full tree and exceeds 12 nodes.

- [ ] **Step 3: Implement root-window routing and validation**

Load the height-2 root, locate bound children with separator-floor routing, and select:

```rust
let window_start = first_child.saturating_sub(1);
let window_end = last_child.saturating_add(3).min(root.len());
```

Load internal children with `load_many_ordered`, validate level/format/key/value/count invariants, and flatten `NodeSummary { cid, first_key, count }` entries in order.

- [ ] **Step 4: Classify covered leaves without loading them**

For each summary at index `i`, use its first key and the next summary's first key:

```rust
let wholly_before = next_first.is_some_and(|next| next.as_slice() <= start);
let wholly_covered = summary.first_key.as_slice() >= start
    && next_first.is_some_and(|next| next.as_slice() <= end);
```

Reuse `wholly_before`, omit `wholly_covered`, and load every other summary needed for predecessor context, partial bounds, and right-side resynchronization. Never classify the global rightmost leaf as wholly covered without reading it.

- [ ] **Step 5: Replay boundary leaves and prove leaf resynchronization**

Feed entries outside `[start, end)` through the same `LeafEmitter` used by point canonical writes. Stop only when `LeafEmitter::is_aligned_with` matches an unchanged old summary after the range or the global right edge is reached. If the window ends first, return `Ok(None)` so the fallback runs.

- [ ] **Step 6: Rebuild internal summaries and prove internal resynchronization**

Use `BatchBuilder::build_level_serial_deferred(replacement_leaf_summaries, 1)`. For a nonterminal root window, require the final replacement CID to equal the old final window CID. Splice keys, CIDs, and counts into a cloned root and validate ordering, soft entry cap, and hard byte cap.

- [ ] **Step 7: Batch-write only new content**

Deduplicate against old leaf/internal CIDs, serialize the new root only when changed, issue one `Store::batch_put`, record metrics/stats, and cache write sets no larger than `LOCAL_WRITE_CACHE_LIMIT`.

- [ ] **Step 8: Verify GREEN and broaden the oracle**

Run:

```sh
cargo test --test canonical_range_delete -- --nocapture
cargo test --test canonical_roots
cargo test --test canonical_write_stats
```

Expected: all pass; the focused test reports at most 12 reads.

- [ ] **Step 9: Add randomized layouts/policies coverage**

For 50 fixed seeds and each built-in layout, generate 2,000 unique entries and random half-open bounds. Compare range deletion with a clean `BatchBuilder` of survivors. Add explicit tests for custom-layout fallback and a forced height-3 fallback.

- [ ] **Step 10: Add a store-failure publication test**

Use a store wrapper whose next `batch_put` returns an injected error. Require `delete_range` to return that error, the source root to remain readable and unchanged, and no result root to be published by the operation.

- [ ] **Step 11: Commit the optimized splice**

```sh
git add src/prolly/canonical_range_delete.rs src/prolly/canonical.rs src/prolly/builder.rs tests/canonical_range_delete.rs
git commit -m "perf: elide covered leaves in range deletion"
```

---

### Task 3: Async Range Deletion

**Files:**
- Modify: `src/prolly/mod.rs:4558-4570`
- Modify: `tests/async_store.rs`

**Interfaces:**
- Produces: `AsyncProlly::delete_range(&Tree, &[u8], &[u8]) -> Result<Tree, Error>`.
- Produces: `AsyncProlly::delete_range_with_stats(&Tree, &[u8], &[u8]) -> Result<(Tree, CanonicalWriteStats), Error>`.

- [ ] **Step 1: Write failing async parity tests**

Build the same six-key fixture through `AsyncProlly`, delete `[b, e)`, and compare entries/root semantics with the sync operation. Include empty/reversed no-ops.

- [ ] **Step 2: Verify RED**

Run: `cargo test --features async-store --test async_store async_delete_range -- --nocapture`

Expected: compilation fails because the async method is absent.

- [ ] **Step 3: Implement the correct async path**

Use the async range iterator to collect only keys in `[start, end)`, map them to `Mutation::Delete`, and call the existing async `batch`. Return early for invalid/empty bounds. `delete_range_with_stats` records input/effective mutation counts and the before/after async manager metric deltas in `CanonicalWriteStats`; `delete_range` returns only its tree. This preserves async atomic write behavior without duplicating the sync splice.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test --features async-store --test async_store async_delete_range -- --nocapture`

Expected: async parity and no-op tests pass.

- [ ] **Step 5: Commit async parity**

```sh
git add src/prolly/mod.rs tests/async_store.rs
git commit -m "feat: add async range deletion"
```

---

### Task 4: Binding Surface and Regeneration

**Files:**
- Modify: `bindings/uniffi/src/lib.rs`
- Modify: `bindings/wasm/src/lib.rs`
- Modify generated and handwritten binding files selected by their `PROVENANCE.md` instructions
- Modify each binding's existing smoke/parity test file

**Interfaces:**
- Produces: `ProllyEngine.delete_range(tree, start, end)` in UniFFI.
- Produces: idiomatic `deleteRange`/`delete_range` wrappers with the same half-open semantics.

- [ ] **Step 1: Add failing UniFFI and WASM tests**

In facade tests, build `a..f`, call `engine.delete_range(tree, b"b", b"e")`, and require `a,e,f`. Add the same direct test to `bindings/wasm/test/wasm.test.ts`.

- [ ] **Step 2: Verify RED**

Run:

```sh
cargo test --manifest-path bindings/uniffi/Cargo.toml --target-dir target delete_range
cargo test --manifest-path bindings/wasm/Cargo.toml --target-dir target delete_range
```

Expected: methods are absent.

- [ ] **Step 3: Add facade methods**

Add UniFFI `delete_range(tree, start, end)` delegating to sync `Prolly::delete_range`. Add direct WASM methods to memory and browser engines using `Uint8Array` bounds. Convert errors through existing binding error helpers.

- [ ] **Step 4: Regenerate checked-in glue**

Run the exact commands in every binding `PROVENANCE.md`: build `libprolly_bindings`, generate Python/Kotlin/Ruby/Swift glue, run the Node native build, and rebuild WASM declarations. Preserve the documented local `PROLLY_BINDINGS_LIBRARY` adaptations when generators overwrite them.

- [ ] **Step 5: Add idiomatic wrapper tests**

Add one half-open range-delete assertion to Python, Go, Node, Kotlin, Java, Ruby, and Swift smoke/parity suites. Kotlin/Java async convenience wrappers should delegate through their existing future/coroutine patterns.

- [ ] **Step 6: Run the documented binding matrix**

Run every command in `bindings/VERIFICATION.md`. Record Ruby SDK/dependency blockers separately; do not label an unexecuted Ruby test as passing.

- [ ] **Step 7: Commit binding support**

Stage only source/generated binding files and tests; exclude native binaries, `node_modules`, `target`, `.build`, `pkg`, `__pycache__`, and local lockfiles. Commit:

`git commit -m "feat: expose range deletion in bindings"`

---

### Task 5: Fair SQLite Range-Delete Benchmark

**Files:**
- Modify: `stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs`
- Test: existing support/harness tests under `stores/prolly-store-sqlite`

**Interfaces:**
- Current revision resolves `manager.delete_range(...)` to the new inherent method.
- The original revision resolves the same call to a benchmark-only extension trait that expands the deterministic interval into its existing point-delete batch path.

- [ ] **Step 1: Add a failing benchmark validation test**

Add a support test proving the clustered interval bounds are `key(start)` and `key(start + count)` and that expected survivors equal `records - count`.

- [ ] **Step 2: Verify RED**

Run: `cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml --bench sqlite_workload_bench clustered_range_bounds`

Expected: helper/test absent.

- [ ] **Step 3: Add deterministic range bounds and current call**

For `ClusteredBatchDeletes`, derive the half-open bounds from `clustered_indexes`; time `manager.delete_range(&base, &start, &end)`. Keep random deletes on point mutations. Validation must sample deleted interior keys and both surviving neighboring keys.

- [ ] **Step 4: Add the shared-source baseline adapter**

Define a local extension trait named `BenchmarkRangeDelete` with the same `delete_range(&Tree, &[u8], &[u8])` signature for `Prolly<Arc<SqliteStore>>`. Its implementation parses the benchmark's fixed-width `key-{id:020}` bounds and calls `batch` with the corresponding point-delete mutations. Rust resolves the current revision's inherent method before the trait method; the original revision, which has no inherent method, uses the trait fallback. The shared benchmark files remain byte-identical in both builds and the original tree algorithms remain untouched.

- [ ] **Step 5: Run a 200K smoke comparison**

```sh
SQLITE_BENCH_SIZES="200000" \
SQLITE_BENCH_RUNS=1 \
SQLITE_BENCH_PROFILES="normal" \
SQLITE_BENCH_WORKLOADS="clustered_batch_deletes" \
SQLITE_BENCH_RESULT_DIR="performance-results/sqlite-range-delete-smoke-2026-07-14" \
scripts/run_sqlite_workload_report.sh
```

Expected: both rows validate; current node reads are bounded near the two edges and materially below baseline.

- [ ] **Step 6: Commit benchmark support**

```sh
git add stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs
git commit -m "bench: compare clustered range deletion"
```

---

### Task 6: Full Verification and Performance Gate

**Files:**
- Create: `performance-results/sqlite-range-delete-2026-07-14/`
- Modify: implementation/tests only if a reproduced correctness or performance root cause requires it

**Interfaces:**
- Consumes all prior tasks.
- Produces final merge recommendation and durable raw evidence.

- [ ] **Step 1: Run source quality gates**

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
git diff --check
```

Expected: all exit zero.

- [ ] **Step 2: Run the complete Rust suite**

Run: `cargo test --all-features`

Expected: all unit, integration, and doctests pass, excluding only tests already marked ignored.

- [ ] **Step 3: Run the focused SQLite matrix**

```sh
SQLITE_BENCH_SIZES="1000000 10000000" \
SQLITE_BENCH_RUNS=5 \
SQLITE_BENCH_PROFILES="full normal" \
SQLITE_BENCH_WORKLOADS="clustered_batch_deletes" \
SQLITE_BENCH_RESULT_DIR="performance-results/sqlite-range-delete-2026-07-14" \
scripts/run_sqlite_workload_report.sh
```

- [ ] **Step 4: Generate and audit the report**

Run `cargo run --quiet --bin prolly-sqlite-report -- performance-results/sqlite-range-delete-2026-07-14`. Check all manifest rows, validation flags, operation counts, median/range classifications, node/byte I/O, writes, fixture size, and machine metadata.

- [ ] **Step 5: Re-run binding verification**

Execute the full matrix in `bindings/VERIFICATION.md` against the final library build. Record exact pass totals and environment-only blockers in `bindings-verification.md` beside the performance report.

- [ ] **Step 6: Apply the merge gate**

Mark the implementation merge-ready only if all correctness rows validate, all source/binding gates pass or have an explicitly external environment blocker, and neither 1M nor 10M shows a material latency regression against the original revision. Otherwise retain the branch, report the residual honestly, and do not merge main.

- [ ] **Step 7: Commit durable evidence**

```sh
git add performance-results/sqlite-range-delete-2026-07-14
git commit -m "bench: record range deletion evaluation"
```
