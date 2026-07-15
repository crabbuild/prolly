# Clustered Batch-Delete Regression Mitigation Plan

> **Execution note:** Follow the red-green-refactor loop and retain canonical-root equivalence as a hard correctness gate.

**Goal:** Remove the clustered batch-delete latency regression caused by eager internal-frontier point reads, without changing configured chunk boundaries, node layout, or canonical convergence.

**Root cause:** The canonical fallback enumerates every internal node before replaying the small changed leaf interval. On the 10M SQLite workload this performs 177 node reads (1.93 MB) for four node writes, versus 37 reads (0.43 MB) in the original revision. The leaf replay already resynchronizes locally; 134 internal nodes are currently hydrated through separate point reads.

**Approach:** Hydrate each internal tree frontier with the store's ordered batch-read path and decode it in stable child order. Preserve the existing leaf-summary order, format validation, child-count fallback, boundary replay, and canonical rebuild. Measure the focused SQLite workload after this change. If latency still exceeds the original beyond the established noise threshold, add a second stage that replaces only the resynchronized child interval and canonically propagates that splice through ancestor levels.

---

## Task 1: Add a failing batched-frontier regression test

**Files:**
- Modify: `tests/canonical_write_stats.rs`

1. Add a small in-memory `Store` wrapper that records point reads and ordered batch reads and returns `true` from `prefers_batch_reads`.
2. Build a sufficiently wide, multi-level tree, clear the manager cache and metrics, then delete one contiguous middle cluster.
3. Assert the resulting root equals a full canonical rebuild of the surviving entries.
4. Assert internal enumeration uses at least one ordered batch read and keeps point reads bounded to the local leaf replay plus path overhead.
5. Run `cargo test --test canonical_write_stats clustered_batch_delete_batches_internal_frontier_reads -- --exact` and confirm it fails before production changes because the current collector only issues point reads.

## Task 2: Batch internal-frontier hydration

**Files:**
- Modify: `src/prolly/canonical.rs`
- Test: `tests/canonical_write_stats.rs`

1. Replace recursive one-child-at-a-time collection with ordered, breadth-first frontier collection.
2. For stores that prefer batch reads, use `load_many_ordered_with_parallelism`; retain simple ordered loading for point-read stores.
3. Validate every hydrated node against the tree format and structural invariants before collecting its children.
4. Preserve deterministic left-to-right leaf summaries and the complete old-internal CID set used to suppress duplicate writes.
5. Preserve the level-1 child-count optimization; only load a leaf when an older node lacks a logical child count.
6. Run the new regression test and the complete `canonical_write_stats` integration suite.

## Task 3: Verify canonical correctness broadly

**Files:**
- No source changes expected

1. Run `cargo test --all-features`.
2. Run formatting and lint gates: `cargo fmt --all -- --check` and `cargo clippy --all-targets --all-features -- -D warnings`.
3. Confirm configurable chunking rules and node layouts remain covered by existing format/canonical tests.

## Task 4: Measure the focused SQLite performance gate

**Files:**
- Use: `scripts/run_sqlite_workload_report.sh`
- Create: `performance-results/sqlite-clustered-delete-mitigation-2026-07-14/`

1. Run the shared-revision harness for `clustered_batch_deletes` at 1M and 10M records, NORMAL and FULL durability, with at least five alternating-order repetitions.
2. Compare median latency, min/max spread, node/byte reads, store read-call shape, writes, final entry count, and validation status against revision `fa7c219`.
3. Require no material latency regression at 10M. Treat results inside the existing noise threshold as neutral, not a claimed gain.
4. If the regression remains material, proceed to Task 5; otherwise stop at the smaller, lower-risk fix.

## Task 5: Add localized canonical ancestor splicing only if required

**Files:**
- Modify: `src/prolly/canonical.rs`
- Modify: `tests/canonical_write_stats.rs`

1. Add a failing test that bounds total hydrated nodes, not only store calls, for a wide tree and clustered structural deletion.
2. Retain the start/end root-to-leaf paths and enumerate only the leaf interval needed for predecessor context and boundary resynchronization.
3. Replace the affected child-summary range at level 1, then replay canonical internal chunking from one predecessor node until an unchanged internal CID resynchronizes.
4. Repeat the splice level by level to the root, reusing untouched subtrees and preserving logical child counts.
5. Fall back to the full collector for unsupported custom layouts, missing counts, or any case that cannot prove canonical resynchronization.
6. Re-run Tasks 3 and 4 and keep this stage only if it materially improves the focused workload without harming correctness.

## Task 6: Verify language bindings are unaffected

**Files:**
- No source changes expected

1. Run the repository's Python, Node, Java, Go, C/C++, and Ruby binding smoke/tests documented by the existing binding verification scripts.
2. Distinguish code failures from unavailable local SDK/toolchain dependencies.
3. Record exact commands, versions, pass/fail totals, and any environment-only blocker in the final report.

## Execution result

- A breadth-first full-frontier read experiment did not improve cold SQLite latency and changed unrelated read-routing behavior, so it was removed.
- The retained implementation performs a bounded height-2 subtree rewrite, proves right-edge content-ID resynchronization, and falls back when that proof is unavailable.
- The focused regression test and the full Rust suite pass.
- The mitigation reduces the 10M workload from 177 to 48 node reads, but the final five-run SQLite matrix still shows a +22.9% to +28.3% latency regression versus the original revision. It therefore does not satisfy the strict no-regression merge gate.
- Count-and-range based elision of fully deleted leaves was investigated and rejected: delete batches may contain absent keys, so counts and separator bounds alone do not prove exact key-set equality. Safe payload-free elision requires either a persisted key-set commitment or an explicit range-delete operation.
