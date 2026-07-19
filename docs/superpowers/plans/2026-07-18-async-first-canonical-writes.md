# Async-First Canonical Writes Implementation Plan

**Goal:** Make `ProllyEngine<S: AsyncStore>` the only implementation of put,
delete, batch, and append while preserving exact canonical roots and committing
all rewritten nodes atomically.

**Architecture:** Move the existing async route planner and collector into
`engine/write.rs`. Mutations route through validated engine loads, rebuild each
touched leaf and ancestor once, and flush one deduplicated batch only after the
new root is complete. `AsyncProlly` delegates directly. `Prolly` wraps the same
future with the sealed ready-only adapter. The full-tree async `BTreeMap`
fallback and facade-local mutation algorithms are deleted after equivalence.

## Invariants

- Input `Tree.config.format` is authoritative for every loaded and emitted node.
- Preprocess mutations once with sorted last-write-wins semantics.
- A failed read, decode, or batch write publishes no root and admits no
  unpersisted node to the shared cache.
- Every emitted node validates structurally and hashes to its batch key.
- Empty and idempotent mutations return the original tree without writes.
- Append is an optimization only; its result must equal a clean sorted build.
- Sync execution polls once and may never block, spawn, or construct a runtime.

## Task 1: Characterize Canonical and I/O Behavior

- [ ] Add sync/async/engine root-equivalence matrices for empty, upsert,
  overwrite, missing delete, existing delete, mixed batch, duplicate mutation,
  append, random, and clustered workloads across all built-in layouts.
- [ ] Add a counting async store test proving a one-key update on a multi-level
  tree does not scan every leaf and uses one atomic `batch_put` publication.
- [ ] Add injected read/write failure tests proving the old root remains readable
  and no collector-only node is cache-visible.
- [ ] Run focused tests and capture the full-rebuild I/O assertion RED.

## Task 2: Install the Engine Write Boundary

- [ ] Create `src/prolly/engine/write.rs` with operation-local collector,
  validated owned loaders, mutation routing frames, child replacements, and
  canonical parent/root construction.
- [ ] Expose `ProllyEngine::{put, delete, batch}` and keep format/execution state
  on the engine. Reuse the shared cache and cumulative metrics only after a
  successful store batch.
- [ ] Wire the existing localized coalesced planner; delete the full-tree
  `BTreeMap` rebuild.
- [ ] Run Task 1 plus canonical root, invariant, and write-stat suites.

## Task 3: Canonical Append

- [ ] Move right-edge discovery, hint validation, split propagation, and atomic
  hint publication behind the engine.
- [ ] Treat a missing/stale/malformed hint as a cache miss and fall back to a
  validated root-to-rightmost path.
- [ ] Prove all policies/layouts match clean sorted roots and repeated appends
  avoid redundant reads.

## Task 4: Facade Cutover and Legacy Deletion

- [ ] Make `AsyncProlly` delegate put/delete/batch to the engine.
- [ ] Make `Prolly` delegate the same APIs through `SyncStoreAsAsync::ready` and
  `run_ready`; route range deletion through engine batch.
- [ ] Delete facade-local collectors, route frames, rebuild helpers, and duplicate
  point mutation implementations once no caller remains.
- [ ] Remove broad `allow(dead_code)` from the async facade and require strict
  Clippy to identify residue.

## Task 5: Completion Gate

- [ ] Run formatting, strict Clippy, no-feature/all-feature workspace tests,
  doctests, and wasm check.
- [ ] Re-run built-in-layout root vectors and a release mutation sentinel.
- [ ] Record results in
  `performance-results/async-first-canonical-writes-2026-07-18/report.md`.
- [ ] Commit only with no unexplained correctness or critical performance
  regression.
