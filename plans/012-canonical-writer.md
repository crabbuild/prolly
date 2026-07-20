# Canonical Resynchronizing Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> and TDD.

**Goal:** Make point writes, batches, and bulk builds converge on identical
roots while reusing unaffected old-tree prefixes and suffixes.

**Architecture:** `MutationMerger` creates one sorted stream.
`CanonicalWriter` owns a `LevelWriter` stack and old-boundary cursors. Each
level emits `(first_key, cid, logical_count)` upward and stops streaming after
a new cut aligns with the corresponding old cut.

**Tech Stack:** Rust 1.81, existing `Store`, cursor, collector, and batch APIs.

## Global constraints

- Retain first-key separators.
- Routing and prefetch optimize reads but do not choose boundaries.
- Publish a root only after all content-addressed writes succeed.
- Do not name the reference implementation in code, tests, docs, or comments.

## Status

- **Priority**: P0
- **Effort**: XL
- **Risk**: HIGH
- **Depends on**: plans 010 and 011
- **Planned at**: `fa7c219afc7`, 2026-07-13

### Task 1: Characterize history independence

**Files:** create `tests/canonical_roots.rs` and
`tests/canonical_write_stats.rs`.

- [ ] Require equal roots for ascending, descending, randomized, batch,
  delete/reinsert, and sorted-build histories under every built-in policy.
- [ ] Require stable leaf ranges for a value-only key-hash update.
- [ ] Run the tests against the old writer and retain the failing output as the
  red TDD state.

### Task 2: Normalize mutations and traverse old boundaries

**Files:** create `src/prolly/canonical/mod.rs`,
`src/prolly/canonical/mutation.rs`, `src/prolly/canonical/old_cursor.rs`.

**Interfaces:**

```rust
pub(crate) struct OldBoundary {
    pub first_key: Vec<u8>,
    pub last_entry_key: Vec<u8>,
    pub cid: Cid,
    pub logical_count: u64,
}
pub(crate) struct OldLevelCursor;
```

- [ ] Test last-write-wins normalization and no-op removal.
- [ ] Test forward boundary traversal from a point key at every level.
- [ ] Use batch reads when the store advertises that preference.

### Task 3: Implement the level stack

**Files:** create `src/prolly/canonical/writer.rs`; expose only the collector
operations required from `src/prolly/batch.rs`.

**Interfaces:**

```rust
pub struct CanonicalWriteStats {
    pub entries_streamed: u64,
    pub nodes_read: u64,
    pub nodes_written: u64,
    pub nodes_reused: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub resync_distance_entries: u64,
    pub resync_distance_nodes: u64,
}
pub(crate) fn apply<S: Store>(
    prolly: &Prolly<S>, tree: &Tree, mutations: Vec<Mutation>,
) -> Result<(Tree, CanonicalWriteStats), Error>;
```

- [ ] Match empty-tree leaf emission to `SortedBatchBuilder`.
- [ ] Test parent propagation, empty deletion, and one-child-root removal.
- [ ] Test that matching new/old end boundaries reuse the suffix without loading
  its descendant leaves.
- [ ] Flush one collector only after the root is complete.

### Task 4: Converge public write paths

**Files:** modify `src/prolly/mod.rs`, `src/prolly/batch.rs`,
`src/prolly/builder.rs`; remove production use of localized split/merge in
`src/prolly/rebalance.rs`.

- [ ] Route `put`, `delete`, batch APIs, `BatchBuilder`, and
  `SortedBatchBuilder` through the canonical writer or its empty build path.
- [ ] Keep an append or parallel fast path only when its output is asserted
  equivalent to the canonical detector.
- [ ] Run `cargo test --test canonical_roots --test batch_behavior --test invariants`.

### Task 5: Prove atomicity and reuse

**Files:** extend `tests/canonical_write_stats.rs` using existing store-fault
patterns.

- [ ] Inject read and batch-write failures and assert the input root remains
  readable and unchanged.
- [ ] Assert a middle point update reuses distant prefix and suffix nodes.
- [ ] Run `cargo test --test canonical_write_stats --test performance_hints`.

## Done criteria

- History permutations produce equal roots for a shared `TreeFormat`.
- Value updates under key-only policies preserve boundaries at every level.
- Store/cache/async/proof/diff/merge suites pass.
- Local split/merge is no longer production shape authority.

## STOP conditions

- First-key boundary alignment cannot be proven from stored cursor state.
- A correctness path bypasses the canonical writer.
- Resynchronization requires scanning the complete unaffected suffix.

