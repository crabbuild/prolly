# Bounded Read-Through Write Session Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> and TDD.

**Goal:** Provide read-your-writes point/range reads, bounded canonical flushes,
and savepoint/revert over an immutable base tree.

**Architecture:** `WriteSession` holds a `BTreeMap<Vec<u8>, PendingValue>` plus
an immutable base `Tree`. Point reads check the overlay first. Range reads merge
the ordered overlay and tree cursor. Flush sends normalized edits through the
canonical writer and clears the overlay only on success.

**Tech Stack:** Rust 1.81 `BTreeMap`, existing range and batch APIs.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MEDIUM
- **Depends on**: plan 012
- **Planned at**: `fa7c219afc7`, 2026-07-13

### Task 1: Implement bounded overlay semantics

**Files:** create `src/prolly/write_session.rs`; modify `src/lib.rs`,
`src/prolly/mod.rs`; create `tests/write_session.rs`.

```rust
pub enum PendingValue { Value(Vec<u8>), Deleted }
pub struct WriteSession<S: Store> {
    prolly: Prolly<S>,
    base: Tree,
    overlay: BTreeMap<Vec<u8>, PendingValue>,
    max_bytes: usize,
    current_bytes: usize,
    generation: u64,
    journal: Vec<(Vec<u8>, Option<PendingValue>)>,
}
pub struct Savepoint { generation: u64, journal_len: usize }
```

- [ ] Test repeated upsert/delete byte accounting and last-write-wins behavior.
- [ ] Test point read-through for present, updated, inserted, and deleted keys.
- [ ] Return `BufferFull` before exceeding the byte budget.

### Task 2: Merge ordered range reads

- [ ] Compare merged range results with a canonical flushed-tree oracle across
  inclusive/exclusive bounds and tombstones.
- [ ] Implement a two-way ordered merge without materializing the base range.
- [ ] Run `cargo test --test write_session`.

### Task 3: Flush and savepoints

- [ ] Test flush root equality with direct batch and retain overlay/base after
  injected failure.
- [ ] Journal overlay changes so `savepoint` and `revert` work before and after
  successful flush; reject foreign/stale savepoints.
- [ ] Run focused tests plus `cargo test --test batch_behavior`.

## Done criteria

- Reads reflect pending edits.
- Flush is canonical and failure-atomic.
- Savepoints restore exact logical state.
- Byte accounting remains correct after replacement and revert.

## STOP conditions

- Range reads must materialize the complete base tree.
- Revert can cross a flush without retaining a valid immutable base root.
