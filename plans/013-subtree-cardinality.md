# Subtree Cardinality and Ordinal Navigation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> and TDD.

**Goal:** Maintain logical child counts and expose length, rank, select,
range-count, and ordinal-page APIs.

**Architecture:** `child_counts[i]` is the number of leaf records below internal
child `vals[i]`; a leaf count is `keys.len()`. The canonical writer carries
counts upward. Readers navigate prefix sums without scanning unrelated leaves.

**Tech Stack:** Rust 1.81, existing sync/async stores and cursor patterns.

## Status

- **Priority**: P1
- **Effort**: L
- **Risk**: MEDIUM
- **Depends on**: plan 012
- **Planned at**: `fa7c219afc7`, 2026-07-13

### Task 1: Encode and validate counts

**Files:** modify `src/prolly/node.rs`, built-in layout codecs, conformance
fixtures, and `tests/node_layouts.rs`.

- [ ] Add failing round-trip and corrupt-count tests.
- [ ] Encode internal counts as canonical unsigned varints.
- [ ] Reject missing, zero, extra, and overflowing counts.
- [ ] Run `cargo test --test node_layouts`.

### Task 2: Maintain counts in every writer

**Files:** modify `src/prolly/canonical/writer.rs`, `src/prolly/builder.rs`, and
every remaining internal constructor found by `rg "vals\\.(push|insert)" src/prolly`.

- [ ] Add a recursive mixed-write count validator.
- [ ] Carry checked `logical_count` in every child summary.
- [ ] Return `SubtreeCountOverflow` on checked-add failure.
- [ ] Run `cargo test --test invariants --test canonical_roots`.

### Task 3: Add sync APIs

**Files:** create `src/prolly/ordinal.rs`; modify `src/prolly/mod.rs`,
`src/lib.rs`; create `tests/ordinal.rs`.

**Interfaces:** `Prolly::len`, `rank`, `select`, `range_count`, and
`range_by_ordinal`.

- [ ] Add empty, exact-boundary, out-of-range, and randomized scan-oracle tests.
- [ ] Navigate internal prefix sums and batch-read multi-leaf pages.
- [ ] Run `cargo test --test ordinal`.

### Task 4: Add async parity

**Files:** extend `src/prolly/ordinal.rs` and `tests/async_store.rs` behind
`async-store`.

- [ ] Match sync results with async oracle tests.
- [ ] Use ordered unique reads and configured parallelism.
- [ ] Run `cargo test --features async-store --test async_store`.

## Done criteria

- Root length requires one root read.
- Rank/select/count match full scans on randomized trees.
- Sync and async results match.
- All count arithmetic is checked.

## STOP conditions

- An internal constructor lacks a source logical count.
- An ordinal API silently scans the complete tree.

