# Bounded External Bulk Builder Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> and TDD.

**Goal:** Bulk-load unsorted data with explicit memory, disk, and open-file
budgets while producing the canonical root.

**Architecture:** Buffer entries by byte size, sort/deduplicate deterministic
runs, spill length-prefixed run files, compact runs when the file cap is
reached, and k-way merge into the canonical sorted build path.

**Tech Stack:** Rust 1.81 standard filesystem/I/O and existing builders.

## Status

- **Priority**: P1
- **Effort**: L
- **Risk**: MEDIUM
- **Depends on**: plan 012
- **Planned at**: `fa7c219afc7`, 2026-07-13

### Task 1: Define resource policy and run format

**Files:** create `src/prolly/external_builder.rs`; modify `src/lib.rs`,
`src/prolly/mod.rs`, `src/prolly/error.rs`; create `tests/external_builder.rs`.

```rust
pub struct ExternalBuildConfig { pub memory_bytes: usize, pub max_open_files: usize, pub temp_dir: PathBuf }
pub struct ExternalBatchBuilder<S: Store> {
    store: S,
    tree_config: Config,
    build_config: ExternalBuildConfig,
    buffer: Vec<(u64, Vec<u8>, Vec<u8>)>,
    buffered_bytes: usize,
    runs: Vec<PathBuf>,
    next_sequence: u64,
}
```

- [ ] Reject zero/too-small budgets and unwritable temp directories.
- [ ] Define a versioned length-prefixed key/value run encoding with checked lengths.
- [ ] Run `cargo test --test external_builder`.

### Task 2: Spill and compact deterministic runs

- [ ] Test forced multi-run spill with duplicate keys; last input occurrence wins.
- [ ] Sort by key plus monotonic sequence number, write atomically, and delete
  partial files after errors.
- [ ] Compact the smallest level when `max_open_files` would be exceeded.
- [ ] Run the focused test.

### Task 3: Merge into canonical construction

- [ ] K-way merge with a binary heap and feed sorted unique entries into the
  canonical empty-tree writer.
- [ ] Assert roots equal in-memory batch and sorted builders for all policies.
- [ ] Test cleanup on success, explicit cancellation, decode failure, and store failure.
- [ ] Run `cargo test --test external_builder --test builder_policy_equivalence`.

## Done criteria

- Peak buffered entry bytes respect the configured memory budget except one
  explicitly accepted oversized record.
- Open run count never exceeds the file budget.
- Roots equal canonical in-memory construction.
- Temporary files are cleaned on every exit path.

## STOP conditions

- Duplicate resolution depends on merge scheduling.
- Temporary files can escape the configured directory.
