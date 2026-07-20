# Selectable Deterministic Chunking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> and TDD.

**Goal:** Support deterministic user-selected chunk measurement and boundary
algorithms, including entry-count plus key-only hash thresholds.

**Architecture:** `ChunkingSpec` is persisted in `TreeFormat`.
`BoundaryDetector` is a resettable per-level state machine shared by builders
and mutation writers. The caller supplies layout-aware encoded-entry length.

**Tech Stack:** Rust 1.81, xxhash-rust, standard floating-point math.

## Global constraints

- Same stream, level, layout, and spec yield identical cuts.
- Key-only policies never hash values or child CIDs.
- Every policy applies `hard_max_node_bytes` deterministically.
- No source/test identifiers or comments name the reference implementation.

## Status

- **Priority**: P0
- **Effort**: L
- **Risk**: MEDIUM
- **Depends on**: plan 010
- **Planned at**: `fa7c219afc7`, 2026-07-13

### Task 1: Validate policies and add presets

**Files:** create `src/prolly/chunking.rs`; modify `src/prolly/format.rs`,
`src/prolly/error.rs`, `src/lib.rs`; create `tests/chunking_policies.rs`.

**Interfaces:** Uses the descriptor types from plan 010 and produces presets
`entry_count_key_value_hash()`,
`entry_count_key_hash()`, `logical_bytes_key_weibull()`, and
`logical_bytes_rolling_hash()`.

- [ ] Add failing validation tests for min/target/max order, zero factor,
  invalid shape/window, and an impossible hard cap.
- [ ] Implement canonical serde descriptors and preset constructors.
- [ ] Run `cargo test --test chunking_policies`; expect pass.

### Task 2: Implement boundary state

**Files:** replace `src/prolly/boundary.rs`; extend the focused test.

```rust
pub struct BoundaryDetector {
    spec: ChunkingSpec,
    level: u16,
    entries: u64,
    logical_bytes: u64,
    encoded_bytes: u64,
    rolling_window: VecDeque<u8>,
    rolling_hash: u64,
}
impl BoundaryDetector {
    pub fn new(spec: ChunkingSpec, level: u16) -> Result<Self, Error>;
    pub fn observe(
        &mut self,
        key: &[u8],
        value: &[u8],
        encoded_entry_bytes: usize,
    ) -> Result<bool, Error>;
    pub fn reset(&mut self);
}
```

- [ ] Add golden-cut tests for every preset.
- [ ] Prove two value sets with equal keys have equal cuts under
  `entry_count_key_hash()`.
- [ ] Implement level-salted xxHash64, conditional Weibull checks, and a
  deterministic rolling BuzHash window generated from the configured seed.
- [ ] Run `cargo test --test chunking_policies`.

### Task 3: Route builders through the detector

**Files:** modify `src/prolly/builder.rs`, `src/prolly/batch.rs`,
`src/prolly/rebalance.rs`; create `tests/builder_policy_equivalence.rs`.

- [ ] Compare batch and sorted-builder roots for every built-in policy and
  prefix/plain layout.
- [ ] Replace direct boundary helpers with per-level detectors.
- [ ] Keep parallel precomputation only for stateless threshold rules.
- [ ] Run `cargo test --test builder_policy_equivalence --test invariants`.

## Done criteria

- Four built-in policies are selectable and persisted.
- Key-only cut positions are value-stable at leaf and internal levels.
- All builders share one detector implementation.
- Oversized single entries return `EntryTooLarge`.

## STOP conditions

- A policy needs platform-dependent hashing or non-canonical float state.
- Builder and streaming modes disagree on cuts.
