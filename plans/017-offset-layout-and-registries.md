# Offset-Table Layout and Custom Registry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> and TDD.

**Goal:** Add a safe backing-buffer node layout and explicitly supplied
registries for application-defined layouts and boundary algorithms.

**Architecture:** `EncodedNode` owns `Arc<[u8]>` and validated key/value offset
tables. `NodeAccess` lets readers consume owned `Node` or an encoded view.
Registries map stable IDs to factories and are owned by `Prolly`, never global.

**Tech Stack:** Rust 1.81, `Arc<[u8]>`, existing serde/store APIs; no unsafe code.

## Status

- **Priority**: P2
- **Effort**: L
- **Risk**: HIGH
- **Depends on**: plans 010 and 012
- **Planned at**: `fa7c219afc7`, 2026-07-13

### Task 1: Implement offset-table encoding and validation

**Files:** create `src/prolly/layout.rs`; modify `src/prolly/node.rs`;
extend `tests/node_layouts.rs`.

- [ ] Golden-test deterministic bytes for leaf and internal nodes.
- [ ] Validate monotonic offsets, payload bounds, counts, key order, allocation
  caps, and trailing bytes before exposing slices.
- [ ] Fuzz-like test truncated and bit-flipped buffers without panics.

### Task 2: Introduce `NodeAccess`

```rust
pub trait NodeAccess {
    fn level(&self) -> u16;
    fn is_leaf(&self) -> bool;
    fn len(&self) -> usize;
    fn key(&self, index: usize) -> Option<&[u8]>;
    fn value(&self, index: usize) -> Option<&[u8]>;
    fn child_count(&self, index: usize) -> Option<u64>;
}
```

- [ ] Implement for `Node` and `EncodedNode`.
- [ ] Migrate search/cursor hot paths without changing public results.
- [ ] Benchmark allocation counts and range-read throughput.

### Task 3: Add explicit registries

**Files:** create `src/prolly/registry.rs`; modify manager constructors and
`src/prolly/error.rs`; create `tests/custom_formats.rs`.

- [ ] Register custom implementations by non-empty stable ID and reject duplicates.
- [ ] Persist only ID plus canonical parameters; never serialize function state.
- [ ] Return `UnknownNodeLayout` or `UnknownChunkingAlgorithm` before traversal.
- [ ] Verify two managers with equivalent registries produce equal roots.

## Done criteria

- Offset nodes expose validated borrowed slices without unsafe code.
- Built-in behavior remains unchanged.
- Custom formats are deterministic and explicitly registered.
- Missing/duplicate IDs fail clearly.

## STOP conditions

- A custom implementation can affect runtime-global state.
- Opening a custom tree can silently fall back to another codec or chunker.

