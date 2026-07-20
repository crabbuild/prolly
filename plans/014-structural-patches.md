# Logical and Structural Patch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> and TDD.

**Goal:** Represent changes as durable logical edits or validated subtree-range
replacements and apply them through the canonical writer.

**Architecture:** `LogicalPatch` is portable key-level semantics.
`StructuralPatch` is bound to an expected base root and format digest and may
splice verified subtree CIDs when levels align. Overlapping subtree patches are
refined until leaf conflicts can be resolved.

**Tech Stack:** Rust 1.81, serde, existing diff/merge/store APIs.

## Status

- **Priority**: P1
- **Effort**: L
- **Risk**: HIGH
- **Depends on**: plans 012 and 013
- **Planned at**: `fa7c219afc7`, 2026-07-13

### Task 1: Define patch formats

**Files:** create `src/prolly/patch.rs`; modify `src/lib.rs`,
`src/prolly/mod.rs`, `src/prolly/error.rs`; create `tests/patches.rs`.

```rust
pub enum LogicalPatch { Upsert { key: Vec<u8>, old: Option<Vec<u8>>, new: Vec<u8> }, Delete { key: Vec<u8>, old: Vec<u8> } }
pub struct StructuralPatch { pub base_root: Option<Cid>, pub format_digest: Cid, pub edits: Vec<StructuralEdit> }
pub enum StructuralEdit { Point(LogicalPatch), Subtree { start_exclusive: Option<Vec<u8>>, end_inclusive: Vec<u8>, level: u16, cid: Option<Cid>, logical_count: u64 } }
```

- [ ] Test deterministic serde round trips and reject unordered/overlapping ranges.
- [ ] Validate base root, format digest, level, count, CID presence, and bounds.
- [ ] Run `cargo test --test patches`.

### Task 2: Generate patches from structural diff

**Files:** modify `src/prolly/diff.rs`, `src/prolly/patch.rs`.

- [ ] Prove equal CIDs emit no edits and changed disjoint child ranges emit
  subtree edits rather than leaf points.
- [ ] Fall back to logical points when formats differ or spans cannot align.
- [ ] Run `cargo test --test patches --test structural_diff_cursor`.

### Task 3: Apply patches canonically

**Files:** modify `src/prolly/patch.rs`, `src/prolly/canonical/writer.rs`.

- [ ] Apply point edits through normal mutation normalization.
- [ ] Splice a referenced subtree only after CID verification and level/range
  alignment; otherwise refine it to lower-level edits.
- [ ] Assert `apply(diff_patch(base, target))` yields `target.root`.
- [ ] Run `cargo test --test patches`.

### Task 4: Integrate merge without replacing existing fallbacks

**Files:** modify `src/prolly/diff.rs`; extend merge tests.

- [ ] Use patch streams for disjoint structural changes.
- [ ] Preserve current conflict resolver semantics for overlapping point edits.
- [ ] Run `cargo test --test merge_explain --test range_limited_merge`.

## Done criteria

- Patch generation/application round trips roots.
- Invalid base, format, ranges, counts, or CIDs fail before root publication.
- Existing logical diff and merge APIs remain available.

## STOP conditions

- A subtree can be spliced without proving its key range and format.
- Patch application needs a non-canonical write path.

