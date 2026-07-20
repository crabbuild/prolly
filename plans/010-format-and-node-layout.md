# Selectable Tree Format and Node Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans`
> to implement this plan task-by-task. Use TDD for each behavior change.

**Goal:** Separate persisted tree format from runtime tuning and retain one
public `Node` type whose deterministic bytes are selected by `NodeLayoutSpec`.

**Architecture:** `Config` owns `TreeFormat` and `RuntimeConfig`. `Node` keeps
its current key/value access surface during the hard cutover, adds child counts
and format identity, and delegates encoding to built-in layouts. The compact
prefix layout remains the default.

**Tech Stack:** Rust 1.81, serde, serde_cbor, sha2, existing store traits.

## Global constraints

- No CRAB v1 decoding or CID compatibility.
- Keep the public type named `Node`; no versioned node type.
- Do not name the reference implementation in source, tests, fixtures, public
  docs, comments, or identifiers.
- Runtime cache and I/O settings never affect bytes or CIDs.
- Do not stage, commit, or push unless requested.

## Status

- **Priority**: P0
- **Effort**: L
- **Risk**: HIGH
- **Depends on**: none
- **Planned at**: `fa7c219afc7`, 2026-07-13

## Drift check

Run `git diff --stat fa7c219afc7..HEAD -- src/prolly/config.rs src/prolly/node.rs src/prolly/tree.rs src/prolly/mod.rs src/lib.rs tests`.
If `Config`, `Node`, or `Tree` no longer matches document 000, stop and update
this plan before editing.

### Task 1: Define canonical format descriptors

**Files:** create `src/prolly/format.rs`; modify `src/prolly/mod.rs`,
`src/lib.rs`; create `tests/tree_format.rs`.

**Interfaces:**

```rust
pub struct TreeFormat {
    pub chunking: ChunkingSpec,
    pub node_layout: NodeLayoutSpec,
    pub value_encoding: Encoding,
}
pub enum ChunkMeasure { EntryCount, LogicalBytes, EncodedBytes }
pub enum BoundaryInput { Key, KeyValue }
pub enum HashAlgorithm { XxHash64 }
pub enum BoundaryRule {
    HashThreshold { factor: u32 },
    Weibull { shape: u32 },
    RollingBuzHash { window: u16 },
}
pub struct ChunkingSpec {
    pub measure: ChunkMeasure,
    pub input: BoundaryInput,
    pub hash: HashAlgorithm,
    pub rule: BoundaryRule,
    pub min: u64,
    pub target: u64,
    pub max: u64,
    pub hash_seed: u64,
    pub level_salt: bool,
    pub hard_max_node_bytes: u64,
}
pub enum NodeLayoutSpec {
    PrefixCompressed,
    Plain,
    OffsetTable,
    Custom { id: String, parameters: Vec<u8> },
}
impl TreeFormat {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, Error>;
    pub fn digest(&self) -> Result<Cid, Error>;
    pub fn validate(&self) -> Result<(), Error>;
}
```

- [ ] Write tests proving equal descriptors have equal bytes/digests, runtime
  settings are absent, and empty custom IDs are rejected.
- [ ] Run `cargo test --test tree_format`; expect compile failure for missing types.
- [ ] Implement the chunking descriptor types plus fixed-tag canonical format
  serialization without maps. Behavioral validation and detector state belong
  to plan 011.
- [ ] Export types and rerun; expect pass.

### Task 2: Split persisted and runtime configuration

**Files:** modify `src/prolly/config.rs` and all files returned by
`rg -l "config\\.(min_chunk_size|max_chunk_size|chunking_factor|hash_seed|encoding|node_cache_)" src tests benches stores`.

**Interfaces:**

```rust
pub struct Config { pub format: TreeFormat, pub runtime: RuntimeConfig }
pub struct RuntimeConfig {
    pub node_cache_max_nodes: Option<usize>,
    pub node_cache_max_bytes: Option<usize>,
    pub read_parallelism: usize,
}
```

- [ ] Rewrite `config::tests` first to the nested fields.
- [ ] Run `cargo test config::tests`; expect compile failures at old fields.
- [ ] Migrate manager cache access, binaries, benches, and test helpers.
- [ ] Run `cargo check --all-targets`; expect exit 0.

### Task 3: Make `Node` format-aware

**Files:** modify `src/prolly/node.rs`, construction helpers in
`src/prolly/mod.rs` and `src/prolly/builder.rs`; create
`tests/node_layouts.rs`.

**Interfaces:**

```rust
pub struct Node {
    pub keys: Vec<Vec<u8>>,
    pub vals: Vec<Vec<u8>>,
    pub child_counts: Vec<u64>,
    pub leaf: bool,
    pub level: u16,
    pub layout: NodeLayoutSpec,
    pub format_digest: Cid,
}
```

- [ ] Add failing leaf/internal invariant, deterministic prefix/plain byte,
  distinct-layout CID, and format-digest mismatch tests.
- [ ] Remove chunking fields from `Node`; migrate every direct literal returned
  by `rg "Node \\{" src tests benches stores`.
- [ ] Implement prefix and plain layout dispatch. Return an explicit
  unsupported-layout error for offset/custom until plan 017.
- [ ] Validate sorted unique keys, matching key/value lengths, empty leaf
  counts, and matching internal counts.
- [ ] Run `cargo test --test node_layouts` and `cargo test node::tests`.

### Task 4: Complete the hard cutover

**Files:** update conformance fixtures and binaries plus
`docs/wire-format.md`, `docs/design-spec.md`.

- [ ] Remove legacy CBOR fallback and every v1 decoder branch.
- [ ] Regenerate deterministic fixtures through `prolly-conformance`.
- [ ] Run `cargo test --test conformance_fixtures --test basic_ops`.
- [ ] Run `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings`.

## Done criteria

- `Config` separates persisted and runtime state.
- One public `Node` supports compact prefix and plain layouts.
- Runtime changes do not change node CIDs.
- `rg -n -i "dolt" src tests benches conformance` returns no matches.

## STOP conditions

- A stored tree can be opened without an associated `TreeFormat`.
- Store traversal cannot determine a built-in layout from node bytes.
- The change would alter store CID key width.
