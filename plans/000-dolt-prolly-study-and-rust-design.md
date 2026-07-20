# Dolt Prolly Tree Study and Rust Target Design

## Status and provenance

- **Document status**: design approved in conversation; awaiting review of this written synthesis
- **Rust repository commit**: `fa7c219afc7e1ee5769dd85e5223ea5dde9e3074`
- **Dolt repository commit**: `6b2372c7d4ded1a54f55c6204304dbb72a33835c`
- **Audit date**: 2026-07-13
- **Primary workload**: mixed random updates and version-to-version diff/merge
- **Secondary workload**: efficient bulk import
- **Compatibility decision**: hard cutover; no CRAB v1 read or CID compatibility requirement

Baseline verification completed during the audit:

- `cargo test --test invariants --test batch_behavior` — 8 passed, 0 failed;
- `go test ./store/prolly/tree -run 'TestContentAddress' -count=1` — passed.

This document records implementation ideas and behavior. Dolt source files are
Apache-2.0 licensed. Reimplement algorithms from the described behavior and
tests, or preserve the required license and NOTICE material if code is copied.
Do not copy source mechanically into an MIT-only distribution without a license
review.

## Executive conclusion

Dolt's largest advantage is not FlatBuffers or its cache. It is the combination
of deterministic content-defined chunking and a streaming mutation writer that
continues only until new boundaries re-synchronize with the old tree. This
keeps mutations local while making final tree shape a function of ordered
content rather than edit history.

The Rust implementation already has a broader platform: compact prefix
compression, SHA-256 CIDs, sync and async stores, batched I/O, cache controls,
structural diff/merge, proofs, transactions, named roots, garbage collection,
large-value offload, and secondary/versioned map layers. The right strategy is
therefore to replace the shape and mutation core while preserving those outer
capabilities.

The agreed design makes both chunking and physical node layout user-selectable.
A single logical `Node` type is encoded by a selected layout. A canonical writer
uses any deterministic chunking policy, so the current Rust policy can gain
Dolt-style resynchronization as well as the new Dolt-inspired policies.

The canonical contract is:

> The same ordered content and the same canonical `TreeFormat` produce the same
> root CID, independent of insertion order, batching, or builder path.

## Existing Rust strengths to preserve

These are not replacement targets:

1. **Compact deterministic wire encoding.** `Node` currently prefix-compresses
   adjacent keys and encodes lengths as varints (`src/prolly/node.rs:96-184`).
2. **Strong content IDs.** Nodes use the crate's 32-byte SHA-256 `Cid`
   (`src/prolly/cid.rs:1-15`), rather than Dolt's 20-byte truncated SHA-512
   address (`dolt/go/store/hash/hash.go:24-36`,
   `dolt/go/store/hash/hash.go:60-80`).
3. **Storage abstraction.** The store layer has sync and async APIs, ordered
   batch reads, capability hints, and parallelism controls
   (`src/prolly/store/mod.rs:102-259`, `src/prolly/store/mod.rs:303-520`).
4. **Operational caching.** The manager already has count and byte budgets,
   pinning, metrics, and rightmost-path support (`src/prolly/mod.rs:320-491`,
   `src/prolly/mod.rs:693-698`). Dolt's striped LRU is not a wholesale upgrade.
5. **Batch planning and append paths.** The batch subsystem routes mutations,
   coalesces rebuilds, prefetches, reports statistics, and has append-specific
   reuse (`src/prolly/batch.rs:1-120`, `src/prolly/batch.rs:6848-6890`).
6. **Structural comparison.** Rust already prunes equal CIDs
   (`src/prolly/diff.rs:593-612`) and supports structural merge with recursive
   CID reuse (`src/prolly/diff.rs:3126-3349`), including async and prefetch
   paths.
7. **Higher-level facilities.** Proofs, named roots, transactions, versioned
   maps, garbage collection, large-value offload, and indexes are implemented
   as reusable layers rather than baked into the node engine.

## Dolt strengths and transferable ideas

### Adoption portfolio

| Idea | Priority | Impact | Effort | Risk | Confidence |
|------|----------|--------|--------|------|------------|
| Selectable chunking policies | P0 | High | M | Medium | High |
| Canonical resynchronizing writer | P0 | Very high | L | High | High |
| Subtree counts and ordinal APIs | P1 | High | L | Medium | High |
| Safe offset-table node view | P2 | Medium | L | High | Medium |
| Hierarchical structural patches | P1 | High | L | High | High |
| Mutable `WriteSession` overlay | P2 | Medium | M | Medium | High |
| External bulk sorter | P1 | High for imports | M | Medium | High |
| Specialized maps and structured blobs | P3 | Workload-dependent | L | High | Medium |

The canonical writer is the highest-impact and highest-risk item because it
changes the source of truth for every mutation path. Implement format and codec
boundaries first so it can be tested behind one stable interface.

### 1. Byte-aware, key-stable boundary selection

Dolt's default splitter tracks `len(key) + len(value)`, hashes only the key with
a level-specific salt, and uses a Weibull conditional probability to decide
boundaries (`dolt/go/store/prolly/tree/node_splitter.go:167-205`,
`dolt/go/store/prolly/tree/node_splitter.go:216-269`). Its normal byte limits
are 512 through 16,384 bytes with a 4 KiB target
(`dolt/go/store/prolly/tree/node_splitter.go:34-37`). It also provides a rolling
BuzHash splitter with a 67-byte window for low-entropy data
(`dolt/go/store/prolly/tree/node_splitter.go:77-100`).

Key-only boundaries matter because changing a value does not move leaf
boundaries. At internal levels, hashing the separator rather than the child CID
also prevents a changed descendant CID from shifting ancestor boundaries. Byte
measurement prevents a few very large records from creating oversized nodes.

Rust currently counts entries for minimum and maximum limits and hashes both
key and value (`src/prolly/config.rs:10-22`,
`src/prolly/boundary.rs:29-98`). Its bulk builder precomputes that predicate and
then applies count-based ranges (`src/prolly/builder.rs:147-194`). This is a
useful policy, but it should no longer be the only one.

**Adopt directly as policies**, including the requested midpoint:

- entry count + key/value hash threshold (current Rust behavior);
- entry count + key-only hash threshold;
- logical bytes + key-only salted Weibull;
- logical bytes + rolling BuzHash;
- custom registered policies with stable identifiers and canonical parameters.

Every policy also obeys a deterministic hard maximum encoded-node byte limit.
A single entry beyond that limit returns `EntryTooLarge` rather than creating an
unbounded node.

### 2. Streaming mutation with boundary resynchronization

Dolt builds one chunker per level and initializes it at the affected old-tree
cursor (`dolt/go/store/prolly/tree/chunker.go:32-84`). Its `advanceTo` logic
streams new entries until a new boundary coincides with an old node boundary,
then skips the unchanged suffix (`dolt/go/store/prolly/tree/chunker.go:147-255`).
Emitted child summaries are fed to the parent chunker, carrying last key, CID,
and subtree count (`dolt/go/store/prolly/tree/chunker.go:381-415`). Completion
removes redundant single-child roots (`dolt/go/store/prolly/tree/chunker.go:417-507`).

The sorted mutation layer merges an edit cursor with an old-tree cursor,
eliminates no-ops, and feeds insert, update, and delete operations through that
same chunker (`dolt/go/store/prolly/tree/mutator.go:33-159`).

Rust point writes currently mutate a leaf and use localized split/merge rules
(`src/prolly/mod.rs:1281-1408`, `src/prolly/rebalance.rs:90-130`). Splitting is
normally triggered only after a node exceeds `max_chunk_size`, with a forced
local split if no boundary is found (`src/prolly/rebalance.rs:183-299`). This
can preserve history-dependent local divisions instead of re-establishing the
same boundaries a fresh build would choose. Existing tests compare logical
contents more broadly than root identity; for example, batch behavior compares
entries without requiring builder and mutation roots to match
(`tests/batch_behavior.rs:37-70`).

**Adopt directly** as a `CanonicalWriter` shared by point mutations, batches,
bulk builds, and future structural-patch application. Retain Rust's first-key
separator convention; port the resynchronization behavior, not Dolt's last-key
representation.

### 3. Subtree cardinalities and ordinal navigation

Dolt stores child subtree counts and a total tree count in internal messages
(`dolt/go/store/prolly/message/prolly_map.go:84-102`). It decodes child counts
lazily (`dolt/go/store/prolly/tree/node.go:207-228`). This enables constant-time
root count (`dolt/go/store/prolly/tree/map.go:213-219`), seek by ordinal and
cursor rank (`dolt/go/store/prolly/tree/node_cursor.go:100-159`), ordinal range
iteration (`dolt/go/store/prolly/tree/map.go:379-445`), and key-range
cardinality (`dolt/go/store/prolly/tree/map.go:464-519`). Leaf spans can be
loaded eagerly with batched reads (`dolt/go/store/prolly/tree/node_cursor.go:260-328`).

Rust internal nodes currently store keys and raw child values without logical
counts (`src/prolly/node.rs:21-42`).

**Adopt directly** in the logical internal `Node` representation. Add O(1)
logical length and APIs for `rank(key)`, `select(ordinal)`, `range_count`, and
ordinal pagination. Every writer and builder must maintain the counts, and
property tests must compare them with full scans.

### 4. Encoded-node views and lazy materialization

Dolt keeps the serialized message and offset tables in its `Node`, returning
item slices from the backing bytes instead of allocating every key and value
(`dolt/go/store/prolly/tree/node.go:37-60`,
`dolt/go/store/prolly/message/item_access.go:30-91`). Hash and subtree-count
work are lazy (`dolt/go/store/prolly/tree/node.go:123-167`,
`dolt/go/store/prolly/tree/node.go:207-228`).

Rust's decoder reconstructs and allocates every key and value
(`src/prolly/node.rs:187-257`).

**Borrow selectively.** Keep safe Rust and introduce an offset-table
`NodeLayout` backed by `Arc<[u8]>` or `bytes::Bytes`. Do not port unsafe
FlatBuffer access literally. The canonical engine should consume a small
`NodeAccess` interface so owned prefix-compressed nodes and lazy views can
coexist.

### 5. Hierarchical subtree patches

Dolt represents changes either as leaf point edits or as ordered subtree range
replacements carrying an address, level, and subtree count
(`dolt/go/store/prolly/tree/patch_generator.go:22-34`). It emits the highest
safe subtree patch, splits a range patch only when it intersects another change,
and applies aligned subtrees by splicing their CIDs directly
(`dolt/go/store/prolly/tree/patch_generator.go:335-489`,
`dolt/go/store/prolly/tree/tree_patcher.go:147-212`). Its three-way merge
combines base-to-left and base-to-right patch streams
(`dolt/go/store/prolly/tree/merge.go:58-115`).

Rust already has strong structural diff and merge, so replacing them would add
risk without clear benefit. **Borrow the patch representation and application
primitive** to unify synchronization, merge transport, and the future VCS
layer. Use two types:

- `LogicalPatch`: key-level old/new values for durable user-visible semantics;
- `StructuralPatch`: base-root- and format-bound subtree ranges for efficient
  transfer and apply.

A structural patch must validate its base root, format digest, ordering,
non-overlap, referenced CIDs, and subtree counts. Incompatible formats fall
back to logical diff and rebuild.

### 6. Bounded mutable overlay and savepoints

Dolt overlays pending edits in a skip list and merges that iterator with the
immutable tree for reads (`dolt/go/store/prolly/tree/mutable_map.go:23-49`,
`dolt/go/store/prolly/tuple_mutable_map.go:227-257`). It flushes after a pending
edit threshold and supports checkpoint/revert across flushes
(`dolt/go/store/prolly/tuple_mutable_map.go:122-130`,
`dolt/go/store/prolly/tuple_mutable_map.go:158-224`).

Rust has a byte-bounded `MutationBuffer`, but it is a vector that drains into a
batch and is not a read-through ordered overlay (`src/prolly/batch.rs:644-760`).

**Borrow as a higher-level `WriteSession`**, implemented over an ordered map,
read-your-writes merged iteration, byte-budgeted flush through the canonical
batch writer, and savepoints. Keep it outside the core `Node` engine.

### 7. Bounded external bulk sorting

Dolt's external sorter uses a memory budget, disk spill runs, a file-count cap,
k-way merge, and leveled spill compaction
(`dolt/go/store/prolly/sort/external.go:32-100`,
`dolt/go/store/prolly/sort/external.go:119-195`).

Rust's unsorted `BatchBuilder` retains all entries before sorting
(`src/prolly/builder.rs:53-58`, `src/prolly/builder.rs:117-133`). Its
`SortedBatchBuilder` streams leaf data, but still retains summaries for the next
levels (`src/prolly/builder.rs:62-71`).

**Adopt as `ExternalBatchBuilder`** with explicit memory and temporary-storage
budgets, deterministic duplicate handling, cancellation cleanup, and k-way
merge into the same canonical writer. Preserve parallel boundary precomputation
for stateless policies; rolling policies use the sequential streaming path.

### 8. Specialized structures worth deferring

Dolt demonstrates that the same tree machinery can support fixed-width address
maps, commit closure indexes, constraint/conflict artifacts, vector proximity
indexes, chunked blobs, and indexed JSON documents. Notable examples include:

- commit closure ordering by height and hash
  (`dolt/go/store/prolly/commit_closure.go:34-55`);
- durable conflict and constraint artifacts
  (`dolt/go/store/prolly/artifact_map.go:36-103`);
- 4 KiB hierarchical blob chunks
  (`dolt/go/store/prolly/tree/blob_builder.go:31-55`,
  `dolt/go/store/prolly/tree/blob_builder.go:162-241`);
- JSON path lookup and chunk-aware JSON diff
  (`dolt/go/store/prolly/tree/json_indexed_document.go:233-257`,
  `dolt/go/store/prolly/tree/indexed_json_diff.go:25-80`).

These are useful product-layer directions after the canonical core is stable.
The proximity map is explicitly not a borrow-now target because parts of its
range/prefix surface remain unimplemented
(`dolt/go/store/prolly/tree/proximity_map.go:52-73`).

## Approved Rust target design

### Configuration boundaries

The current `Config` mixes persisted shape with runtime cache controls
(`src/prolly/config.rs:10-34`). Replace it conceptually with:

```rust
pub struct Config {
    pub format: TreeFormat,
    pub runtime: RuntimeConfig,
}

pub struct TreeFormat {
    pub chunking: ChunkingSpec,
    pub node_layout: NodeLayoutSpec,
    pub value_encoding: Encoding,
}
```

`TreeFormat` is canonically encoded. Each tree retains the full descriptor, and
each encoded node carries a layout identifier plus the descriptor digest.
Runtime cache sizes and I/O parallelism do not affect CIDs.

Changing a persisted format is an explicit `rebuild_with_format` operation.
Structural subtree reuse requires equal format digests. Logical iteration and
logical diff remain available across formats.

### User-selectable chunking

Chunk measurement, boundary input, and boundary rule are separate choices:

```rust
pub enum ChunkMeasure {
    EntryCount,
    LogicalBytes,
    EncodedBytes,
}

pub enum BoundaryInput {
    Key,
    KeyValue,
}

pub enum BoundaryRule {
    HashThreshold { factor: u32 },
    Weibull { shape: f64 },
    RollingBuzHash { window: u16 },
}
```

`ChunkingSpec` also contains minimum, target, maximum, hash choice, seed,
level-salt behavior, and `hard_max_node_bytes`. Invalid combinations are
rejected at configuration construction rather than corrected silently.

For the requested entry-count + key-only policy, leaves hash only the map key
and internal levels hash only the separator. Values and child CIDs do not move
boundaries. All levels use deterministic salts.

Custom algorithms use a stable ID and canonical parameter bytes resolved by an
explicit registry. They must be deterministic and reset their state at a cut.
Opening a tree without the implementation returns `UnknownChunkingAlgorithm`.

### One logical `Node`, selectable physical layouts

There is no `NodeV2` public type. Refactor the existing `Node`, which currently
contains keys, raw values, and duplicated chunking configuration
(`src/prolly/node.rs:21-42`), into a logical structure:

```rust
pub struct Node {
    pub level: u16,
    pub kind: NodeKind,
}

pub enum NodeKind {
    Leaf(Vec<LeafEntry>),
    Internal(Vec<ChildEntry>),
}

pub struct ChildEntry {
    pub separator: Vec<u8>,
    pub cid: Cid,
    pub logical_count: u64,
}
```

A `NodeLayout` encodes, decodes, and estimates encoded length. Initial layouts:

1. `PrefixCompressed`, evolving the current compact CRAB encoding;
2. `OffsetTable`, a safe backing-buffer view for lower-allocation reads;
3. `Plain`, a deterministic conformance/debug representation.

Application layouts are registered by stable ID. Layout controls bytes, not
ordering, separator semantics, subtree-count semantics, or correctness.

### Canonical mutation data flow

All writers converge on one path:

```text
point write --+
batch edits --+--> MutationMerger --> CanonicalWriter --> Store
bulk build  --+
```

1. Normalize mutations into sorted, unique operations and remove no-ops.
2. Start at the beginning of the first affected old chunk; reuse the prefix.
3. Stream old entries plus edits through a leaf `LevelWriter`.
4. At a cut, encode the `Node` with the selected layout and send its first-key
   separator, CID, and logical count to the next level.
5. When a new boundary coincides with the corresponding old boundary, both
   splitters are reset at the same ordered position. Reuse the unchanged suffix.
6. Propagate resynchronization upward and remove redundant one-child roots.
7. Return or publish the new root only after all content-addressed writes
   succeed. Failed operations may leave unreachable nodes but cannot mutate an
   existing root.

Existing routing, prefetch, batch writes, and append hints remain performance
facilities. They do not choose final boundaries. Bulk build uses the same writer
with an empty old cursor and is the reference path for root conformance.

### Errors and safety

Add explicit errors for invalid format parameters, unknown custom algorithms or
layouts, format mismatch, oversized single entries, corrupt layout offsets,
subtree-count overflow, and inconsistent node format digests.

Decoders must validate entry ordering, uniqueness, node kind, child CID width,
child counts, offset ranges, trailing bytes, and allocation limits before
exposing a `Node` or view.

### Verification contract

For every supported `TreeFormat`, build the same map through:

- sorted bulk input;
- ascending, descending, and randomized point writes;
- multiple batch partitions;
- delete/reinsert histories;
- value-only updates;
- rebuild from another format.

All roots must match within the same format. Additional property tests verify:

- key-only policies preserve boundary positions for value-only changes;
- a value-only point update normally writes one node per affected level;
- all count and hard-byte limits hold;
- subtree counts, rank, select, and range count match full iteration;
- resynchronization reuses unaffected prefix and suffix ranges;
- layout encode/decode is deterministic and panic-free on corrupt input;
- store failure cannot publish a partial root.

Extend write statistics with entries streamed, nodes and bytes read/written,
nodes reused, and resynchronization distance. Benchmark uniform, variable-size,
prefix-heavy, sequential, low-entropy, and random-update data. Capture the
current implementation first, then set evidence-based regression budgets for
build throughput, range reads, diff reuse, and write amplification.

## Recommended delivery order

1. Capture baseline root-history and performance behavior.
2. Separate persisted `TreeFormat` from `RuntimeConfig`; define policy and
   layout descriptors plus validation.
3. Refactor the logical `Node` and reproduce the current prefix layout through
   the new codec boundary.
4. Implement built-in chunking policies, including entry-count + key-only hash.
5. Implement the canonical level writer and resynchronization cursor.
6. Route point, batch, and bulk writes through the canonical writer; add
   cross-path root conformance tests.
7. Add subtree counts and ordinal APIs.
8. Add the offset-table layout and custom registries.
9. Add structural patches, external bulk sorting, and `WriteSession` as
   independent follow-up plans.

Steps 2 through 6 form the minimum canonical-core cutover. Do not start the
patch, external-sort, or overlay work until the new core is proven.

## Ideas deliberately not copied

- **FlatBuffers as the only representation:** Rust's prefix layout is compact;
  layout choice should remain pluggable.
- **Unsafe zero-copy access:** use validated safe backing-buffer views.
- **Dolt's last-key separators:** keep Rust's first-key convention and adapt
  boundary synchronization accordingly.
- **Dolt's address width or hash choice:** retain Rust's SHA-256 `Cid`.
- **A wholesale cache replacement:** benchmark contention before considering a
  striped global cache; the Rust cache already has broader controls.
- **A wholesale merge replacement:** add structural patches underneath or
  alongside the existing structural merge rather than discarding it.
- **Proximity-map APIs now:** the inspected Dolt implementation is incomplete.

## Review questions resolved

- Backward compatibility is not required; use a hard cutover.
- Optimize mixed random mutations and diff/merge first, bulk import second.
- Users choose chunking and boundary behavior.
- Entry-count + key-only hash threshold is a required built-in policy.
- Canonical resynchronization is independent of the chosen chunking policy.
- Users choose physical node layout.
- Retain a single logical public `Node`; do not expose a `NodeV2` type.
