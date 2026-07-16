# Prolly Zero-Copy Read Architecture

**Status:** Accepted architecture; implementation and performance validation in progress

**Date:** 2026-07-15; revised 2026-07-16

**Scope:** Program-level design for a safe zero-copy read substrate across the
core prolly tree, diff and merge, versioned maps, secondary indexes, proximity
maps and accelerators, async execution, proofs and maintenance, stores, and
language bindings. Packed immutable read nodes and callback-scoped borrowed
views are the canonical internal read path. Owned APIs remain explicit boundary
adapters. Secondary-index and proximity-map consumers may migrate in stages,
but their codecs, caches, candidate ownership, and query contracts must use the
same foundation now so that no second lifetime or cache redesign is required.

## Summary

Before this work, the Rust implementation paid ownership and cache-management
costs on read paths that Dolt's Go implementation avoids. A warm point read took an
exclusive node-cache lock, updates eviction metadata even when the cache is
unbounded, hashes the root CID on every request, and clones the returned value.
A range scan clones every key and value and clones the key a second time to
maintain a resumable cursor. Diff and merge repeat the same pattern at a larger
scale by materializing owned `Diff`, `Conflict`, lookup-result, and `Mutation`
buffers.

This design makes callback-scoped borrowed views the canonical Rust read
mechanism. Existing owned APIs remain source-compatible wrappers and allocate
only at their explicit ownership boundary. A reusable `ReadSession` retains the
packed root directly, keeps session-local routing state, and traverses immutable
`Arc`-backed node handles. Cache hits use a read-only path when eviction is
disabled, and no cache lock remains held during traversal callbacks.

The same foundation supplies:

- zero-allocation warm point reads and steady-state range scans;
- borrowed structural diff and conflict views;
- merge decisions that select base, left, or right without cloning values;
- borrowed secondary-index payload views and allocation-reusing composite-key
  decoding;
- borrowed proximity-record views, safe encoded-vector access, and bounded
  candidate handles;
- synchronous callbacks inside async traversal without references crossing an
  `.await`;
- owned language-binding and persistence boundaries with no API break.

"Zero-copy" in this design has a precise scope. The preferred store contract
returns a retained `Arc<[u8]>`; the cache and packed `ReadNode` keep that same
allocation alive; values and plain/offset-table keys are slices of it; and a
callback borrows those slices while the handle is strongly retained. A store
that only supports owned `Vec<u8>` reads is adapted once into `Arc<[u8]>` at the
store boundary. Prefix-compressed keys are reconstructed once per node into one
contiguous retained arena, not allocated once per key or result. Existing node
bytes and CIDs are unchanged.

Correctness is non-negotiable. Borrowed references cannot escape their callback,
cached content is immutable and fully validated before admission, snapshots stay
root-bound, visitor reentrancy never occurs under a cache lock, and every owned
legacy result must be byte-for-byte equivalent to the borrowed traversal it
wraps. Unsafe indexing is optional, isolated to a validated packed-node accessor,
and accepted only with a documented safety proof plus safe-path differential,
Miri, fuzz, and malformed-input coverage.

## Normative Decision Snapshot

This section is the short authoritative decision record. If older staging text
elsewhere in this document describes packed nodes, secondary-index views, or
proximity candidate handles as merely hypothetical, this section takes
precedence.

1. **One canonical read representation.** Normal tree reads load
   `Arc<ReadNode>`, not decoded `Arc<Node>`. The write representation may be
   materialized lazily only when a mutation algorithm genuinely needs owned
   vectors. A read must never decode through `Node::from_bytes` merely because
   an owned public result is requested.
2. **One retained byte allocation.** A native shared store read, cache entry,
   packed node, traversal handle, secondary-index decoder, and proximity record
   decoder share immutable bytes through `Arc<[u8]>`. Backends without retained
   buffers use a correct default adapter with one unavoidable boundary move.
3. **Borrow inside, own at escape boundaries.** Callback/HRTB views are the
   internal source of truth. Ownership is introduced only for persisted output,
   public owned return types, cursors/pages/proofs, FFI/WASM, synthesized merge
   values, and final proximity neighbors.
4. **No borrowed reference crosses suspension.** Async methods await a retained
   handle, invoke a synchronous callback, drop all borrows, and only then await
   again. Async application callbacks receive owned data or use bounded pages.
5. **Caches retain handles, never references.** Eviction removes discoverability
   but cannot invalidate an active `Arc`. Cache byte accounting includes packed
   bytes, metadata, prefix arenas, decoded typed objects, and externally retained
   candidate bytes.
6. **Read-session locality is bounded.** A session retains its root and a small
   bounded set of internal route nodes. Leaf retention is adaptive so random
   workloads do not pay continual locality checks or pin a large working set.
   Session state is worker-local and never changes snapshot identity.
7. **Secondary indexes compose views.** Physical-key and index-value decoders
   lend views from the tree callback or bounded scratch. Source joins retain
   bounded locations/keys and use ordered multi-get; they never eagerly own the
   full posting list and source result set.
8. **Proximity search retains candidate sources.** Native hierarchy, eligible
   exact, HNSW, PQ, and composite search retain `(Arc<typed node>, entry_index,
   score)` or an equivalent typed handle. Authoritative reranking borrows exact
   directory records and reuses vector scratch. Only the final `k` results own
   keys and application values.
9. **Persisted objects remain self-contained.** Tree nodes, manifests,
   secondary-index descriptors/checkpoints/bundles, proximity descriptors and
   accelerator objects, cursors, and proofs never serialize process-local
   handles or borrowed offsets.
10. **Performance never weakens validation.** CID, format, ordering, bounds,
    cardinality, vector, projection, snapshot, and accelerator-source checks run
    before cache admission or callback exposure. Fast paths may change how data
    is retained, not what data is accepted.

## Relationship to Existing Designs

This document extends the existing architecture rather than replacing it:

- `docs/architecture.md` remains authoritative for immutable snapshots,
  content addressing, storage, diff, and merge semantics.
- `docs/superpowers/specs/2026-07-14-production-engine-foundation-design.md`
  remains authoritative for deployment profiles, limits, errors, metrics, and
  binding requirements.
- `docs/superpowers/specs/2026-07-14-secondary-index-production-improvements-design.md`
  remains authoritative for indexed-collection consistency, bounded queries,
  and secondary-index persistence.
- `docs/superpowers/specs/2026-07-15-proximity-enhancements-design.md` remains
  authoritative for proximity planning, authoritative reranking, search
  runtime policy, and accelerator lifecycle.
- `docs/prolly-go-rust-benchmark.md` defines the initial cross-language workload
  and correctness digest.

Where those designs require an owned serialized page, cursor, proof, manifest,
or binding value, this design does not weaken that requirement. It removes
intermediate ownership inside the Rust process.

## Pre-implementation Evidence and Measured Motivation

### Cross-language benchmark

The existing one-worker, in-memory comparison uses identical deterministic
string keys, values from 1 through 100 bytes, append/random/clustered workloads,
a 30% mixed mutation phase, and post-write point reads and full range scans.

For the repeated one-million-key fresh/random workload before the packed-read
implementation, the measured median was:

| Operation | Rust | Dolt Go | Current leader |
|---|---:|---:|---:|
| Point read | 1,563 ns/op | 1,436 ns/op | Dolt 1.09x |
| Range scan | 173 ns/row | 112 ns/row | Dolt 1.55x |

To beat the observed Dolt median by 1.5x, Rust must reach no more than
957 ns/point read and 74.6 ns/scanned row on that workload. A 2x result requires
no more than 718 ns/point read and 55.9 ns/scanned row. These are targets, not
predicted or fabricated results.

### Original point-read path

The original synchronous path in `src/prolly/mod.rs`:

1. probes process-global recent-leaf state using atomics and a lock;
2. starts from a root CID rather than a retained root node;
3. takes the node cache's exclusive `RwLock::write` guard for every hit;
4. performs `contains_key` followed by `get_mut`, hashing the CID twice;
5. advances generation-LRU state and appends an access-log record even when the
   default cache has no node or byte limit;
6. clones an `Arc<Node>` for every level;
7. clones the leaf value into a new `Vec<u8>`.

A five-second optimized sample of the existing point-read benchmark attributed
roughly 30% of sampled point-read time to node-cache lookup/bookkeeping and
roughly 14% to leaf-value allocation/copy. That benchmark used a more local
access pattern than the cross-language random workload, so the sample is
bottleneck evidence, not a replacement comparison result.

### Original range path

`RangeIter` owns its start and end bounds, holds an `Arc<Node>` traversal stack,
and yields `Result<(Vec<u8>, Vec<u8>), Error>`. Each row currently:

1. clones the leaf key;
2. clones the leaf value;
3. clones the returned key again into `last_key` for cursor resumption.

Node-cache work is amortized across a leaf, so per-row ownership dominates more
strongly than it does for point reads.

### Original diff and merge paths

Structural diff prunes identical subtrees correctly, but changed leaf entries
become owned `Diff` values and are often queued in `VecDeque<Diff>`. Misaligned
subtree fallback collects both subtrees into owned entry vectors and then clones
again into `Diff` results.

Merge already has valuable fast paths: identical roots and unchanged branches
reuse complete trees, aligned structural merge can reuse child CIDs, and one
leaf path uses `Cow<[u8]>`. Its fallback path still commonly materializes:

```text
owned right Diff list
  -> owned batched left values
  -> owned Conflict
  -> owned Mutation list
  -> rewritten Node values
```

### Original secondary-index paths

Secondary-index exact, prefix, and range convenience methods collect complete
owned result sets. Physical `(term, primary_key)` keys are decoded into two new
vectors, and index projection envelopes copy their payload. `records()` first
owns all index matches and then owns every source lookup value.

### Original proximity-map paths

An exact proximity lookup owns the directory value, decodes the vector into a
new `Vec<f32>`, and owns the application value. Search paths frequently copy
keys, candidate vectors, and authoritative directory records. Search results
must be owned, but most intermediate candidates do not need to be.

## Goals

1. Make a warm cached `get_with` hit allocate zero bytes for the returned value.
2. Make steady-state `scan_range` allocate zero bytes per emitted row.
3. Retain a tree root directly for repeated reads, matching Dolt's root-node
   ownership model without changing persisted `Tree` handles.
4. Remove exclusive cache locking, duplicate hashing, and eviction bookkeeping
   from unbounded-cache hits.
5. Make borrowed traversal the internal source of truth for owned core APIs.
6. Provide borrowed diff and conflict events without materializing a result per
   change.
7. Remove avoidable merge copies while preserving structural CID reuse and
   exact three-way semantics.
8. Establish codec/view contracts that secondary indexes and proximity maps can
   adopt without another foundational API redesign.
9. Preserve synchronous, async, versioned, transaction, proof, and language
   binding correctness.
10. Measure allocation counts, latency, throughput, cache behavior, and peak
    memory honestly across the existing benchmark matrix.

## Non-Goals

The architecture does not:

- promise zero-copy across an FFI, WASM, serialization, or process boundary;
- return a free-standing `&[u8]` whose lifetime is detached from a callback or
  read handle;
- change the persisted node, secondary-index, proximity, proof, cursor, or
  manifest format;
- make `Store::get` borrowed; stores continue to return owned bytes;
- make a standard Rust `Iterator` yield references tied to each mutable
  `next()` call;
- allow an async visitor to retain borrowed content across `.await`;
- guarantee the 1.5x-to-2x performance target before measurements demonstrate
  it;
- require every secondary-index build/maintenance or proximity accelerator to
  migrate in the same core release, although their shared view/handle foundation
  is mandatory;
- use unsafe lifetime extension, self-referential structures, or transmute;
- weaken full validation, snapshot isolation, resource budgets, or deterministic
  result ordering for speed.

## Terminology and Copy Boundaries

### Borrowed read

A borrowed read exposes a slice into an immutable decoded or packed content
handle while that handle is strongly retained. The slice is valid only for the
lexical callback invocation.

### Zero allocation per item

Traversal setup may allocate a path stack, bounds, or reusable scratch space.
"Zero allocation per item" means no heap allocation proportional to each
visited entry once traversal is established. Scratch buffers may grow to a
bounded high-water mark and then be reused.

### Decoded borrowing

An owned `Arc<Node>` can lend its `Vec<Vec<u8>>` keys and values and is still a
valid compatibility or mutation representation. It avoids a result copy, but it
is not the canonical read representation because decoding allocates per key and
value and produces poor cache locality.

### Packed zero-copy

`ReadNode` retains `Arc<[u8]>` encoded bytes plus compact validated offsets, so
values and keys under plain and offset-table layouts are borrowed without
constructing `Vec<Vec<u8>>`. Prefix-compressed keys are reconstructed once into
a contiguous `Arc<[u8]>` arena and addressed by the same metadata. This is the
canonical tree read representation.

### Required ownership

Ownership remains required when data must outlive its content handle, cross a
language/process boundary, be serialized, be persisted, remain in a result heap
after its backing node is released, or represent a newly synthesized value.

## Correctness Invariants

The following are release-blocking:

1. A `ReadSession` is bound to exactly one immutable tree root and persisted
   `TreeFormat` for its complete lifetime.
2. No safe public API permits a borrowed key, value, vector, projection, diff,
   or conflict slice to outlive the callback in which it is supplied.
3. A node or typed content object is fully length-, format-, structure-, and
   CID-validated before it is admitted to a shared cache.
4. A cache entry is immutable after publication. Eviction removes cache
   reachability but never invalidates an active `Arc` handle.
5. No node-cache or typed-content-cache lock is held while invoking application
   code, a resolver, an extractor, a distance callback, or another store API.
6. Owned legacy APIs return the same bytes, ordering, cardinality, cursor
   semantics, error semantics, and snapshot identity as before.
7. Forward scans remain ascending and reverse scans remain descending under raw
   byte ordering. Start bounds are inclusive and end bounds are exclusive.
8. A resumable cursor is materialized only from the last successfully delivered
   entry and resumes strictly after or before it according to direction.
9. A stopped or failed visitor does not mutate persisted tree state. Partial
   delivery before a later read error is explicit in the streaming contract.
10. Diff continues to prune equal CIDs and emits exactly one ordered event per
    logical change.
11. Merge results remain byte-for-byte and CID-equivalent to a clean application
    of the same logical decisions under the configured canonical format.
12. Secondary-index queries remain bound to the selected source version, index
    version, definition fingerprint, direction, and logical bounds.
13. Proximity search continues to rerank returned candidates using authoritative
    vectors and preserves deterministic key tie-breaking.
14. Logical work and budget accounting do not change with cache warmth.
15. The safe packed accessor is the semantic reference. Any unsafe hot accessor
    is isolated, cannot construct references before complete validation, proves
    index and byte-range validity from immutable metadata, and is continuously
    differential-tested against the safe accessor.

## Alternatives Considered

### Runtime `ReadMode::Borrowed | Owned`

This cannot make an API returning `Vec<u8>` zero-copy. Rust return types do not
change at runtime, and a mode branch would add overhead without removing the
required final allocation. It is rejected.

### Change `get` to return a guard

Returning `Option<ValueGuard>` could borrow safely by retaining the leaf node,
but it breaks every Rust caller and language binding, makes values retain whole
nodes for arbitrary durations, complicates cache-byte accounting, and makes
ergonomics worse for simple reads. It is not the compatibility path.

### Lending iterators as the only scan API

An inherent `next(&mut self) -> Result<Option<EntryRef<'_>>, Error>` can be safe,
but it is not a standard `Iterator`, interacts awkwardly with combinators, and
would still require a separate callback form for diff, merge, and bindings.
The architecture leaves room for a future `BorrowedRangeCursor`, but callback
scans are the primary interface.

### Copy everything but optimize the allocator

Arena or slab allocation could lower allocator overhead, but it would continue
copying every byte, retain unnecessary memory, and leave the cache/root gap
untouched. It may complement owned wrappers but is not the core solution.

### Replace the node wire format to obtain packed reads

Rejected. The current compact encodings already contain enough structure to
build validated offsets over retained bytes. A packed in-memory `ReadNode` does
not require a format cutover, so lifetime, cache, and API improvements remain
independent of persisted-format migration. Any future wire-format change must
still be justified separately by measured read/write and storage gains.

## Architecture Overview

```text
Tree + Prolly manager
        |
        v
  ReadSession ------------------- direct root Arc<ReadNode>
        |                         session-local recent leaf
        |                         request-local metrics
        v
  Borrowed traversal core
   |       |       |       |
   |       |       |       +------ scan_diff / scan_conflicts
   |       |       +-------------- scan_range / reverse / prefix
   |       +---------------------- get_many_with / select_with
   +------------------------------ get_with / contains
        |
        v
  EntryRef / DiffRef / ConflictRef
        |
        +---- native callback: no result copy
        |
        +---- legacy Rust API: clone once at owned boundary
        |
        +---- binding/page/proof: serialize owned result

Arc<ReadNode> obtains immutable content through:

  Disabled cache | Unbounded read-mostly cache | Bounded cache
        |                    |                         |
        +--------------------+-------------------------+
                             |
                  shared retained-byte load
                             |
                 CID + format + layout validation
                             |
                    packed Arc<ReadNode>
```

## API Coverage and Adoption Policy

Borrowed traversal is the default internal implementation, not a runtime mode
that changes public return types. The complete API surface adopts it as follows:

| API family | Borrowed/internal behavior | Existing external boundary |
|---|---|---|
| `get`, `contains_key` | `get_with`; presence does not copy value | `get` returns owned `Vec<u8>` |
| `get_many` | route to leaf locations and lend each value | ordered owned result vector |
| `range`, `prefix`, reverse scans | callback traversal over `EntryRef` | owned iterator/page/cursor |
| `len`, `rank` | direct-root scalar traversal | unchanged scalar |
| `select`, first/last, lower/upper bound | internal entry callback | clone one returned entry |
| large-value envelope inspection | parse borrowed stored bytes | resolved blob remains owned |
| typed/value-codec reads | decode inside callback | typed owned result |
| versioned/historical snapshots | snapshot-retained read session | unchanged owned conveniences |
| transactions/write sessions | borrowed overlay/base merge | unchanged owned results |
| cursor navigation | manager-backed node handles | legacy store-only cursor remains compatible |
| diff/range-diff | `DiffRef` visitor | owned diff, iterator, page, proof |
| conflicts/merge | `ConflictRef` and symbolic decisions | legacy owned resolver/error |
| patch/CRDT comparison | borrowed point/diff inputs | persisted patch/result remains owned |
| stats/debug/verification | borrowed traversal and scalar aggregation | owned report structures/text |
| proof generation/verification | borrowed node/entry inspection | proofs remain self-contained and owned |
| sync/GC/content walking | borrow packed nodes while collecting CIDs | transfer manifests and CID sets own data |
| secondary-index query/build | borrowed match/projection/source views | pages, cursors, bundles remain owned |
| proximity exact/search/build | record/vector views and candidate handles | `Neighbor`, result, proof remain owned |
| async equivalents | sync callback after each await | owned streams/pages unchanged |
| language bindings/WASM | serialize directly from borrowed internals | foreign values remain owned |

### Complete migration ledger

The following ledger prevents an optimized point-read implementation from
leaving hidden allocation islands in higher-level APIs. “Borrowed core” means
the operation must consume `ReadNode`, a typed retained content handle, or a
callback-scoped view; it must not call an owned convenience API internally.

| Subsystem/API family | Canonical internal input | Required owned boundary | Foundation decision |
|---|---|---|---|
| point reads, contains, large-value envelope | retained leaf/value slice | legacy `Vec`, resolved external blob | `get_with` and `ValueRefView` |
| multi-get | bounded `ValueLocation { Arc<ReadNode>, index }` | ordered legacy result vector | preserve duplicates, misses, and input order |
| forward/reverse range and prefix | traversal stack plus live leaf handle | iterator item, page, serialized cursor | clone cursor key only at cursor/page boundary |
| rank/select/bounds/first/last | packed internal routing and one leaf view | one optional `KeyValue` | scalar paths never copy values |
| write-session/transaction reads | stable overlay view merged with base session | existing owned read result | no callback while overlay lock is held |
| diff/range-diff | two retained subtree cursors and `DiffRef` | `Diff`, page, proof, sync artifact | no eager changed-subtree vectors |
| conflicts/three-way merge | retained base/left/right views and symbolic decision | synthesized resolution and encoded output nodes | no intermediate owned Diff→Conflict→Mutation chain |
| CRDT/patch | borrowed comparisons and conflict views | persisted patch/result | policy semantics unchanged |
| snapshots/versioned/typed maps | root-bound session for exact historical tree | typed decoded object or legacy bytes | typed decode happens inside callback |
| stats/debug/verify | packed traversal and scalar accumulators | report structures/text | no entry ownership unless report contains bytes |
| membership/range/diff/search proofs | borrowed node/record inspection | self-contained proof object | proof validity never depends on cache lifetime |
| sync/export/import/GC/content graph | borrowed child-CID inspection | CID set, transfer manifest, encoded bundle | transfer/persistence remains owned |
| secondary exact/prefix/range/reverse | `SecondaryIndexMatchRef` over tree entry plus scratch | match/page/cursor | physical ordering and snapshot identity unchanged |
| secondary source joins | bounded posting chunk plus ordered borrowed source reads | requested final records/page | never collect the full match set first |
| secondary build/verify/repair/replace | source scan plus streaming extractor views | sorted run/output nodes, descriptors/checkpoints | copy each derived entry at most once into build state |
| secondary bundle/retention/GC | borrowed tree walk | self-contained bundle/catalog/GC plan | persisted metadata remains canonical |
| proximity exact/contains/record scan | `StoredRecordRef`/`ProximityRecordRef` | exact owned record | validate encoded vector before exposure |
| proximity native hierarchy | retained proximity node and entry index | final neighbors | candidate count and pinned bytes are bounded |
| proximity eligible exact | borrowed directory records | final neighbors | filter eligibility precedes authoritative score |
| HNSW/PQ/SQ8/composite search | typed accelerator handles plus bounded scratch | final reranked neighbors | approximate scores never replace authoritative rerank |
| proximity build/rebuild/mutate/verify | record scan and vector view/scratch | encoded nodes, accelerators, descriptors | representation chosen by reuse and measured kernel cost |
| proximity proof | retained visited typed objects | self-contained proof | proof ordering/completeness independent of cache warmth |
| async variants | retained handles followed by synchronous callbacks | stream/page or caller-owned copy across await | deterministic delivery after concurrent fetch |
| FFI/WASM/language bindings | borrowed internal traversal | target-runtime allocation/serialization | existing foreign signatures need not change |

This ledger is an implementation acceptance checklist. A subsystem is migrated
only when its owned APIs are adapters over the borrowed core and equivalence,
allocation, and malformed-input tests cover that adapter.

Write APIs necessarily create persisted bytes. `put`, `delete`, `batch`, append,
builders, range delete, and merge should accept crate-private borrowed mutation
views where internal producers already have slices, avoiding intermediate
copies. Public owned `Mutation` remains unchanged. A future public
`batch_borrowed` is justified only by write benchmarks; it is not required for
the read architecture.

The existing public `Cursor::at_item(&Store, ...)` has no manager and therefore
cannot use the packed manager cache or direct-root session. It remains a
low-level compatibility API, while `Prolly::cursor`, `range_cursor`, and all new
performance APIs route through manager-backed traversal. Documentation must not
present the store-only cursor as the preferred high-throughput path.

## Normative API Change Set

This section is the implementation contract for the API surface. Later sections
define behavior and algorithms; if an illustrative signature later in the
document is abbreviated, the signatures here take precedence. Core public
read-view types live in `prolly::read` and are re-exported from the crate root
beside the existing `Prolly`, `Tree`, `Diff`, and `Conflict` types. Deferred
secondary-index and proximity views live in their domain modules and follow the
crate's existing root re-export policy.

The status labels used below are:

- **add now**: public API delivered in core phases 1–4;
- **update internally**: an existing public signature remains unchanged, but its
  implementation must use the borrowed primitive and allocate only at its owned
  boundary;
- **crate-private**: foundation required now without a public stability promise;
- **foundation now**: public/typed view contract and cache/lifetime behavior are
  fixed now; individual secondary-index or proximity consumers may migrate in
  later performance phases;
- **unchanged**: no signature, ownership, serialization, or binding change.

### Public types added in core phases 1–4

```rust,ignore
pub struct EntryRef<'a> { /* private fields */ }

impl<'a> EntryRef<'a> {
    pub fn key(&self) -> &'a [u8];
    pub fn value(&self) -> &'a [u8];
    pub fn to_owned(self) -> KeyValue;
}

pub struct ScanOutcome<B> {
    pub visited: u64,
    pub break_value: Option<B>,
}

pub enum ValueRefView<'a> {
    Inline(&'a [u8]),
    Blob { cid: Cid, len: u64 },
}

impl ValueRefView<'_> {
    pub fn to_owned(self) -> blob::ValueRef;
}

pub struct ReadSession<'manager, 'tree, S: Store> { /* private fields */ }

#[derive(Clone, Copy, Debug)]
pub enum DiffRef<'a> {
    Added { key: &'a [u8], value: &'a [u8] },
    Removed { key: &'a [u8], value: &'a [u8] },
    Changed {
        key: &'a [u8],
        old: &'a [u8],
        new: &'a [u8],
    },
}

impl DiffRef<'_> {
    pub fn key(&self) -> &[u8];
    pub fn to_owned(self) -> Diff;
}

#[derive(Clone, Copy, Debug)]
pub struct ConflictRef<'a> {
    pub key: &'a [u8],
    pub base: Option<&'a [u8]>,
    pub left: Option<&'a [u8]>,
    pub right: Option<&'a [u8]>,
}

impl ConflictRef<'_> {
    pub fn to_owned(self) -> Conflict;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeDecision {
    UseBase,
    UseLeft,
    UseRight,
    Value(Vec<u8>),
    Delete,
    Unresolved,
}

pub trait BorrowedMergeResolver: Send + Sync {
    fn resolve(&self, conflict: ConflictRef<'_>) -> MergeDecision;
}
```

`ValueRefView::Blob` owns its fixed-size CID because the CID parser already
validates and materializes that small identifier. The potentially large inline
payload remains borrowed. No public view contains a reference that can outlive
its callback.

### `Prolly` and `ReadSession` additions

The root-bound session is the primary high-throughput interface. Direct
`Prolly` visitor methods are one-shot conveniences that create a session and
delegate to it; they do not contain a second traversal implementation.

```rust,ignore
impl<S: Store> Prolly<S> {
    pub fn read<'manager, 'tree>(
        &'manager self,
        tree: &'tree Tree,
    ) -> Result<ReadSession<'manager, 'tree, S>, Error>;

    pub fn get_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error>;

    pub fn contains_key(&self, tree: &Tree, key: &[u8]) -> Result<bool, Error>;

    pub fn get_value_ref_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error>;

    pub fn get_many_with<K, F>(
        &self,
        tree: &Tree,
        keys: &[K],
        visit: F,
    ) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        F: for<'value> FnMut(usize, &[u8], Option<&'value [u8]>);

    pub fn select_with<R>(
        &self,
        tree: &Tree,
        ordinal: u64,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error>;

    pub fn first_entry_with<R>(
        &self,
        tree: &Tree,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error>;

    pub fn last_entry_with<R>(
        &self,
        tree: &Tree,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error>;

    pub fn lower_bound_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error>;

    pub fn upper_bound_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error>;

    pub fn scan_range(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;

    pub fn scan_range_until<B>(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;

    pub fn scan_prefix(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;

    pub fn scan_prefix_until<B>(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;

    pub fn scan_range_reverse(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;

    pub fn scan_range_reverse_until<B>(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;

    pub fn scan_prefix_reverse(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;

    pub fn scan_prefix_reverse_until<B>(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
}
```

`ReadSession` exposes the same operations without the `tree` parameter:

```rust,ignore
impl<S: Store> ReadSession<'_, '_, S> {
    pub fn tree(&self) -> &Tree;
    pub fn len(&self) -> Result<u64, Error>;
    pub fn rank(&mut self, key: &[u8]) -> Result<u64, Error>;
    pub fn get_with<R>(&mut self, key: &[u8], read: impl FnOnce(&[u8]) -> R)
        -> Result<Option<R>, Error>;
    pub fn get_value_ref_with<R>(
        &mut self,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn contains_key(&mut self, key: &[u8]) -> Result<bool, Error>;
    pub fn get_many_with<K, F>(&mut self, keys: &[K], visit: F) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        F: for<'value> FnMut(usize, &[u8], Option<&'value [u8]>);
    pub fn select_with<R>(
        &mut self,
        ordinal: u64,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn first_entry_with<R>(
        &mut self,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn last_entry_with<R>(
        &mut self,
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn lower_bound_with<R>(
        &mut self,
        key: &[u8],
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn upper_bound_with<R>(
        &mut self,
        key: &[u8],
        read: impl for<'entry> FnOnce(EntryRef<'entry>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn scan_range(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub fn scan_range_until<B>(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
    pub fn scan_prefix(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub fn scan_prefix_until<B>(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
    pub fn scan_range_reverse(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub fn scan_range_reverse_until<B>(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
    pub fn scan_prefix_reverse(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub fn scan_prefix_reverse_until<B>(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
}
```

`get_many_with` invokes the callback exactly once per input position, in input
order, including duplicate keys and misses. The key argument is the caller's
input slice; the optional value is borrowed only for that invocation. Reverse
ranges retain the existing half-open `[start, end)` semantics and merely change
delivery order.

### Diff and merge additions

```rust,ignore
impl<S: Store> Prolly<S> {
    pub fn scan_diff(
        &self,
        base: &Tree,
        other: &Tree,
        visit: impl for<'diff> FnMut(DiffRef<'diff>),
    ) -> Result<u64, Error>;

    pub fn scan_diff_until<B>(
        &self,
        base: &Tree,
        other: &Tree,
        visit: impl for<'diff> FnMut(DiffRef<'diff>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;

    pub fn scan_range_diff(
        &self,
        base: &Tree,
        other: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'diff> FnMut(DiffRef<'diff>),
    ) -> Result<u64, Error>;

    pub fn scan_range_diff_until<B>(
        &self,
        base: &Tree,
        other: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'diff> FnMut(DiffRef<'diff>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;

    pub fn scan_conflicts(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        visit: impl for<'conflict> FnMut(ConflictRef<'conflict>),
    ) -> Result<u64, Error>;

    pub fn scan_conflicts_until<B>(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        visit: impl for<'conflict> FnMut(ConflictRef<'conflict>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;

    pub fn merge_with(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        resolver: Option<&dyn BorrowedMergeResolver>,
    ) -> Result<Tree, Error>;

    pub fn merge_range_with(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        resolver: Option<&dyn BorrowedMergeResolver>,
    ) -> Result<Tree, Error>;

    pub fn merge_prefix_with(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        prefix: &[u8],
        resolver: Option<&dyn BorrowedMergeResolver>,
    ) -> Result<Tree, Error>;
}
```

The `*_until` methods are the sole public early-stop variants; ordinary visitor
methods delegate with `ControlFlow::Continue(())`. `merge_with` is intentionally
distinct from the existing `merge(..., Option<Resolver>)`, so existing resolver
closures keep their owned `Conflict` contract and source compatibility.

### Versioned-map additions

Snapshot methods reuse the core types rather than introduce snapshot-specific
entry views. These are **add now** because secondary indexes already depend on
`MapSnapshot` and need a stable zero-copy source-reader foundation.

```rust,ignore
impl<'snapshot, S: Store> MapSnapshot<'snapshot, S> {
    pub fn read<'tree>(
        &'tree self,
    ) -> Result<ReadSession<'snapshot, 'tree, S>, Error>;
    pub fn get_with<R>(
        &self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn get_value_ref_with<R>(
        &self,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn get_many_with<K, F>(&self, keys: &[K], visit: F) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        F: for<'value> FnMut(usize, &[u8], Option<&'value [u8]>);
    pub fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub fn scan_prefix(
        &self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
}

impl<'engine, S: Store> VersionedMap<'engine, S> {
    pub fn get_with<R>(
        &self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn get_at_with<R>(
        &self,
        id: &MapVersionId,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn get_value_ref_with<R>(
        &self,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn get_value_ref_at_with<R>(
        &self,
        id: &MapVersionId,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub fn scan_prefix(
        &self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub fn scan_range_at(
        &self,
        id: &MapVersionId,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub fn scan_prefix_at(
        &self,
        id: &MapVersionId,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
}
```

`MapSnapshot::read` borrows the manager for `'snapshot` and the snapshot tree
for `'tree`; the implementation may shorten the manager borrow to the method
borrow if Rust's inferred variance permits a simpler concrete signature. The
observable guarantee is that the session cannot outlive either snapshot input.
The typed map decodes inside `get_with` internally; it does not expose encoded
borrowed bytes through `TypedVersionedMap<K, V>`.

### Async additions behind `async-store`

The async API awaits node loads and then invokes a synchronous callback. It does
not hold a borrowed value across `.await`, and it does not accept an async
visitor. This makes callback lifetime safety identical to the synchronous API.

```rust,ignore
pub struct AsyncReadSession<'manager, 'tree, S: AsyncStore> { /* private */ }

impl<S: AsyncStore> AsyncProlly<S> {
    pub async fn read<'manager, 'tree>(
        &'manager self,
        tree: &'tree Tree,
    ) -> Result<AsyncReadSession<'manager, 'tree, S>, Error>;
    pub async fn get_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error>;
    pub async fn get_value_ref_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error>;
    pub async fn get_many_with<K, F>(
        &self,
        tree: &Tree,
        keys: &[K],
        visit: F,
    ) -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        F: for<'value> FnMut(usize, &[u8], Option<&'value [u8]>);
    pub async fn scan_range(
        &self,
        tree: &Tree,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub async fn scan_prefix(
        &self,
        tree: &Tree,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub async fn scan_diff(
        &self,
        base: &Tree,
        other: &Tree,
        visit: impl for<'diff> FnMut(DiffRef<'diff>),
    ) -> Result<u64, Error>;
    pub async fn scan_conflicts(
        &self,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        visit: impl for<'conflict> FnMut(ConflictRef<'conflict>),
    ) -> Result<u64, Error>;
}

impl<S: AsyncStore> AsyncReadSession<'_, '_, S> {
    pub async fn get_with<R>(
        &mut self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error>;
    pub async fn get_value_ref_with<R>(
        &mut self,
        key: &[u8],
        read: impl for<'value> FnOnce(ValueRefView<'value>) -> R,
    ) -> Result<Option<R>, Error>;
    pub async fn get_many_with<K, F>(&mut self, keys: &[K], visit: F)
        -> Result<(), Error>
    where
        K: AsRef<[u8]>,
        F: for<'value> FnMut(usize, &[u8], Option<&'value [u8]>);
    pub async fn scan_range(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
    pub async fn scan_prefix(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
}
```

The complete async addition set is listed below. I/O methods have the matching
synchronous signature above with `async fn`, `AsyncReadSession`, or the async
snapshot type substituted; pure accessors such as `tree` remain synchronous and
callbacks remain synchronous.

| Async type | Added method names |
|---|---|
| `AsyncProlly` | `read`, `get_with`, `get_value_ref_with`, `get_many_with`, `scan_range`, `scan_range_until`, `scan_prefix`, `scan_prefix_until`, `scan_range_reverse`, `scan_range_reverse_until`, `scan_prefix_reverse`, `scan_prefix_reverse_until`, `scan_diff`, `scan_diff_until`, `scan_range_diff`, `scan_range_diff_until`, `scan_conflicts`, `scan_conflicts_until`, `merge_with`, `merge_range_with`, `merge_prefix_with` |
| `AsyncReadSession` | `tree`, `len`, `rank`, `get_with`, `get_value_ref_with`, `contains_key`, `get_many_with`, `select_with`, `first_entry_with`, `last_entry_with`, `lower_bound_with`, `upper_bound_with`, `scan_range`, `scan_range_until`, `scan_prefix`, `scan_prefix_until`, `scan_range_reverse`, `scan_range_reverse_until`, `scan_prefix_reverse`, `scan_prefix_reverse_until` |
| `AsyncMapSnapshot` | `read`, `get_with`, `get_value_ref_with`, `get_many_with`, `scan_range`, `scan_prefix` |
| `AsyncVersionedMap` | `get_with`, `get_at_with`, `get_value_ref_with`, `get_value_ref_at_with`, `scan_range`, `scan_prefix`, `scan_range_at`, `scan_prefix_at` |

Async borrowed merge resolvers execute only after all values for one conflict
are retained and before the next await. There is no async borrowed resolver
trait; an application that must await resolution uses the existing owned
conflict workflow.

### Existing public methods updated internally

No method in this table changes its signature. Each must become a thin owned or
scalar adapter over the new foundation, so optimizations benefit the entire API
instead of only callers that opt into visitors.

| Type/module | Existing methods routed through borrowed traversal |
|---|---|
| `Prolly` | `get`, `get_value_ref`, `get_large_value`, `get_many`, `len`, `rank`, `select`, `first_entry`, `last_entry`, `lower_bound`, `upper_bound`, `range`, `prefix`, `range_after`, `range_from_cursor`, `prefix_page`, `range_page`, `reverse_page`, `prefix_reverse_page`, `reverse_range_page`, `cursor`, `cursor_window`, `range_cursor` |
| diff/merge | `diff`, `range_diff`, `diff_from_cursor`, `diff_page`, `structural_diff_page`, `diff_cursor`, `stream_diff`, `stream_conflicts`, `merge`, `merge_explain`, `merge_range`, `merge_prefix`, CRDT merge variants |
| writes/transactions | `put`, `put_large_value`, `delete`, `delete_range`, all batch/append/parallel-batch variants, write-session reads and overlay/base ordered merge |
| maintenance | stats, debug comparison, verification, proofs, reachability, missing-node planning/copying, export/import, and GC walkers use `ReadNode`/`NodeHandle` accessors, but keep their owned reports and artifacts |
| `MapSnapshot` | `get`, `get_value_ref`, `get_large_value`, `contains_key`, `get_many`, `first_entry`, `last_entry`, `lower_bound`, `upper_bound`, `range`, `stream_range`, `prefix`, `stream_prefix`, `range_page`, `prefix_page`, `reverse_page`, `prefix_reverse_page`, `reverse_scan`, `prefix_reverse_scan`, `cursor_window`, stats/debug/proof/sync helpers |
| `VersionedMap` | current and historical `get`, `get_large_value`, `contains_key`, `get_many`, `range`, `prefix`, `range_page`, `prefix_page`, `get_at`, `get_many_at`, `range_at`, `prefix_at`, `range_page_at`, `prefix_page_at`, `diff`, and `changes_since` families |
| async types | every async counterpart of the core, snapshot, versioned, diff, merge, cursor/page, proof, and maintenance families above |

The legacy `RangeIter`, cursor, diff iterator, page, proof, `KeyValue`, `Diff`,
`Conflict`, and blob result types still own their yielded data. They clone once
when an item crosses that compatibility boundary; they must not trigger an
additional intermediate clone inside traversal.

### Crate-private foundation added now

These names are implementation-oriented and may evolve without semver impact:

| Area | Crate-private additions | Purpose |
|---|---|---|
| retained content | `NodeHandle = Arc<ReadNode>`, typed content handles | keep validated bytes and metadata alive for callback scope |
| routing | `LeafWindow`, `SessionNodeTable`, `ValueLocation`, `ReadPath` | adaptive recent leaf, retained internal routes, and point lookup location |
| scans | `RangeTraversal`, `ReverseRangeTraversal`, `BorrowedLeafCursor` | bounded forward/reverse leaf traversal shared by visitors and owned adapters |
| comparison | `BorrowedDiffWalker`, `BorrowedConflictWalker` | aligned and misaligned subtree streaming without eager vectors |
| writes | `BorrowedMutation<'a>`, `BorrowedMutationSource` | remove temporary owned mutation/diff copies before final node encoding |
| cache | `CachePolicy`, `CacheLookup`, `CacheAdmission`, `PinnedReadHandle` | separate read hits from bounded-policy bookkeeping and pin callback backing |
| instrumentation | `LocalReadMetrics`, `AllocationClass` | aggregate hot-loop counters and distinguish required boundary allocation |

The existing public `Store`, `AsyncStore`, `BlobStore`, `Tree`, `Config`, `Node`,
`Mutation`, `Resolver`, `Resolution`, and persistence formats are **unchanged**.
In particular, zero-copy does not require storage backends to return borrowed
buffers, and it does not change canonical node CIDs.

### Secondary-index public API and migration contract

These names and semantics are part of the shared foundation. Borrowed codecs,
query visitors, source joins, and streaming extraction are implemented first;
build/maintenance consumers migrate behind the unchanged owned API surface:

```rust,ignore
pub enum IndexValueRef<'a> {
    KeysOnly,
    Included(&'a [u8]),
    FullSource(&'a [u8]),
}

impl<'a> IndexValueRef<'a> {
    pub fn decode(bytes: &'a [u8], limit: usize) -> Result<Self, Error>;
    pub fn to_owned(self) -> IndexValue;
}

pub struct DecodedPhysicalIndexKeyRef<'a> {
    pub term: &'a [u8],
    pub primary_key: &'a [u8],
}

pub struct SecondaryIndexMatchRef<'a> {
    pub term: &'a [u8],
    pub primary_key: &'a [u8],
    pub projection: Option<&'a [u8]>,
}

impl SecondaryIndexMatchRef<'_> {
    pub fn to_owned(self) -> SecondaryIndexMatch;
}

pub struct IndexedSourceRecordRef<'a> {
    pub term: &'a [u8],
    pub primary_key: &'a [u8],
    pub projection: Option<&'a [u8]>,
    pub source_value: &'a [u8],
}

impl IndexedSourceRecordRef<'_> {
    pub fn to_owned(self) -> IndexedSourceRecord;
}

impl<'snapshot, S: Store> SecondaryIndexSnapshot<'snapshot, S> {
    pub fn scan_exact(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error>;
    pub fn scan_prefix(
        &self,
        prefix: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error>;
    pub fn scan_range(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error>;
    pub fn scan_records(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(IndexedSourceRecordRef<'row>),
    ) -> Result<u64, Error>;
    pub fn scan_exact_until<B>(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>)
            -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
    pub fn scan_prefix_until<B>(
        &self,
        prefix: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>)
            -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
    pub fn scan_range_until<B>(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>)
            -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
    pub fn scan_records_until<B>(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(IndexedSourceRecordRef<'row>)
            -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
    pub fn scan_exact_reverse(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error>;
    pub fn scan_prefix_reverse(
        &self,
        prefix: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error>;
    pub fn scan_range_reverse(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error>;
    pub fn scan_exact_reverse_until<B>(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>)
            -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
    pub fn scan_prefix_reverse_until<B>(
        &self,
        prefix: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>)
            -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
    pub fn scan_range_reverse_until<B>(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>)
            -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
}

impl<'engine, S> IndexedMap<'engine, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    pub fn get_with<R>(
        &self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error>;
}

pub struct SecondaryIndexEntryRef<'a> {
    pub term: &'a [u8],
    pub projection: Option<&'a [u8]>,
}

pub trait StreamingSecondaryIndexExtractor: Send + Sync {
    fn extract(
        &self,
        primary_key: &[u8],
        source_value: &[u8],
        emit: &mut dyn FnMut(SecondaryIndexEntryRef<'_>)
            -> Result<(), SecondaryIndexError>,
    ) -> Result<(), SecondaryIndexError>;
}
```

`SecondaryIndexSnapshot::{exact,prefix,range,primary_keys,projected,records}`
and all page/reverse-page methods become owned adapters. `IndexedMap::get`,
build, verify, repair, replace, and mutation coordination consume core snapshot
visitors internally. `IndexValueRef` and physical-key decoding validate exactly
the same limits and canonical encoding as their owned counterparts. Escaped
physical-key components may use bounded reusable scratch, so the guarantee is
steady-state allocation-free decoding, not universal direct subslicing.
Forward and reverse visitors preserve the exact logical ordering used by the
corresponding owned page API. Early stop returns the last delivered count but
does not manufacture a serialized cursor; callers that need resumability use
the existing owned page/cursor boundary.

### Proximity-map public API and migration contract

Public views expose safe logical records. Byte-layout helpers remain
crate-private so an alignment or encoding change does not leak into the API.

```rust,ignore
#[derive(Clone, Copy, Debug)]
pub struct ProximityVectorRef<'a> { /* private encoded bytes and dimensions */ }

impl ProximityVectorRef<'_> {
    pub fn dimensions(&self) -> usize;
    pub fn component(&self, index: usize) -> Option<f32>;
    pub fn iter(&self) -> impl ExactSizeIterator<Item = f32> + '_;
    pub fn copy_to_slice(&self, output: &mut [f32]) -> Result<(), Error>;
    pub fn to_vec(&self) -> Vec<f32>;
}

#[derive(Clone, Copy, Debug)]
pub struct ProximityRecordRef<'a> {
    pub vector: ProximityVectorRef<'a>,
    pub value: &'a [u8],
}

impl ProximityRecordRef<'_> {
    pub fn to_owned(self) -> ExactProximityRecord;
}

pub struct ProximityReadSession<'map, S: Store> { /* private fields */ }

impl<S: Store> ProximityMap<S> {
    pub fn read(&self) -> Result<ProximityReadSession<'_, S>, Error>;
    pub fn get_with<R>(
        &self,
        key: &[u8],
        read: impl for<'record> FnOnce(ProximityRecordRef<'record>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn scan_records(
        &self,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>),
    ) -> Result<u64, Error>;
    pub fn scan_records_until<B>(
        &self,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>)
            -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
}

impl<S: Store> ProximityReadSession<'_, S> {
    pub fn get_with<R>(
        &mut self,
        key: &[u8],
        read: impl for<'record> FnOnce(ProximityRecordRef<'record>) -> R,
    ) -> Result<Option<R>, Error>;
    pub fn contains_key(&mut self, key: &[u8]) -> Result<bool, Error>;
    pub fn scan_records(
        &mut self,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>),
    ) -> Result<u64, Error>;
    pub fn scan_records_until<B>(
        &mut self,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>)
            -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error>;
}
```

`StoredRecordRef` and `EncodedVectorRef` are the crate-private wire views that
back public `ProximityRecordRef` and `ProximityVectorRef`. They validate format,
vector dimensions, finite canonical components, and value bounds before the
callback.
They never cast persisted bytes to `&[f32]`. `ProximityMap::{get,contains_key}`,
rebuild/mutate, verification, exact search, SQ8/PQ/HNSW build, and async search
then consume these views internally. Existing `ExactProximityRecord`,
`Neighbor`, `SearchResult`, descriptors, proofs, and persisted accelerator
artifacts remain owned and unchanged.

`AsyncProximityMap` gains `read`, `get_with`, and `scan_records` counterparts
with synchronous post-await callbacks when its exact-read surface is migrated.
Approximate-search callback results are deliberately not exposed: candidate
heaps use retained internal handles, but a public result remains self-contained.

### Source-file ownership and delivery

| Source area | API/design ownership | Delivery phase |
|---|---|---|
| new `src/prolly/read.rs` plus root re-exports | public views, sessions, scan outcomes | 1–2 |
| new/extracted `src/prolly/cache.rs`, existing `src/prolly/node.rs` | retained read handles, accounting, `ReadNode` accessors | 1 |
| `src/prolly/mod.rs`, `range.rs`, `cursor.rs`, `blob.rs` | point/bound/scan visitors and owned adapters | 2 |
| `src/prolly/diff.rs`, `error.rs`, merge/write modules | borrowed diff/conflict views, symbolic resolver, streaming apply | 3–4 |
| `src/prolly/versioned_map.rs`, typed map layer, async modules | snapshot/session forwarding and decode-in-callback | 2–4 |
| `src/prolly/secondary_index/{storage,snapshot,definition,coordinator}.rs` | deferred index views/query/build adoption | 5 |
| `src/prolly/proximity/storage/record.rs`, `map.rs`, `search/`, `accelerator/`, `proof/` | record/vector views, retained candidate handles, authoritative borrowed rerank | 6 |
| language bindings and WASM adapters | no signature change; serialize/copy at FFI boundary | after each native phase |

Every public borrowed-view addition requires rustdoc examples for both
allocation-free consumption and explicit `to_owned` escape. Compile-fail
doctests must prove
that `EntryRef`, `DiffRef`, `ConflictRef`, secondary-index scratch views, and
proximity record views cannot escape their callback. Existing binding APIs are
regression-tested but do not expose Rust lifetimes.

## Core Types

Names and behavior in this section explain the normative API set above. Core
public read views live in `prolly::read` and are re-exported from the crate
root; crate-private helpers follow the source-file ownership table.

### Entry view

```rust,ignore
#[derive(Clone, Copy, Debug)]
pub struct EntryRef<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> EntryRef<'a> {
    pub fn key(&self) -> &'a [u8];
    pub fn value(&self) -> &'a [u8];
    pub fn to_owned(self) -> (Vec<u8>, Vec<u8>);
}
```

Fields remain private so packed-node metadata and typed views can preserve
invariants without changing construction sites. `EntryRef` is copyable, but the
compiler still prevents it from escaping its callback lifetime.

### Read session

```rust,ignore
pub struct ReadSession<'manager, 'tree, S: Store> {
    manager: &'manager Prolly<S>,
    tree: &'tree Tree,
    root: Option<Arc<ReadNode>>,
    recent_leaf: Option<LeafWindow>,
    route_nodes: SessionNodeTable,
    local_metrics: LocalReadMetrics,
}

impl<S: Store> Prolly<S> {
    pub fn read<'manager, 'tree>(
        &'manager self,
        tree: &'tree Tree,
    ) -> Result<ReadSession<'manager, 'tree, S>, Error>;
}
```

`read()` validates that the tree format matches the manager and loads the root
once. Empty trees produce a session with no root and no store read. A session is
intended for one worker and its hot tree. It is movable but not concurrently
mutated; callers create one session per worker. The manager cache remains shared.

The session owns no persisted state and does not alter tree identity. Dropping it
releases its root and recent-leaf handles and flushes any local metric aggregate.

### Packed node handle and representation boundary

```rust,ignore
type NodeHandle = Arc<ReadNode>;

struct ReadEntryMeta {
    key_start: u32,
    key_len: u32,
    value_start: u32,
    value_len: u32,
    child_count: u64,
}

pub(crate) struct ReadNode {
    bytes: Arc<[u8]>,
    prefix_keys: Option<Arc<[u8]>>,
    entries: Box<[ReadEntryMeta]>,
    leaf: bool,
    level: u8,
    format: TreeFormat,
}

struct NodeCacheEntry {
    read: OnceLock<Arc<ReadNode>>,
    owned: OnceLock<Arc<Node>>,
    retained_bytes: usize,
    /* eviction metadata */
}
```

`bytes` is the immutable encoded allocation returned by `Store::get_shared` or
created once by the owned-store adapter. Plain and offset-table keys and all
values point into `bytes`. `prefix_keys` exists only for prefix-compressed
layouts and contains all reconstructed keys in one immutable arena. Offsets are
32-bit only after the parser rejects any object that cannot be addressed by
them. This makes metadata compact and gives validation an explicit maximum
object size.

Traversal code accesses `ReadNode` through concrete inlineable methods:

```rust,ignore
fn len(&self) -> usize;
fn level(&self) -> u8;
fn is_leaf(&self) -> bool;
fn key(&self, index: usize) -> Option<&[u8]>;
fn value(&self, index: usize) -> Option<&[u8]>;
fn child_cid(&self, index: usize) -> Result<Cid, Error>;
fn child_count(&self, index: usize) -> Option<u64>;
fn search(&self, key: &[u8]) -> Result<usize, usize>;
```

This is not a trait object and introduces no virtual call or representation
branch per key comparison. Parsing validates the compact header, canonical tree
format, layout-specific offsets, sorted unique keys, internal-node shape, value
ranges, and trailing bytes before publication. CID equality is checked by the
loader before cache admission.

The cache's `read` representation is authoritative for reads. `owned` is
materialized lazily only for a write/legacy structural algorithm that cannot yet
consume `ReadNode`. Cache insertion must not retain independent packed and
decoded source allocations accidentally: conversion reuses the entry and byte
accounting reflects every simultaneously live representation. New read code is
not allowed to request `owned`.

The binary-search hot path may use one isolated unchecked accessor after full
validation. Its safety proof is: the entry index is bounded by `entries.len()`;
all `(start, len)` pairs were range-checked against an immutable `Arc` backing;
metadata and backing bytes cannot mutate; and the returned slice cannot outlive
`&self`. A safe accessor remains available for validation and differential tests.

Write paths may continue using owned `Node` during early phases. Read and write
representations must share validation fixtures and canonical encoding tests.

## Public Core Read API

### Point read

```rust,ignore
impl<S: Store> ReadSession<'_, '_, S> {
    pub fn get_with<R>(
        &mut self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error>;

    pub fn contains_key(&mut self, key: &[u8]) -> Result<bool, Error>;
}

impl<S: Store> Prolly<S> {
    pub fn get_with<R>(
        &self,
        tree: &Tree,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error>;
}
```

The callback runs exactly once for an exact hit and never for a miss. `R` cannot
borrow from the callback value because its type is independent of that value's
lifetime. It may be a checksum, decoded application object, comparison result,
or owned copy. A caller that needs fallible decoding returns `Result<R, E>` and
transposes the nested result.

The existing API becomes:

```rust,ignore
pub fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    self.get_with(tree, key, <[u8]>::to_vec)
}
```

`contains_key` stops after exact-key validation and never clones the value.

`get_value_ref_with` parses the large-value envelope while the stored leaf value
is borrowed. Raw and envelope-inline payloads are lent as
`ValueRefView::Inline`; blob references validate their CID and length without
loading blob content. Existing `get_value_ref` converts the view once to owned
`blob::ValueRef`, while `get_large_value` necessarily owns bytes returned by the
separate `BlobStore` contract.

### Range scan

The simple full-delivery API is:

```rust,ignore
impl<S: Store> ReadSession<'_, '_, S> {
    pub fn scan_range(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;

    pub fn scan_prefix(
        &mut self,
        prefix: &[u8],
        visit: impl for<'entry> FnMut(EntryRef<'entry>),
    ) -> Result<u64, Error>;
}
```

An early-stop form uses `std::ops::ControlFlow` and returns both the number of
delivered entries and the break value:

```rust,ignore
pub struct ScanOutcome<B> {
    pub visited: u64,
    pub break_value: Option<B>,
}

pub fn scan_range_until<B>(
    &mut self,
    start: &[u8],
    end: Option<&[u8]>,
    visit: impl for<'entry> FnMut(EntryRef<'entry>) -> ControlFlow<B>,
) -> Result<ScanOutcome<B>, Error>;
```

A visitor with its own error uses `Break(error)` and handles the break value
after the tree operation returns. This keeps engine read errors and application
errors separate without boxing or constraining the application's error type.

Reverse counterparts use a separate specialized loop rather than a per-row
direction branch:

```rust,ignore
pub fn scan_range_reverse(...);
pub fn scan_prefix_reverse(...);
```

### Cursor semantics

The zero-copy scanner tracks the last delivered location as `(NodeHandle,
index)`, not an owned `last_key`. It clones a key only when the caller requests a
cursor, a page boundary is reached, or an owned iterator wrapper must expose its
existing resumability API. This removes the current second key copy from every
row.

The existing `RangeIter` remains a standard owned iterator. It delegates its
navigation to the same traversal core and calls `EntryRef::to_owned()` once per
item. It does not become the foundation for borrowed scans because the
`Iterator` trait cannot express its per-`next` borrow.

### Multi-get

Multi-get requires special lifetime handling because results may reside in
different leaves. The read substrate records lightweight locations rather than
owned values:

```rust,ignore
struct ValueLocation {
    node: NodeHandle,
    index: usize,
}
```

`get_many_with` preserves caller order, including duplicates, and invokes the
visitor once per input position:

```rust,ignore
pub fn get_many_with<K>(
    &mut self,
    keys: &[K],
    visit: impl for<'value> FnMut(usize, &[u8], Option<&'value [u8]>),
) -> Result<(), Error>
where
    K: AsRef<[u8]>;
```

Routing still batches shared ancestors and leaves. Duplicate inputs share the
same leaf handle. The temporary location vector is O(number of requested keys),
but values are not copied. A future unordered/event form may reduce location
storage for callers that do not require input ordering; it is not needed for
compatibility.

The existing `get_many` visits locations in input order and clones only present
values into its result vector.

### Rank, select, and bounds

`len` and `rank` already return scalars and benefit automatically from direct
root retention and cache improvements. Add internal `select_with`,
`lower_bound_with`, `upper_bound_with`, `first_entry_with`, and
`last_entry_with` primitives. Public additions should be limited to demonstrated
native demand; existing owned methods delegate to these primitives and clone one
final entry.

Large-value reference parsing uses `get_with` so inline/envelope inspection does
not first clone stored bytes. Resolving an external blob still returns owned
bytes because the blob-store contract is owned.

## Point-Read Algorithm

For a reusable session:

1. If the tree is empty, return `None`.
2. If leaf locality remains enabled, check the session-local recent leaf using
   its first and last key. This uses no atomics or global lock. After a small
   bounded run of misses, disable the check for the session so random workloads
   do not pay it forever.
3. Start with the session's retained root handle.
4. Search the current node for the greatest key less than or equal to the query.
5. For an internal node, load its child handle after releasing all cache locks.
6. For a leaf, verify exact key equality.
7. Store the leaf in the session-local recent-leaf window only while the
   locality predictor remains useful.
8. Invoke the callback with the leaf value slice while the leaf handle is alive.

Random reads pay only a bounded number of recent-leaf checks per session.
Clustered and sequential reads can avoid the complete root-to-leaf traversal.
The existing process-global atomics and `RwLock<Option<RecentLeafRead>>` are
removed from the new session path. The convenience `Prolly::get_with` uses an
ephemeral session; high-throughput users and benchmarks use a reusable session.

The session also has a fixed-size set-associative route-node table keyed by CID.
It retains internal nodes strongly for the session lifetime, avoids repeated
global-cache locking and `Weak::upgrade` atomics, and deliberately excludes
leaves so a random point workload cannot pin its entire leaf working set. Table
size and associativity are performance constants validated across 1M, 5M, and
10M working sets; they are not persisted or part of tree semantics.

The root is never looked up in the cache after session construction. This is the
direct counterpart of Dolt retaining `StaticMap.Root`.

## Range-Scan Algorithm

The scanner owns a traversal stack of `(NodeHandle, next_index)` and bound
metadata. It allocates the stack once to tree height. On each leaf:

1. binary-search the first relevant key only when entering the initial leaf;
2. walk entries linearly;
3. compare the key with the exclusive end bound;
4. construct `EntryRef` directly from the live leaf handle;
5. invoke the visitor;
6. increment the index without copying key or value;
7. load the next leaf only when the current leaf is exhausted.

Stores that prefer batch reads may prefetch child frontiers, but prefetching may
not alter logical order, errors, budgets, or callback delivery. Prefetched node
handles can be evicted from the cache while retained by the scan.

Bounds supplied by the caller remain borrowed for the duration of the call.
Owned iterator/page wrappers copy bounds because they outlive the initiating
call.

## Node Cache and Loading

### Shared store buffers

The storage contract has additive retained-buffer methods:

```rust,ignore
pub type SharedReadBatch = Vec<Option<Arc<[u8]>>>;

pub trait Store {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    fn get_shared(&self, key: &[u8])
        -> Result<Option<Arc<[u8]>>, Self::Error>;

    fn batch_get_shared_ordered_unique(&self, keys: &[&[u8]])
        -> Result<SharedReadBatch, Self::Error>;

    fn has_native_shared_reads(&self) -> bool;
}
```

The default `get_shared` converts the owned vector into an `Arc` without a
second byte copy. The default shared batch delegates to the backend's ordered
batch operation before converting each result, so adding zero-copy support does
not silently disable remote multi-get. A native retained-buffer store returns
the same allocation it already owns and reports `has_native_shared_reads =
true`; `MemStore` is the reference implementation.

`AsyncStore` has the same contract. `Arc<S>`, references, sync-as-async adapters,
and search-runtime store wrappers must delegate the shared methods instead of
falling back to the owned default. This is a conformance requirement because one
missing delegation can reintroduce a full object copy in every read.

Shared bytes are immutable by contract. A store must never mutate or recycle an
allocation after returning it. A backend whose buffer pool cannot guarantee
that property must use the owned adapter. `has_native_shared_reads` is only a
performance capability signal; correctness cannot depend on its value.

### Explicit cache modes

The cache implementation selects one mode at manager construction:

```rust,ignore
enum NodeCacheMode {
    Disabled,
    Unbounded(UnboundedNodeCache),
    Bounded(BoundedNodeCache),
}
```

The mode is runtime-only and does not affect tree identity.

### Unbounded fast path

The default runtime configuration currently has no node or byte limit. In this
mode:

- hits take a shared read lock;
- lookup performs one hash-table probe;
- no generation is advanced;
- no access-log entry is appended;
- no compaction or eviction check runs;
- the ready `NodeHandle` is cloned and the lock is released before traversal.

Insertion takes the write lock only on a miss. A second lookup after decoding
handles a concurrent winner and avoids replacing an equivalent ready handle.

Because CIDs are SHA-256 content hashes, a cache-specific hasher may fold the
already-random 32 bytes instead of applying SipHash again. The implementation
must use enough CID bits and include collision tests; equality still compares
the complete CID. This optimization is accepted only after a microbenchmark and
adversarial collision review.

### Bounded cache

Bounded mode retains eviction recency, pinning, byte accounting, and current
configuration semantics. Its first correction is a single hash lookup per hit.
Contention improvements may shard ready entries and use approximate segmented
LRU/CLOCK metadata, but eviction policy is a performance property, never a
correctness property.

No bounded-cache hit may hold a global exclusive lock while hashing unrelated
CIDs. A staged implementation may initially retain the existing lock for
bounded mode while the unbounded mode receives the fast path, then measure
whether sharding is justified.

### Load coalescing

Concurrent misses for one CID should share one decode operation. A cache slot is
either `Loading` or `Ready`. The owner reads and validates bytes; waiters observe
the same success or error. Errors are never cached. No store I/O occurs while a
cache shard lock is held.

Sync waiters use a condition-backed slot. Async waiters use a shared future or
notification primitive. Load coalescing is a later core milestone unless
contention profiling makes it necessary for the point-read gate.

### Validation before admission

On a miss:

1. fetch retained immutable bytes through `get_shared` (or its owned adapter);
2. compute `Cid::from_bytes(bytes)` and require equality with the requested CID;
3. parse compact metadata and any prefix-key arena under explicit size and entry
   limits;
4. validate sorted keys, key/value cardinality, child counts, level, leaf flag,
   and persisted format;
5. require the node format to equal the session tree format;
6. calculate encoded bytes, metadata bytes, prefix-arena bytes, and any lazy
   owned representation already present;
7. publish an immutable `Arc<ReadNode>` in the cache entry.

If existing store conformance intentionally delegates CID validation elsewhere,
the final implementation spec may avoid duplicate SHA-256 only when that
validation is statically guaranteed. Untrusted or custom stores must fail
closed.

### Metrics

Global atomics on every node hit can become visible after other overhead is
removed. A `ReadSession` accumulates plain local counters and flushes them in one
operation at method/session boundaries. Metrics retain their documented totals.
Diagnostic per-node tracing is optional and disabled on the benchmark path.

Allocation metrics distinguish:

- traversal setup allocations;
- per-entry result allocations;
- cold decode allocations;
- owned compatibility-boundary allocations;
- cache retained and externally pinned bytes.

## Diff Architecture

### Borrowed diff view

```rust,ignore
#[derive(Clone, Copy, Debug)]
pub enum DiffRef<'a> {
    Added { key: &'a [u8], value: &'a [u8] },
    Removed { key: &'a [u8], value: &'a [u8] },
    Changed {
        key: &'a [u8],
        old: &'a [u8],
        new: &'a [u8],
    },
}
```

Public visitor primitives mirror range scans:

```rust,ignore
pub fn scan_diff(
    &self,
    base: &Tree,
    other: &Tree,
    visit: impl for<'diff> FnMut(DiffRef<'diff>),
) -> Result<u64, Error>;

pub fn scan_range_diff(...);
pub fn scan_diff_until<B>(...);
```

The walker retains the base and other node handles while invoking a changed
event. It does not queue borrowed events. Added and removed subtree walks invoke
the visitor directly from each leaf.

### Misaligned subtree fallback

The current fallback collects complete subtrees into `Vec<(Vec<u8>, Vec<u8>)>`.
Replace it with two bounded borrowed leaf cursors:

1. create a cursor for each subtree/span;
2. compare current borrowed keys;
3. emit removed/added/changed inline;
4. advance only the relevant cursor;
5. release exhausted leaf handles.

Scratch and stack memory are O(tree height plus prefetched frontier), not
O(subtree entries). Ordering matches the existing merge of sorted entry vectors.

### Owned compatibility

`diff()` and `range_diff()` collect `DiffRef::to_owned()` results. The existing
`stream_diff()` remains an owned standard iterator. Diff pages, structural
checkpoints, proofs, and serialized sync artifacts own all data. They may use
borrowed traversal internally and clone once at emission/checkpoint boundaries.

Append-only detection continues to use structural CIDs and borrowed right-edge
walks. It must not first construct an owned append diff merely to decide whether
the path applies.

## Merge Architecture

### Borrowed conflict view

```rust,ignore
#[derive(Clone, Copy, Debug)]
pub struct ConflictRef<'a> {
    pub key: &'a [u8],
    pub base: Option<&'a [u8]>,
    pub left: Option<&'a [u8]>,
    pub right: Option<&'a [u8]>,
}
```

`scan_conflicts` consumes borrowed right-side diffs and borrowed left lookups.
It constructs an owned `Conflict` only for the legacy iterator or when an
unresolved conflict is returned in `Error::Conflict`.

### Zero-copy resolver decisions

```rust,ignore
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MergeDecision {
    UseBase,
    UseLeft,
    UseRight,
    Value(Vec<u8>),
    Delete,
    Unresolved,
}

pub trait BorrowedMergeResolver: Send + Sync {
    fn resolve(&self, conflict: ConflictRef<'_>) -> MergeDecision;
}
```

`UseBase`, `UseLeft`, and `UseRight` select an existing slice without copying.
`Value` is used only when application code synthesizes new bytes. Built-in
prefer-left, prefer-right, delete-wins, and equivalent-value resolvers use
symbolic decisions.

The existing `Resolver = Box<dyn Fn(&Conflict) -> Resolution>` remains valid.
Its adapter builds an owned `Conflict` only for an actual conflict, calls the
legacy resolver, and consumes its owned resolution.

### Streaming merge application

Merge should not require an owned right-diff vector. The structural walker
drives merge directly:

1. reuse complete roots or subtrees whenever one side matches base or both
   sides match;
2. compare aligned leaves using borrowed values;
3. for changed keys, read left through the same retained read session;
4. apply non-conflicting selections directly to a rolling canonical builder;
5. invoke a borrowed resolver only for genuine conflicts;
6. materialize an owned value only for a rewritten output node or synthesized
   resolution.

A new or rewritten persisted node must ultimately own or encode its bytes. The
target is no temporary Diff/Conflict/Mutation copy and at most one required
copy/encoding into output. Complete unchanged nodes retain their existing CID.

Sparse fallback may initially retain a bounded owned mutation batch if the
current canonical batch editor requires random access. That batch must be
bounded and filled from borrowed events without first owning `Diff` and
`Conflict` layers. A later `push_borrowed(key, value)` rolling builder can encode
directly from slices.

### Merge correctness

Property tests compare the borrowed merge engine with a simple three-way model
and the legacy engine over inserts, updates, deletes, additions on both sides,
all resolver decisions, misaligned chunk boundaries, and every node layout.
For identical logical decisions, resulting roots must match a clean canonical
rebuild where the existing API promises CID equivalence.

## Versioned Maps, Snapshots, Transactions, and Write Sessions

`MapSnapshot` and historical snapshots already bind an immutable `Tree`. They
gain `read()` and borrowed convenience methods that delegate to a core
`ReadSession`. A snapshot may cache its root handle for repeated reads because
its tree never changes.

`VersionedMap::get`, `get_at`, `range`, `prefix`, and page APIs retain existing
owned signatures. Native additions expose `get_with`, `scan_range`, and
`scan_prefix`. Typed maps decode inside `get_with`, avoiding an intermediate
`Vec<u8>` before producing the owned typed value.

Transaction and write-session overlays require a two-source read view:

- an overlay upsert can lend its in-memory value directly;
- an overlay delete returns no value;
- an absent overlay key delegates to the base `ReadSession`;
- ordered overlay/base range merge invokes a visitor while the selected source
  entry is alive;
- owned write-session APIs remain wrappers.

No callback is invoked while a transaction/overlay mutex is held. The overlay
entry must be copied into a short-lived stable handle or the lock released via a
snapshot before application code runs.

## Secondary-Index Foundation

Secondary-index borrowed codecs and query composition are part of the shared
foundation. Build and maintenance consumers may migrate after the core read
path, but they must do so without changing the lifetime/cache model.

### Borrowed stored-value view

Add strict non-owning parsing:

```rust,ignore
pub enum IndexValueRef<'a> {
    KeysOnly,
    Included(&'a [u8]),
    FullSource(&'a [u8]),
}

impl<'a> IndexValueRef<'a> {
    pub fn decode(bytes: &'a [u8], limit: usize) -> Result<Self, Error>;
    pub fn to_owned(self) -> IndexValue;
}
```

It validates magic, format version, tag, canonical length, limit, and trailing
bytes exactly as `IndexValue::from_bytes`, but it borrows the payload.

### Composite physical keys

Physical index keys escape zero bytes, so arbitrary logical term and primary-key
components cannot always be direct subslices of the encoded key. The decoder
therefore provides a zero-allocation steady-state view using two reusable
scratch buffers:

```rust,ignore
pub struct DecodedPhysicalIndexKeyRef<'a> {
    pub term: &'a [u8],
    pub primary_key: &'a [u8],
}
```

For a component with no escape sequence, the view borrows the encoded key
directly. When unescaping is required, it writes into a bounded session scratch
buffer and lends that slice for the callback. Scratch capacity is limited by the
existing term/key limits and reused across rows. The implementation must never
describe escaped-key decoding as byte-zero-copy; it is allocation-free after
scratch warmup.

### Borrowed index match

```rust,ignore
pub struct SecondaryIndexMatchRef<'a> {
    pub term: &'a [u8],
    pub primary_key: &'a [u8],
    pub projection: Option<&'a [u8]>,
}
```

`SecondaryIndexSnapshot` gains `scan_exact`, `scan_prefix`, and `scan_range`
visitor methods. They:

1. build physical bounds once;
2. use core `scan_range`;
3. decode key components with reusable scratch;
4. validate `IndexValueRef` against the descriptor projection mode;
5. invoke the callback;
6. discard or reuse scratch before the next entry.

Existing eager methods and pages clone once into `SecondaryIndexMatch`. Cursors
remain owned, snapshot-bound serialized values.

### Source-record join

`records()` currently owns all matches and all source values. A future
`scan_records` uses bounded chunks:

1. scan physical index matches into a bounded list of primary-key locations or
   owned keys only where escaped decoding requires it;
2. issue an ordered batched read against the exact source snapshot;
3. invoke a callback with primary key, optional projection, and borrowed source
   value;
4. fail on a missing source record as `IndexCheckpointMismatch`;
5. release the batch before continuing.

Batch size and retained bytes obey `QueryBudget`. Exact posting lists preserve
primary-key order; broader term ranges preserve physical `(term, primary_key)`
order.

The chunk must not store `SecondaryIndexMatchRef` or a slice into reusable
unescape scratch. It stores either (a) a retained physical leaf handle plus
entry index when later decoding can be repeated cheaply, or (b) one bounded
owned primary key/projection needed to cross the callback boundary. This is an
intentional required copy, bounded by chunk policy, and is still materially
different from owning the complete posting list. After ordered source reads,
the callback receives a view valid only while both the posting chunk item and
source leaf handle are alive.

### Extraction and builds

The current extractor already receives borrowed primary-key and source-value
slices but returns an owned `Vec<SecondaryIndexEntry>`. Introduce a streaming
extractor/sink contract for future builds:

```rust,ignore
pub struct SecondaryIndexEntryRef<'a> {
    pub term: &'a [u8],
    pub projection: Option<&'a [u8]>,
}

pub trait StreamingSecondaryIndexExtractor: Send + Sync {
    fn extract(
        &self,
        primary_key: &[u8],
        source_value: &[u8],
        emit: &mut dyn FnMut(SecondaryIndexEntryRef<'_>) -> Result<(), SecondaryIndexError>,
    ) -> Result<(), SecondaryIndexError>;
}
```

The legacy extractor adapts by iterating its owned result. A streaming extractor
can derive terms into reusable application scratch and the index builder copies
each final physical key/projection exactly once into its sort/run or output
buffer. Source builds use `scan_range` and no longer clone the complete source
record before extraction.

### Secondary-index adoption gates

Each migrated secondary-index consumer must demonstrate:

- exact owned/borrowed result equivalence for arbitrary escaped terms and keys;
- zero steady-state allocations per `KeysOnly` match without escaped bytes;
- bounded scratch and chunk memory for `Include`, `All`, and source joins;
- unchanged cursor, snapshot, projection, and checkpoint validation;
- no unbounded eager query path in production profiles.

## Proximity-Map Foundation

Proximity values combine encoded vectors and application bytes, so their safe
zero-copy representation differs from ordinary key/value entries.

### Stored record view

```rust,ignore
pub(crate) struct StoredRecordRef<'a> {
    vector: EncodedVectorRef<'a>,
    pub value: &'a [u8],
}

pub(crate) struct EncodedVectorRef<'a> {
    bytes: &'a [u8],
    dimensions: u32,
}
```

`StoredRecordRef::decode` validates magic, version, encoding, dimensions,
canonical varints, vector byte length, finite components, negative zero, value
length, and trailing bytes without allocating the application value.
After validation, it is wrapped as the public `ProximityRecordRef` and
`ProximityVectorRef` defined by the normative API set. This keeps persisted
byte-layout details private while preserving borrowed logical access.

The persisted vector starts after a variable-length header and is not guaranteed
to be aligned for `f32`. It is little-endian encoded. The implementation must not
cast it to `&[f32]` or rely on allocator alignment. `EncodedVectorRef` provides
safe component access and iterators using byte loads. Optimized kernels may use
unaligned SIMD loads when implemented safely for the target or decode into a
reusable aligned scratch buffer.

Whether direct encoded-byte kernels or reusable decoding is faster is decided by
benchmarks. Re-decoding every component for repeated distance calculations may
be slower than one bounded decode into cached aligned storage; "zero-copy" is
not allowed to override measured end-to-end performance.

### Exact lookup and scan

`ProximityMap::read()` creates a lightweight proximity read session containing
a core directory `ReadSession`, immutable proximity descriptor information, and
reusable vector scratch. Future native APIs are:

```rust,ignore
pub struct ProximityReadSession<'map, S: Store> {
    directory: ReadSession<'map, 'map, S>,
    dimensions: u32,
    vector_scratch: Vec<f32>,
}

pub fn get_with<R>(
    &mut self,
    key: &[u8],
    read: impl for<'record> FnOnce(ProximityRecordRef<'record>) -> R,
) -> Result<Option<R>, Error>;

pub fn scan_records(
    &mut self,
    visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>),
) -> Result<u64, Error>;
```

`contains_key` performs the same strict record validation as today's `get`
without allocating vector or value. Existing `get` converts one record to the
owned `(Vec<f32>, Vec<u8>)` result.

Build, verification, SQ8, PQ, and HNSW construction can consume
`scan_records`. Components that need vectors beyond a callback copy them into a
bounded build buffer or retain a typed decoded content handle.

### Candidate handles

Search results must own their `Neighbor` keys and values, but candidate heaps can
avoid early copies. A bounded candidate identifies immutable backing content:

```rust,ignore
struct CandidateHandle {
    source: CandidateSourceHandle,
    index: usize,
    distance: f64,
}
```

`CandidateSourceHandle` retains an `Arc` to a decoded proximity node, graph
record, PQ page, or directory leaf. Key and vector comparisons borrow through
that handle. Only final accepted `k` neighbors clone key/value into
`SearchResult`. Retained candidate bytes count against the request frontier and
runtime pinned-byte budgets.

For authoritative reranking, ordered directory multi-get returns record
locations or invokes bounded callbacks rather than owning every full record.
Result order and tie-breaking remain based on authoritative full-precision
distance and key bytes.

An optional future `visit_neighbors` can invoke a callback over final borrowed
neighbors for native callers that do not need an owned result. It must still
retain all required candidate handles until final sorting is complete.

Candidate ownership is backend-specific but follows one protocol:

| Backend | Candidate state retained before rerank | Forbidden intermediate ownership |
|---|---|---|
| native hierarchy | `Arc<ProximityNode>`, entry index, approximate/exact score | cloned entry key and vector per frontier visit |
| eligible exact | bounded directory leaf/record location and score | owned full directory record for every eligible row |
| HNSW | graph-node/route handle, logical key location, navigation score | cloned application value during navigation |
| PQ | code-page handle, code index, approximate distance | decoded full vector per candidate |
| SQ8 | quantized-page handle, entry index, approximate distance | owned dequantized vector per candidate |
| composite | tagged handle to base or delta source plus shadow decision | flattening both sources into owned candidates |

The candidate set is kept sorted or heap-bounded to the planner's oversampling
limit. Insertion and truncation must be deterministic on `(score.total_cmp,
key-bytes)` and must not leave a handle with an invalid entry index. Duplicate
keys from composite/base/delta sources are resolved before authoritative rerank
according to shadow semantics.

Authoritative reranking opens one reusable directory `ReadSession`, retrieves
each exact `StoredRecordRef` through `get_with`/ordered locations, verifies that
the stored key/vector matches the candidate source contract, copies components
into aligned scratch only when the selected distance kernel benefits, and
computes the exact metric. Approximate scores may order the rerank input but
never the returned result. The final stable sort uses exact score then raw key
bytes; only those final `k` rows clone key and application value.

For repeated distance evaluation, the representation choice is explicit:

- one-pass exact scan: iterate encoded components or decode into one reusable
  aligned query-sized scratch buffer;
- repeated HNSW routing-vector use: cache validated aligned `Arc<[f32]>` when
  profiling shows that decoding dominates;
- PQ/SQ8: retain compact codes and tables; decode only final authoritative
  directory vectors;
- public exact records: expose the safe encoded view and let callers opt into
  `copy_to_slice`/`to_vec`.

This hybrid is intentional: eliminating allocations is mandatory on the hot
path, while eliminating every byte copy is not. A bounded copy into reusable
SIMD-aligned scratch is correct when it lowers end-to-end latency.

### Search runtime integration

The existing qualified `SearchRuntime` cache remains authoritative for typed
proximity and accelerator content. Core node cache and search runtime need not be
one physical cache, but they share a handle protocol:

- immutable `Arc` content;
- qualified identity including store namespace, content kind, CID, and decoder
  version;
- validation before admission;
- no lock during callbacks or distance work;
- encoded plus retained decoded byte weights;
- externally retained/pinned byte metrics;
- error-not-cached and optional load coalescing semantics.

Packed typed objects retain `Arc<[u8]>` plus validated offsets. Decoded aligned
vectors retain `Arc<[f32]>`. Cache policy may choose either representation by
content kind and measured reuse.

### Proximity adoption gates

Each migrated proximity consumer must prove:

- owned exact lookup equals `ProximityRecordRef::to_owned` for every valid
  vector;
- malformed formats fail identically without partial result delivery;
- scalar and SIMD distance outputs remain bit/ordering compatible under current
  tolerances and canonical tie rules;
- warm/cold cache state cannot change plans, logical budgets, neighbors,
  completion, or proofs;
- candidate handle memory is bounded and accounted;
- only final results or explicitly retained build/search state are owned.

## Proofs, Maintenance, Sync, and Content Walking

Zero-copy applies to inspection while these APIs run, not to the artifact they
return. A membership, multi-key, range, diff, or proximity search proof must be
self-contained and verifiable after every cache/session handle is dropped.
Therefore proof generation borrows node keys, values, child CIDs, and typed
records while deciding what to include, then copies/encodes each required proof
component exactly once into the owned proof. Verification parses proof-owned
bytes through the same strict view decoders where possible.

Stats, debug, verification, reachability, GC, sync planning, missing-node copy,
export, and content-graph walkers use `ReadNode`/typed handles for traversal.
They own only aggregates, CIDs that must enter sets/queues, diagnostic samples,
transfer manifests, or persisted bundles. Child CID extraction is a fixed
32-byte value operation and is not treated as a large-value copy. Walkers must
retain logical visit/budget semantics independently of cache warmth.

Import and write-side verification remain fail-closed:

1. validate serialized object length and kind;
2. verify CID before cache admission;
3. parse all layout/codec metadata and canonical constraints;
4. publish an immutable retained handle only after validation;
5. materialize owned output only if the import/write algorithm requires it.

No proof, cursor, sync bundle, GC plan, manifest, descriptor, checkpoint, or
catalog may contain an in-process offset, pointer, `Arc`, cache namespace, or
session identifier. Snapshot/root CIDs remain the only durable identity.

## Async Architecture

`AsyncReadSession` mirrors the synchronous root-bound session. It may await node
loads, but borrowed data is supplied only to a synchronous callback after the
await completes:

```rust,ignore
pub async fn scan_range(
    &mut self,
    start: &[u8],
    end: Option<&[u8]>,
    visit: impl for<'entry> FnMut(EntryRef<'entry>),
) -> Result<u64, Error>;
```

The callback cannot return a future borrowing the entry. An application that
must await per item must copy the required bytes or use a bounded owned-page
API. This restriction prevents references from crossing suspension points and
keeps futures safely movable.

Async prefetch and batched loads retain deterministic callback order. A task may
load concurrently, but it commits results in key order. Cancellation is checked
between loads and callbacks. Already delivered entries remain delivered; no
borrow survives cancellation.

Async diff, conflict inspection, secondary-index scans, and proximity record
scans use the same synchronous callback rule. Async merge resolvers remain
synchronous while holding borrowed conflict views. A truly async application
resolver requires an owned conflict request/response workflow.

## Language Bindings and WASM

Existing UniFFI, Node, Python, Java, Kotlin, Swift, Ruby, Go, and WASM APIs remain
owned. Foreign runtimes cannot safely retain Rust node-backed slices under the
current contracts.

Bindings still benefit internally:

- point reads avoid an intermediate Rust `Vec` before filling the binding
  buffer where the binding implementation permits direct serialization;
- range and diff pages clone/serialize once into their foreign result rather
  than cloning through an intermediate owned iterator;
- typed decoders can parse borrowed Rust bytes before creating the final foreign
  object;
- cache and root improvements apply unchanged.

Foreign callback streaming may be added for bounded-memory use, but it is not
advertised as cross-language zero-copy. The foreign call normally copies into
the target runtime, must remain synchronous, and must not reenter Rust while an
internal lock is held.

Generated binding signatures and conformance fixtures are unchanged during the
core phase.

## Error, Panic, and Reentrancy Semantics

1. Tree/store/format errors return `Error` and stop traversal.
2. Streaming APIs may have delivered earlier entries before a later store error.
   Callers requiring all-or-nothing results use owned collection/page APIs.
3. `scan_*_until` distinguishes an application break value from an engine error.
4. A callback panic unwinds through immutable handles only. Cache maps and
   traversal stacks remain structurally valid, and no persistent write occurs.
5. A callback may call other public read APIs. This is safe because no cache,
   overlay, transaction, or metrics lock is held during callback execution.
6. Reentering the same `&mut ReadSession` is prevented by Rust borrowing. A
   callback may use another session or manager convenience method.
7. Resolver panic behavior remains Rust unwind behavior; transactional writes
   have not been published at callback time.

## Resource Ownership and Limits

The architecture must bound:

- traversal stack by validated tree height;
- node entry counts and decoded bytes by format/profile limits;
- cache encoded and retained decoded bytes;
- active handle/pinned bytes separately from evictable cache bytes;
- multi-get locations by input count and request limit;
- diff frontier and fallback scratch by tree height/frontier policy;
- merge rolling output and mutation fallback batches;
- secondary-index key scratch, posting batch, source fetches, and result bytes;
- proximity candidate handles, vector scratch, decoded-vector cache, and
  authoritative rerank batch;
- async in-flight loads and prefetch width.

An object larger than its cache partition can be validated and used without
admission. Cache eviction may temporarily leave memory retained by active
handles; metrics expose this rather than violating lifetime safety.

The default unbounded cache is preserved for compatibility in the immediate
core phase, but production deployment profiles should provide explicit byte
limits. "Unbounded" changes hit bookkeeping, not resource policy documentation.

## Observability

Add or derive the following metrics without changing logical results:

- read sessions opened and empty sessions;
- direct-root uses;
- session recent-leaf hits/misses;
- cache hits by mode;
- cache read-lock and write-lock acquisitions;
- cache lookup probes and eviction-log operations;
- coalesced loads and waiters;
- encoded, decoded, evictable, and externally retained bytes;
- callback entries delivered and early stops;
- owned-boundary key/value bytes copied;
- cold decode allocations and bytes;
- diff events visited versus owned;
- conflict views versus owned conflicts;
- merge reused CIDs, selected borrowed values, synthesized values, and output
  bytes copied;
- secondary-index scratch high-water and source-join batches;
- proximity vector view decodes, aligned-scratch decodes, cached decoded vectors,
  candidate pinned bytes, and final result copies.

Benchmark-only allocation counters must be feature-gated or use a harness
allocator so production allocation paths are not instrumented per operation.

## Testing Strategy

### API and lifetime compile tests

- `get_with` and scan callbacks can consume slices and return owned/scalar data.
- compile-fail tests prove a callback cannot store or return `EntryRef`,
  `ValueRefView`, `DiffRef`, `ConflictRef`, `IndexValueRef`, or
  `ProximityRecordRef` beyond its invocation;
- async compile-fail tests prove borrowed entries cannot cross `.await`;
- `ReadSession` is `Send` when its manager/store permit moving between threads,
  but mutation requires exclusive access;
- no public API requires callers to use unsafe code; implementation unsafe is
  confined to audited packed/SIMD internals.

### Core equivalence properties

For generated trees and all `NodeLayoutSpec` variants:

- `get == get_with(...to_vec)` for hits and misses;
- `get_value_ref` equals `get_value_ref_with(...to_owned)` for raw inline,
  escaped inline, and blob envelopes, including malformed inputs;
- `get_many` equals ordered `get_many_with` collection for unique and duplicate
  inputs, hits, misses, and caller order;
- `range.collect()` equals `scan_range` collection;
- forward/reverse scans are exact inverses under matching bounds;
- prefix scans equal filtered full scans;
- select/rank/bounds match a `BTreeMap` model;
- cursors resume without duplicates or omissions;
- empty, single-leaf, multi-level, and malformed trees behave correctly.

Keys and values include empty values, embedded zero, `0xff`, long shared
prefixes, maximum permitted lengths, and non-UTF-8 bytes.

### Cache tests

Run the same operations with disabled, unbounded, node-bounded, byte-bounded,
and mixed bounded caches. Verify:

- identical results and logical metrics;
- eviction during an active callback cannot invalidate a slice;
- `clear_cache` does not invalidate active sessions;
- concurrent hit/miss/clear/load sequences do not deadlock;
- one admitted node per CID under coalescing;
- failed/corrupt loads are not cached;
- CID and tree-format mismatches fail closed;
- no application callback observes a held cache lock.

Use loom or deterministic synchronization tests for cache slot state transitions
where practical, plus stress tests under ThreadSanitizer-compatible builds.

### Diff and merge properties

- borrowed diff collected to owned equals existing diff and a `BTreeMap` model;
- aligned and deliberately misaligned chunk boundaries emit identical diffs;
- equal-root diff visits no entries;
- stopped scans report the correct last delivered key;
- borrowed conflict scan collected to owned equals legacy conflict streaming;
- every merge decision matches the simple three-way model;
- legacy and borrowed resolvers agree;
- canonical roots match expected clean rebuilds;
- resolver reentrancy and panic do not publish partial writes.

### Secondary-index foundation tests

- `IndexValueRef::to_owned == IndexValue::from_bytes`;
- borrowed physical-key decoding equals owned decoding for every byte value;
- scratch buffers stay bounded and are reused;
- borrowed matches collected to owned equal exact/prefix/range/page results;
- reverse and early-stop visitors match reverse pages and the corresponding
  forward-prefix subsets;
- missing source records still fail snapshot validation;
- projection tags, limits, cursors, and snapshot IDs remain strict.

### Proximity foundation tests

- `ProximityRecordRef::to_owned == StoredRecord::decode`;
- vector views reject every malformed header/component/trailing-byte case;
- unaligned vector offsets never use an invalid `&[f32]` cast;
- encoded and scratch distance paths agree with decoded scalar/SIMD results;
- candidate handles preserve deterministic top-k and key ties;
- `scan_records_until` reports the exact delivered count and never exposes a
  view after return;
- owned search results and proofs remain unchanged;
- cache eviction and cancellation cannot invalidate active vector/value views.

### Miri, fuzzing, and sanitizers

- run Miri over focused callback lifetime, cache eviction, and codec-view tests;
- fuzz node, index-value, physical-index-key, and proximity-record view decoders;
- compare owned and borrowed decoder acceptance/rejection sets;
- run address/thread sanitizers or platform equivalents for concurrent cache and
  async-load stress tests;
- require every unsafe packed/SIMD block to state isolated invariants, retain a
  safe or Miri-compatible reference path, and pass differential tests. No unsafe
  lifetime extension, aliasing, unchecked parsing, or callback escape is
  permitted.

## Performance Evaluation

### Measurement rules

Performance claims use optimized release binaries, identical generated input,
one worker for the primary comparison, correctness digests, and real elapsed
time. Rust and Dolt run as separate processes. The runner records compiler,
revision, OS, CPU, cache mode, allocation count, and RSS where available.

Each reported result uses warmup and repeated measured runs. Medians are primary;
dispersion and raw rows remain available. Implementation order is alternated or
randomized to reduce thermal/order bias. A failed validation row is never used in
a performance ratio.

### Core matrix

Retain the approved matrix:

- sizes: 10K, 50K, 1M, 5M, 10M;
- workloads: append, random, clustered;
- phases: fresh build and 30% mixed mutation;
- values: deterministic random length from 1 through 100 bytes;
- post-write operations: point reads and full range scans;
- primary storage: in-memory;
- primary concurrency: one worker.

Report separately:

1. Dolt Go zero-copy callback/slice APIs;
2. Rust `ReadSession::get_with` and `scan_range`;
3. Rust convenience `Prolly::get_with` with ephemeral session;
4. Rust legacy owned `get` and `range`;
5. cold-cache and bounded-cache variants.

Equivalent benchmark callbacks compute the same digest from borrowed bytes.
Neither implementation may omit value consumption.

### Allocation gates

After warmup on an already decoded tree:

- reusable-session `get_with`: zero allocations per hit;
- `contains_key`: zero allocations per hit/miss;
- `scan_range`: zero allocations per row after traversal setup;
- cursor materialization: at most one key allocation per requested cursor;
- borrowed aligned-leaf diff: zero allocations per emitted change after setup;
- legacy `get`: exactly the ownership required for its returned value, with no
  intermediate result copy;
- legacy range: one owned key and one owned value per row, with no second cursor
  key copy unless a cursor is requested.

### Latency gates

The desired cross-language gate is Rust at least 1.5x faster than Dolt for
equivalent zero-copy point reads and range scans across representative 1M and
larger workloads, with 2x aspirational. The implementation is not declared
successful merely because it improves relative to the old Rust result.

If the target is missed, profile in this order:

1. remaining cache/hash/lock and global metric overhead;
2. root/session and recent-leaf effectiveness;
3. key comparison and binary-search cost;
4. callback/inlining and traversal state cost;
5. decoded `Vec<Vec<u8>>` locality;
6. packed metadata size, key-arena locality, and child-CID extraction;
7. node-size/chunking and persisted-layout effects.

Only one variable changes per diagnostic benchmark. Persisted format changes
require their own design and compatibility decision.

### Later consumer benchmarks

Secondary-index benchmarks measure exact posting scans, broad term ranges,
projection modes, escaped keys, source joins, and build extraction. Proximity
benchmarks measure exact lookup, authoritative scan/build, eligible-exact,
native hierarchy, PQ/HNSW reranking, vector dimensions, and candidate memory.
These are adoption gates, not phase-one blockers.

The minimum secondary-index matrix is:

- posting cardinalities 1, 10, 100, 10K, and broad-range/full-index;
- `KeysOnly`, included projection, and full-source projection;
- no-escape and worst-case escaped term/primary keys;
- owned collection, borrowed visitor, page/cursor, reverse, and early-stop;
- warm/cold cache, source join hit/missing-source failure, and bounded batch
  sizes;
- build, verify, repair, and replace throughput plus peak retained memory.

The minimum proximity matrix is:

- dimensions 32, 128, 384, 768, and 1536 where supported;
- `k` 1, 10, and 100 with planner oversampling limits recorded;
- exact/native hierarchy, eligible exact at several selectivities, HNSW, PQ,
  SQ8, and composite delta/shadow workloads;
- encoded-component, reusable aligned scratch, and cached decoded-vector kernels;
- candidate generation separately from authoritative rerank and final result
  materialization;
- latency, vectors/second, recall/completion, exact ordering digest, allocations,
  physical reads, cache hits, candidate count, pinned bytes, and peak RSS.

Every optimization row must pass the same correctness digest or exact neighbor
comparison required by that backend. Approximate backends report recall and
completion explicitly; they are never compared as though they performed exact
work. Performance ratios use medians of interleaved runs on the same machine.
Raw rows, revisions, compiler versions, CPU, configuration, and failed
validation rows are retained. A 1.5x–2x target is an engineering objective, not
permission to omit a regression or manufacture a result.

## Delivery Sequence

### Phase 0: Benchmark and semantic lock

- retain current Rust/Dolt baselines and raw results;
- add allocation-count support and reusable-session benchmark columns;
- freeze owned API equivalence fixtures and cursor semantics;
- add compile-fail lifetime tests.

### Phase 1: Core point-read substrate

- introduce `EntryRef`, `ValueRefView`, retained shared store reads, packed
  `ReadNode`, and
  `ReadSession`;
- parse plain/offset keys and values as slices of `Arc<[u8]>`, reconstruct
  prefix-compressed keys into one retained arena, and make the packed loader the
  sole normal read path;
- validate/load and retain the root;
- implement session-local recent leaf;
- implement `get_with`, `get_value_ref_with`, allocation-free `contains_key`,
  and callback-based select/bound entry reads;
- make owned `get` a wrapper;
- implement unbounded-cache read-only hit path and single-probe bounded hit;
- benchmark after each independently measurable change.

### Phase 2: Core range substrate

- implement shared forward borrowed traversal;
- add `scan_range`, prefix, early-stop, and reverse variants;
- track cursor location without per-row key clone;
- adapt owned range, prefix, bounds, pages, and versioned snapshots;
- verify zero per-row allocations and rerun the complete comparison matrix.

### Phase 3: Multi-get, async, overlays, and bindings

- implement location-based `get_many_with`;
- add snapshot/versioned `get_with` and scan forwarding, then adapt typed reads
  and write-session overlays;
- add `AsyncReadSession` and synchronous borrowed callbacks;
- route bindings through borrowed internals without changing generated APIs;
- add load coalescing if concurrency profiling justifies it.

### Phase 4: Diff and merge

- add `DiffRef`, borrowed structural walker, and borrowed fallback cursors;
- adapt owned diff/stream/page/proof boundaries;
- add `ConflictRef`, `scan_conflicts`, and symbolic `MergeDecision`;
- stream merge application and preserve canonical CID reuse;
- benchmark diff density, append-only, sparse changes, misalignment, and conflict
  workloads.

### Phase 5: Secondary-index adoption

- add `IndexValueRef`, `DecodedPhysicalIndexKeyRef`, and borrowed match views;
- add `scan_exact`, `scan_prefix`, `scan_range`, bounded `scan_records`, and
  `IndexedMap::get_with`;
- adapt owned pages and bindings;
- migrate build, verify, repair, replace, bundle, retention, and GC consumers to
  source scan/streaming extraction with bounded owned output state.

### Phase 6: Proximity adoption

- add public `ProximityRecordRef`/`ProximityVectorRef` and private wire views;
- add `ProximityReadSession`, `get_with`, and `scan_records`;
- migrate exact reads, verification, and authoritative scans;
- introduce bounded candidate handles and borrowed rerank reads;
- integrate handle accounting with `SearchRuntime`;
- preserve owned search/proof boundaries.

### Phase 7: Read-path hardening and representation tuning

- profile packed point/range, diff/merge, secondary-index, and proximity paths;
- tune metadata layout, session routing, leaf-locality prediction, batch width,
  cache sharding, and decoded-vector representation one variable at a time;
- compare cold parse time, warm performance, allocations, RSS, retained bytes,
  and write materialization impact;
- consider a new persisted layout only if the current packed parser remains a
  measured bottleneck, through a separate format-identity and migration design.

## Implementation Record (2026-07-16)

The implementation follows this design without changing persisted bytes or
existing owned/binding signatures. The primary anchors are
`src/prolly/read.rs`, `src/prolly/node.rs`, and the shared-read methods in
`src/prolly/store/mod.rs`.

Implemented foundation:

- callback-scoped `EntryRef`, `ValueRefView`, `DiffRef`, and `ConflictRef`;
- reusable synchronous and async read sessions with retained packed roots,
  adaptive recent-leaf state, and a bounded strong internal-route table;
- borrowed point, multi-get, forward/reverse range, prefix, bound, rank, select,
  diff, conflict, and symbolic merge APIs, including early termination;
- `Store`/`AsyncStore` retained immutable reads and ordered shared batches, with
  native `MemStore` support and owned-backend adapters;
- packed `ReadNode` parsing for prefix-compressed, plain, and offset-table
  layouts, with values and applicable keys borrowed from retained wire bytes;
- a validated per-node common-prefix/order-word search accelerator that avoids
  repeated full-key comparisons while retaining a full byte-order fallback;
- hybrid cache entries that make packed nodes authoritative for reads and lazily
  materialize owned nodes only for remaining write-side consumers;
- owned core APIs adapted at their result boundary, with binding signatures
  unchanged;
- packed structural diff traversal and bounded mutation application in the
  synchronous borrowed merge fallback;
- versioned/typed-map and write-session forwarding;
- secondary-index borrowed wire views, forward/reverse posting scans, bounded
  source joins, and streaming extractor contracts;
- proximity record/vector views, borrowed exact/authoritative record scans,
  retained native candidates, and callback-scoped authoritative reranking for
  native, eligible, HNSW, PQ, and composite paths; and
- identical Rust/Dolt benchmark runners with process isolation, workload/result
  validation, source/binary provenance, and retained raw output.

### Post-implementation performance evidence

On 2026-07-16, the final release binary was measured with one worker and the
same deterministic in-memory fresh/random workload, key/value generator,
100,000 point targets, full scan, digest, and result-count checks as the frozen
Dolt Go comparator. Each cell below is the median of three successful runs.
The final Rust reruns followed the profiling-guided search optimization; the
frozen Dolt medians were measured minutes earlier on the same machine.

| Records | Operation | Rust | Dolt Go | Rust speedup |
|---:|---|---:|---:|---:|
| 1M | write | 601 ns/op | 2,314 ns/op | 3.85x |
| 1M | point read | 478 ns/op | 1,368 ns/op | 2.86x |
| 1M | range scan | 5.02 ns/row | 25.0 ns/row | 4.98x |
| 5M | write | 763 ns/op | 4,927 ns/op | 6.46x |
| 5M | point read | 876 ns/op | 1,613 ns/op | 1.84x |
| 5M | range scan | 5.54 ns/row | 38.2 ns/row | 6.89x |
| 10M | write | 767 ns/op | 9,411 ns/op | 12.27x |
| 10M | point read | 975 ns/op | 1,573 ns/op | 1.61x |
| 10M | range scan | 6.04 ns/row | 37.8 ns/row | 6.27x |

All compared rows reported identical workload digests and validated result
counts. These measurements establish the requested 1.5x point-read floor for
these representative large random workloads; they do not imply the same ratio
for an unmeasured machine, store, key distribution, or proximity backend.

A separate absolute 5,000-record, 128-dimension release run measured exact
SIMD search at 265 microseconds, PQ search at 1.07 milliseconds, and HNSW
search at 11.26 milliseconds. Those values are not presented as speedups
because no equivalent pre-change proximity baseline was retained.

Verification after the final optimization passed 479 library tests, the full
integration suite, 97 documentation tests, and strict all-feature Clippy.

Remaining staged migration and hardening:

- eliminate remaining owned staging in async conflict/merge paths where doing so
  does not permit a borrow to cross an await;
- migrate every secondary-index build/verify/repair/replace/bundle path to the
  streaming foundation and measure each projection/source-join workload;
- extend retained-candidate accounting to any future proximity accelerator and
  add repeatable before/after baselines for native, eligible, HNSW, PQ, SQ8,
  and composite search independently;
- run Miri/fuzz and allocation-count gates in addition to the completed
  malformed-input, cache, full-feature, and cross-language checks; and
- report any scale/workload that misses the desired 1.5x ratio without
  extrapolation or fabricated results.

These boundaries do not weaken correctness or lifetime guarantees. They are
explicit optimization milestones under the unchanged APIs above.

## Compatibility and Migration

Phases 1 through 6 are additive at the public Rust API and make no persisted
format change. Existing `get`, `get_many`, `range`, `prefix`, cursor/page, diff,
merge, secondary-index, proximity, async, and binding signatures remain valid.

New borrowed APIs are native Rust optimizations. Existing applications opt in by
calling them or by reusing a `ReadSession`. Internal consumers may migrate
without changing their external contract.

No cache state, node handle, read session, scratch buffer, or borrowed view is
serialized. Reopen behavior depends only on existing roots and formats.

The packed read representation decodes the current bytes and is an in-process
implementation choice. If a new wire layout/default is required, it must define
format identity, coexistence or deliberate hard cutover, migration, conformance
fixtures, and rollback independently.

## Risks and Mitigations

### API surface growth

Adding borrowed variants for every convenience method would be noisy. The
public surface centers on `ReadSession`, `get_with`, range/prefix scans,
multi-get, diff, and conflict scans. Scalar/bounds helpers may remain internal
until demand is demonstrated.

### Callback ergonomics

Callbacks are less composable than iterators. Owned iterators remain available,
and a future lending cursor can be layered over the same traversal state. Clear
examples cover checksum, decode-in-place, early stop, and collection.

### Cache memory retained by handles

Evicted entries can remain alive while sessions/scans/candidates retain `Arc`s.
Budgets and metrics account for externally retained bytes. Long-lived public
guards are intentionally avoided.

### Packed representation complexity

Packed `ReadNode` is the canonical read representation, while decoded `Node`
remains the semantic and write-side reference. The mitigation is strict parser
validation, identical fixtures for every layout, owned/packed differential
tests, isolated unsafe access, and lazy owned materialization for algorithms not
yet migrated. Persisted bytes are not changed by this choice.

### Zero-copy vector slowdown

Byte-wise f32 decoding can cost more than copying once into aligned storage.
Proximity uses measured representation choice and reusable/cached decoded
vectors. Safety and end-to-end latency take precedence over a zero-copy label.

### Partial streaming delivery

A later store error can follow successful callback invocations. This is
documented and tested. Owned/page APIs remain the choice for callers requiring
an atomic result object.

### Optimization changes error timing

Prefetch, lazy decoding, and borrowed traversal may discover corruption at a
different moment. Validation-before-admission and equivalence tests require the
same acceptance/rejection set. Streaming APIs document partial delivery, while
owned APIs preserve fail-without-returning-a-result behavior.

## Acceptance Criteria

The program design is successfully implemented when:

1. Safe Rust prevents every borrowed view from escaping its callback.
2. Warm reusable-session point hits and steady-state scans meet allocation gates.
3. Native shared stores preserve one immutable byte allocation from store
   through packed node and callback, while owned stores incur only their defined
   boundary adaptation.
4. Packed parsing accepts and rejects exactly the same canonical node set as the
   owned decoder for every supported layout; unsafe hot access passes its stated
   proof, Miri, fuzz, and differential gates.
5. Unbounded cache hits perform no eviction bookkeeping and require no exclusive
   cache lock.
6. Repeated reads retain the exact root directly and use bounded session-local
   route and adaptive leaf locality without global atomics.
7. Existing owned core APIs pass byte/order/error/cursor equivalence tests.
8. Cache eviction, clear, concurrency, corruption, and reentrancy tests pass.
9. Borrowed diff and conflict scans match owned/model results, including
   misaligned trees.
10. Merge avoids owned Diff/Conflict layers on its native path and preserves
   canonical results and structural reuse.
11. Async traversal never holds borrowed data across `.await`.
12. Language bindings and persisted fixtures remain compatible.
13. Secondary-index query/source-join paths use borrowed views and bounded
    scratch; build and maintenance paths consume the same streaming foundation
    without an eager full-source or full-posting materialization.
14. Proximity exact, native, eligible, HNSW, PQ, SQ8, and composite paths use
    retained candidates and authoritative borrowed reranking; candidate bytes
    are bounded/accounted and only final neighbors own application data.
15. Proof, sync, GC, verification, and content-walk APIs inspect retained views
    but return self-contained owned artifacts.
16. The full benchmark matrix and raw results are published without fabricated
    ratios, failed validations, or omitted regressions.
17. Rust reaches the 1.5x target where measurements prove it; any misses are
    reported honestly with profiles and the next ranked bottleneck.

## Final Recommendation

Adopt packed retained nodes, callback-scoped borrowed views, and a reusable
root-bound `ReadSession` as the canonical internal architecture. Preserve owned
APIs as explicit boundary adapters, fix cache policy overhead independently of
data ownership, and require diff/merge, secondary-index, and proximity consumers
to compose over the same retained-handle protocol.

This sequence attacks the measured point-read and range-scan bottlenecks first,
keeps correctness and compatibility intact, and establishes a stable handle,
codec-view, and traversal foundation for every higher-level prolly API. Packed
nodes are part of that foundation. Unsafe SIMD, decoded-vector caches, metadata
tuning, and wire-format changes remain measured, separately validated
optimizations rather than assumptions.
