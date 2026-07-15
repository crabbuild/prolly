# Secondary Index Production Improvements

**Status:** Proposed

**Date:** 2026-07-14

**Scope:** Program-level design for the next secondary-index architecture. Each
milestone below receives a focused implementation specification before code is
changed.

## Summary

Prolly already has the correct basic secondary-index model: index entries are
ordered by `(term, primary_key)`, source mutations derive index mutations, and
historical catalog checkpoints select exact source and index versions. The
next work should preserve those mechanics while replacing the publication
boundary and removing unbounded build and query behavior.

The central decision is to make one indexed-collection state root the only
authoritative visibility point. Every source tree, index tree, descriptor, and
retained snapshot is immutable and content-addressed. A write uploads those
objects first, then publishes a new collection state with one root
compare-and-swap. Readers therefore observe the old complete collection or the
new complete collection. Correctness no longer depends on a backend providing
an atomic transaction across several named roots.

This design also adds the following directions in dependency order:

1. bounded query, build, verification, and telemetry contracts;
2. spillable and resumable online index builds;
3. typed composite terms and unique indexes;
4. a deliberately small snapshot-bound query layer;
5. indexed rollback, merge, portable proofs, and binding parity.

Strict synchronous indexes remain the default. Deferred indexes may be added
later as a separately named consistency tier with explicit watermarks; they do
not weaken the strict snapshot contract.

## Relationship to Existing Designs

This document extends, rather than restates, the V1 design in
`docs/superpowers/specs/2026-07-13-indexed-map-secondary-index-design.md`.
The V1 implementation already provides:

- non-unique, sparse, multi-valued indexes;
- `KeysOnly`, `Include`, and `All` projections;
- exact, prefix, and range lookup with forward and reverse pages;
- source/index/catalog publication through `versioned_maps_transaction`;
- historical snapshots, verification, repair, replacement, retention, and
  portable bundles;
- a control root that rejects raw writes to managed maps.

This document also depends on the capability, limit, error, root-manifest, and
observer contracts proposed in
`docs/superpowers/specs/2026-07-14-production-engine-foundation-design.md`.
Where the two designs meet, the production-engine types are authoritative.
This design adds a node-publication visibility capability that the
single-root algorithm requires.

## Current Evidence

The improvement priorities follow directly from the current implementation:

- `IndexedMap::open` accepts any store for which
  `supports_transactions()` returns `true`.
- `FileNodeStore` returns `true`, but publishes transaction nodes and named
  roots as separate filesystem writes under an in-process mutex. An abrupt
  process or machine failure can expose only part of a multi-root commit.
- a mutation publishes the source head, each changed index head, the catalog
  head, and the control state through one `versioned_maps_transaction` call;
- `build_index_tree` collects the complete derived index in an in-memory
  `BTreeMap` before passing sorted entries to `SortedBatchBuilder`;
- `exact`, `prefix`, `range`, and `records` collect complete result sets into
  `Vec`s even though bounded page APIs also exist;
- `SecondaryIndexLimits::max_indexes` and `build_page_size` are validated but
  are not enforced by the coordinator;
- counters named `*_nodes_written` count logical map categories rather than
  physical node writes;
- descriptor identity contains an application-controlled extractor ID, but
  the engine cannot detect changed native callback behavior when the
  application reuses that ID;
- building several indexes requires a separate source scan and temporary
  materialization for every index;
- a shadow build restarts after a conflicting source publication and has no
  durable catch-up state.

The existing secondary-index, transaction, and versioned-map test suites prove
the in-process happy path and conflict behavior. They do not prove crash/reopen
atomicity, cross-process publication, a fixed memory bound, or progress under a
continuous write workload.

## Goals

1. Preserve exact source/index snapshot consistency on local and remote stores
   that provide durable immutable-node writes and one suitable root CAS.
2. Make publication require one authoritative CAS, not a multi-root
   transaction.
3. Bound heap use, result size, spill space, retries, and verification work in
   production profiles.
4. Build or rebuild indexes while source writes continue, with durable state
   and an explicit lag/progress contract.
5. Support composite ordered terms, timestamps, tags, normalized application
   fields, covering projections, and unique constraints.
6. Keep historical queries tied to the exact definition and physical index
   root built for that source state.
7. Preserve structural sharing and content-derived identity.
8. Expose enough metrics and conformance proof to make amplification,
   conflicts, and build health operationally visible.
9. Keep the core a key/value indexing engine rather than growing a SQL layer.

## Non-Goals

This program does not:

- make arbitrary native extractor code portable or verifiably deterministic;
- provide full SQL parsing, joins, cost-based relational planning, or
  transactions across unrelated indexed collections;
- guarantee online-build completion when the sustained mutation rate is at or
  above catch-up throughput;
- make `FileNodeStore` crash-atomic;
- add implicit compatibility readers for V1 catalog and hidden-map layouts;
- make an eventually consistent index appear strict;
- define application-specific email, locale, or Unicode normalization rules;
- retain every historical snapshot indefinitely;
- delete unreachable objects immediately after a failed publication.

## Invariants

The following are release-blocking invariants:

1. A visible indexed snapshot references exactly one source tree and exactly
   one tree for every index generation selected by that snapshot.
2. One successful collection-root CAS is the linearization point for source
   mutations, index mutations, definition changes, retention changes, and
   build activation.
3. Before that CAS, every referenced immutable object is durably written and
   readable according to the active deployment profile.
4. A failed or conflicting CAS changes no visible collection state. Uploaded
   but unreachable objects are harmless and are reclaimed only after the GC
   grace period.
5. Index trees are derived data. Merge, rollback, repair, and import validate
   or rederive them from source data; they never independently merge
   conflicting derived trees.
6. Query cursors, pages, and proofs are bound to one indexed snapshot ID, index
   descriptor hash, direction, and logical bounds.
7. Every production operation has explicit limits. No public production path
   allocates in proportion to an unbounded result set or complete index.
8. Unique indexes contain at most one distinct primary-key owner for each
   comparable logical term.
9. Runtime extractor registration must exactly match the persisted descriptor
   hash before any write, rebuild, verification, merge, or repair.
10. GC marks current, retained, and build-pinned snapshots before sweeping
    immutable objects older than the grace period.

## Alternatives

### A. Harden the Existing Multi-Root Transaction

The smallest change is to replace `supports_transactions() -> bool` with exact
capabilities and allow `IndexedMap` only on stores that prove crash-atomic or
distributed-atomic multi-root transactions.

This is useful as an immediate safety correction, but it excludes ordinary
object stores and remote systems that expose a reliable single-key CAS without
an arbitrary multi-key transaction. It also retains separate component heads
whose consistency must always be coordinated. It does not satisfy the target
deployment model.

### B. One Authoritative Indexed-Collection Root

Source and index trees remain independent immutable prolly trees, but no tree
has an independently authoritative head. A canonical indexed snapshot records
their exact roots. One collection-state root selects the current snapshot,
retained snapshots, active definitions, and builds. Publication uploads
immutable objects and then performs one CAS on that root.

This is the selected design. It matches content-addressed storage, works with
single-key CAS backends, simplifies snapshot reads, and turns partial uploads
into unreachable garbage instead of torn visible state. Its cost is a V2
persistence cutover and a meaningful coordinator refactor.

### C. Deferred Materialized Indexes

Source commits append mutations or advance a source head, while background
workers update index roots later. This lowers foreground write amplification
and can scale independently, but an index query is not exact unless it names a
source watermark and waits for the index to reach it.

This remains a possible later tier named `DeferredIndexedMap`. Its API must
return an indexed-through snapshot ID and expose wait/fail behavior. It is not
a fallback for strict `IndexedMap`, and strict queries never silently use a
stale deferred index.

## V2 Persistence Architecture

### Authoritative Root

Each indexed collection owns one reserved root name:

```text
\0prolly/indexed-collection/v2/<hex-source-map-id>/state
```

Its root manifest points to the current `IndexedCollectionState` tree. The
manifest is the only mutable visibility reference for the collection. Source,
index, snapshot-record, statistics, and build-candidate trees are immutable and
content-addressed.

The existing source map ID remains the stable application identifier. Raw
`VersionedMap` mutation checks for the V2 collection root and rejects a managed
ID. V2 collection creation is allowed only for a new source ID or during an
explicit quiesced migration. This avoids pretending that a separate control
root can atomically fence an already-running old writer.

### Collection State

The state tree has a canonical key grammar equivalent to:

```text
meta/format                         -> IndexedCollectionFormatV2
meta/source-map-id                  -> exact application map ID
head                                -> IndexedSnapshotRef
snapshots/<snapshot-id>             -> IndexedSnapshotRef
definitions/<name>/<generation>     -> IndexDescriptorV2
active/<name>                       -> generation
retired/<name>/<generation>         -> RetirementRecord
builds/<build-id>                   -> IndexBuildRecord
```

The current snapshot must appear in `snapshots/`. A descriptor referenced by a
current, retained, or build-pinned snapshot must remain present. Canonical
decoding rejects unknown format versions, malformed keys, duplicate logical
records, a head absent from the retained set, and references whose descriptor
hash does not match the stored descriptor.

The state tree is intentionally small relative to source and index trees.
Retention compacts it by removing old snapshot and descriptor references; it
does not rewrite source or index contents.

### Indexed Snapshot

`IndexedSnapshotRecordV2` is a canonical, content-addressed object represented
by a small prolly tree:

```rust
struct IndexedSnapshotRecordV2 {
    format: IndexedSnapshotFormat,
    source_map_id: Vec<u8>,
    parent: Option<IndexedSnapshotId>,
    source: SourceSnapshotRef,
    indexes: Vec<IndexSnapshotRef>,
}

struct SourceSnapshotRef {
    source_version: MapVersionId,
    tree: PersistedTreeRef,
}

struct IndexSnapshotRef {
    name: Vec<u8>,
    generation: u64,
    descriptor_hash: Cid,
    tree: PersistedTreeRef,
    entry_count: u64,
    stats: Option<PersistedTreeRef>,
}
```

`PersistedTreeRef` contains only the root CID and canonical `TreeFormat`; it
does not persist cache, prefetch, or I/O tuning. Index entries are sorted by
name, and all encodings are length-delimited and versioned. The snapshot ID is
the content identity of the canonical snapshot-record tree, so changing the
source root, any selected index root, a descriptor hash, or deterministic
statistics changes the snapshot ID.

The parent is history metadata, not a correctness dependency. Diff and merge
operate on tree roots and remain possible after an ancestor is pruned when the
required roots are otherwise available.

### Why Component Heads Disappear

V2 does not publish separate source, hidden-index, catalog, or control heads.
Those heads create the multi-root atomicity requirement. Existing tree
algorithms remain reusable, but the trees are opened from an indexed snapshot
rather than discovered through independent named roots.

No component head is maintained as a compatibility mirror. A cache may memoize
an immutable tree by CID, but a cache is never a second source of truth.

## Publication Protocol

An ordinary mutation follows this protocol:

1. Read the current collection-state manifest and decode its head snapshot.
2. Normalize source mutations by primary key with last-write-wins semantics.
3. Batch-read old source values from the head source tree.
4. Derive old and new emissions for every active index using the descriptor
   registered for that exact generation.
5. Build candidate source and index trees from the immutable head trees.
6. Build the canonical snapshot record and the next collection-state tree.
7. Write every new immutable node. Verify successful writes satisfy the store's
   node-visibility contract.
8. Compare-and-swap the one collection-state root from the observed manifest
   to the new state manifest.
9. Return the new snapshot only after the CAS succeeds.

The source and index candidates are never visible through authoritative names
before step 8. If any upload fails, the operation returns the classified store
error. If the CAS conflicts, the coordinator reloads the head and retries from
step 2 subject to both attempt and elapsed-time limits. Extractors rerun because
old source values and uniqueness ownership may have changed.

An operation that produces no source, index, descriptor, retention, or build
state change returns `Unchanged` and performs no CAS.

### Store Requirements

Strict correctness depends on narrower capabilities than a multi-root
transaction:

- content-addressed node writes are immutable and idempotent;
- an acknowledged node write is readable before a root may reference it, or
  the store exposes an explicit publication barrier;
- root compare-and-swap has the coordination scope required by the deployment
  profile;
- acknowledged node and root writes have the required persistence level;
- a root CAS never reports success without durably selecting the exact new
  manifest.

Add `NodePublication::Unknown | ReadAfterWrite` to `StoreCapabilities`.
Development profiles may accept process-scoped CAS and volatile publication.
An embedded multi-process profile requires host-scoped CAS. A remote service
profile requires distributed CAS, read-after-write node publication, and the
configured durable or replicated persistence level. Multi-root transaction
atomicity is optional for V2 correctness.

Startup fails closed with `index.store_capability` when these guarantees are
missing. The current `FileNodeStore` remains suitable only for explicitly weak
development profiles until its own durability design changes.

## Read Protocol and Historical Snapshots

A current read loads the collection-state manifest once, opens its immutable
head snapshot record, and then opens the referenced source and index trees.
There is no double-read validation because all dependencies are immutable and
the state manifest selects them together.

`snapshot_at(id)` succeeds only when the current state retains or build-pins
that snapshot. A returned snapshot handle contains the exact descriptor and
tree references needed by every query. Removing retention prevents new opens;
already-running readers are protected by the GC grace period. Workloads with
queries that can exceed the grace period must explicitly retain the snapshot
for the required duration.

Rollback creates a new snapshot record whose parent is the current head and
whose source/index roots equal the selected historical state. It does not move
the head pointer backward without recording a new history event.

## Descriptor and Extraction Contract

### Canonical Descriptor

`IndexDescriptorV2` contains every persisted semantic choice:

```rust
struct IndexDescriptorV2 {
    name: Vec<u8>,
    generation: u64,
    mode: IndexMode,
    term_schema: TermSchema,
    extractor: ExtractorIdentity,
    projection: IndexProjectionV2,
    limits: IndexSemanticLimits,
    physical_layout: IndexLayoutVersion,
}

enum IndexMode {
    NonUnique,
    Unique { nulls: UniqueNullSemantics },
}

enum ExtractorIdentity {
    Native { semantic_digest: [u8; 32] },
    Portable { program: Vec<u8>, program_digest: [u8; 32] },
}
```

The descriptor hash is computed from its complete canonical encoding. Runtime
registration supplies a descriptor and executable extractor; the complete hash
must match the persisted descriptor. A generation alone is insufficient.

Native extraction remains an application trust boundary. The engine can prove
that the registered semantic digest matches the persisted digest, but it cannot
prove that arbitrary native code still implements those semantics. Applications
must derive the digest from versioned schema or module bytes and change it when
behavior changes. Portable deterministic extractor programs are a follow-up
execution surface and must be separately specified and sandboxed before they
are accepted in production profiles.

Queries and proof verification need the term schema but not executable source
extraction. Writes, builds, logical verification, repair, and source merge fail
with `index.extractor_missing` or `index.definition_mismatch` unless the exact
extractor is registered.

### Typed Composite Terms

Extractors emit typed tuples rather than pre-encoded opaque term bytes. V2
starts with these stable components:

- `Null`;
- `Bool`;
- `I64` and `U64`;
- raw `Bytes`;
- UTF-8 bytes with binary ordering;
- UTC timestamp microseconds as signed `i64`.

Each component declares ascending or descending order. The memcomparable tuple
codec uses type tags, escaped length-delimited payloads, sign-bit transforms
for signed integers, big-endian numeric bytes, and complemented component bytes
for descending order. Prefix encoding ends only on component boundaries.

Floating-point, locale collation, Unicode case folding, and application field
normalization are excluded from the first codec. They require version-pinned
semantics. Email canonicalization remains application policy: an application
may emit a normalized value, but Prolly does not silently lowercase or rewrite
it.

The physical non-unique key remains:

```text
encode(term_tuple) || encode(primary_key)
```

This preserves deterministic order, contiguous owners for one logical term,
and a primary-key tie-breaker. Projection bytes remain the value. The physical
layout version is part of the descriptor hash.

## Unique Indexes

Unique indexes use the same `(term, primary_key)` physical layout and add the
invariant that at most one distinct primary key owns a comparable term.
Keeping one layout preserves range behavior, structural diff, and non-unique
to unique rebuild mechanics.

For a normalized mutation batch, uniqueness validation is performed against a
logical overlay:

1. remove every old emission owned by a changed primary key;
2. add new emissions and reject two new owners for one term;
3. prefix-read existing owners for each newly owned term;
4. ignore owners removed by the same batch;
5. reject any remaining different owner with `index.unique_violation`;
6. build candidate trees and publish through the ordinary collection CAS.

A concurrent writer can pass preflight against the same old head, but only one
CAS succeeds. The loser reloads the winning index root, reruns validation, and
returns the uniqueness violation rather than publishing a duplicate.

`NullsDistinct` is the initial default: null terms do not conflict with one
another. `NullsEqual` is explicit. A sparse extractor emits no term. A
multi-valued unique extractor may emit several distinct terms for one record,
but no term may be owned by another record. Duplicate identical emissions from
one record collapse before validation.

## Projection and Amplification Policy

V2 retains the existing projection modes:

- `KeysOnly`: empty physical value;
- `Include`: application-defined covering bytes;
- `All`: complete source value.

`All` remains explicit because it multiplies source bytes by index fanout.
Production profiles enforce maximum emissions per record, term bytes,
projection bytes, and total derived bytes per source mutation. Health and
metrics report the configured worst-case amplification.

Blob-backed covering values or source-value references may be considered only
after measurements show repeated large `All` projections. The initial design
does not add another storage indirection.

## Bounded Query Layer

### Primitive Scans

Page and streaming scans become the production primitives. Every request names
an indexed snapshot and carries a `QueryBudget`:

```rust
struct QueryBudget {
    max_entries: usize,
    max_result_bytes: usize,
    max_source_fetches: usize,
    max_scanned_entries: usize,
}
```

The engine also enforces profile ceilings that callers cannot exceed.
`exact_page`, `prefix_page`, and `range_page` return an opaque cursor containing
the snapshot ID, descriptor hash, direction, logical-bound hash, and last
physical key. Reusing a cursor with another snapshot, index, direction, or
bound returns `index.cursor_mismatch`.

`records_page` performs one bounded ordered batch read against the source tree
selected by the same snapshot. A missing source record is
`index.snapshot_mismatch`; it is never silently skipped.

Existing convenience methods that collect all matches are removed from the
production API or require an explicit eager-result budget. They are not
implemented as unbounded wrappers over paging.

### Composite Predicates

The first query builder remains intentionally small:

- equality on a complete term;
- equality on a leftmost composite prefix;
- a range on the next component after an equality prefix;
- forward or reverse order;
- limit and cursor;
- optional projected or source-record materialization.

This directly matches the ordered tuple layout. Arbitrary filters run after a
bounded index scan and count against `max_scanned_entries`.

### Intersection, Union, and Planning

A later query milestone may add:

- streaming merge intersection of exact posting lists, which are individually
  primary-key ordered;
- heap-merge union with primary-key deduplication;
- a bounded range driver followed by exact membership checks;
- deterministic per-snapshot entry counts and distinct-count sketches;
- `explain()` output naming the chosen index, bounds, estimated scan, required
  source fetches, and rejected alternatives.

It must not pretend every range result is globally primary-key ordered. A
general cost-based planner is out of scope. Explicit index selection remains
available and is the first stable API.

## Build, Replace, and Repair

### External Sort

Initial build and logical verification stop collecting the complete index in a
`BTreeMap`. An `IndexRunBuilder` owns a fixed memory budget, sorts and
deduplicates each run, spills canonical runs to a configured temporary store,
and performs a k-way merge directly into `SortedBatchBuilder`.

The algorithm guarantees:

- heap use is `O(sort_memory + merge_fan_in + source_page)` rather than
  `O(index_entries)`;
- spill bytes and open runs are bounded;
- conflicting projections and unique duplicates are detected during sorted
  merge;
- temporary runs are deleted on success and best-effort cleanup; stale runs
  are discoverable by build ID;
- a build never uses a verification-entry limit as its capacity limit.

Several indexes can share one source scan. `ensure_indexes` opens one run
builder per requested descriptor, decodes each source record once where the
registered extraction family supports it, and atomically activates the group
only after every index succeeds. `ensure_index` delegates to a one-element
group.

### Online Catch-Up

Each build is represented in collection state:

```rust
struct IndexBuildRecord {
    build_id: BuildId,
    descriptors: Vec<IndexDescriptorV2>,
    base_snapshot: IndexedSnapshotId,
    stage: IndexBuildStage,
    candidate_roots: Vec<PersistedTreeRef>,
    caught_up_snapshot: Option<IndexedSnapshotId>,
    attempts: u32,
}
```

The lifecycle is:

1. `Registered`: CAS a build record that pins the base snapshot.
2. `Scanning`: scan the base source once and external-sort candidate indexes.
3. `CatchingUp`: diff the source tree from the last caught-up snapshot to the
   current head and apply only changed records to each candidate index.
4. `Ready`: persist candidate roots and the exact snapshot through which they
   are caught up.
5. `Active`: when the caught-up snapshot is still the current head, create a
   new indexed snapshot with the same source root and the new index roots, then
   activate descriptors and remove the build record in one collection CAS.

If the activation CAS conflicts because the source advanced, the builder diffs
from its last caught-up source root to the new head and retries. Prolly tree
diff supplies the net changed primary keys, so foreground writes do not need a
second change log.

The base scan may restart after a process crash because local spill runs are
not durable application state. Once candidate roots are recorded, catch-up is
resumable. Build attempts, elapsed time, lagged source versions, spill bytes,
and catch-up records are bounded. If mutation throughput prevents convergence,
the operation returns `index.build_lagging` with the durable build record left
resumable. It does not block source writes indefinitely or silently activate a
stale index.

Replacement uses the same lifecycle while the old generation remains active.
Repair derives the expected tree for the same source snapshot and descriptor,
then publishes a corrected snapshot. Retained old snapshots continue to point
to their original roots.

## Verification

Startup performs only bounded structural validation:

- collection-state format and key grammar;
- head and retention closure;
- descriptor hash and generation consistency;
- referenced root availability within the configured probe budget;
- exact runtime descriptor matches for operations that require extraction.

Full logical verification is explicit. It external-sorts expected emissions
from the source tree and merge-compares them with the selected index tree,
reporting the first bounded set of missing, extra, or mismatched entries plus
aggregate counts. Verification is tied to one immutable snapshot, so concurrent
writes do not force a retry.

Incremental verification may compare source/index changes between two retained
snapshots. Sampling may supplement, but never replace, full verification when a
caller requests proof of complete correctness.

## Retention and Garbage Collection

Retention is a collection-state mutation. `keep_last(n)` always retains the
current snapshot and rejects `n == 0`. Build base and caught-up snapshots are
implicitly pinned until the build completes or is explicitly abandoned.

GC marks from:

- the current collection-state root;
- every retained snapshot record;
- source, index, and statistics roots referenced by those snapshots;
- every active build's base and candidate roots;
- any explicit export or maintenance pin supported by the store.

Sweep considers only unmarked objects older than the configured grace period.
This protects readers that loaded the previous state immediately before a
retention CAS and reclaims nodes uploaded by failed or conflicting
publications. Long-running distributed readers must use explicit retention;
in-process object lifetimes are not distributed GC leases.

Retired descriptors are deleted only when no retained snapshot or build refers
to them. Index deactivation creates a new snapshot without that active index;
it does not mutate historical snapshots.

## Export, Import, and Proofs

An indexed bundle starts from one retained `IndexedSnapshotRecordV2` and
includes its canonical record, descriptors, source tree, selected index trees,
and optional statistics. Export applies node, byte, version, and elapsed-time
limits. It never enumerates independent component heads.

Import verifies all CIDs, descriptor hashes, key/value formats, snapshot
closure, and optional full logical consistency before publication. It writes
immutable objects first and attaches the imported snapshot to collection state
with one CAS. Import into a non-empty collection is explicit and never silently
replaces the head.

The proof milestone adds `IndexedQueryProof`:

```rust
struct IndexedQueryProof {
    snapshot: IndexedSnapshotRecordV2,
    index_name: Vec<u8>,
    descriptor: IndexDescriptorV2,
    bounds: EncodedIndexBounds,
    index_range_proof: RangeProof,
    source_proof: Option<MultiProof>,
}
```

Verification hashes the canonical descriptor and checks the trusted snapshot
ID, descriptor/index-root binding, range completeness, decoded physical-key
bounds, and optional source records against the source root in the same
snapshot. A proof for one snapshot cannot be replayed as proof for another
snapshot even when the logical query text is identical.

## Merge and Rollback

Rollback was defined above as a new snapshot event referencing retained roots.

Merge treats source data as authoritative:

1. choose base, left, and right indexed snapshots;
2. require an explicit definition choice when active descriptor hashes differ;
3. merge source trees with the caller's conflict policy;
4. diff the merged source against a selected parent source;
5. derive index changes using the chosen exact descriptors;
6. verify unique constraints;
7. publish the merged source and derived indexes as one new snapshot CAS.

Index trees are never three-way merged independently. Identical descriptor and
source subtrees may be reused through structural sharing. A definition conflict
requires choosing a generation and rebuilding it; the engine does not guess
that two extractor IDs are semantically compatible.

## Limits and Resource Ownership

Semantic limits belong to descriptors only when they affect accepted record
shape:

- maximum emissions per record;
- maximum encoded term bytes;
- maximum projection bytes;
- maximum derived bytes per record.

Operational limits belong to `EngineLimits` or an operation budget:

- active indexes and simultaneous builds;
- query entries, bytes, source fetches, and scanned entries;
- source page size;
- external-sort memory, spill bytes, run count, and merge fan-in;
- build retries, elapsed time, and catch-up work;
- verification findings and work;
- bundle nodes and bytes;
- optimistic CAS retries and elapsed time.

Every declared limit is either enforced at the allocation/amplification
boundary or removed. In particular, V1 `max_indexes` and `build_page_size`
must not survive as inert configuration. A production profile cannot select an
unbounded eager query or unbounded sort.

## Errors and Retry Policy

The design adds stable errors with structured categories and recovery advice:

- `index.store_capability` — deployment cannot meet strict publication;
- `index.definition_mismatch` — runtime and persisted descriptor hashes differ;
- `index.extractor_missing` — operation requires unavailable extraction code;
- `index.unique_violation` — another primary key owns a unique term;
- `index.query_budget_exceeded` — query crossed an explicit work/result bound;
- `index.cursor_mismatch` — cursor is not valid for this exact request;
- `index.snapshot_not_retained` — requested historical state is no longer
  protected;
- `index.build_lagging` — bounded online catch-up could not converge;
- `index.snapshot_mismatch` — snapshot index and source contents disagree;
- `index.migration_requires_quiescence` — safe V1 cutover precondition failed.

Raw terms, primary keys, source values, and projection bytes are not included
in default error text or telemetry. Diagnostic APIs may return bounded values
only when the caller explicitly requests sensitive details.

Only classified transient store failures and collection-root CAS conflicts are
automatically retried. Definition, uniqueness, limit, corruption, and
capability failures terminate immediately. All retry loops enforce attempts,
elapsed time, cancellation, and backoff.

## Observability

The current logical counters are replaced or renamed so their units are true.
Physical node and byte metrics come from the store observer rather than being
estimated from the number of changed maps.

Required indexed-collection observations include:

- mutation records, old/new extractions, emissions, deduplicated terms, and
  derived bytes;
- source/index nodes and bytes read, reused, written, and uploaded;
- CAS attempts, conflicts, backoff, and latency;
- query pages, scanned entries, returned entries, result bytes, and source
  fetches;
- build stage, source scan position, entries, spill bytes, merge passes,
  catch-up changes, lag, attempts, and elapsed time;
- verification compared entries and mismatch counts;
- retained roots, build pins, orphan candidates, and GC reclaimed bytes.

Labels use collection and descriptor hashes or bounded operator-assigned names.
They never contain raw indexed terms or record identifiers.

## Failure Handling

The visible outcomes are deliberately simple:

- failure before root CAS: old state remains visible; uploaded nodes are
  unreachable;
- successful root CAS followed by client disconnect: new state is visible;
  retrying the same content is idempotent or returns the observed new head;
- root CAS conflict: old observed state remains unmodified; reload and rederive;
- process crash during external sort: durable build record remains; local runs
  are cleaned and the scan restarts;
- process crash during catch-up: last recorded candidate root and caught-up
  snapshot remain resumable;
- missing referenced object after successful CAS: store contract violation and
  corruption error, never an empty result or fallback to another head;
- corrupt index with valid source: queries fail closed; repair derives a new
  candidate from the immutable source snapshot.

Cancellation is checked between source pages, spill runs, merge batches,
catch-up diffs, upload batches, and retry backoff. Cancellation before CAS
publishes nothing.

## Testing and Proof Matrix

### Model and Property Tests

- random put/delete/edit sequences match a reference multimap for every
  retained snapshot;
- term encoding preserves declared tuple order and prefix boundaries;
- index delta application equals a clean rebuild;
- unique overlay validation matches a reference ownership map;
- rollback and source-derived merge match clean rebuilt indexes;
- cursors neither duplicate nor skip entries across page boundaries;
- retention never removes current or build-pinned roots.

### Deterministic Failure Injection

Inject failure or crash at every boundary:

- before, during, and after source/index/snapshot/state node uploads;
- immediately before and after the collection-root CAS;
- after a successful CAS but before the client receives success;
- during each external-sort spill and merge pass;
- between catch-up diff and activation CAS;
- during import, retention, and repair.

Reopen must reveal the complete old state or complete new state. No test may
accept a mixed source/index snapshot.

### Store Conformance

Every store used for strict indexes runs a shared suite proving:

- immutable write idempotence and CID validation;
- acknowledged node read-after-write or publication-barrier behavior;
- root CAS conflict and success semantics at its declared coordination scope;
- concurrent independent writers cannot both win one expected manifest;
- reopen behavior matches declared persistence;
- failed CAS never changes the authoritative root;
- the root never references an object the successful publication did not make
  readable.

Physical power-loss guarantees remain adapter claims backed by the adapter's
documented database or provider contract; runtime probes can disprove but not
prove them.

### Build and Query Scale Tests

- build peak heap remains within the configured memory budget as index size
  grows;
- spill exhaustion returns the stable limit error and publishes nothing;
- several indexes use one source scan;
- online catch-up activates under a write rate below measured catch-up
  throughput and reports `index.build_lagging` above it;
- query heap is bounded by page and batch budgets, not total match count;
- `All` and multi-valued amplification is measured and limited;
- benchmark output reports actual node/byte I/O and CAS conflicts, with
  regression thresholds owned by CI.

### End-to-End Validation

Level-3 validation performs a real application write through `IndexedMap`,
reopens the store through an independent client, queries by an alternate field,
and verifies the resulting source record. Error-path validation covers unique
conflicts, capability rejection, query bounds, crash/reopen publication, and a
resumed online build on at least one durable local backend and one distributed
CAS backend.

## Migration and Compatibility

V2 is a hard persistence cutover. New code does not probe V1 roots and silently
fall back. Existing V1 data moves through an explicit offline migration or
verified export/import:

1. stop all V1 writers and prove quiescence;
2. open and structurally validate the V1 source, catalog, control, and selected
   index checkpoints;
3. optionally perform full logical verification;
4. encode V2 descriptors and snapshot records;
5. prewrite V2 immutable objects;
6. reread every observed V1 head and abort if any changed;
7. create the V2 collection-state root with CAS-on-absence;
8. reopen through V2 and verify the selected source and index roots;
9. leave V1 roots untouched until an explicit later cleanup after the rollback
   window.

Because old clients do not understand the V2 fence, quiescence is mandatory.
The migration refuses to run when the V2 root already exists or when V1 heads
change during validation. `doctor` reports residual V1 roots and the explicit
cleanup path. Runtime aliases, dual writes, and mirrored compatibility heads
are not added.

## Delivery Milestones

This program is too broad for one implementation plan. Work is divided into
the following independently reviewable specifications.

### Milestone 0: Immediate Contract Corrections

- gate V1 strict indexes on exact transaction capabilities;
- remove or enforce inert limits;
- make eager query bounds explicit;
- correct misleading node metrics;
- add current-backend crash/conformance gaps to health output.

This makes the existing surface honest while V2 is built.

### Milestone 1: Indexed Collection State V2

- canonical collection state and indexed snapshot records;
- one-root CAS publication and read protocol;
- V2 retention, bundle, GC roots, health, and explicit migration;
- sync API on memory and one crash-durable local store;
- shared single-root conformance suite.

This is the foundational milestone and must land before semantic or planner
expansion.

### Milestone 2: Bounded External Build and Online Catch-Up

- spillable sorted runs and streaming verification;
- durable build records and source-diff catch-up;
- batch multi-index build with one source scan;
- build progress, cancellation, cleanup, and scale gates.

### Milestone 3: Typed Composite and Unique Indexes

- canonical tuple codec and cross-language fixtures;
- descriptor V2 runtime matching;
- unique overlay validation and concurrency tests;
- query builders for equality-prefix plus range.

### Milestone 4: Query Composition and Statistics

- bounded record pages;
- exact-list intersection and union;
- deterministic minimal statistics and `explain()`;
- no general SQL or arbitrary unbounded planner.

### Milestone 5: Versioned Operations and Proofs

- source-derived indexed merge and rollback;
- indexed query proofs;
- portable V2 import/export completion;
- retention-aware repair and GC verification.

### Milestone 6: Async and Binding Parity

- async indexed collection over `AsyncStore` and `AsyncManifestStore`;
- Rust, Python, Kotlin, JavaScript, Swift, and C surface parity appropriate to
  each binding;
- shared canonical fixtures and live backend matrix;
- deferred indexes only under a separate approved design.

## Acceptance Criteria

The program is complete when all of these statements are proved:

1. Strict publication uses exactly one authoritative root CAS.
2. Crash/reopen and independent-writer tests expose a complete old or new
   indexed snapshot, never torn source/index state.
3. A service deployment works on a backend with distributed single-key CAS and
   durable read-after-write immutable objects without requiring multi-root
   transactions.
4. Query memory is bounded by declared page/work budgets regardless of total
   matches.
5. Initial build memory is bounded independently of source and index size.
6. An online build can resume after interruption and activate only when caught
   up to the exact current source snapshot.
7. Composite numeric and timestamp ranges follow canonical order in every
   supported binding.
8. Concurrent unique writes cannot publish two owners for one comparable term.
9. Historical queries, cursors, exports, and proofs bind to exact snapshot and
   descriptor identities.
10. Merge, rollback, and repair produce the same index roots as a clean rebuild
    for the selected source and descriptor when canonical tree format is held
    constant.
11. Every configured secondary-index limit is enforced or removed.
12. Metrics report measured physical node/byte work and contain no indexed
    application data.

## Final Recommendation

Do not replace the prolly-tree index mechanics or weaken strict consistency.
Replace the multi-root publication model with a content-addressed indexed
snapshot selected by one collection-state CAS. Then address resource bounds
and online build mechanics before adding unique constraints or a wider query
surface.

This order fixes the correctness boundary first, makes scale failures explicit,
and lets later query and schema features build on one stable snapshot contract.
