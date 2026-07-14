# IndexedMap: Strict Secondary Indexes for VersionedMap

## Status

Approved design for implementation planning.

This document defines the first stable Rust-core milestone for secondary
indexes. It narrows and resolves the v1 decisions left open in
[`docs/secondary-index-design.md`](../../secondary-index-design.md). That
document remains useful as the broader long-term design; this specification is
normative when the two documents differ about the v1 name, scope, physical
layout, or lifecycle.

## Decision Summary

The engine will add a public `IndexedMap` coordinator above `VersionedMap`.
Each secondary index is a hidden derived `VersionedMap`. A hidden catalog
associates every published source version with the exact index versions derived
from it. A persistent control root fences every ordinary source-head mutation.
The source, indexes, checkpoints, catalog, and control state publish in one
strict store transaction.

The hardened v1 supports:

- strict synchronous maintenance;
- non-unique indexes;
- zero-to-many terms emitted per source record;
- `KeysOnly`, `Include`, and `All` projection modes;
- runtime Rust definitions and callbacks;
- dynamic index creation on empty or populated maps;
- retryable snapshot builds;
- exact current and historical indexed snapshots;
- verification, repair, generation replacement, and logical deactivation;
- reference-aware `keep_last(n)` retention;
- verified current-snapshot export and import;
- startup validation, resource limits, and structured errors.

The source remains authoritative. Index maps are never directly writable and
are never merged independently.

## Goals

1. Make source and index publication atomic.
2. Prevent accidental writes that bypass active indexes.
3. Make every indexed read use an exact immutable source/index selection.
4. Preserve the existing source `MapVersionId` contract.
5. Reuse prolly trees, managed-map transactions, snapshots, range scans,
   structural sharing, deterministic builds, and GC.
6. Allow an index to be added after an engine or populated source map exists.
7. Make incremental maintenance verifiable against a deterministic full
   rebuild.
8. Provide enough lifecycle and recovery operations that v1 does not trap an
   application on one extractor or retain unbounded history.
9. Keep persisted formats independent of Rust closures and future derive
   macros.
10. Support compact key-only indexes and bounded covering projections without
    changing source authority or snapshot consistency.

## Non-goals

V1 does not include:

- unique constraints;
- a derive macro;
- guaranteed-progress online builds under continuous writes;
- async APIs or language bindings;
- deferred or eventually consistent indexes;
- general query planning, joins, aggregates, full-text, or vector search;
- indexed merge or general rollback publication;
- immediate physical purge of retired generations;
- full-history portable backups.
- automatic blob offload for projected index values.

Unsupported source-head operations fail behind the write fence. They do not
silently skip index maintenance.

## Core Invariants

### Source authority

Source records are authoritative. An index entry is derived state and may be
verified or rebuilt from a retained source version.

### Exact derivation

Every checkpoint names one source map, one source `MapVersionId`, one
definition fingerprint, one hidden index map, and one index `MapVersionId`.
The query API never presents that index version as belonging to another source
version.

### Atomic publication

For active strict indexes, source head, index heads, immutable version roots,
checkpoint records, catalog head, and control state commit together through
`TransactionalStore`.

### Snapshot-first reads

An indexed read resolves one immutable catalog version first. It then opens
only the immutable source and index versions named by that catalog snapshot.
It never combines independently loaded mutable heads.

### No public bypass

Every public `VersionedMap` operation that can move a source head or invalidate
retained checkpoints validates the deterministic control root. This includes
`VersionedMapsTransaction::apply`. Only the crate-private coordinator can
stage writes with an index-maintenance permit.

### Deterministic extraction

Given a definition generation, primary key, and source value, an extractor
must return the same terms in every process and on every retry. Extractors are
side-effect-free and perform no I/O.

### Fail closed

Missing runtime definitions, descriptor mismatches, invalid checkpoints,
missing version roots, cursor mismatches, and resource-limit violations return
structured errors before results or writes are exposed.

## Architecture

```text
Application
    |
    v
IndexedMap
    |-- source VersionedMap
    |-- runtime SecondaryIndexRegistry
    |-- hidden catalog VersionedMap
    |-- hidden VersionedMap per definition generation
    `-- crate-private IndexCoordinator
             |
             v
       VersionedMapsTransaction
             |
             v
       TransactionalStore
```

Secondary-index policy stays out of node encoding, boundary selection,
rebalancing, diff traversal, and raw `Tree` operations. `IndexedMap` composes
existing primitives above that layer.

### Public type family

- `IndexedMap<'a, S>` is the source-map facade and write coordinator.
- `IndexedSnapshot<'a, S>` is one immutable catalog-selected read view.
- `IndexedVersion` is one coordinated publication result.
- `IndexedSnapshotId` identifies a source and catalog version pair.
- `SecondaryIndex` is one runtime definition.
- `SecondaryIndexRegistry` owns the runtime definitions supplied at open.
- `SecondaryIndexSnapshot<'a, S>` queries one checkpointed hidden index map.
- `SecondaryIndexMatch` contains a logical term, primary key, and optional
  projected bytes.

`IndexCoordinator`, hidden map IDs, persisted record codecs, and write permits
are crate-private implementation details.

`IndexedVersion` returns the published source and catalog versions plus the
sorted checkpoints selected by that catalog version. Callers therefore receive
the complete coordinated result rather than only a new source ID.

### Construction

Opening a handle validates persisted active definitions but does not
automatically begin a potentially expensive build:

```rust,ignore
let registry = SecondaryIndexRegistry::new()
    .register(SecondaryIndex::non_unique(
        "by-status",
        1,
        "app.users.by-status/v1",
        |primary_key, value| {
            let user = decode_user(value)?;
            Ok(vec![status_term(&user.status)])
        },
    ))?
    .register(SecondaryIndex::non_unique(
        "by-tag",
        1,
        "app.users.by-tag/v1",
        |primary_key, value| {
            let user = decode_user(value)?;
            Ok(user.tags.into_iter().map(tag_term).collect())
        },
    ))?;

let users = engine.indexed_map(b"users", registry)?;
users.ensure_index("by-status")?;
users.ensure_index("by-tag")?;
```

Extra runtime definitions are allowed and inactive. Every persisted active
definition must have an exact runtime match. `ensure_index` is idempotent for
an already-active matching generation.

## Runtime Definition Contract

The initial low-level contract is byte-oriented and object-safe:

```rust,ignore
pub trait SecondaryIndexExtractor: Send + Sync + 'static {
    fn extract(
        &self,
        primary_key: &[u8],
        source_value: &[u8],
    ) -> Result<Vec<SecondaryIndexEntry>, SecondaryIndexError>;
}

pub struct SecondaryIndexEntry {
    pub term: Vec<u8>,
    pub projection: Option<Vec<u8>>,
}

pub enum IndexProjection {
    KeysOnly,
    Include,
    All,
}
```

`SecondaryIndex::non_unique` remains a concise `KeysOnly` constructor whose
callback returns terms. The general builder selects a projection mode and uses
the entry-returning extractor. `extract_terms` is also available for `All`,
where the engine supplies the stored source bytes automatically.

Projection semantics are:

- `KeysOnly`: emitted entries must not contain projection bytes;
- `Include`: every emitted entry contains deterministic application-encoded
  projection bytes;
- `All`: emitted entries must not contain projection bytes, and the engine
  copies the complete raw source value stored at that source version.

`SecondaryIndex::non_unique` binds an extractor to:

- an arbitrary-byte stable name, with UTF-8 convenience constructors;
- a positive generation number;
- an application-controlled extractor ID;
- one `IndexProjection` mode;
- physical layout version 1;
- explicit resource limits, using engine defaults when omitted.

The persisted descriptor records semantic fields, not callback code. Its
fingerprint hashes canonical descriptor bytes including source map ID, name,
generation, extractor ID, index mode, projection mode, and physical layout
version. Applications must change the extractor ID or generation when
extraction or projection semantics change.

The engine sorts entries by canonical term bytes and deduplicates emissions
with identical term and projection bytes. Emitting one term more than once with
different projections from the same source record is
`ConflictingIndexProjection`, because both emissions address the same physical
key. Extractor and projection errors abort without publication. A Rust panic
unwinds without committing; structured panic conversion is deferred to the
FFI phase.

## Persisted State

All secondary-index records use a deterministic, versioned binary codec with a
magic value, explicit format version, bounded lengths, and rejection of unknown
required fields. Canonical fixtures lock down bytes and fingerprints.

### Hidden IDs

IDs use `KeyBuilder` segments rather than string concatenation:

```text
catalog map ID:
  (system, secondary-index-catalog, source-map-id)

index map ID:
  (system, secondary-index, source-map-id, index-name,
   definition-fingerprint)
```

Each definition generation receives its own hidden index map. Old and new
generations can therefore coexist while snapshots still reference the old one.

### Catalog records

The hidden catalog contains:

```text
format
definitions/<index-name>/<generation>
checkpoints/<source-version>/<index-name>/<generation>
current
retired/<index-name>/<generation>
```

The semantic record shapes are:

```rust,ignore
pub struct SecondaryIndexDescriptor {
    pub format_version: u32,
    pub source_map_id: Vec<u8>,
    pub name: Vec<u8>,
    pub generation: u64,
    pub extractor_id: String,
    pub fingerprint: Cid,
    pub projection: IndexProjection,
    pub physical_layout_version: u32,
}

pub struct IndexCheckpoint {
    pub source_map_id: Vec<u8>,
    pub source_version: MapVersionId,
    pub index_name: Vec<u8>,
    pub generation: u64,
    pub definition_fingerprint: Cid,
    pub index_map_id: Vec<u8>,
    pub index_version: MapVersionId,
}

pub struct IndexedHeadRecord {
    pub source_version: MapVersionId,
    pub indexes: Vec<IndexCheckpoint>,
}
```

`IndexedHeadRecord.indexes` is sorted by raw index name and rejects duplicate
active names.

Wall-clock timestamps are named-root metadata and are not stored in checkpoint
tree values. Rebuilding the same descriptors and source/index selections must
produce the same catalog tree root in every process.

### Control root

Every source map has a deterministic control-root name beneath its managed-map
namespace. Absence means no strict index is active. Presence names the catalog
map ID and the sorted active descriptor fingerprints.

All ordinary source writes add the observed control root, including absence,
to their transaction conditions. This absence condition prevents a writer
that began before first-index activation from committing after activation.

## Physical Index Layout

One logical non-unique relationship is:

```text
term -> primary key + optional projected bytes
```

Physical layout version 1 stores:

```text
key   = encode_segment(term) || encode_segment(primary_key)

KeysOnly value = empty bytes
Include value  = canonical IndexValue::Included(projection_bytes)
All value      = canonical IndexValue::FullSource(source_value)
```

The primary key is decoded from the physical key and is not duplicated in the
value. The non-empty value envelope has a magic, version, projection kind, and
bounded payload. Segment encoding preserves byte ordering and makes component
boundaries unambiguous.

`All` copies the exact raw bytes stored in the source tree. If those bytes are
an application or engine blob reference, the reference is copied rather than
the referenced blob contents. Projected index values remain inline in v1 and
must satisfy the configured size limits.

Changing projection mode or included-byte encoding requires a new definition
generation. For `KeysOnly`, a source change that preserves emitted terms
produces no index mutation. For `Include`, a projection-only change produces an
upsert for the existing physical key. For `All`, every source-value change
rewrites each index entry emitted by that record.

The module adds an internal segment-prefix encoder that escapes a partial term
without appending a segment terminator. Query translation is:

- exact term: prefix of the completed encoded term;
- term range: encoded `[start_term, end_term)`;
- term prefix: escaped partial term without its terminator;
- complete iteration: the whole hidden index tree.

Key, term, and projected-value limits are checked before proportional physical
key or value buffers are allocated.

## Dynamic Registration

### Empty source

Registration creates the empty hidden index map, its checkpoint, descriptor,
catalog current record, and control root in one transaction. If the source map
has never published a head, the same transaction initializes its canonical
empty source version.

### Populated source

V1 uses a retryable shadow build:

1. Pin source head `Sbase`.
2. Stream the immutable source snapshot.
3. Run the extractor, construct projection values, enforce limits, and produce
   sorted physical entries.
4. Build the hidden index with the existing sorted builder under a memory
   budget.
5. Start an activation transaction.
6. Validate that source head is still `Sbase` and that control and catalog
   roots are unchanged.
7. Publish the index version, descriptor, checkpoint, catalog version, index
   head, and control root atomically.
8. Retry from the new source head when validation conflicts, up to the
   configured limit.

Existing readers and writers continue during the shadow build. Under
continuous writes the build may retry or exhaust its limit; guaranteed-progress
online catch-up is not a v1 guarantee.

A crash before activation leaves no visible partial index. Unreachable
content-addressed nodes may remain and are reclaimable by GC.

### Adding an index to an already indexed source

Existing indexes remain active during the new shadow build. Final activation
conditions on the source head, current catalog, and control root. An indexed
write racing with activation therefore conflicts and retries against the new
definition set.

## Atomic Mutation Algorithm

For `W` normalized changed source keys:

1. Start `ProllyTransaction` and `VersionedMapsTransaction`.
2. Load and validate the exact control root and active descriptors.
3. Load source head and catalog `current` through the transaction overlay.
4. Require catalog source version to equal the source head.
5. Enforce an optional caller-supplied expected source version.
6. Normalize duplicate source mutations with existing last-write-wins rules.
7. Batch-load old source values for affected primary keys.
8. Determine final new values.
9. Extract, project, sort, and deduplicate old and new entries for every active
   index.
10. Compare physical key and value bytes: delete removed keys, insert new keys,
    and upsert keys whose projected value changed.
11. Enforce per-record and per-transaction resource limits.
12. Stage the source mutation batch and obtain `Snext`.
13. Stage one mutation batch per affected hidden index and obtain each `Inext`.
14. Write checkpoints for `Snext` and replace catalog `current`.
15. Stage source, index, catalog, immutable version, and control-related roots.
16. Commit once.
17. On optimistic conflict, discard staged state and rerun from step 1.

The transaction overlay needs an ordered `get_many` path so old-value and
future unique-owner reads do not issue unnecessary point operations.

If final source content is unchanged, no source, index, checkpoint, or catalog
version is published.

## Write Authority and Fence Coverage

`versioned_map.rs` gains one centralized staged-head publication path with a
crate-private authority argument:

```rust,ignore
enum MapWriteAuthority<'a> {
    Unmanaged,
    IndexMaintenance(&'a IndexMaintenancePermit),
}
```

An unmanaged write succeeds only when the deterministic control root is absent.
The permit is unconstructable outside the secondary-index coordinator and is
bound to the source map ID and exact observed control fingerprint.

The refactor must cover:

- initialize and import-as-head;
- apply, conditional apply, put, delete, and edit;
- append, parallel apply, and rebuild;
- rollback and merge publication;
- restore and migration helpers;
- public multi-map transaction apply helpers;
- version pruning that could invalidate checkpoints.

V1 `IndexedMap` implements ordinary apply/put/delete/edit operations. Other
fenced operations return a structured coordinator-required or unsupported
error until an index-aware implementation exists.

## Read and Query API

`IndexedMap::snapshot()`:

1. Opens one immutable catalog head.
2. Decodes and validates `current`.
3. Loads the immutable source version it names.
4. Loads every immutable index version its checkpoints name.
5. Validates runtime fingerprints and map ownership.
6. Returns `IndexedSnapshot`.

Concurrent publication cannot tear this selection because all later reads use
immutable version roots.

```rust,ignore
let snapshot = users.snapshot()?;

let keys = snapshot
    .index("by-status")?
    .primary_keys(&active_term)?;

let records = snapshot
    .index("by-status")?
    .records(&active_term)?;

let projected = snapshot
    .index("by-status")?
    .projected(&active_term)?;
```

`records` performs one ordered `get_many` against the pinned source snapshot
and preserves index order for every projection mode. It is the authoritative
record API. `projected` reads only the index: it returns no payload for
`KeysOnly`, application-encoded bytes for `Include`, and the exact stored source
bytes for `All`.

`SecondaryIndexSnapshot` supports exact-term lookup, term-prefix and term-range
iteration, forward and reverse cursor pages, primary-key iteration, and batched
record resolution. Match iteration exposes term, primary key, and optional
projected bytes.

### Historical snapshots

`snapshot_at(source_version)` uses only checkpoints for that exact retained
source version. If an index did not exist then, it returns
`IndexUnavailableAtVersion`. It does not silently scan the source or substitute
another index version.

This convenience method selects the generations active in the current catalog
and then looks for exact checkpoints at `source_version`. Consequently, a newly
replaced generation may be unavailable for an older source version.

Because source content identity does not identify which index generations were
available, a reproducible selection is:

```rust,ignore
pub struct IndexedSnapshotId {
    pub source_version: MapVersionId,
    pub catalog_version: MapVersionId,
}
```

`snapshot_by_id(indexed_snapshot_id)` reopens that exact historical catalog
selection when both named versions are still retained. Retention may make an
old ID unavailable; an already pinned in-process snapshot remains valid until
its nodes are reclaimed after it is released.

### Cursor identity

An index cursor carries or is accompanied by source version, index version,
definition fingerprint, direction, bounds, and raw tree cursor. Resuming it
against another selection returns `IndexCursorVersionMismatch`.

## Hardened V1 Lifecycle

### Startup validation and health

Opening `IndexedMap` performs bounded structural validation:

- catalog format is supported;
- active descriptors have exact runtime matches;
- control root and catalog agree;
- `current` names the actual source head;
- checkpoint ownership and fingerprints are valid;
- named source and index version roots exist;
- the store advertises strict transaction support.

`health()` returns the active generations, source/catalog/index versions,
checkpoint validation status, and store capability status. It does not run a
full semantic scan.

### Semantic verification

`verify_index(name, source_version)` rebuilds the expected index from the
retained immutable source snapshot under a resource budget and compares the
resulting root with the checkpointed index root. `verify_all` repeats this for
selected active indexes.

Verification never mutates state and never trusts index entries as source
data.

### Repair

`repair_index(name, source_version)` performs the same deterministic rebuild,
then atomically replaces the bad checkpoint and relevant catalog selection.
Queries never trigger repair implicitly. Repair requires the source version and
runtime definition to remain available and exact.

### Definition replacement

`replace_index(name, new_definition)` uses the populated-source shadow-build
algorithm. The old generation remains active during the build. Activation
atomically selects the new generation for the unchanged current source version
and marks the old generation retired. Pinned old catalog snapshots continue to
resolve the old generation. The replacement must use a strictly greater
generation and a different descriptor fingerprint.

### Logical deactivation

`deactivate_index(name)` atomically removes the index from the active catalog
selection and control root. Historical checkpoints and hidden version roots
remain retained. If the last index is deactivated, the control root is removed
and ordinary future `VersionedMap` writes are allowed again.

V1 deliberately does not call this operation `drop_index`: it makes no promise
of immediate physical deletion.

### Reference-aware retention

`IndexedMap::keep_last(n)` always retains the current source version and treats
each kept source checkpoint as a transitive root:

```text
kept source version
    -> checkpoint records
        -> exact index versions
            -> hidden index tree nodes
```

In one strict transaction it removes pruned source checkpoints, unreferenced
source/index immutable version roots, and obsolete catalog version roots while
preserving current and retained selections. Retired generations remain while
any retained checkpoint references them.

`keep_last(0)` is defined as retaining the current source version. Indexed
snapshot IDs whose catalog versions are removed become unavailable to future
opens. Removing named roots does not delete nodes immediately, so an already
open snapshot remains readable until node GC. Node GC must honor live engine
pins or be operationally serialized with readers.

Node GC continues to compute reachability from every remaining named root in
the shared store. V1 exposes index-aware planning and does not permit raw
source pruning while a control root is active.

### Current-snapshot export and import

`export_current()` emits a verified bundle containing:

- source map ID and exact source version;
- catalog version and canonical catalog records;
- control state and active descriptors;
- exact index checkpoints and index versions;
- all required content-addressed nodes.

`import_current(bundle, expected_source)` verifies bundle hashes, descriptors,
ownership, version roots, and checkpoint consistency before atomically
publishing all heads and version roots. V1 imports only into the same source map
ID and uses `expected_source` for optimistic overwrite protection.

Full catalog history and policy-rich backups remain later work.

## Errors and Retry Semantics

V1 adds structured variants equivalent to:

```rust,ignore
InvalidIndexDefinition { reason }
IndexRuntimeDefinitionMissing { name, generation }
IndexDefinitionMismatch { name, persisted, runtime }
IndexesRequireIndexedMap { map_id, active_indexes }
IndexOperationUnsupported { operation }
IndexExtractionFailed { name, primary_key, reason }
IndexProjectionMismatch { name, mode, primary_key }
ConflictingIndexProjection { name, primary_key, term }
IndexBuildConflictLimitExceeded { name, attempts }
IndexUnavailableAtVersion { name, source_version }
IndexCheckpointMismatch { name, source_version, reason }
IndexCursorVersionMismatch { expected, actual }
IndexResourceLimitExceeded { resource, limit, actual }
InvalidIndexedSnapshotBundle { reason }
```

Only optimistic transaction and build-activation conflicts retry
automatically. Definition, extraction, checkpoint, bundle, and resource errors
do not retry.

## Resource Limits

Configuration bounds:

- active definitions per source map;
- term bytes;
- projection bytes per entry;
- projected bytes per source record;
- projected bytes per source transaction;
- raw source-value bytes accepted by an `All` index;
- emitted terms per record;
- derived mutations per source transaction;
- extractor output bytes per transaction;
- build page size and temporary sort memory;
- automatic write retries and build activation retries;
- export/import bundle nodes and bytes;
- verification and repair work budgets.

Limits are checked before allocations proportional to untrusted nested output
where possible. Exceeding a limit aborts before commit.

## Crash and Concurrency Semantics

For a conforming `TransactionalStore`:

- a crash before commit exposes neither new source nor new index state;
- a crash after commit exposes the complete new catalog selection;
- a failed extractor or exhausted limit publishes nothing;
- unpublished build nodes are unreachable and GC-reclaimable;
- concurrent indexed writers validate source, catalog, and control roots and
  retry from fresh state;
- concurrent activation, replacement, deactivation, retention, or repair
  conflicts through catalog/control conditions;
- an unmanaged writer started before activation cannot commit afterward;
- pinned reads remain valid because they use immutable version roots.

Stores must pass multi-writer and crash/reopen conformance tests before
advertising strict indexed-map support.

## Testing Strategy

### Unit tests

- canonical descriptor, checkpoint, control, and bundle codecs;
- descriptor fingerprints and hidden ID construction;
- physical key encoding, decoding, exact/range/prefix bounds, and ordering;
- projection value envelope encoding and decoding;
- old/new emission comparison, projection-only upserts, and deduplication;
- rejection of projection-mode mismatches and conflicting projections;
- sparse and multi-term extraction;
- resource-limit enforcement;
- cursor identity validation;
- centralized fence behavior for every head-changing public path.

### Property tests

The primary oracle is:

```text
incrementally maintained index root
==
full deterministic rebuild root
```

Generate arbitrary inserts, replacements, deletes, duplicate mutations,
non-indexed-field changes, empty emissions, multi-term emissions, projection
changes, and operation orderings across all projection modes. Also assert that
every physical entry resolves to a source record that emits the same term and
projection, and every emitted entry has one physical entry.

### Transaction and fault tests

- source, multiple indexes, checkpoints, and catalog commit all-or-nothing;
- extraction failure and resource exhaustion publish nothing;
- invalid, conflicting, or oversized projections publish nothing;
- source writer versus first-index activation;
- indexed writer versus replacement, deactivation, retention, and repair;
- pre-activation unmanaged writer cannot commit post-activation;
- raw writes and raw pruning are rejected after activation;
- concurrent indexed writers retry from fresh snapshots;
- injected commit failures and crash/reopen expose no torn selection;
- missing nodes and corrupted catalog records fail closed;
- retry exhaustion returns a structured error.

### Lifecycle tests

- registration on empty and populated sources;
- registration retry after source movement;
- adding an index while other indexes remain active;
- reopen with matching, missing, and mismatched runtime definitions;
- semantic verification and repair;
- generation replacement with pinned old snapshots;
- deactivation of one and the final active index;
- reference-aware retention across shared index versions;
- current-snapshot export/import round trip;
- historical unavailability when no exact checkpoint exists.

### Conformance fixtures

Commit portable fixtures for descriptor bytes and fingerprints, control and
catalog records, physical key and projection-value bytes for every projection
mode, hidden IDs, snapshot bundle manifests, and expected
source/index/catalog roots. The later derive macro and bindings must produce
these same low-level definitions and roots.

## Performance Model and Benchmarks

For `W` changed source keys, `D` active indexes, `E` emitted terms, and `P`
projected bytes written, a normal write should scale with `W * D + E + P`, not
total source or index size.

Required benchmarks cover:

- one and several active indexes;
- indexed-field and non-indexed-field updates;
- sparse and high-fanout extractors;
- `KeysOnly`, small and large `Include`, and bounded `All` projections;
- term-preserving projection changes and full-value write amplification;
- exact, prefix, range, forward-page, and reverse-page queries;
- batched source record resolution;
- current and historical snapshots;
- populated-source build and activation retries;
- verification and repair throughput;
- retention and current-snapshot export/import;
- concurrent writer conflicts.

Metrics expose source keys normalized, records extracted, terms emitted,
projected bytes, physical inserts/deletes/upserts, unchanged emissions skipped,
nodes written by map, commit retries, build attempts, verification outcomes,
and retained roots by generation.

## Implementation Milestones

### Milestone 0: persisted and transactional foundation

1. Add canonical record types/codecs and conformance fixtures.
2. Add hidden map/control naming helpers.
3. Add transaction-overlay ordered `get_many`.
4. Centralize staged managed-head publication behind write authority.
5. Apply control-root absence conditions to every public head mutation and
   public multi-map apply path.

Exit: a test can atomically publish one source, hidden index, checkpoint, and
catalog, and no public managed-map path bypasses an active fence.

### Milestone 1: strict non-unique core

1. Add runtime definitions and registry.
2. Add `IndexedMap`, `IndexedSnapshot`, and index query handles.
3. Implement dynamic shadow builds and `ensure_index`.
4. Implement incremental delta planning and atomic writes.
5. Implement all three projection modes and their canonical value codec.
6. Add exact, prefix, range, cursors, historical snapshots, projected reads,
   and batched authoritative record resolution.

Exit: arbitrary incremental mutation sequences equal deterministic rebuilds,
and current/historical queries are snapshot-exact.

### Milestone 2: hardened lifecycle

1. Add startup validation and health.
2. Add semantic verification and repair.
3. Add blocking generation replacement and logical deactivation.
4. Add reference-aware `keep_last(n)` and indexed GC planning.
5. Add verified current-snapshot export/import.
6. Complete crash, fault, concurrency, lifecycle, and benchmark coverage.

Exit: v1 satisfies every acceptance criterion below.

## Acceptance Criteria

V1 is complete when:

1. Applications can declare runtime non-unique indexes whose records emit zero
   or more arbitrary-byte terms.
2. Definitions support deterministic `KeysOnly`, bounded `Include`, and
   bounded `All` projections, with `KeysOnly` as the default.
3. Indexes can be added safely after source-map creation and population.
4. Every successful source write atomically publishes all active index and
   catalog changes.
5. No public managed-map mutation or prune operation can accidentally bypass
   the fence.
6. Every indexed query uses an exact immutable catalog selection.
7. Historical queries use exact checkpoints or return unavailable.
8. Incremental roots, including projected value bytes, equal full rebuild roots
   under property testing.
9. Startup validation, verification, and repair detect and recover from index
   drift without silent query-time mutation.
10. Definitions can be replaced and indexes logically deactivated without
   invalidating pinned historical snapshots.
11. `keep_last(n)` never removes an index version referenced by a retained
    source checkpoint.
12. Current indexed snapshots export and import with full structural and
    checkpoint verification.
13. Crash/reopen and concurrent-writer tests never expose torn source/index
    state.
14. Persisted bytes and roots are locked by conformance fixtures before derive
    macros, async APIs, or bindings are added.

## Final Rule

An `IndexedMap` is a source `VersionedMap` plus strictly derived hidden index
maps. Every visible index version is checkpointed to the exact immutable source
version that produced it, and every safe write atomically publishes the source,
indexes, catalog, and control state while preventing bypass.
