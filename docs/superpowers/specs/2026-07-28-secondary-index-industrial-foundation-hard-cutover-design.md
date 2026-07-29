# Secondary Index Industrial Foundation Hard-Cutover Design

**Status:** Approved

**Date:** 2026-07-28

**Scope:** Replace the current secondary-index coordination architecture with
one canonical, production-grade foundation. This is a hard cutover. It does
not preserve the current storage layout or public compatibility surfaces.

## Summary

The current secondary-index implementation has a strong logical model but does
not meet the required production bar. Its source, hidden-index, catalog, and
control heads are read and published independently. That permits transient
mixed observations under concurrency and requires a backend to provide a
crash-atomic transaction across several mutable roots. Its query, build,
verification, and transfer paths also contain work that is not bounded before
allocation.

This design replaces that coordination model. One immutable
`IndexedCollectionState`, selected by one authoritative root compare-and-swap,
is the only visibility point for a managed source and all of its secondary
indexes. Source and index trees remain immutable, content-addressed prolly
trees. Writers upload candidate objects first and make them visible with one
CAS. Readers load one state root and therefore observe one complete indexed
snapshot.

Every production operation receives a finite, typed budget. Index builds use
bounded sorted runs and an injected spill workspace instead of collecting the
complete index in memory. Queries are page- or visitor-based and validate
cursor positions against their logical and physical bounds. Errors are
structured and redacted. Metrics report measured physical work. Deterministic
concurrency, failure-injection, resource, store-conformance, and scale tests
become release gates.

The first delivery is an industrial foundation only. Unique indexes, typed
composite terms, query composition, deferred maintenance, and resumable online
builds are deliberately deferred.

## Canonical Cutover Policy

There is one secondary-index architecture and one supported persisted format
at a time.

- Public types and persisted records use canonical names such as
  `IndexedCollectionState`, `IndexedSnapshot`, and `IndexDescriptor`.
- Mutable root names contain no `v2`, `v3`, generation suffix, or compatibility
  namespace.
- A release supports exactly one secondary-index format discriminator.
- An incompatible future change replaces the supported discriminator, types,
  fixtures, and implementation in one hard cutover.
- The implementation does not dual-read, dual-write, auto-migrate, or retain
  a legacy compatibility mode.
- Opening a store that contains a known obsolete managed-index marker fails
  with `index.format_unsupported`; it never creates a second authoritative
  collection beside obsolete roots.
- Data required across a cutover must be exported or rebuilt before installing
  the cutover release. The new release does not decode the old index format.

This document supersedes the foundation portion and suffix-based naming in
`2026-07-14-secondary-index-production-improvements-design.md`. Later feature
directions in that document remain proposals, not part of this delivery.

## Goals

1. Make one root CAS the sole linearization point for every visible
   secondary-index state change.
2. Guarantee that a reader observes a complete old or complete new indexed
   snapshot, never mixed source, index, descriptor, or retention state.
3. Require only immutable content writes, declared visibility semantics, and
   one atomic named-root CAS from a production store.
4. Bound memory, result size, scans, source fetches, spill space, retries, and
   elapsed time for every production operation.
5. Preserve canonical, deterministic source and index roots.
6. Retain every extractor generation required by a current or retained
   snapshot.
7. Expose stable, retry-aware, redacted errors in Rust and all portable
   bindings.
8. Measure physical node, byte, retry, spill, and latency work accurately.
9. Make concurrency, crash/reopen, corruption, resource, and performance
   evidence mandatory release gates.
10. Keep the secondary-index subsystem store-neutral and independent of a SQL
    planner.

## Non-Goals

This delivery does not add:

- unique indexes;
- typed composite terms or a new term codec;
- intersection, union, query planning, or statistics-based selection;
- deferred or asynchronously maintained indexes;
- resumable online builds or build catch-up under concurrent writes;
- indexed merge, rollback, or portable query proofs;
- a production-grade `FileNodeStore`;
- an old-format migration reader;
- compatibility aliases for removed APIs or record types.

Synchronous build, replace, repair, and verification operations may retry a
conflicting final activation, but they are not guaranteed to complete under a
continuous write rate.

## Release-Blocking Invariants

1. The collection root is the only mutable visibility reference for a managed
   indexed collection.
2. A visible state references exactly one source tree and exactly one tree and
   descriptor for every active index.
3. All objects referenced by a candidate state are readable under the active
   store profile before its root CAS is attempted.
4. Failed and conflicting CAS operations do not change visible collection
   state.
5. An indexed snapshot ID changes when its source root, any index root, any
   selected descriptor, or canonical snapshot metadata changes.
6. A query cursor cannot widen or change its original logical request.
7. No production API allocates in proportion to an unbounded result set,
   complete source, complete index, or untrusted transfer payload.
8. Every configured limit is enforced before the allocation or amplification
   it is intended to bound.
9. A runtime extractor must match the exact persisted descriptor fingerprint
   used by the selected snapshot.
10. Default errors and telemetry never contain primary keys, terms, source
    values, projections, query bounds, or extractor-provided text.
11. Physical node and byte metrics come from builders, workspaces, and stores;
    they are never inferred from logical map counts.
12. Garbage collection cannot remove content reachable from the current,
    retained, explicitly pinned, or safely leased snapshots.

## Persistence Architecture

### Authoritative Root

Each indexed collection owns one reserved mutable root:

```text
\0prolly/indexed-collection/<hex-source-map-id>/state
```

The value is the root CID of the canonical `IndexedCollectionState` tree. It is
the only mutable head for the source map, its indexes, its descriptor history,
and its retained snapshots.

There are no independently authoritative source, hidden-index, catalog, or
control heads. A component tree may be cached by CID, but a cache or helper
root is never a source of truth.

### Collection State

The state tree uses this canonical logical key grammar:

```text
meta/format                         -> supported format discriminator
meta/source-map-id                  -> exact application source-map ID
meta/collection-policy              -> canonical CollectionIndexPolicy
head                                -> IndexedSnapshotId
snapshots/<snapshot-id>             -> IndexedSnapshot
descriptors/<name>/<fingerprint>    -> IndexDescriptor
active/<name>                       -> descriptor fingerprint
retired/<name>/<fingerprint>        -> RetirementRecord
pins/<pin-id>                       -> SnapshotPin
```

Entries are length-delimited and sorted by encoded key. Canonical decoding
rejects malformed keys, unknown record kinds, duplicate logical entries, an
unsupported format discriminator, a mismatched source-map ID, a head absent
from `snapshots/`, and references to absent or fingerprint-mismatched
descriptors.

`CollectionIndexPolicy` stores semantic collection rules required to interpret
the state: maximum active indexes, retained snapshots, descriptor generations,
and durable pins. These limits bound canonical-state decoding and publication.
Per-operation execution budgets are runtime policy and are not persisted.

The head snapshot must remain in `snapshots/`. A descriptor referenced by the
head, a retained snapshot, or a pin must remain in `descriptors/`. Retirement
removes a descriptor from `active/`; it does not remove descriptor history
still required by a retained snapshot.

### Indexed Snapshot

An `IndexedSnapshot` is a canonical content-addressed record with the
equivalent shape:

```rust
struct IndexedSnapshot {
    source_map_id: Vec<u8>,
    parent: Option<IndexedSnapshotId>,
    source: SourceSnapshotRef,
    indexes: Vec<IndexSnapshotRef>,
}

struct SourceSnapshotRef {
    tree: PersistedTreeRef,
    entry_count: u64,
}

struct IndexSnapshotRef {
    name: Vec<u8>,
    descriptor_fingerprint: Cid,
    tree: PersistedTreeRef,
    entry_count: u64,
}

struct PersistedTreeRef {
    root: Cid,
    format: TreeFormat,
}
```

Index references are sorted by name. Runtime cache settings, prefetch policy,
worker count, timestamps used only for telemetry, and other nondeterministic
data are excluded. `IndexedSnapshotId` is the CID of the canonical snapshot
record.

The parent is history metadata. Read correctness depends only on the exact
tree and descriptor references in the selected snapshot.

### Descriptor and Extractor Identity

`IndexDescriptor` contains the logical name, generation, extractor identity,
projection mode, semantic record-shape limits, and canonical fingerprint.
Runtime extractors are registered by `(name, descriptor_fingerprint)`, not
only by name.

The registry retains all registered generations required by the current
handle. Opening a snapshot that references a descriptor for which no exact
runtime extractor is registered returns `index.extractor_missing`; it never
substitutes another generation.

Changing extractor behavior requires a new extractor identity and descriptor
fingerprint. The engine cannot prove arbitrary callback determinism, but it
fails closed when persisted and runtime identity differ.

## Store Capability Contract

Store support is described by an exact profile rather than
`supports_transactions() -> bool`.

### Production Profile

A production store must provide and pass conformance for:

1. Idempotent content-addressed immutable writes with CID validation.
2. Declared read-after-write or publication-barrier semantics.
3. Atomic compare-and-swap for one named collection root.
4. CAS correctness across independent handles and processes.
5. Durable acknowledgement semantics documented by the adapter.
6. Consistent root reopen after an acknowledged CAS.
7. Enumerability or an adapter-specific mechanism required for safe GC.
8. Lease, pin, or operational-quiescence integration required by its GC
   profile.

Runtime probes can disprove these properties. Claims about physical power-loss
durability also require the underlying database or provider contract.

### Verification Profile

A verification store may exercise tree construction, query semantics, CAS
conflicts, fixtures, and local reopen behavior without claiming production
durability or coordination.

`FileNodeStore` belongs to this profile. Its current local transaction behavior
may remain for verification, but it cannot satisfy or report the production
profile. The secondary-index documentation and capability API state this
directly.

## Publication Protocol

Every mutation, index activation, replacement, repair, retention change, pin
change, and verified import follows the same protocol:

1. Read the collection root once, obtaining `expected_state_root`.
2. Load and validate that immutable `IndexedCollectionState`.
3. Resolve the selected `IndexedSnapshot` and exact runtime descriptors.
4. Derive candidate source, index, descriptor, retention, and snapshot
   changes under the operation budget.
5. Write all candidate immutable nodes and records.
6. Complete the store's required publication barrier or read-back checks.
7. Build and write the candidate collection-state tree.
8. CAS the collection root from `expected_state_root` to
   `candidate_state_root`.
9. Return success only after the CAS acknowledgement.

The successful CAS is the linearization point.

Failure before CAS leaves the old state visible. A conflict leaves the winning
state visible and the losing candidate unreachable. A successful CAS followed
by client disconnect leaves the new state visible; retry is idempotent when
the desired content already matches the observed state.

Only classified transient store failures and CAS conflicts are retried.
Retries always reload the complete state and rederive the candidate. They are
bounded by attempts, elapsed time, cancellation, and backoff.

### Reads

A normal read loads the collection root once and opens the selected immutable
snapshot. It does not reread mutable component heads.

Historical reads select an `IndexedSnapshotId` retained by the loaded state,
then open its exact source, index, and descriptor references. Retention removal
is observed on the next independent state load; an already protected snapshot
remains valid through its pin or lease.

### Managed Source Ownership

A managed source tree has no independently mutable `VersionedMap` head. Public
source writes go through `IndexedMap` and the collection CAS. Snapshot APIs may
expose read-only source trees by immutable reference.

Opening a raw mutable map for a managed source ID fails with
`index.managed_source`. The cutover deletes the old control-root fence and
hidden-map publication mechanism rather than maintaining them as mirrors.

## Bounded Execution

### Budget Types

Execution limits are separated by responsibility:

```text
MutationBudget
  input_records
  input_bytes
  derived_entries
  derived_bytes
  accounted_memory_bytes
  cas_attempts
  elapsed

QueryBudget
  page_entries
  returned_entries
  returned_bytes
  scanned_entries
  source_fetches
  accounted_memory_bytes
  elapsed

MaintenanceBudget
  source_entries
  derived_entries
  verification_findings
  accounted_memory_bytes
  spill_bytes
  spill_runs
  merge_fan_in
  cas_attempts
  elapsed

TransferBudget
  encoded_bytes
  nodes
  decoded_bytes
  verification_work
  accounted_memory_bytes
  elapsed
```

All production defaults are finite and nonzero. Callers may select stricter
values. Values whose arithmetic overflows or cannot be represented by an
adapter fail validation before work begins. The cutover preserves the current
finite semantic descriptor defaults unless a limit moves to the appropriate
operation budget:

| Semantic limit | Default |
|---|---:|
| Encoded term bytes | 4 KiB |
| Projection bytes per emission | 64 KiB |
| Source value bytes for `All` | 1 MiB |
| Emissions per source record | 1,024 |
| Derived projection bytes per source record | 1 MiB |
| Active indexes per collection | 32 |

Operational default values are centralized in one production profile and one
verification profile. A default cannot be unbounded. Benchmarks may override
budgets explicitly but do not alter production defaults.

### Mutation

Mutation input is normalized only after input-record and input-byte admission.
Last-write-wins deduplication remains canonical.

For each source record, the engine:

1. validates source and descriptor semantic limits;
2. extracts and canonicalizes terms within the emissions budget;
3. computes checked amplification before cloning projections;
4. charges `All` projection bytes as
   `canonical_term_count * source_value_bytes`;
5. rejects the record before allocating derived buffers when a limit would be
   exceeded.

The coordinator checks `max_active_indexes` before descriptor activation and
before allocating multi-index mutation state. Transaction-level derived entry,
byte, and memory budgets are charged as candidate deltas are created, not
after a complete delta map exists.

### Query

Production query primitives are bounded pages and streaming visitors. Exact,
prefix, range, primary-key, projection, and source-record queries do not expose
an unbudgeted collector.

Before allocating a result buffer, the engine rejects a requested page size
larger than `QueryBudget.page_entries`. Iteration stops at the first returned,
scanned, fetched, byte, memory, cancellation, or elapsed limit. Source joins
use batches bounded by both entry count and encoded bytes.

A cursor binds:

- collection state and indexed snapshot IDs;
- index name and descriptor fingerprint;
- direction;
- logical query kind and logical bounds;
- physical continuation key.

Cursor validation decodes the continuation key, proves it belongs to the
selected index and logical request, and checks it lies within the computed
physical bounds. Forward resume retains both physical bounds and continues
strictly after the key. Reverse resume retains both bounds and continues
strictly before it. A cursor mismatch returns an error before scanning.

### Build, Replace, Repair, and Verification

These operations never collect the complete derived index in a `BTreeMap`.
They use:

1. a bounded page scan of one pinned source snapshot;
2. memory-bounded sorted runs;
3. an injected `IndexBuildWorkspace` for spill files or store-backed runs;
4. a bounded k-way merge into `SortedBatchBuilder`;
5. root, count, descriptor, and canonical-order validation;
6. collection-root CAS activation.

The workspace accounts for memory, bytes, run count, and merge fan-in.
Temporary runs are namespaced by operation and cleaned after success, failure,
or cancellation. Orphan cleanup after process death is adapter policy and
must be safe to repeat.

An environment without an approved spill workspace may build only an index
that fits its finite memory budget. Crossing memory or spill limits returns
`index.budget_exceeded` and publishes nothing.

Verification streams the source and selected index under a
`MaintenanceBudget`. Its result states whether it completed or stopped at a
budget boundary. A partial verification is never reported as complete.

### Bundles

Bundle import rejects the encoded envelope size before decoding. Streaming
decode charges nodes, encoded bytes, decoded bytes, verification work, and
accounted memory before retaining each item.

Verification checks content IDs, canonical records, exact reachability,
source/index ownership, descriptor fingerprints, and the selected source
state without constructing a duplicate full-size `MemStore`. Publication
occurs only after complete validation through the collection CAS.

Export streams reachable content under `TransferBudget`; it does not first
collect the complete node set into an unbounded vector.

## Errors and Retry Policy

Every secondary-index error exposes:

- a stable code;
- operation category;
- retry advice: `Never`, `RetryFreshState`, or `RetryAfter`;
- safe structured fields;
- an optional bounded causal chain.

Required codes include:

```text
index.format_unsupported
index.managed_source
index.store_capability
index.definition_invalid
index.definition_mismatch
index.extractor_missing
index.extraction_failed
index.budget_exceeded
index.deadline_exceeded
index.cancelled
index.cas_conflict
index.retry_exhausted
index.cursor_mismatch
index.snapshot_not_retained
index.corruption
index.bundle_invalid
index.gc_unsafe
```

Default text, metrics, and traces exclude primary keys, terms, source values,
projections, logical bounds, physical cursor keys, and extractor-provided
messages. Safe context uses counts, limit dimensions, bounded operator-assigned
names, and content hashes. An explicitly sensitive diagnostic API may return
raw application data; portable errors do not.

Bindings preserve code, category, retry advice, and structured safe fields.
They do not collapse secondary-index errors into `Internal`.

## Observability

Operation-local statistics are collected from tree builders, stores, query
iterators, and spill workspaces. Required observations include:

- source, index, state, and spill nodes read, reused, written, and uploaded;
- bytes read, written, decoded, returned, and spilled;
- source records scanned and fetched;
- extracted, canonicalized, inserted, removed, and deduplicated entries;
- source and projection amplification bytes;
- CAS attempts, conflicts, retries, backoff, and latency;
- query pages, scanned entries, returned entries, and early termination;
- build run count, merge fan-in, merge passes, and peak accounted memory;
- verification coverage, findings, and completeness;
- retained and pinned snapshots and GC-reclaimed nodes and bytes;
- terminal error code and total operation latency.

Metrics named `*_nodes_written` report physical nodes written. Logical
publication counts use distinct names. No counter is incremented by assuming
that one logical map change equals one physical node.

Labels use bounded collection/index names or hashes. Application data is never
a metric label.

## Retention, Pins, and Garbage Collection

Retention changes are collection-state CAS operations. The candidate state
must keep:

- the head snapshot;
- snapshots selected by retention policy;
- explicitly pinned snapshots;
- every descriptor and immutable tree referenced by those snapshots.

GC marks from one pinned collection state and traverses its complete content
closure. It does not combine roots observed at different times.

Unpublished objects and objects removed by retention are swept only after the
configured grace interval. Long-lived readers must hold a collection pin or a
store-provided lease. A production GC operation fails with `index.gc_unsafe`
when the adapter cannot prove a safe lease, pin, grace-period, or explicit
quiescence policy.

## Health and Verification

`health()` is a bounded structural check. It loads one collection state and
verifies format, ownership, snapshot closure, descriptors, referenced roots,
counts that can be checked without a full scan, store profile, and GC safety.

`verify_index()` is a bounded logical rederivation against one immutable source
snapshot. It reports:

- descriptor and snapshot identity;
- entries compared;
- source entries scanned;
- mismatch counts by safe category;
- consumed budget;
- `Complete` or `BudgetStopped`.

Queries fail closed on missing or corrupt referenced content. Repair builds a
new candidate from the immutable source snapshot and activates it only by the
collection CAS.

## Verification and Release Gates

### Model and Property Tests

- Incremental maintenance equals a clean rebuild across inserts, updates,
  deletes, sparse output, duplicate terms, all projections, and multiple
  active indexes.
- Canonical roots are invariant across input order, batch shape, retry count,
  and supported worker counts.
- Every retained snapshot matches a reference multimap.
- Cursor pages neither duplicate, skip, nor escape the logical request.
- Replacement through at least three extractor generations preserves exact
  historical reopening.

The fast deterministic seed set runs on every change. An extended scheduled
campaign increases seeds and dataset shapes without changing the oracle.

### Deterministic Concurrency

Tests place barriers around every state load, candidate build, object
publication, read-back, and root-CAS boundary. They cover:

- concurrent source mutations;
- mutation versus index activation, replacement, repair, retention, and pin;
- reader versus writer and reader versus GC;
- CAS success, conflict, retry, exhaustion, and cancellation;
- independent handles and processes where supported.

The existing mixed source/catalog observation failure becomes a regression
test proving that the new architecture can expose only complete old or new
snapshots.

### Failure Injection

Inject returned failure, cancellation, or simulated crash:

- before, during, and after immutable source/index/state writes;
- before and after the publication barrier;
- immediately before and after root CAS;
- after successful CAS but before success reaches the caller;
- during every sorted-run spill and merge pass;
- during bundle decode, verification, and publication;
- during retention, pin, repair, and GC planning.

Reopen must expose the complete old or new state. No accepted outcome contains
a mixed source/index snapshot or a root referencing unavailable content.

### Store Conformance

Every production adapter runs one shared suite proving the production-profile
properties. At least one test uses independent handles and one uses independent
processes. Verification-profile stores run a separately named suite that
cannot produce a production capability result.

### Resource Tests

- Accounted peak memory stays within the operation budget plus documented
  constant adapter overhead as dataset cardinality grows.
- Query allocation is rejected before scan when the requested page exceeds its
  budget.
- Builds spill at deterministic thresholds and fail before crossing memory,
  run-count, merge-fan-in, or spill-byte limits.
- `All` and multi-valued amplification is rejected before projection cloning.
- Bundle decode rejects oversized input before complete materialization.
- Every integer limit receives zero, maximum representable, overflow, exact
  boundary, and one-past-boundary coverage.
- Budget exhaustion, deadline, and cancellation publish nothing.

### Performance and Scale

Benchmarks use repeated samples, isolated fixtures, declared hardware and
backend provenance, and warm/cold separation. Reports include p50, p95, and
p99 latency together with memory, physical node I/O, bytes, spill work, and CAS
conflicts.

Required fixtures cover:

- source-only mutation and one, several, and maximum active indexes;
- insert, update-with-same-terms, update-with-new-terms, and delete;
- exact, prefix, bounded range, projection, and source-record pages;
- cold and warm build, replace, repair, and verification;
- retention and GC;
- conflict-free and contended publication.

Tracked CI runs a bounded smoke profile. Scheduled CI runs declared production
adapters at 1 million and 10 million source records. A performance gate uses
both relative and absolute thresholds, a minimum sample count, and stored
baseline provenance. One-shot measurements and logical node estimates cannot
pass or fail a release.

### Required CI

The tracked required workflow runs:

- formatting;
- `cargo check --all-targets`;
- strict Clippy;
- unit, integration, and documentation tests;
- canonical fixture verification;
- deterministic concurrency and failure matrices;
- production and verification store-conformance suites;
- portable binding error, budget, cursor, and metrics parity;
- sanitizer and bounded fuzz smoke jobs;
- benchmark smoke and regression classification.

Extended fuzz, model, scale, and crash campaigns run on a published schedule
and block release when unresolved.

## Hard-Cutover Delivery Order

The implementation lands as one architectural cutover branch but is reviewed
in dependency order:

1. **Canonical format and store contract**
   - introduce the canonical state/snapshot/descriptor records;
   - introduce production and verification store profiles;
   - add conformance and canonical-format fixtures.
2. **Single-root coordinator**
   - implement state loading, reads, publication, retries, retention, pins, and
     managed-source ownership;
   - delete multi-root source/index/catalog/control publication.
3. **Correct bounded query surface**
   - replace eager production collectors;
   - enforce query budgets and validate cursor continuation keys.
4. **Bounded mutation and maintenance**
   - enforce preallocation amplification accounting;
   - implement spillable build, replace, repair, and verification.
5. **Bounded transfer, GC, and health**
   - stream bundle processing;
   - implement state-rooted retention, safe pins/leases, GC, and structural
     health.
6. **Errors, metrics, and binding parity**
   - replace generic error mapping and logical node counters;
   - propagate typed budgets and measured observations.
7. **Release evidence**
   - add deterministic fault/concurrency/resource gates;
   - replace the secondary-index benchmark harness;
   - add tracked required CI and scheduled scale campaigns.
8. **Cutover cleanup**
   - delete obsolete layouts, types, helpers, tests, fixtures, docs, and
     compatibility terminology;
   - verify repository-wide absence of old root names and multi-root paths.

No intermediate commit from this branch is released as a supported library.
The release occurs only after the final cutover gates pass.

## Acceptance Criteria

The industrial foundation is complete only when all statements below are
proved:

1. Strict publication has exactly one mutable visibility root and one CAS
   linearization point.
2. Concurrent and crash/reopen tests expose a complete old or new indexed
   snapshot, never a mixed state.
3. A production adapter needs no multi-root transaction and passes the exact
   single-root conformance suite.
4. `FileNodeStore` is classified and documented as verification-only.
5. Query memory is bounded by declared work and result budgets regardless of
   total matches.
6. Build, replace, repair, and verification memory is bounded independently of
   source and index size, with bounded spill behavior.
7. Mutation amplification and bundle decoding are rejected before unsafe
   allocation.
8. Tampered cursors cannot change or widen the original request.
9. Historical snapshots reopen the exact registered extractor fingerprint
   across at least three replacements.
10. Every production operation has finite attempts, elapsed time, memory, and
    relevant work limits.
11. Errors are stable, retry-aware, binding-preserved, and redacted by
    default.
12. Metrics report measured physical node and byte work.
13. Retention and GC cannot remove current, retained, pinned, or safely leased
    snapshot content.
14. The deterministic fault, concurrency, resource, conformance, binding, and
    performance release gates pass.
15. The repository contains one canonical secondary-index architecture, no
    suffix-named replacement architecture, and no compatibility reader for the
    removed layout.

## Final Recommendation

Replace the multi-root coordinator rather than incrementally hardening it.
Keep the existing immutable prolly-tree mechanics and strict synchronous index
semantics, but select every complete indexed snapshot through one canonical
collection root.

Deliver atomic visibility, bounded execution, exact error and metric contracts,
and mandatory release evidence before expanding index semantics. This fixes
the production boundary first and leaves one coherent architecture on which
later features can be built through deliberate hard cutovers.
