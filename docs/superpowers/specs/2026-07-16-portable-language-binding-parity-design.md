# Portable Language-Binding Parity Design

Status: approved design

Date: 2026-07-16

Scope: Rust binding facade, Python, Go, Node/TypeScript, Kotlin, Java, Ruby,
Swift, and browser/standalone WASM

## Summary

The language bindings do not currently provide complete parity with the
application-facing Rust API. In particular, the Rust crate exports the
VersionedMap, IndexedMap/secondary-index, and ProximityMap families while the
UniFFI, Go, Node native, and WASM facades expose no indexed-map or proximity-map
API. The current verification matrix describes broad P0/P1 coverage plus smoke
coverage, not complete public-surface parity.

This change is a hard cutover to one portable application API across every
binding. The implementation will use a shared Rust binding-domain facade,
versioned packed transport, retained native sessions, and thin idiomatic
language adapters. Correctness and optimized hot reads are release
requirements for every binding.

Existing binding APIs will not remain as public compatibility aliases.
Persisted node bytes, CIDs, canonical ordering, snapshot formats, proof
formats, and Rust engine semantics remain unchanged.

## Goals

1. Bind every portable application-facing Rust API to Python, Go,
   Node/TypeScript, Kotlin, Java, Ruby, Swift, and WASM.
2. Cover versioned maps, indexed maps, proximity maps, sessions, proofs,
   maintenance, transactions, blobs, roots, snapshots, sync, GC, metrics, and
   native async forms.
3. Give Rust-only generic, trait, iterator, and lifetime abstractions idiomatic
   equivalents without weakening their behavior.
4. Preserve the retained-session, packed-node, borrowed traversal, and
   bounded-page performance work already implemented in Rust.
5. Keep index filtering and source joins inside Rust.
6. Keep proximity candidate traversal, filtering, approximate search, exact
   reranking, proof construction, and deterministic ordering inside Rust.
7. Make binding drift mechanically detectable in CI.
8. Ship one coordinated hard-cutover release only after all language,
   lifecycle, conformance, and performance gates pass.

## Non-Goals

1. Reimplementing the prolly engine in each host language.
2. Preserving source compatibility with the current binding packages.
3. Claiming zero-copy for host-owned return values.
4. Changing persisted formats, content identifiers, canonical tree shape, or
   merge semantics.
5. Exposing crate-private implementation details.
6. Giving browser WASM filesystem, SQLite, or OS-thread capabilities that the
   platform cannot provide.
7. Moving candidate vectors or unfiltered index source rows into host code for
   processing.

## Definition of Portable Parity

The public Rust crate is the source inventory. Every public re-export and every
public operation reachable from those exports must appear in a checked-in
parity manifest with one classification:

- portable: exposed in every binding;
- idiomatic: exposed through a behaviorally equivalent host abstraction;
- platform-excluded: impossible on a named platform and accompanied by a
  tested replacement or explicit unsupported error;
- Rust-language-only: compile-time machinery with no runtime meaning, mapped
  to a documented host-language pattern.

An item may not disappear merely because it is generic, lifetimed, callback
based, feature gated, or inconvenient to generate. Its manifest entry must
describe the equivalent API and its conformance test.

The parity manifest records:

- Rust symbol and stable operation identifier;
- semantic family and owning object;
- input, output, error, and ordering contract;
- synchronous and asynchronous availability;
- owned, paged, or scoped-view performance tier;
- host-language symbol for all eight bindings;
- platform exclusions and replacement behavior;
- conformance, lifetime, and performance test identifiers;
- documentation and cookbook coverage.

A pinned public-API inventory job compares the Rust export inventory with the
manifest. A new portable Rust API without a mapping fails CI. A mapped
operation without a binding test also fails CI.

## Platform Boundary

Python, Go, Node/TypeScript, Kotlin, Java, Ruby, and Swift receive the complete
native portable surface.

WASM receives the same logical map, index, proximity, proof, snapshot, sync,
GC, and async behavior using browser-safe storage. It excludes:

- filesystem node and blob stores;
- SQLite stores;
- APIs whose only meaning is blocking an OS thread;
- guarantees of native thread parallelism.

WASM exposes browser storage protocols and asynchronous equivalents. Every
exclusion appears in the parity manifest and has a test asserting the intended
unsupported or replacement behavior.

## Architecture

The design has four layers.

### 1. Rust engine

The existing prolly-map crate remains authoritative for data structures,
storage formats, validation, deterministic ordering, proofs, transactions,
indexes, and proximity search.

### 2. Shared binding-domain facade

A Rust facade translates generic engine APIs into portable domain objects,
stable records, callback contracts, and opaque resource handles. All language
adapters call this same semantic layer so error mapping and behavior do not
drift.

The facade owns:

- store and engine dispatch;
- stable error codes and structured details;
- portable config and result records;
- codec, resolver, extractor, policy, and store callback bridges;
- versioned handle lifecycle;
- packed request and result codecs;
- cancellation and progress checkpoints;
- transport metrics.

### 3. Versioned packed transport

A narrow native transport carries hot-path operations. It is additive to the
Rust engine but is the required implementation underneath the new binding API.
The transport has an explicit ABI version, record-kind tags, little-endian
fixed headers, checked offsets and lengths, and deterministic validation.

It supports:

- retained root-bound read sessions;
- write sessions and bounded batch writers;
- point and ordered multi-get;
- forward, reverse, prefix, diff, conflict, and change cursors;
- index match and joined-record cursors;
- packed proximity neighbor pages;
- packed mutation arenas;
- value and page leases;
- transport metrics and leak counters.

### 4. Idiomatic language adapters

Each package owns naming, native iteration, typed convenience wrappers, async
integration, cancellation integration, and package documentation. It does not
reimplement engine behavior.

## Public Object Model

Every binding exposes the following conceptual families with idiomatic casing
and naming.

### Engine and stores

- memory, file, SQLite, callback-backed, and platform-appropriate stores;
- engine configuration and runtime configuration;
- named roots and compare-and-swap;
- transactions and conflict results;
- metrics, cache control, debug views, and hints;
- explicit close or deterministic disposal where the host requires it.

### Versioned maps

- map creation and lookup by identifier;
- initialization, head, head ID, version lookup, and history;
- point read, contains, ordered multi-get, range, prefix, pages, and cursors;
- immutable snapshots and historical snapshots;
- apply, put, delete, conditional apply, editors, and batch results;
- compare, diff, changes-since, prepared merge, and merge results;
- subscriptions and polling;
- backup, restore, import-as-head, pruning, and retention;
- typed key/value wrappers implemented through host codecs;
- sync and async forms where the Rust semantics support them.

### Indexed maps and secondary indexes

- index definitions, descriptors, limits, projections, and direction;
- host extractor and streaming extractor protocols;
- registry construction and validation;
- indexed map creation, head, health, metrics, and immutable versions;
- editors and coordinated source/index mutations;
- build, rebuild, verify, repair, replacement, retirement, and retention;
- current and historical indexed snapshots;
- exact, prefix, and range term queries;
- forward/reverse cursors and bounded pages;
- projected results and native source-record joins;
- snapshot bundles, summaries, verification, import, and export;
- index control, checkpoints, catalog inspection, and health errors.

### Proximity maps

- dimensions, distance metric, hierarchy, overflow, vector storage, and
  quantization configuration;
- bulk build, mutations, mutation statistics, and immutable proximity trees;
- retained proximity read sessions;
- exact and approximate search requests, filters, budgets, policies, kernels,
  backends, completion states, plans, and statistics;
- deterministic top-K neighbors with optional payload delivery;
- HNSW, product-quantization, composite, and catalog accelerator management;
- accelerator build, rebuild, limits, statistics, and selection;
- membership, structural, and search proofs and their verification;
- synchronous and asynchronous search, cancellation, deadlines, and runtime
  policies.

### Sessions and streaming

- root-bound read sessions;
- write sessions, savepoints, rollback, and finalization;
- point, multi-get, range, prefix, reverse, diff, and conflict operations;
- owned iterators and explicitly closable paged iterators;
- callback-scoped views with early termination;
- index-match and neighbor-page iterators;
- per-session cache and transport metrics.

### Proof and maintenance services

- key, multi-key, range, range-page, diff-page, index, proximity membership,
  proximity structural, and proximity search proofs;
- proof inspection, bundle verification, HMAC authentication, and envelopes;
- snapshot export/import, summaries, digests, and verification;
- missing-node planning and copy sync;
- node and blob reachability, GC planning, and sweeping;
- content-graph roots, walk, copy, publication, and GC;
- blob stores, large values, value references, and blob GC;
- tombstone and CRDT helpers;
- portable encoding, key, cursor, manifest, and wire-format helpers already
  present in the binding surface.

## Idiomatic Equivalents for Rust Abstractions

Rust traits become host protocols, interfaces, or callback objects. This
includes stores, blob stores, resolvers, CRDT resolvers, merge policies,
secondary-index extractors, streaming extractors, and codecs.

Generic typed maps become host-side typed wrappers around a byte-oriented
native map and registered key/value codecs. Kotlin, Java, Swift, Go, and
TypeScript use generic wrappers where their type systems support them. Python
and Ruby use codec objects with runtime validation.

Rust iterators become native iterables, sequences, streams, or enumerators
backed by retained bounded cursors.

Borrowed Rust references become callback-scoped views. The owned API remains
the default when a caller needs to retain data.

Compile-time marker types or lifetime parameters with no runtime behavior are
documented through their host ownership rule rather than represented as fake
runtime objects.

## Retained Handles and Lifecycle

The facade registry owns engines, stores, immutable sessions, mutable cursors,
pages, values, indexed snapshots, proximity sessions, accelerators, editors,
and batch writers.

Every handle contains a type tag, slot, and generation. Operations reject the
wrong type, a stale generation, or a closed resource. Close is idempotent.

Immutable snapshots and read sessions are shareable when the backing store
permits concurrent reads. Mutable cursors, pages, editors, and batch writers
have single-owner advancement. Closing a parent prevents new child work and
coordinates with active operations before releasing storage.

Finalizers are a leak safety net only. Public docs and examples use explicit
close, context-manager, defer, use, try-with-resources, or equivalent scoped
ownership.

## Packed Page Contract

One envelope format carries multiple record kinds:

- entries;
- optional multi-get values;
- diffs;
- conflicts;
- index matches;
- joined index records;
- proximity neighbors;
- progress or statistics records where streaming is useful.

The envelope extends the existing PRPG layout: four-byte magic, u16 format
version, u16 record kind, u32 flags, u32 record count, u32 fixed-table byte
length, u64 arena byte length, then the fixed record table and variable arena.
All integers are little endian. Decoders validate the magic, exact supported
version, known kind and flags, per-kind record width, configured record and
arena limits, exact total length, checked offset arithmetic, and every
offset/length pair before exposing a view. Variable bytes live in one aligned
arena. Floating-point values use little-endian IEEE-754 bits and non-finite
inputs are rejected where the Rust API rejects them.

Owned adapters validate the page then copy directly into final host objects.
Scoped-view adapters validate once and expose slices into the lease without
allocating a key, value, match, or neighbor object per record.

No page or view changes persisted bytes or CIDs.

## Read Data Flow

### Point and multi-get

1. Resolve a live immutable session.
2. Validate key input during the native call.
3. Traverse retained packed nodes and session-local routing state.
4. Return a scoped value lease or copy once into the final owned host buffer.
5. Preserve input ordering and missing-value distinctions for multi-get.

The tree and root are not decoded again for each operation.

### Range, diff, and conflict

1. Open a native cursor bound to compatible retained sessions.
2. Advance by a bounded page.
3. Return a packed page or invoke a scoped host callback.
4. Preserve cursor ordering, resume semantics, early stop, and typed errors.
5. Close the cursor on exhaustion, error, cancellation, or explicit disposal.

### Indexed queries

1. Validate source version, index version, descriptor fingerprint, and health.
2. Convert exact, prefix, or range terms into physical index bounds in Rust.
3. Scan the retained index cursor.
4. Apply projection and filtering in Rust.
5. Batch source-record joins in Rust when requested.
6. Emit only final ordered matches or joined records as packed pages.

Unfiltered source rows do not cross the boundary.

### Proximity search

1. Borrow the query vector for a synchronous call or copy it once for async
   execution.
2. Validate dimensions, finiteness, filters, policy, budget, and backend.
3. Run candidate lookup, filtering, accelerator search, and exact reranking in
   Rust.
4. Apply deterministic distance and key tie-breaking.
5. Build requested proofs in Rust.
6. Emit only top-K neighbor records and payloads explicitly requested for
   those neighbors.

Candidate vectors never cross the boundary for host-side scoring.

## Write Data Flow

Small synchronous writes use a packed mutation arena borrowed only for the
duration of the call. Rust copies only when sorting, deduplication, ownership,
or deferred work requires it.

Async writes own their input before suspension.

Large writes use a bounded native batch writer:

1. accept validated packed chunks;
2. spool mutations in Rust ownership;
3. preserve global last-write-wins behavior;
4. build one logical canonical result;
5. return the same root, CID, and statistics as the Rust batch API;
6. release each host chunk after ingestion.

Applying chunks as unrelated tree mutations is not an acceptable substitute
unless equivalence is proven for canonical roots and statistics.

## Language Adapters

### Go

Go uses cgo over the packed ABI, byte slices for owned values, callback-scoped
views, context-aware methods, explicit Close, and runtime keep-alive rules. No
Go pointer is retained by Rust.

### Python

Python uses a native extension with bytes for owned values and memoryview-like
scoped views. Context managers own sessions and pages. Awaitables copy or move
inputs before scheduling and translate task cancellation.

### Kotlin and Java

The JVM packages share JNI transport and direct byte buffers. Kotlin exposes
coroutines, sequences, and use blocks. Java exposes CompletableFuture for
async operations, Iterable for synchronous cursor traversal, explicit page
objects for paged traversal, AutoCloseable resources, and try-with-resources
examples.

### Node and TypeScript

Node uses Node-API, Buffer/Uint8Array owned values, external buffers for
leased pages, async iterables, promises, and AbortSignal cancellation.

### Swift

Swift uses C interop, Data for owned values, scoped UnsafeRawBufferPointer
views, Sequence/AsyncSequence adapters, async functions, and deterministic
resource wrappers.

### Ruby

Ruby uses a native extension, binary String for owned values, scoped binary
views, Enumerable/Enumerator adapters, explicit close blocks, and futures for
async operations.

### WASM

WASM uses wasm-bindgen domain objects, Uint8Array owned values, guarded
typed-array page views, async iterables, and AbortSignal cancellation. A view
cannot survive memory growth, callback return, or an await.

## Zero-Copy Terminology

Zero-copy is used only for callback-scoped or explicitly leased views that
reference native page memory without allocating per-record host buffers.

Owned return values perform one required boundary copy. Async input performs
one ownership copy when it must outlive the call. Documentation and benchmark
reports distinguish:

- retained traversal;
- packed bounded transfer;
- scoped zero-copy view;
- owned one-copy result.

## Errors

The facade defines stable error codes with structured details for:

- invalid configuration or input;
- malformed persisted or packed data;
- missing nodes, blobs, roots, maps, versions, indexes, or accelerators;
- named-root, map-version, and transaction conflicts;
- stale or unhealthy index snapshots;
- index extraction, verification, and repair failures;
- proximity dimension, vector, filter, backend, and budget failures;
- proof and bundle verification failures;
- cancellation and deadline expiration;
- invalid, stale, wrong-type, or closed handles;
- unsupported platform capabilities;
- internal panics caught at the native boundary.

Each adapter maps these into idiomatic errors or exceptions while preserving
the stable code and structured context. Host callback failures return through
the same model and retain the original host error as the cause where supported.

## Reentrancy and Locking

No engine, store cache, registry, session, page, or metrics lock is held while
calling a host callback.

Reentry through another session is allowed. Recursively advancing the same
cursor is rejected with a stable reentrancy error.

Panic boundaries wrap every foreign entry point. A panic never unwinds into a
foreign runtime.

## Async and Cancellation

Any operation that can outlive the call owns all input and native resources.
Callback-scoped views never cross an await.

Cancellation is checked between bounded:

- cursor pages;
- mutation chunks;
- index build and repair stages;
- accelerator build stages;
- proximity budget units;
- sync and GC batches.

Go contexts, Python task cancellation, Kotlin coroutine cancellation, Java
future cancellation, JavaScript AbortSignal, Swift task cancellation, and Ruby
future cancellation all release temporary cursors, pages, and leases before
returning.

## Compatibility and Cutover

This is a major binding API version. Current public binding types and methods
are removed or replaced rather than retained as aliases.

The implementation may keep old facades temporarily as internal conformance
oracles. They are not exported by the released packages.

The release includes:

- migration guides with old-to-new operation mappings;
- regenerated packages and type declarations;
- new examples and cookbooks;
- explicit lifecycle and ownership guidance;
- performance-tier documentation;
- coordinated package version changes.

Persisted data remains readable without migration.

## Testing Strategy

### Test-first implementation

Each portable operation begins with a failing Rust facade or binding
conformance test. The implementation is added only after the failure is
observed for the intended missing behavior.

### Shared conformance fixtures

Rust generates fixtures for:

- version history, conditional updates, subscriptions, backups, and pruning;
- index build, query direction, exact/prefix/range bounds, joined records,
  historical snapshots, stale health, repair, retention, and bundles;
- proximity metrics, filters, policies, budgets, exact/approximate completion,
  accelerator selection, deterministic ties, and proofs;
- stable errors and malformed inputs.

Every language consumes the same fixtures and validates values, ordering,
versions, CIDs, roots, proofs, bundle bytes, and stable error codes.

### Differential transport tests

Owned, paged, scoped-view, synchronous, and asynchronous forms are compared
against the direct Rust result. The test set covers empty values, missing
values, large values, Unicode-independent binary keys, page boundaries, early
stop, reverse scans, cursor resume, and cancellation.

### Lifetime and concurrency tests

Tests cover:

- use after close;
- stale generations;
- wrong handle types;
- double close;
- parent close with active work;
- callback retention attempts;
- callback reentry;
- concurrent immutable reads;
- mutable cursor single-owner enforcement;
- cancellation cleanup;
- balanced handle and lease counts.

### Fuzzing and platform tests

Rust and adapters fuzz packed headers, record counts, offsets, lengths, record
kinds, checksums, float encodings, and handle generations.

WASM tests memory growth, view invalidation, browser storage behavior, and
async lifetime rules. JVM tests direct-buffer lifetime and alignment. Go runs
race-enabled close/read tests. Native runtimes run leak and stress suites.

## Performance Gates

Structural gates:

1. Session reads do not deserialize the tree or root per operation.
2. Scoped scans allocate no host key/value, index-match, or neighbor buffers
   per row.
3. Index filtering and joins execute inside Rust.
4. Proximity candidates and vectors remain in Rust through exact reranking.
5. Memory is bounded by retained session state, page size, mutation chunk
   size, and requested top-K rather than total result or candidate count.
6. Transport counters prove bounded crossings, copies, leases, and closures.

Measured gates:

- Existing optimized point, multi-get, range, diff, and conflict workloads may
  not regress by more than 10 percent in median latency or peak RSS on the
  documented benchmark host without explicit approval.
- New index benchmarks vary selectivity, projection, source-join ratio, page
  size, result count, and historical/current snapshots.
- New proximity benchmarks vary dimensions, metric, filter ratio, candidate
  count, accelerator, query policy, proof request, payload delivery, and top-K.
- Every benchmark records direct Rust, binding-owned, and binding-scoped-view
  layers where applicable.
- Raw results, machine description, binary hashes, and failures remain in the
  repository report.

## Delivery Slices

The change is one hard-cutover release implemented through independently
testable internal slices:

1. parity manifest, stable error model, lifecycle contract, and packed format;
2. shared Rust binding-domain facade and handle registry;
3. versioned map, session, proof, transaction, snapshot, sync, GC, blob, and
   maintenance parity in every binding;
4. indexed-map parity and native join transport in every binding;
5. proximity-map parity and packed neighbor transport in every binding;
6. native async/cancellation adapters and browser-safe WASM variants;
7. complete conformance, fuzz, lifetime, race, leak, and performance gates;
8. legacy public-surface removal, package regeneration, docs, migration
   guidance, and coordinated release.

No partial slice is published as the new public API.

## Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Rust API grows faster than bindings | Public inventory and parity manifest fail CI on unmapped symbols |
| Generated bindings drift | Shared facade semantics plus per-language generated matrix |
| View escapes its lifetime | Scoped APIs, generation checks, poisoning, and lifetime tests |
| Handle leaks or double frees | Explicit close, idempotent registry close, counters, and stress tests |
| Host callback deadlock | No native locks held while callbacks run |
| Async borrow escapes | All scheduled work owns input; no scoped views across await |
| Packed decoder vulnerability | Checked arithmetic, version tags, fuzzing, and adapter validation |
| Index/source inconsistency | Native version/fingerprint/health validation before result delivery |
| Proximity performance lost at boundary | Candidate processing and reranking remain in Rust |
| False zero-copy claims | Separate retained, packed, scoped-view, and owned terminology |
| WASM capability mismatch | Explicit manifest exclusions and browser-safe replacements |
| Hard cutover surprises users | Major versions, complete migration mapping, and updated cookbooks |
| Scope produces a partial release | One coordinated release gate across every binding |

## Acceptance Criteria

1. Every public Rust export and reachable public operation is classified in
   the parity manifest.
2. Every portable operation has a host mapping and conformance test in all
   eight bindings.
3. Every Rust-language-only abstraction has a documented idiomatic equivalent.
4. WASM exclusions are explicit, tested, and limited to genuine platform
   constraints.
5. VersionedMap, IndexedMap, and ProximityMap application workflows are
   complete in every binding.
6. Proof, snapshot, sync, GC, blob, transaction, metrics, and maintenance
   workflows are complete in every binding.
7. Sync, async, owned, paged, and scoped-view results match direct Rust
   semantics where each form applies.
8. Index queries perform filtering and source joins inside Rust.
9. Proximity queries perform candidate processing and exact reranking inside
   Rust and return only final top-K records.
10. Retained sessions, packed pages, and mutation arenas satisfy lifecycle and
    bounded-memory tests.
11. No foreign pointer or scoped view outlives its permitted call, callback,
    page, or await boundary.
12. Stable error codes and structured context survive every language adapter.
13. Persisted bytes, CIDs, roots, ordering, proof bytes, and bundle bytes are
    unchanged.
14. All language builds, conformance suites, race/leak/lifetime tests, and
    performance gates pass.
15. Existing public binding surfaces are absent from the coordinated major
    release and migration documentation is complete.

## Decision

Use one shared Rust binding-domain facade and a narrow versioned packed
transport, then expose thin idiomatic adapters for all eight languages. Make
correctness parity and optimized hot reads joint release requirements. Perform
a hard public-API cutover while preserving all persisted and canonical Rust
semantics.
