# Secondary Index Industrial Foundation Hard-Cutover Implementation Plan

> **Execution style:** Implementation-first with context. Implement the production
> boundary in each task, then add focused verification and commit the completed
> tranche. Do not use a TDD red/green loop.

**Goal:** Replace the secondary-index multi-root coordinator with one canonical
collection-state root, bounded operations, exact store capabilities, measured
observability, and release-blocking production evidence.

**Architecture:** Source and index trees remain immutable content-addressed
prolly trees. One `IndexedCollectionState` tree contains the current and
retained snapshot records, descriptors, active selections, and pins; one named
root CAS is the only visibility transition. Queries, mutations, maintenance,
transfer, and GC operate from one pinned state and consume typed finite
budgets.

**Tech stack:** Rust 2021, Rust 1.81, existing `Prolly`, `Store`,
`ManifestStore`, `ManifestStoreScan`, `serde`, `serde_cbor`, and portable
UniFFI bindings. Add no dependency unless an existing primitive cannot meet a
release-blocking invariant.

**Design:** `docs/superpowers/specs/2026-07-28-secondary-index-industrial-foundation-hard-cutover-design.md`

## Global Constraints

- This is a hard cutover. Do not add old-format readers, dual publication,
  migration shims, compatibility aliases, or suffix-named replacement APIs.
- Keep canonical public names versionless.
- Do not release an intermediate commit from this branch.
- Preserve the physical secondary-index entry grammar `(term, primary_key)` and
  the existing `KeysOnly`, `Include`, and `All` semantics.
- Every production operation has finite memory, work, retry, and elapsed
  limits.
- Expose one indexed-map constructor. Store configuration and deployment
  qualification belong to the application, not to a profile taxonomy.
- Default errors, metrics, and traces contain no indexed application data.
- Physical work metrics must originate at the builder, workspace, iterator, or
  store boundary.
- Preserve unrelated working-tree changes, especially changes under
  `stores/prolly-store-mysql/` and unrelated plan files.
- Use `apply_patch` for edits and stage only files belonging to the current
  task.

## Implementation Context

The current implementation is concentrated in five files:

- `src/prolly/secondary_index/storage.rs` persists the multi-root catalog,
  control, checkpoint, descriptor, and physical index records.
- `src/prolly/secondary_index/coordinator.rs` opens independent heads, validates
  them, derives mutations, publishes multi-root transactions, and implements
  lifecycle operations.
- `src/prolly/secondary_index/snapshot.rs` opens historical component roots and
  implements query, page, cursor, projection, and source-join behavior.
- `src/prolly/secondary_index/bundle.rs` materializes transfer bundles and
  verifies/imports them.
- `src/prolly/secondary_index/definition.rs` owns runtime extractors,
  descriptors' semantic limits, and generation history.

The implementation should split by stable responsibility instead of expanding
the 2,000-line coordinator:

```text
src/prolly/secondary_index/
  budget.rs        typed budgets and accounting
  bundle.rs        bounded streaming transfer
  coordinator.rs   public IndexedMap facade and retry orchestration
  definition.rs    runtime extractors and semantic record limits
  lifecycle.rs     build, replace, repair, verify, retention, pins, health
  metrics.rs       operation-local measured observations
  publication.rs   one-root load, barrier, and CAS protocol
  query.rs         bounded scans, pages, cursors, and source joins
  snapshot.rs      immutable snapshot handles
  state.rs         canonical persisted state/snapshot/descriptor records
  workspace.rs     bounded sorted runs and spill abstraction
```

Keep a file combined when its implementation remains small; the responsibility
boundaries and exported interfaces are mandatory, not the exact line count.

---

### Task 1: Canonical persisted state and hard-cutover fixtures

**Context:** All later work depends on one deterministic representation of the
collection state. Build this representation before changing publication.

**Files:**

- Create: `src/prolly/secondary_index/state.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Modify: `src/lib.rs`
- Modify: `tests/conformance_fixtures.rs`
- Modify: `conformance/prolly-fixtures.v1.json`
- Modify: `conformance/README.md`
- Remove during this task: obsolete secondary-index fixture entries that
  encode catalog, control, checkpoint, or component-head state

**Interfaces produced:**

- `indexed_collection_root_name(source_map_id: &[u8]) -> Vec<u8>`
- `IndexedCollectionState`
- `IndexedSnapshotRecord`
- `IndexedSnapshotId`
- `SourceSnapshotRef`
- `IndexSnapshotRef`
- `IndexDescriptor`
- `CollectionIndexPolicy`
- `SnapshotPin`
- Canonical `to_bytes`, `from_bytes`, state-tree encode/decode, and
  `validate_closure` operations

**Implementation:**

- [ ] Define the canonical root grammar
  `\0prolly/indexed-collection/<hex-source-map-id>/state`; reject empty and
  malformed source IDs consistently with existing map-ID rules.
- [ ] Move descriptor persistence from `SecondaryIndexDescriptor` into
  `IndexDescriptor`; retain name, generation, extractor identity, projection,
  semantic limits, and content fingerprint.
- [ ] Implement canonical state keys for format, source ID, collection policy,
  head, snapshots, descriptors, active selections, retired descriptors, and
  pins exactly as specified.
- [ ] Encode snapshot index references sorted by name and exclude runtime
  tuning and timestamps from content identity.
- [ ] Make decoding fail closed on unsupported format, malformed keys,
  duplicate logical records, absent head, wrong source ID, missing
  descriptors, descriptor-fingerprint mismatch, and unsorted index references.
- [ ] Replace crate-root exports for old catalog/control/checkpoint persisted
  records with the canonical state records.
- [ ] Replace secondary-index conformance fixtures with canonical state,
  snapshot, descriptor, root-name, and physical index vectors.

**Verification:**

- [ ] Add round-trip, trailing-byte, wrong-format, duplicate, missing-reference,
  fingerprint-mismatch, and canonical-order cases in
  `tests/conformance_fixtures.rs` and state module tests.
- [ ] Run `cargo test --test conformance_fixtures`.
- [ ] Run `cargo test secondary_index::state`.
- [ ] Run `cargo check --all-targets`.

**Commit:** `refactor(index): define canonical collection state`

---

### Task 2: One indexed-store contract and shared conformance

**Context:** `supports_transactions() -> bool` is not the indexed-map
contract. Secondary indexes need immutable writes, one root CAS, visibility
confirmation, and GC safety; they do not need a multi-root transaction or a
second production-only constructor.

**Files:**

- Create: `src/prolly/secondary_index/publication.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Modify: `src/prolly/store/memory.rs`
- Modify: `src/prolly/store/file.rs`
- Modify: `stores/prolly-store-test/src/lib.rs`
- Modify as supported:
  `stores/prolly-store-{pglite,redb,rocksdb,slatedb,sqlite}/src/lib.rs`
- Modify: `tests/store_conformance.rs`
- Modify: `tests/file_node_store.rs`

**Interfaces produced:**

- `IndexedStore: Store + ManifestStore`
- `IndexedStore::confirm_indexed_publication`
- One shared `assert_indexed_store` conformance entry point in
  `prolly-store-test`

**Implementation:**

- [ ] Add the indexed publication contract without changing the general
  `TransactionalStore` contract used by unrelated multi-map APIs.
- [ ] Implement the same indexed-store contract for every supported
  synchronous adapter.
- [ ] Add the publication confirmation hook used after immutable writes and
  before root CAS.
- [ ] Audit each synchronous adapter for native CAS, visibility, reopen, and
  durability semantics without encoding deployment policy in the constructor.
- [ ] Keep durability and topology choices explicit in adapter configuration
  and deployment documentation.

**Verification:**

- [ ] Exercise CAS success/conflict across separate handles where an adapter
  supports multiple handles.
- [ ] Add an independent-process CAS case for at least SQLite or redb.
- [ ] Run `cargo test --test store_conformance --test file_node_store`.
- [ ] Run the conformance suites in each synchronous indexed-store adapter.

**Commit:** `feat(index): require one indexed store publication contract`

---

### Task 3: One-root state loading and publication engine

**Context:** This task removes the mixed-root read problem and establishes the
only legal visibility transition. Keep mutation semantics out until the
publication engine itself is complete.

**Files:**

- Modify: `src/prolly/secondary_index/publication.rs`
- Rewrite relevant sections: `src/prolly/secondary_index/coordinator.rs`
- Modify: `src/prolly/secondary_index/snapshot.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Modify: `tests/secondary_index.rs`
- Modify: `tests/node_publication.rs`

**Interfaces produced:**

- `LoadedIndexedState<S>` containing the observed manifest, state tree, decoded
  state, head record, and immutable source/index handles
- `IndexedPublication<S>` containing expected and candidate state roots plus
  measured publication statistics
- `IndexedMap::open_verification`
- `IndexedMap::open_production`
- Internal `load_indexed_state`, `prepare_publication`,
  `confirm_candidate_objects`, and `compare_and_swap_collection`

**Implementation:**

- [ ] Make open load exactly one collection manifest and decode one immutable
  state closure; remove sequential source/control/catalog head selection.
- [ ] Implement empty collection creation as immutable state construction plus
  `expected=None` root CAS.
- [ ] Implement the common publication sequence: derive candidate objects,
  publish nodes, confirm visibility, write candidate state, CAS the single
  root, and classify the result.
- [ ] Make CAS retries reload the entire state, rederive the candidate, honor
  cancellation/deadline/backoff, and return structured exhaustion.
- [ ] Open public snapshots exclusively from one `LoadedIndexedState`.
- [ ] Reject known obsolete managed-index markers with
  `index.format_unsupported`; do not decode them.
- [ ] Route managed-source mutation exclusively through `IndexedMap`; remove
  reliance on the old control-root write fence.

**Verification:**

- [ ] Replace the current two-writer test with barrier-controlled state-load,
  node-publication, and CAS interleavings.
- [ ] Verify that a reader at every barrier observes exactly the old or new
  snapshot.
- [ ] Inject conflict and failure before/after confirmation and CAS; assert
  that only one complete state is visible.
- [ ] Run the concurrency test repeatedly with a fixed scheduler/interleaving,
  not probabilistic shell repetition.
- [ ] Run `cargo test --test secondary_index --test node_publication`.

**Commit:** `refactor(index): publish through one collection root`

---

### Task 4: Incremental mutation on canonical snapshots

**Context:** Preserve the good incremental delta mechanics while changing
their authority and allocation discipline.

**Files:**

- Create: `src/prolly/secondary_index/budget.rs`
- Modify: `src/prolly/secondary_index/definition.rs`
- Modify: `src/prolly/secondary_index/coordinator.rs`
- Modify: `src/prolly/secondary_index/state.rs`
- Modify: `tests/secondary_index.rs`
- Modify: `tests/node_publication.rs`

**Interfaces produced:**

- `MutationBudget`
- `BudgetCounter` with checked `charge_*` methods
- Canonical `SecondaryIndexRegistry` lookup by
  `(name, descriptor_fingerprint)`
- `IndexedMapEditor` and `IndexedMapUpdate` using collection-state CAS

**Implementation:**

- [ ] Separate semantic per-record descriptor limits from operational mutation
  limits; remove inert `max_indexes` and build fields from descriptor limits.
- [ ] Admit input record and byte counts before last-write-wins normalization.
- [ ] Batch-read old source values from the pinned source tree, derive old/new
  emissions, skip unchanged emissions, and apply one sorted delta per changed
  index.
- [ ] Charge term, projection, derived-entry, total-byte, and accounted-memory
  limits before retaining each derived item.
- [ ] Charge `All` amplification as checked
  `canonical_term_count * source_value_bytes` before cloning source values.
- [ ] Enforce collection `max_active_indexes` before allocating per-index
  mutation state.
- [ ] Create a new `IndexedSnapshotRecord` and candidate state, then activate
  both through the Task 3 publication engine.
- [ ] Store runtime definitions by exact descriptor fingerprint and retain all
  generations required by the handle.

**Verification:**

- [ ] Adapt the 100-seed incremental-versus-rebuild oracle to canonical states.
- [ ] Cover exact-limit and one-past-limit input, fanout, projection,
  transaction, memory, retry, and elapsed boundaries.
- [ ] Verify extractor failure, cancellation, budget exhaustion, and CAS
  exhaustion publish no visible state.
- [ ] Add a three-generation replacement/history registry case.
- [ ] Run `cargo test --test secondary_index`.

**Commit:** `feat(index): maintain canonical snapshots within budgets`

---

### Task 5: Bounded query API and tamper-proof cursors

**Context:** The current page machinery is reusable, but eager collectors and
unvalidated continuation keys are not production-safe.

**Files:**

- Create: `src/prolly/secondary_index/query.rs`
- Modify: `src/prolly/secondary_index/snapshot.rs`
- Modify: `src/prolly/secondary_index/budget.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Modify: `src/lib.rs`
- Modify: `tests/secondary_index.rs`
- Modify: `bindings/uniffi/src/domain/indexed.rs`
- Modify: `bindings/api/secondary-index-snapshot-reconciliation.json`

**Interfaces produced:**

- `QueryBudget`
- `SecondaryIndexQuery`
- Bounded exact, prefix, range, projected, primary-key, and source-record page
  methods
- Visitor methods returning consumed budget and completion state
- Cursor envelope bound to collection state, snapshot, descriptor, query kind,
  direction, logical bounds, and physical continuation key

**Implementation:**

- [ ] Move physical-bound computation, scan accounting, source joins, cursor
  validation, and page construction into `query.rs`.
- [ ] Remove unbudgeted eager collectors from the Rust and binding production
  surfaces.
- [ ] Reject zero/invalid budgets and page requests larger than
  `QueryBudget.page_entries` before allocation.
- [ ] Account returned entries/bytes, scanned entries, source fetches, memory,
  cancellation, and elapsed time during iteration.
- [ ] Keep source joins ordered and batch them by both entry count and bytes.
- [ ] Decode a cursor continuation key and prove it matches the index and
  logical request and lies inside the physical bounds.
- [ ] Resume forward scans strictly after the key while retaining both bounds;
  resume reverse scans strictly before it while retaining both bounds.

**Verification:**

- [ ] Cover exact, prefix, bounded/unbounded range, both directions, zero
  results, all projection modes, and multi-page source joins.
- [ ] Mutate every cursor identity field and use physical keys before/after the
  permitted range; every case must fail before scanning.
- [ ] Request `usize::MAX` page size and verify bounded rejection without
  allocation.
- [ ] Demonstrate constant bounded page memory as matching cardinality grows.
- [ ] Run Rust query tests and the portable snapshot reconciliation suite.

**Commit:** `feat(index): enforce bounded snapshot queries`

---

### Task 6: Spillable build, replacement, repair, and verification

**Context:** Maintenance must be bounded independently of index cardinality.
Online/resumable build is intentionally excluded.

**Files:**

- Create: `src/prolly/secondary_index/workspace.rs`
- Create: `src/prolly/secondary_index/lifecycle.rs`
- Modify: `src/prolly/secondary_index/budget.rs`
- Modify: `src/prolly/secondary_index/coordinator.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Modify: `tests/secondary_index.rs`
- Create: `tests/secondary_index_resources.rs`

**Interfaces produced:**

- `MaintenanceBudget`
- `IndexBuildWorkspace`
- `IndexRunWriter` and `IndexRunReader`
- Verification memory workspace for bounded tests
- Native temporary-directory workspace for approved non-WASM deployments
- `IndexVerification::{Complete, BudgetStopped}`

**Implementation:**

- [ ] Scan the pinned source snapshot in bounded pages and emit canonical
  physical entries into memory-bounded sorted runs.
- [ ] Spill a run before the next entry would exceed the memory budget; charge
  spill bytes and run count before writing.
- [ ] Merge runs with bounded fan-in into `SortedBatchBuilder`, preserving exact
  canonical ordering and duplicate handling.
- [ ] Reuse this engine for ensure/build, replacement, repair, and logical
  verification.
- [ ] Validate root, entry count, descriptor fingerprint, and source snapshot
  identity before activation.
- [ ] Publish activation only through the collection CAS; delete temporary runs
  after success, failure, conflict, and cancellation.
- [ ] When no spill workspace is available, reject work that exceeds the
  finite in-memory maintenance budget.

**Verification:**

- [ ] Compare spillable roots with clean in-memory canonical roots across run
  sizes, merge fan-in, projections, and input cardinalities.
- [ ] Inject failure and cancellation during source page, spill, merge,
  validation, and activation boundaries; assert no visible partial activation.
- [ ] Prove accounted peak memory remains bounded as source size grows.
- [ ] Verify exact-boundary and one-past memory, spill-byte, run-count,
  merge-fan-in, source-entry, finding, retry, and elapsed limits.
- [ ] Run `cargo test --test secondary_index --test secondary_index_resources`.

**Commit:** `feat(index): add bounded spillable maintenance`

---

### Task 7: Bounded transfer, retention, pins, GC, and health

**Context:** These operations must traverse one state closure and must not
materialize untrusted or complete datasets before budget checks.

**Files:**

- Rewrite: `src/prolly/secondary_index/bundle.rs`
- Modify: `src/prolly/secondary_index/lifecycle.rs`
- Modify: `src/prolly/secondary_index/budget.rs`
- Modify: `src/prolly/secondary_index/state.rs`
- Modify: `tests/secondary_index.rs`
- Modify: `tests/gc.rs`
- Modify: `tests/mixed_format_gc.rs`
- Modify: `bindings/uniffi/src/domain/indexed.rs`

**Interfaces produced:**

- `TransferBudget`
- Streaming bundle encoder/decoder and verifier
- `SnapshotPinGuard` plus explicit durable pin operations
- State-rooted retention, GC planning, and health results
- Health fields for closure status, pin/lease safety, and consumed budget

**Implementation:**

- [ ] Reject bundle envelope size before decode and charge every retained node,
  encoded/decoded byte, and verification unit incrementally.
- [ ] Verify CIDs, canonical records, exact reachability, ownership,
  descriptors, and selected snapshot without constructing a duplicate full
  `MemStore`.
- [ ] Stream export in deterministic reachability order under
  `TransferBudget`.
- [ ] Implement retention and pins as candidate state changes activated by one
  collection CAS.
- [ ] Mark GC from one pinned state closure, including head, retained
  snapshots, descriptors, and pins.
- [ ] Require lease, pin, grace-period, or explicit-quiescence proof before a
  production sweep; otherwise return `index.gc_unsafe`.
- [ ] Implement bounded structural `health()` and distinguish it from complete
  logical verification.

**Verification:**

- [ ] Feed oversized, truncated, duplicate, corrupt, unreachable, and
  zero-index bundles and verify rejection at the earliest bounded stage.
- [ ] Inject import failure before/after state write and CAS.
- [ ] Interleave reader pins, retention, publication, and GC; reachable content
  must survive every schedule.
- [ ] Verify production GC refuses unsafe adapters while verification GC can
  run under explicit quiescence.
- [ ] Run `cargo test --test secondary_index --test gc --test mixed_format_gc`.

**Commit:** `feat(index): bound transfer retention and garbage collection`

---

### Task 8: Structured errors, measured metrics, and binding parity

**Context:** Operators and bindings need stable recovery semantics and truthful
physical work data before the subsystem can be certified.

**Files:**

- Create: `src/prolly/secondary_index/metrics.rs`
- Modify: `src/prolly/error.rs`
- Modify all files under `src/prolly/secondary_index/` that construct errors or
  observations
- Modify: `src/lib.rs`
- Modify: `bindings/uniffi/src/lib.rs`
- Modify: `bindings/uniffi/src/domain/indexed.rs`
- Modify: `bindings/api/indexed-map-reconciliation.json`
- Modify: `bindings/api/secondary-index-snapshot-reconciliation.json`
- Modify binding parity tests under `bindings/*/test` or their existing
  language-specific test locations

**Interfaces produced:**

- `IndexErrorCode`
- `RetryAdvice::{Never, RetryFreshState, RetryAfter}`
- Structured safe error fields and sensitive opt-in diagnostics
- `IndexedOperationStats` with physical node/byte, query, mutation, spill,
  verification, CAS, and latency fields
- Binding records that preserve error code, retry advice, budgets, and metrics

**Implementation:**

- [ ] Map every specified error code to an explicit Rust variant and portable
  binding representation.
- [ ] Redact primary keys, terms, source/projection values, bounds, physical
  cursor keys, and extractor text from default formatting and causal output.
- [ ] Add operation-local metrics collectors fed by actual tree builder, store,
  iterator, and workspace statistics.
- [ ] Delete or rename logical counters currently labeled as physical node
  writes.
- [ ] Propagate typed budgets and completion/budget-stopped results through
  UniFFI and all reconciled language surfaces.
- [ ] Keep labels bounded and free of application data.

**Verification:**

- [ ] Snapshot default error strings for every index error class and assert
  sensitive sentinel values are absent.
- [ ] Verify retry advice for CAS conflict, transient store failure, invalid
  definition, budget exhaustion, corruption, and cancellation.
- [ ] Cross-check reported physical node/byte counts against store observer
  deltas for mutation, query, build, and transfer fixtures.
- [ ] Run UniFFI generation and every supported portable parity suite listed in
  `bindings/VERIFICATION.md`.
- [ ] Run strict Clippy for the core and binding crates.

**Commit:** `feat(index): expose structured errors and measured metrics`

---

### Task 9: Remove the obsolete architecture and update public guidance

**Context:** The cutover is incomplete while old catalog/control/component-head
paths remain callable or documented.

**Files:**

- Delete obsolete contents from:
  `src/prolly/secondary_index/storage.rs`
- Modify: `src/prolly/secondary_index/mod.rs`
- Modify: `src/lib.rs`
- Modify: `src/prolly/transaction.rs` only where secondary indexes no longer
  require multi-root transactions
- Rewrite secondary-index portions: `README.md`
- Rewrite: `docs/secondary-index-design.md`
- Modify: `examples/secondary_index.rs`
- Modify: `bindings/VERIFICATION.md`
- Remove obsolete fixture and reconciliation entries repository-wide

**Implementation:**

- [ ] Delete catalog map IDs, control roots, hidden mutable index heads,
  checkpoint records, indexed head records, and their public helpers.
- [ ] Delete the old `validate_state` mixed-head logic and multi-root indexed
  transaction publication path.
- [ ] Remove unbounded eager query APIs, inert limits, old health transaction
  flags, misleading metrics, and generic binding fallbacks.
- [ ] Rename retained canonical types without compatibility aliases.
- [ ] Update the example and documentation to show one-root publication, the
  single indexed-map API, budgets, pins, and spill workspace.
- [ ] Document application ownership of store durability, topology, backup,
  and GC-safety configuration.
- [ ] Add repository checks that reject old root-name constants and suffix
  replacement terminology in the secondary-index implementation.

**Verification:**

- [ ] Use `rg` to prove obsolete root names, catalog/control helpers, public
  aliases, and multi-root coordinator calls are absent.
- [ ] Run `cargo check --all-targets`.
- [ ] Run `cargo test --doc`.
- [ ] Build and run `examples/secondary_index.rs`.
- [ ] Generate docs with warnings denied.

**Commit:** `refactor(index): complete canonical hard cutover`

---

### Task 10: Deterministic release harness, benchmark, and tracked CI

**Context:** The implementation is not industrial until the claimed properties
are continuously demonstrated.

**Files:**

- Create: `tests/secondary_index_faults.rs`
- Create: `tests/secondary_index_concurrency.rs`
- Extend: `tests/secondary_index_resources.rs`
- Rewrite: `benches/prolly_secondary_index_bench.rs`
- Create: `scripts/run-secondary-index-bench.sh`
- Create: `scripts/summarize-secondary-index-bench.sh`
- Create: `.github/workflows/secondary-index-required.yml`
- Create: `.github/workflows/secondary-index-scheduled.yml`
- Modify: `Cargo.toml`
- Modify: `README.md`
- Modify: `bindings/VERIFICATION.md`

**Implementation:**

- [ ] Add a deterministic fault store that can stop before/after each node
  publication, confirmation, state write, root CAS, spill operation, and bundle
  stage and can reopen its durable image.
- [ ] Add barrier-controlled concurrent operation scenarios for mutation,
  activation, replacement, repair, retention, pins, readers, and GC.
- [ ] Replace the one-shot benchmark with isolated repeated fixtures,
  warm/cold modes, p50/p95/p99, declared provenance, and measured node/byte,
  memory, spill, and conflict data.
- [ ] Correct throughput denominators to use the number of records or operations
  actually measured.
- [ ] Add bounded PR smoke, required correctness/conformance/binding jobs, and
  scheduled extended model/fuzz/crash/1M/10M performance jobs.
- [ ] Make regression classification require minimum samples plus both
  absolute and relative thresholds.

**Verification:**

- [ ] Demonstrate each injected failure reopens to the complete old or new
  state.
- [ ] Run the deterministic concurrency matrix without probabilistic retries.
- [ ] Run the resource suite at increasing cardinalities and compare peak
  accounted memory against fixed budgets.
- [ ] Run the benchmark smoke twice and validate its result schema and
  provenance.
- [ ] Validate both workflow files locally where supported and ensure all
  invoked commands exist.

**Commit:** `test(index): gate industrial secondary index guarantees`

---

### Task 11: Final cutover audit and release evidence

**Context:** This is the only release gate. Do not call the work complete based
on individual task tests.

**Files:**

- Create: `docs/secondary-index-release-evidence.md`
- Modify only defects found by the final audit in files already owned by Tasks
  1–10

**Implementation and verification:**

- [ ] Map every acceptance criterion in the approved design to a passing test,
  conformance result, benchmark result, or repository-absence check.
- [ ] Run `cargo fmt --all -- --check`.
- [ ] Run `cargo check --all-targets`.
- [ ] Run `cargo clippy --all-targets -- -D warnings`.
- [ ] Run the complete core test and documentation suite.
- [ ] Run canonical fixtures, deterministic concurrency/fault/resource suites,
  and every synchronous indexed-store conformance suite.
- [ ] Run portable binding generation and parity suites.
- [ ] Run sanitizer and bounded fuzz smoke campaigns.
- [ ] Run the secondary-index benchmark smoke and retain its provenance and
  classification.
- [ ] Audit public APIs for any production operation without finite budgets.
- [ ] Audit default error and metric output with sensitive sentinel data.
- [ ] Audit the repository for obsolete layout readers, roots, aliases,
  misleading metrics, and suffix-named replacement architecture.
- [ ] Record exact commands, revisions, adapter configurations, outcomes, known
  limitations, and scheduled evidence links in
  `docs/secondary-index-release-evidence.md`.
- [ ] Stop and fix any failed criterion; do not mark a partial matrix as
  release-ready.

**Commit:** `docs: record secondary index release evidence`

## Dependency and Review Order

```text
Task 1 canonical format
  -> Task 2 indexed-store contract
  -> Task 3 publication engine
  -> Task 4 mutation
  -> Task 5 query
  -> Task 6 maintenance
  -> Task 7 transfer/lifecycle/GC
  -> Task 8 errors/metrics/bindings
  -> Task 9 hard-cutover cleanup
  -> Task 10 release harness and CI
  -> Task 11 final evidence
```

Tasks 5 and 6 may be developed in parallel after Task 4 if they modify
separate modules. Tasks 7 and 8 may be developed in parallel after Tasks 5 and
6 stabilize. Tasks 9–11 remain sequential because they define the final public
surface and release claim.

## Acceptance-Criteria Coverage

| Design requirement | Owning tasks |
|---|---|
| One authoritative root and CAS | 1, 3 |
| Old-or-new concurrency/crash behavior | 3, 10 |
| Exact production store contract | 2 |
| `FileNodeStore` verification-only | 2, 9 |
| Bounded queries and cursor integrity | 5 |
| Bounded maintenance and spill | 6 |
| Preallocation mutation amplification | 4 |
| Bounded bundle decode/export | 7 |
| Historical extractor generations | 4 |
| Finite retry/time/memory/work budgets | 4–7 |
| Structured redacted errors | 8 |
| Measured physical metrics | 8 |
| Safe retention, pins, leases, and GC | 7 |
| Fault/resource/conformance/performance gates | 10, 11 |
| No compatibility or suffix architecture | 1, 9, 11 |

## Execution Handoff

Execute this plan in order on the existing hard-cutover branch. Use one
reviewable commit per task, run each task's focused verification before its
commit, and reserve the full repository matrix for Task 11. Keep a compact
implementation log in the task checkboxes; do not introduce a second planning
layer.
