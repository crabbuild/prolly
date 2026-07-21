# Prolly Correctness Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one reproducible, model-based correctness gate for every synchronous and asynchronous operation on Prolly's versioned-map, indexed-map, and proximity-map surfaces.

**Architecture:** Add a standalone `correctness-harness` Rust crate that depends only on Prolly's public API. Serializable deterministic traces drive simple independent models and real sync/async adapters; shared invariant checkers compare normalized observations, while a source-AST inventory prevents public API coverage drift. A repository script runs deterministic contracts, saved regressions, generated campaigns, faults, concurrency schedules, and mutation canaries through PR, nightly, and release profiles.

**Tech Stack:** Rust 2021 with MSRV 1.81, `prolly-map` path dependency, `serde`/`serde_json` for traces and reports, `syn` for public-API inventory, `futures-executor` for runtime-neutral async execution, standard-library deterministic RNG and concurrency primitives, Bash entry-point script.

## Global Constraints

- Cover `VersionedMap`, `AsyncVersionedMap`, `IndexedMap`, `ProximityMap`, `AsyncProximityMap`, and the stateful public companion objects through which their operations are invoked.
- Add no production dependency or public production API solely for the harness.
- Exercise public APIs only; never import private Prolly helpers into an oracle or driver.
- Do not reuse production mutation, secondary-index maintenance, distance, search, pagination, or merge algorithms in reference models.
- Every generated run is deterministic from a reported 64-bit seed.
- Fault and concurrency schedules use operation counters, barriers, and controlled hooks, never sleeps or wall-clock races.
- Generated artifacts go under `target/correctness-harness/`; only deliberately promoted regression JSON is committed.
- `pr` runs deterministic contracts, regressions, canaries, and 16 seeds × 250 commands per applicable state machine.
- `nightly` runs 128 seeds × 2,000 commands per applicable state machine.
- `release` runs 256 seeds × 10,000 commands per applicable state machine.
- Preserve all unrelated worktree changes.
- Every production harness function begins with a failing test and follows red-green-refactor.

---

## File Structure

### Standalone crate

- `correctness-harness/Cargo.toml` — isolated package metadata and dependencies.
- `correctness-harness/src/lib.rs` — module exports and top-level `run` orchestration interface.
- `correctness-harness/src/main.rs` — argument parsing, runner invocation, exit status, and concise console output.
- `correctness-harness/src/config.rs` — immutable named profiles and custom override validation.
- `correctness-harness/src/rng.rs` — small stable SplitMix64 generator and boundary-biased byte/number helpers.
- `correctness-harness/src/trace.rs` — versioned serializable trace, commands, outcomes, replay, and shrinking interface.
- `correctness-harness/src/report.rs` — machine-readable report schema and atomic report persistence.
- `correctness-harness/src/contracts.rs` — deterministic contract registry shared by the inventory and runner.
- `correctness-harness/src/inventory.rs` — `syn`-based public-method discovery and ownership validation.
- `correctness-harness/api_inventory.json` — reviewed ownership metadata for every in-scope public operation.
- `correctness-harness/src/invariant.rs` — normalized observations, failures, reusable comparisons, and mutation-canary hooks.
- `correctness-harness/src/model/mod.rs` — model module boundary.
- `correctness-harness/src/model/versioned.rs` — immutable ordered-map history oracle.
- `correctness-harness/src/model/indexed.rs` — independent extractor/rebuild oracle.
- `correctness-harness/src/model/proximity.rs` — scalar vector and exact-search oracle.
- `correctness-harness/src/driver/mod.rs` — driver module boundary and common transcript helpers.
- `correctness-harness/src/driver/versioned.rs` — sync/async versioned-map commands and focused contracts.
- `correctness-harness/src/driver/indexed.rs` — indexed-map commands and focused contracts.
- `correctness-harness/src/driver/proximity.rs` — sync/async proximity commands and focused contracts.
- `correctness-harness/src/fault.rs` — deterministic transactional sync/async store wrapper and fault schedules.
- `correctness-harness/src/concurrency.rs` — controlled schedules and linearizable-outcome validation.
- `correctness-harness/src/runner.rs` — suite ordering, profiles, regression replay, generation, shrinking, and reporting.
- `correctness-harness/regressions/versioned/.gitkeep` — versioned regression directory.
- `correctness-harness/regressions/indexed/.gitkeep` — indexed regression directory.
- `correctness-harness/regressions/proximity/.gitkeep` — proximity regression directory.

### Integration tests and repository entry points

- `correctness-harness/tests/foundation.rs` — profile, trace, report, RNG, inventory, and canary integration tests.
- `correctness-harness/tests/versioned.rs` — model histories, sync/async parity, and deterministic contracts.
- `correctness-harness/tests/indexed.rs` — clean-rebuild equivalence and deterministic contracts.
- `correctness-harness/tests/proximity.rs` — brute-force equivalence and deterministic contracts.
- `correctness-harness/tests/faults.rs` — fault, corruption, cancellation, and reopen contracts.
- `correctness-harness/tests/concurrency.rs` — bounded controlled schedules.
- `scripts/run-correctness-harness.sh` — single repository correctness command.
- `correctness-harness/README.md` — profiles, replay, promotion, reports, API ownership, and guarantee limits.
- `README.md` — short contributor/agent correctness-gate section.

---

### Task 1: Standalone Crate, Stable Profiles, Traces, and Reports

**Files:**
- Create: `correctness-harness/Cargo.toml`
- Create: `correctness-harness/src/lib.rs`
- Create: `correctness-harness/src/main.rs`
- Create: `correctness-harness/src/config.rs`
- Create: `correctness-harness/src/rng.rs`
- Create: `correctness-harness/src/trace.rs`
- Create: `correctness-harness/src/report.rs`
- Create: `correctness-harness/tests/foundation.rs`

**Interfaces:**
- Produces: `ProfileName`, `RunConfig`, `Surface`, `Trace<C>`, `TraceFailure`, `SplitMix64`, `RunReport`, `RunSummary`, and `run(RunConfig) -> Result<RunReport, HarnessError>`.
- Consumes: only serializable owned values; no Prolly private APIs.

- [ ] **Step 1: Write failing foundation tests**

Add tests that require immutable named profiles, stable deterministic RNG output, trace JSON round trips, rejection of unknown trace/report versions, custom-profile marking, and reports defaulting below `../target/correctness-harness/`.

```rust
#[test]
fn named_profiles_have_exact_approved_campaign_sizes() {
    assert_eq!(RunConfig::named(ProfileName::Pr).campaign(), Campaign { seeds: 16, commands: 250 });
    assert_eq!(RunConfig::named(ProfileName::Nightly).campaign(), Campaign { seeds: 128, commands: 2_000 });
    assert_eq!(RunConfig::named(ProfileName::Release).campaign(), Campaign { seeds: 256, commands: 10_000 });
}

#[test]
fn trace_round_trip_preserves_seed_and_commands() {
    let trace = Trace::new(Surface::Versioned, 41, vec![FixtureCommand::Put(b"k".to_vec(), b"v".to_vec())]);
    let bytes = serde_json::to_vec_pretty(&trace).unwrap();
    assert_eq!(Trace::<FixtureCommand>::from_json(&bytes).unwrap(), trace);
}

#[test]
fn splitmix64_sequence_is_a_stable_harness_contract() {
    let mut rng = SplitMix64::new(0);
    assert_eq!(rng.next_u64(), 0xe220_a839_7b1d_cdaf);
    assert_eq!(rng.next_u64(), 0x6e78_9e6a_a1b9_65f4);
}
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test foundation`

Expected: compilation fails because the harness crate and named types do not exist.

- [ ] **Step 3: Implement the minimal crate and schemas**

Use this dependency boundary:

```toml
[package]
name = "prolly-correctness-harness"
version = "0.1.0"
edition = "2021"
rust-version = "1.81"
publish = false

[dependencies]
futures-executor = "0.3"
prolly = { package = "prolly-map", path = ".." }
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
syn = { version = "2.0", features = ["full", "visit"] }

[lints.rust]
unsafe_code = "forbid"
```

Implement exact version constants and immutable profile sizes:

```rust
pub const TRACE_FORMAT_VERSION: u32 = 1;
pub const REPORT_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ProfileName { Pr, Nightly, Release, Custom }

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Campaign { pub seeds: u32, pub commands: u32 }

impl RunConfig {
    pub fn named(profile: ProfileName) -> Self {
        let campaign = match profile {
            ProfileName::Pr => Campaign { seeds: 16, commands: 250 },
            ProfileName::Nightly => Campaign { seeds: 128, commands: 2_000 },
            ProfileName::Release => Campaign { seeds: 256, commands: 10_000 },
            ProfileName::Custom => panic!("custom profiles require explicit overrides"),
        };
        Self::new(profile, campaign)
    }
}
```

`Trace::from_json` must validate `TRACE_FORMAT_VERSION`; `RunReport::from_json` must validate `REPORT_FORMAT_VERSION`. `write_report` writes to a sibling temporary file, flushes it, then renames it to the final path.

- [ ] **Step 4: Implement the minimal CLI parser**

Support `--profile`, `--surface`, `--seed`, `--commands`, `--regression`, and `--report`. Reject missing values and unknown flags with exit status 2. Any seed or command override changes `profile` to `Custom` while retaining `base_profile` in the report.

- [ ] **Step 5: Run foundation tests and verify GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test foundation`

Expected: all foundation tests pass with no warnings.

- [ ] **Step 6: Commit the foundation**

```bash
git add correctness-harness/Cargo.toml correctness-harness/src correctness-harness/tests/foundation.rs
git commit -m "test: scaffold prolly correctness harness"
```

---

### Task 2: Public API Ownership and Contract Registry

**Files:**
- Create: `correctness-harness/src/contracts.rs`
- Create: `correctness-harness/src/inventory.rs`
- Create: `correctness-harness/api_inventory.json`
- Modify: `correctness-harness/src/lib.rs`
- Modify: `correctness-harness/tests/foundation.rs`

**Interfaces:**
- Produces: `ApiKey { type_name, method_name, asyncness }`, `ApiInventoryRow`, `Contract`, `discover_public_methods`, `validate_inventory`, `all_contracts`, and `run_contract`.
- Consumes: `Surface`, repository path derived from `CARGO_MANIFEST_DIR`, and public Rust source files.

- [ ] **Step 1: Write failing discovery and ownership tests**

```rust
#[test]
fn inventory_exactly_owns_every_in_scope_public_method() {
    let root = repository_root();
    let discovered = discover_public_methods(&root, IN_SCOPE_TYPES).unwrap();
    let rows = load_inventory(&root.join("correctness-harness/api_inventory.json")).unwrap();
    validate_inventory(&discovered, &rows, all_contracts()).unwrap();
}

#[test]
fn inventory_rejects_missing_stale_duplicate_and_unknown_contract_rows() {
    let method = ApiKey::sync("VersionedMap", "get");
    assert_eq!(validate_inventory(&[method.clone()], &[], &[]).unwrap_err().kind, InventoryErrorKind::Missing);
    assert_eq!(validate_inventory(&[], &[row(method.clone(), "reads")], &[contract("reads")]).unwrap_err().kind, InventoryErrorKind::Stale);
    assert_eq!(validate_inventory(&[method.clone()], &[row(method.clone(), "reads"), row(method, "reads")], &[contract("reads")]).unwrap_err().kind, InventoryErrorKind::Duplicate);
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test foundation inventory_`

Expected: compilation fails because inventory discovery and validation do not exist.

- [ ] **Step 3: Implement AST discovery and strict validation**

Parse these source roots recursively: `src/prolly/versioned_map.rs`, `src/prolly/secondary_index/`, and `src/prolly/proximity/`. Visit `syn::ItemImpl`, resolve the last identifier of `self_ty`, and collect `ImplItem::Fn` entries with `Visibility::Public` for the explicit type set.

The initial in-scope type set is:

```rust
pub const IN_SCOPE_TYPES: &[&str] = &[
    "VersionedMap", "AsyncVersionedMap", "MapSnapshot", "AsyncMapSnapshot",
    "MapComparison", "MapMerge", "MapReverseIter", "VersionedMapEditor",
    "VersionedMapsTransaction", "TypedVersionedMap", "MapChangeSubscription",
    "AsyncMapChangeSubscription", "VersionedMapBackup", "MapVersionId",
    "VersionedMapUpdate", "ProofAuthentication",
    "IndexedMap", "IndexedMapEditor", "IndexedSnapshot", "SecondaryIndexSnapshot",
    "SecondaryIndexCursor", "SecondaryIndexRegistry", "SecondaryIndex",
    "SecondaryIndexBuilder", "IndexedSnapshotBundle",
    "ProximityMap", "AsyncProximityMap", "ProximityReadSession", "ProximityConfig",
    "SearchBudget", "SearchRequest", "ProximityMembershipProof",
    "ProximityStructuralProof", "ProximitySearchProof",
];
```

Inventory rows must have non-empty `oracle`, `contract`, and `result_fields`, and at least one of `generated_families` or `focused_reason`. `validate_inventory` compares sets in both directions, rejects duplicate keys, and verifies that every referenced contract ID exists.

- [ ] **Step 4: Populate reviewed ownership rows and contract IDs**

Run a temporary diagnostic test that prints sorted discovered keys, then populate one row per key. Use these concrete contract groups:

- `versioned.identity`, `versioned.current_reads`, `versioned.snapshot_reads`, `versioned.history`, `versioned.mutations`, `versioned.conditional`, `versioned.comparison`, `versioned.merge`, `versioned.subscription`, `versioned.typed`, `versioned.proofs`, `versioned.transfer`, `versioned.retention`, `versioned.gc`, `versioned.transaction`, `versioned.async`;
- `indexed.definition`, `indexed.identity`, `indexed.reads`, `indexed.lifecycle`, `indexed.mutations`, `indexed.snapshot_queries`, `indexed.cursor`, `indexed.verify_repair`, `indexed.bundle`, `indexed.retention_gc`;
- `proximity.config`, `proximity.identity_reads`, `proximity.build_load`, `proximity.mutation`, `proximity.exact_search`, `proximity.approximate_search`, `proximity.filters_budgets`, `proximity.verify`, `proximity.proofs`, `proximity.cache`, `proximity.async`.

Register every ID as a real function pointer. Initially each function may return `ContractStatus::Pending`; add a validation test that permits pending contracts only until Tasks 5, 7, and 9 remove the last pending status.

- [ ] **Step 5: Run inventory tests and verify GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test foundation inventory_`

Expected: exact discovered/inventory equality and no unknown contract IDs.

- [ ] **Step 6: Commit API ownership**

```bash
git add correctness-harness/src/contracts.rs correctness-harness/src/inventory.rs correctness-harness/src/lib.rs correctness-harness/api_inventory.json correctness-harness/tests/foundation.rs
git commit -m "test: enforce core map API ownership"
```

---

### Task 3: Normalized Observations, Invariants, and Mutation Canaries

**Files:**
- Create: `correctness-harness/src/invariant.rs`
- Modify: `correctness-harness/src/lib.rs`
- Modify: `correctness-harness/tests/foundation.rs`

**Interfaces:**
- Produces: `Observation`, `OrderedEntry`, `SearchObservation`, `PublicationObservation`, `InvariantFailure`, `InvariantKind`, `check_entries`, `check_pages`, `check_publication`, `check_search`, and `check_sync_async`.
- Consumes: owned normalized values only, so invariant code is independently testable.

- [ ] **Step 1: Write one failing test per canary**

Create focused tests for dropped writes, wrong exclusive range ends, duplicate/skipped pages, missing index entries, torn publications, incorrect distances, wrong tie order, false exhaustive completion, and sync/async divergence.

```rust
#[test]
fn distance_and_tie_canaries_are_rejected() {
    let expected = SearchObservation::exact(vec![neighbor(b"a", 1.0), neighbor(b"b", 1.0)]);
    let wrong_distance = SearchObservation::exact(vec![neighbor(b"a", 2.0), neighbor(b"b", 1.0)]);
    let wrong_tie = SearchObservation::exact(vec![neighbor(b"b", 1.0), neighbor(b"a", 1.0)]);
    assert_eq!(check_search(&expected, &wrong_distance).unwrap_err().kind, InvariantKind::Distance);
    assert_eq!(check_search(&expected, &wrong_tie).unwrap_err().kind, InvariantKind::Order);
}

#[test]
fn torn_publication_canary_is_rejected() {
    let before = publication("s0", &["i0"], "c0");
    let after = publication("s1", &["i0"], "c1");
    assert_eq!(check_publication(&before, &after, PublicationOutcome::Failed).unwrap_err().kind, InvariantKind::Atomicity);
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test foundation canary`

Expected: compilation fails because invariant types and functions are missing.

- [ ] **Step 3: Implement minimal invariant functions**

Use an error type that always captures the command index, surface, invariant kind, expected JSON value, and actual JSON value. Floating comparison must use a named helper with absolute and relative tolerances and must reject non-finite actual distances.

```rust
pub fn distance_eq(expected: f32, actual: f32) -> bool {
    if !expected.is_finite() || !actual.is_finite() { return false; }
    let delta = (expected - actual).abs();
    delta <= 1.0e-5_f32.max(1.0e-5 * expected.abs().max(actual.abs()))
}
```

`check_pages` concatenates pages, rejects repeated cursor bytes, and then calls `check_entries`. `check_publication` accepts either the complete before tuple or complete after tuple according to the operation outcome; mixed tuples fail.

- [ ] **Step 4: Run all canaries and verify GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test foundation canary`

Expected: all nine sabotages are rejected with their specific invariant kind.

- [ ] **Step 5: Commit invariant checkers**

```bash
git add correctness-harness/src/invariant.rs correctness-harness/src/lib.rs correctness-harness/tests/foundation.rs
git commit -m "test: add correctness invariant canaries"
```

---

### Task 4: Versioned-Map Reference Model and Generated Commands

**Files:**
- Create: `correctness-harness/src/model/mod.rs`
- Create: `correctness-harness/src/model/versioned.rs`
- Create: `correctness-harness/src/driver/mod.rs`
- Create: `correctness-harness/src/driver/versioned.rs`
- Create: `correctness-harness/tests/versioned.rs`
- Modify: `correctness-harness/src/lib.rs`

**Interfaces:**
- Produces: `VersionedCommand`, `VersionedModel`, `VersionedModelVersion`, `VersionedDriver`, `VersionedTranscript`, `generate_versioned_trace`, and `run_versioned_trace`.
- Consumes: `Trace<VersionedCommand>`, `SplitMix64`, `Observation`, `Prolly<Arc<MemStore>>`, and `AsyncProlly<SyncStoreAsAsync<Arc<MemStore>>>`.

- [ ] **Step 1: Write failing pure-model tests**

Test immutable history, bytewise range/prefix/bounds, page concatenation, diff, rollback, CAS conflicts, subscription transitions, and retention selection without constructing Prolly.

```rust
#[test]
fn model_keeps_immutable_history_and_reports_stale_cas() {
    let mut model = VersionedModel::new();
    let empty = model.initialize(1).unwrap();
    let one = model.apply(&empty, &[put(b"a", b"1")], 2).unwrap();
    let conflict = model.apply_if(&empty, &[put(b"b", b"2")], 3).unwrap_err();
    assert_eq!(conflict.current, one);
    assert_eq!(model.get(&empty, b"a"), None);
    assert_eq!(model.get(&one, b"a"), Some(b"1".to_vec()));
}
```

- [ ] **Step 2: Run model tests and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test versioned model_`

Expected: compilation fails because the model and commands do not exist.

- [ ] **Step 3: Implement the minimal immutable model**

Represent versions as `Vec<ModelSnapshot>` and stable model IDs as monotonic `u64`; store `BTreeMap` content, parent, timestamp, and publication ordinal. Normalize duplicate mutations with last-write-wins semantics before applying them. Implement reads with standard `BTreeMap::range` and explicit byte-prefix checks.

- [ ] **Step 4: Write a failing generated sync-history test**

```rust
#[test]
fn generated_sync_histories_match_the_model() {
    for seed in 0..16 {
        let trace = generate_versioned_trace(seed, 250);
        run_versioned_trace(&trace, VersionedMode::Sync).unwrap();
    }
}
```

The first generator includes initialize, put, delete, batch apply, point read, contains, multi-get, range, prefix, page, snapshot read, stale conditional apply, diff, rollback, and reopen commands. Each mutation is followed by current-content, historical-snapshot, and canonical-rebuild checks.

- [ ] **Step 5: Run and verify RED against missing driver**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test versioned generated_sync_histories_match_the_model`

Expected: compilation fails because `VersionedDriver` and `run_versioned_trace` do not exist.

- [ ] **Step 6: Implement the synchronous public driver**

Construct `Arc<MemStore>`, `Prolly`, and `VersionedMap` only through public exports. Normalize real `MapVersionId` values into transcript labels while retaining the actual IDs in a driver-only map. After each successful mutation, build the model's sorted entries through a fresh `SortedBatchBuilder` on a fresh store and assert equal root identity.

- [ ] **Step 7: Run versioned generated tests and verify GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test versioned`

Expected: model and synchronous histories pass for all fixed PR seeds.

- [ ] **Step 8: Commit the versioned model and sync driver**

```bash
git add correctness-harness/src/model correctness-harness/src/driver correctness-harness/src/lib.rs correctness-harness/tests/versioned.rs
git commit -m "test: model versioned map histories"
```

---

### Task 5: Complete Versioned-Map Contracts and Async Equivalence

**Files:**
- Modify: `correctness-harness/src/contracts.rs`
- Modify: `correctness-harness/src/driver/versioned.rs`
- Modify: `correctness-harness/src/model/versioned.rs`
- Modify: `correctness-harness/api_inventory.json`
- Modify: `correctness-harness/tests/versioned.rs`
- Modify: `correctness-harness/tests/foundation.rs`

**Interfaces:**
- Produces: complete non-pending `versioned.*` contracts and `VersionedMode::Async`.
- Consumes: Task 4 model/trace interfaces and every versioned inventory row.

- [ ] **Step 1: Write failing sync/async parity and contract-completeness tests**

```rust
#[test]
fn identical_logical_trace_has_equivalent_sync_and_async_transcripts() {
    let trace = generate_versioned_trace(0x5eed, 250);
    let sync = run_versioned_trace(&trace, VersionedMode::Sync).unwrap();
    let asynchronous = run_versioned_trace(&trace, VersionedMode::Async).unwrap();
    check_sync_async(&sync.normalized(), &asynchronous.normalized()).unwrap();
}

#[test]
fn every_versioned_inventory_contract_is_executable() {
    assert_no_pending_contracts(Surface::Versioned).unwrap();
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test versioned identical_logical_trace_has_equivalent_sync_and_async_transcripts`

Expected: fails because asynchronous execution is not implemented.

- [ ] **Step 3: Implement the async driver over the same commands**

Use `AsyncProlly::new(SyncStoreAsAsync::new(store), config)` and `AsyncVersionedMap`. Drive futures with `futures_executor::block_on`; do not introduce Tokio. Normalize results through the same transcript functions as the sync driver.

- [ ] **Step 4: Implement all focused versioned contract groups**

Each contract group must call every owned method from its inventory rows and assert specific behavior:

- identity/current/snapshot reads: accessors, borrowed callbacks, leases, bounds, scans, pages, reverse scans, cursor windows, stats, debug views, pin and hint controls;
- mutation/conditional/history: every timestamped and conditional variant, editor helpers, sorted/parallel builders, append, rollback, versions, backup/restore/import, catalog verification, retention, node/blob GC;
- comparison/merge: eager/streamed/paged/structural diff, proofs, statistics, changed spans, conflict streams, standard/policy/CRDT merge, and CAS publication;
- typed/subscription: bytes/string key codecs, value codecs, typed get/put/delete/migrate, sync and async subscription resume;
- proof/transfer: key/multi/range/prefix/page proofs, authentication, export, plan/copy missing nodes, and push;
- transactions: success and stale all-or-nothing updates across at least three map IDs.

Replace each versioned inventory `Pending` contract with the concrete function name and add each contract function to `all_contracts()`.

- [ ] **Step 5: Verify every versioned contract and parity path is GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test versioned`

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test foundation inventory_`

Expected: all versioned tests pass and no versioned row references a pending contract.

- [ ] **Step 6: Commit complete versioned coverage**

```bash
git add correctness-harness/src/contracts.rs correctness-harness/src/driver/versioned.rs correctness-harness/src/model/versioned.rs correctness-harness/api_inventory.json correctness-harness/tests/versioned.rs correctness-harness/tests/foundation.rs
git commit -m "test: cover versioned map public API"
```

---

### Task 6: Indexed-Map Clean-Rebuild Model and State Machine

**Files:**
- Create: `correctness-harness/src/model/indexed.rs`
- Create: `correctness-harness/src/driver/indexed.rs`
- Create: `correctness-harness/tests/indexed.rs`
- Modify: `correctness-harness/src/model/mod.rs`
- Modify: `correctness-harness/src/driver/mod.rs`
- Modify: `correctness-harness/src/lib.rs`

**Interfaces:**
- Produces: `IndexDefinitionModel`, `IndexEmission`, `IndexedCommand`, `IndexedModel`, `IndexedDriver`, `generate_indexed_trace`, and `run_indexed_trace`.
- Consumes: public `SecondaryIndex`, `SecondaryIndexRegistry`, `IndexedMap`, `IndexedSnapshot`, and `SecondaryIndexSnapshot` APIs.

- [ ] **Step 1: Write failing pure clean-rebuild oracle tests**

Use a stable test record encoding `group\0payload`. Define three independent model extractors: keys-only group, include-payload group, and all-record group. Test empty, sparse, duplicate, binary, multi-term, projection-only, replacement, and delete transitions.

```rust
#[test]
fn clean_rebuild_deduplicates_terms_and_tracks_projection_changes() {
    let definitions = fixture_definitions();
    let mut model = IndexedModel::new(definitions);
    model.put(b"p1".to_vec(), record(&[b"g", b"g"], b"old"));
    assert_eq!(model.entries(b"include"), vec![emission(b"g", b"p1", Some(b"old"))]);
    model.put(b"p1".to_vec(), record(&[b"g"], b"new"));
    assert_eq!(model.entries(b"include"), vec![emission(b"g", b"p1", Some(b"new"))]);
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test indexed clean_rebuild_`

Expected: compilation fails because indexed model types do not exist.

- [ ] **Step 3: Implement independent extractor and query semantics**

Store source records in `BTreeMap`. On every model publication, rerun extractors over the whole source, sort by `(term, primary_key)`, deduplicate identical terms per source key, and retain projection bytes according to `IndexProjection`. Implement exact, prefix, half-open range, forward/reverse, page, projected, primary-key, and source-record views directly on model entries.

- [ ] **Step 4: Write a failing generated incremental-versus-rebuild test**

```rust
#[test]
fn generated_incremental_indexes_equal_clean_rebuilds() {
    for seed in 0..16 {
        let trace = generate_indexed_trace(seed, 250);
        run_indexed_trace(&trace).unwrap();
    }
}
```

Commands include ensure index, put, delete, duplicate batch mutation, projection-only update, exact/prefix/range/reverse/page queries, pinned snapshot query, verify, repair, replace generation, deactivate, reopen, bundle round trip, and retention.

- [ ] **Step 5: Run and verify RED against missing public driver**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test indexed generated_incremental_indexes_equal_clean_rebuilds`

Expected: compilation fails because the indexed driver is missing.

- [ ] **Step 6: Implement the public indexed driver and canonical rebuild check**

Use `Arc<MemStore>`, `Prolly::indexed_map`, and public extractor closures. After every successful mutation, compare every logical query with the model, call `verify_all`, then build the same source and definitions from empty storage through `ensure_index` and compare active descriptor/checkpoint roots by public snapshots.

- [ ] **Step 7: Run indexed state-machine tests and verify GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test indexed`

Expected: all fixed seeds pass for keys-only, include, and all projections.

- [ ] **Step 8: Commit the indexed model and state machine**

```bash
git add correctness-harness/src/model correctness-harness/src/driver correctness-harness/src/lib.rs correctness-harness/tests/indexed.rs
git commit -m "test: model indexed map maintenance"
```

---

### Task 7: Complete Indexed-Map Public Contracts

**Files:**
- Modify: `correctness-harness/src/contracts.rs`
- Modify: `correctness-harness/src/driver/indexed.rs`
- Modify: `correctness-harness/api_inventory.json`
- Modify: `correctness-harness/tests/indexed.rs`
- Modify: `correctness-harness/tests/foundation.rs`

**Interfaces:**
- Produces: complete non-pending `indexed.*` contracts.
- Consumes: Task 6 indexed model and driver.

- [ ] **Step 1: Write failing indexed contract-completeness test**

```rust
#[test]
fn every_indexed_inventory_contract_is_executable() {
    assert_no_pending_contracts(Surface::Indexed).unwrap();
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test indexed every_indexed_inventory_contract_is_executable`

Expected: fails and lists the remaining indexed contract IDs.

- [ ] **Step 3: Implement all focused indexed contract groups**

Exercise and assert:

- definition: builders, projections, limits, registry register/replace/get/iter, extraction errors, deduplication, and binary terms;
- identity/reads: indexed ID, registry, source handle, get/get_with, health, metrics;
- lifecycle/mutations: ensure on empty/populated source, conditional conflicts, editor paths, activation fencing, replacement, repair, deactivation, and retry behavior;
- snapshot queries: current/historical/by-ID selection, all forward/reverse callback scans, exact/prefix/range pages, projection, primary-key, and record resolution;
- cursor: byte round trip plus direction, snapshot, definition-fingerprint, and index-version mismatch rejection;
- bundle/retention/GC: inspect/verify/serialize/export/import, retained checkpoint closure, and indexed GC planning.

For every callback API, test early break and full traversal against the corresponding owned query. For every page API, concatenate pages and compare with the unpaged model result.

- [ ] **Step 4: Verify all indexed contracts and inventory rows are GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test indexed`

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test foundation inventory_`

Expected: no indexed contract is pending and every indexed public method has exact ownership.

- [ ] **Step 5: Commit complete indexed coverage**

```bash
git add correctness-harness/src/contracts.rs correctness-harness/src/driver/indexed.rs correctness-harness/api_inventory.json correctness-harness/tests/indexed.rs correctness-harness/tests/foundation.rs
git commit -m "test: cover indexed map public API"
```

---

### Task 8: Proximity Scalar Oracle and Generated State Machine

**Files:**
- Create: `correctness-harness/src/model/proximity.rs`
- Create: `correctness-harness/src/driver/proximity.rs`
- Create: `correctness-harness/tests/proximity.rs`
- Modify: `correctness-harness/src/model/mod.rs`
- Modify: `correctness-harness/src/driver/mod.rs`
- Modify: `correctness-harness/src/lib.rs`

**Interfaces:**
- Produces: `ProximityCommand`, `ProximityModel`, `ScalarMetric`, `EligibilityModel`, `ProximityDriver`, `generate_proximity_trace`, and `run_proximity_trace`.
- Consumes: public proximity configuration, build/load, mutation, scan, search, verification, and proof APIs.

- [ ] **Step 1: Write failing scalar-oracle tests**

```rust
#[test]
fn scalar_oracle_orders_equal_distances_by_key() {
    let mut model = ProximityModel::new(2, ScalarMetric::L2);
    model.put(b"b".to_vec(), vec![1.0, 0.0], b"b".to_vec()).unwrap();
    model.put(b"a".to_vec(), vec![-1.0, 0.0], b"a".to_vec()).unwrap();
    let neighbors = model.exact(&[0.0, 0.0], 2, EligibilityModel::All).unwrap();
    assert_eq!(neighbors.iter().map(|n| n.key.as_slice()).collect::<Vec<_>>(), vec![b"a", b"b"]);
}
```

Add L2, cosine, inner-product, zero-norm validation, prefix/range/eligible-key filtering, duplicate vector, and k-boundary cases.

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test proximity scalar_`

Expected: compilation fails because the scalar oracle does not exist.

- [ ] **Step 3: Implement scalar formulas without production helpers**

Use explicit loops over `f32` components accumulated in `f64`, then cast normalized public distances to `f32`. Sort with `f32::total_cmp` followed by bytewise key comparison. Reject dimension mismatch and non-finite components before model mutation or search.

- [ ] **Step 4: Write failing generated rebuild/mutation/search test**

```rust
#[test]
fn generated_proximity_histories_match_clean_rebuild_and_brute_force() {
    for seed in 0..16 {
        let trace = generate_proximity_trace(seed, 250);
        run_proximity_trace(&trace, ProximityMode::Sync).unwrap();
    }
}
```

Commands include build, load, get, borrowed get, contains, scan, ranged scan, value-only mutation, vector mutation, delete, rebuild batch, exact search under every filter/metric, approximate search validation, verify, cache clear, proofs, and reopen.

- [ ] **Step 5: Run and verify RED against missing driver**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test proximity generated_proximity_histories_match_clean_rebuild_and_brute_force`

Expected: compilation fails because the proximity driver is missing.

- [ ] **Step 6: Implement sync public driver and clean-rebuild comparison**

Build public `ProximityRecord` values, retain the current public descriptor, and normalize `SearchResult`. After every mutation, compare full record scans and exact queries with the model, invoke `verify`, independently rebuild all model records on a fresh store with identical configuration, and compare descriptors where canonical construction is promised.

- [ ] **Step 7: Run generated proximity tests and verify GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test proximity`

Expected: fixed PR seeds pass for L2, cosine, inner product, ties, filters, and mutation/rebuild equivalence.

- [ ] **Step 8: Commit proximity oracle and sync driver**

```bash
git add correctness-harness/src/model correctness-harness/src/driver correctness-harness/src/lib.rs correctness-harness/tests/proximity.rs
git commit -m "test: model proximity map behavior"
```

---

### Task 9: Complete Proximity Contracts and Async Search Equivalence

**Files:**
- Modify: `correctness-harness/src/contracts.rs`
- Modify: `correctness-harness/src/driver/proximity.rs`
- Modify: `correctness-harness/api_inventory.json`
- Modify: `correctness-harness/tests/proximity.rs`
- Modify: `correctness-harness/tests/foundation.rs`

**Interfaces:**
- Produces: `ProximityMode::Async` and complete non-pending `proximity.*` contracts.
- Consumes: Task 8 model and synchronous driver.

- [ ] **Step 1: Write failing async-equivalence and contract-completeness tests**

```rust
#[test]
fn exact_async_search_matches_sync_and_scalar_oracle() {
    let trace = generated_exact_search_trace(0xace, 128);
    let sync = run_proximity_trace(&trace, ProximityMode::Sync).unwrap();
    let asynchronous = run_proximity_trace(&trace, ProximityMode::Async).unwrap();
    check_sync_async(&sync.normalized(), &asynchronous.normalized()).unwrap();
}

#[test]
fn every_proximity_inventory_contract_is_executable() {
    assert_no_pending_contracts(Surface::Proximity).unwrap();
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test proximity exact_async_search_matches_sync_and_scalar_oracle`

Expected: fails because the async map driver is missing.

- [ ] **Step 3: Implement async load/search over the same source descriptor**

Load `AsyncProximityMap` from the same underlying store through the public async adapter, run identical exact and approximate requests with `block_on`, and normalize results through the synchronous transcript schema.

- [ ] **Step 4: Implement all focused proximity contract groups**

Exercise and assert:

- configuration/budget validation for zero dimensions, metric-specific input, hierarchy, overflow, vector storage, quantization, and zero budget fields;
- build/load/identity for empty, singleton, multilevel, parallel, duplicate-vector, and overflow-heavy maps;
- all point, borrowed, leased, callback, range-callback, and early-break read-session methods;
- rebuild and incremental mutation statistics, canonical equivalence, fallback reporting, and failure atomicity;
- exact/approximate policy, planner, backend preference, kernel, filter, secondary-eligible snapshot, hard-budget, partial-result, and deterministic tie cases;
- verification and all membership, structure, and search proof success/tamper paths;
- cache clear followed by cold-result equivalence;
- async cancellation and completion reporting at controlled boundaries.

- [ ] **Step 5: Verify all proximity contracts and inventory rows are GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test proximity`

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test foundation inventory_`

Expected: no proximity contract is pending and every proximity public method has exact ownership.

- [ ] **Step 6: Commit complete proximity coverage**

```bash
git add correctness-harness/src/contracts.rs correctness-harness/src/driver/proximity.rs correctness-harness/api_inventory.json correctness-harness/tests/proximity.rs correctness-harness/tests/foundation.rs
git commit -m "test: cover proximity map public API"
```

---

### Task 10: Deterministic Fault, Corruption, Cancellation, and Reopen Harness

**Files:**
- Create: `correctness-harness/src/fault.rs`
- Create: `correctness-harness/tests/faults.rs`
- Modify: `correctness-harness/src/lib.rs`
- Modify: `correctness-harness/src/driver/versioned.rs`
- Modify: `correctness-harness/src/driver/indexed.rs`
- Modify: `correctness-harness/src/driver/proximity.rs`

**Interfaces:**
- Produces: `FaultStore`, `FaultSchedule`, `FaultPoint`, `FaultAction`, `DurableImage`, and `run_fault_case`.
- Consumes: public `Store`, `ManifestStore`, `TransactionalStore`, `NodeStoreScan`, and corresponding async adapter contracts.

- [ ] **Step 1: Write failing schedule and atomicity tests**

```rust
#[test]
fn fault_schedule_triggers_only_the_selected_occurrence() {
    let store = FaultStore::new(FaultSchedule::once(FaultPoint::CompareAndSwap, 2, FaultAction::Error));
    assert!(store.record(FaultPoint::CompareAndSwap).is_ok());
    assert!(store.record(FaultPoint::CompareAndSwap).is_err());
    assert!(store.record(FaultPoint::CompareAndSwap).is_ok());
}

#[test]
fn failed_indexed_publication_exposes_old_or_new_complete_tuple_never_mixed() {
    for occurrence in 1..=publication_fault_bound() {
        let observation = run_indexed_fault_case(occurrence).unwrap();
        check_publication(&observation.before, &observation.after, observation.outcome).unwrap();
    }
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test faults`

Expected: compilation fails because deterministic fault storage is missing.

- [ ] **Step 3: Implement a transactional in-memory fault store**

Keep node bytes, hints, manifests, and transaction state in separate `Arc<Mutex<...>>` maps. Count every fault point with `AtomicUsize`; the configured point/occurrence returns an error, `None`, malformed bytes, wrong-CID bytes, or wrong-format bytes. Batch and transaction methods stage changes in local maps and swap them into durable state only on success. `DurableImage` clones only committed state for crash/reopen tests.

- [ ] **Step 4: Add surface-specific failure campaigns**

Versioned cases cover read/write/batch/CAS faults, stale IDs, malformed nodes/manifests/proofs/backups, retry exhaustion, and reopen before/after publication. Indexed cases cover extractor failure, limits, source/index/catalog transaction failure, cursor/catalog/checkpoint corruption, activation/lifecycle races, and repair failure. Proximity cases cover descriptor/node/record corruption, invalid vectors/config/search, failed rebuild/mutation, async `Pending` then cancellation, and reopen from the durable descriptor.

- [ ] **Step 5: Run faults and verify GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test faults`

Expected: every injected failure returns the documented class, no mixed publication is observed, and cold reopen equals the permitted durable state.

- [ ] **Step 6: Commit fault harness**

```bash
git add correctness-harness/src/fault.rs correctness-harness/src/lib.rs correctness-harness/src/driver correctness-harness/tests/faults.rs
git commit -m "test: inject deterministic map failures"
```

---

### Task 11: Controlled Concurrency and Linearization Checks

**Files:**
- Create: `correctness-harness/src/concurrency.rs`
- Create: `correctness-harness/tests/concurrency.rs`
- Modify: `correctness-harness/src/fault.rs`
- Modify: `correctness-harness/src/lib.rs`

**Interfaces:**
- Produces: `Schedule`, `ScheduleGate`, `ConcurrentOutcome`, `allowed_serial_outcomes`, and `check_linearizable`.
- Consumes: `FaultStore` hooks and surface model transitions.

- [ ] **Step 1: Write failing controlled-schedule tests**

```rust
#[test]
fn two_independent_writers_linearize_without_losing_a_success() {
    for seed in 0..16 {
        let outcome = run_versioned_writer_schedule(Schedule::from_seed(seed)).unwrap();
        check_linearizable(&outcome, &allowed_two_writer_outcomes()).unwrap();
    }
}
```

Add stale conditional writers, subscription/publication, indexed writer/activation, writer/replacement, writer/repair, writer/deactivation, writer/retention, and async search cancellation schedules.

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test concurrency`

Expected: compilation fails because schedule gates and linearization checks do not exist.

- [ ] **Step 3: Implement barrier-driven schedules**

`ScheduleGate` exposes named checkpoints through `Mutex<State>` and `Condvar`. A seeded schedule is a finite vector of actor IDs; the gate releases only the next actor at its next checkpoint. Reject schedules that do not consume all declared checkpoints. Capture operation invocation, response, and resulting public roots, then compare the history with the finite set of serial model executions that respect real-time order.

- [ ] **Step 4: Run controlled schedules and verify GREEN**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test concurrency`

Expected: every observed schedule matches an allowed serial history, successful writes are retained, and conflicts report the actual current version.

- [ ] **Step 5: Commit concurrency checks**

```bash
git add correctness-harness/src/concurrency.rs correctness-harness/src/fault.rs correctness-harness/src/lib.rs correctness-harness/tests/concurrency.rs
git commit -m "test: check controlled map concurrency"
```

---

### Task 12: Regression Replay, Shrinking, Profile Runner, and Reports

**Files:**
- Create: `correctness-harness/src/runner.rs`
- Create: `correctness-harness/regressions/versioned/.gitkeep`
- Create: `correctness-harness/regressions/indexed/.gitkeep`
- Create: `correctness-harness/regressions/proximity/.gitkeep`
- Modify: `correctness-harness/src/lib.rs`
- Modify: `correctness-harness/src/main.rs`
- Modify: `correctness-harness/src/trace.rs`
- Modify: `correctness-harness/src/report.rs`
- Modify: `correctness-harness/tests/foundation.rs`

**Interfaces:**
- Produces: `SuiteRunner`, `SuiteResult`, `shrink_failure`, `load_regressions`, and complete `run(RunConfig)`.
- Consumes: all contracts, state machines, fault cases, concurrency schedules, and invariant failures.

- [ ] **Step 1: Write failing order, replay, shrink, and report tests**

```rust
#[test]
fn runner_executes_regressions_then_contracts_then_canaries_then_campaigns() {
    let events = RecordingSuite::default();
    SuiteRunner::new(test_config(), &events).run().unwrap();
    assert_eq!(events.phases(), [Phase::Regressions, Phase::Contracts, Phase::Canaries, Phase::Generated, Phase::Faults, Phase::Concurrency]);
}

#[test]
fn shrinker_preserves_failure_and_removes_irrelevant_commands() {
    let trace = fixture_failing_trace();
    let minimized = shrink_failure(trace, |candidate| fails_with(candidate, InvariantKind::Range)).unwrap();
    assert_eq!(minimized.commands, minimal_range_failure_commands());
}
```

- [ ] **Step 2: Run and verify RED**

Run: `cargo test --manifest-path correctness-harness/Cargo.toml --test foundation runner_ shrinker_`

Expected: compilation fails because suite orchestration and shrinking do not exist.

- [ ] **Step 3: Implement deterministic suite ordering and shrinking**

Load regression files recursively in bytewise path order. Run all phases even when a non-generated phase succeeds; stop at the first failure, shrink only generated traces, and include both original and minimized JSON in the report. Shrink in this fixed order: remove contiguous command chunks, remove individual commands, shrink byte vectors, shrink collections, shrink integers toward `0`, `1`, and boundary values, then simplify configuration. Accept a candidate only if the same `InvariantKind` recurs.

- [ ] **Step 4: Implement complete reports and exit behavior**

Populate inventory totals, contract totals, seeds, commands, fault points, schedules, invariant counts, elapsed diagnostic time, and failure details. Exit 0 only for a complete passing run, 1 for a correctness failure, and 2 for invalid CLI/configuration/input. Always attempt to write the report after valid configuration.

- [ ] **Step 5: Run the test-sized custom profile and verify GREEN**

Run: `cargo run --manifest-path correctness-harness/Cargo.toml -- --profile pr --seed 1 --commands 10 --report target/correctness-harness/test-report.json`

Expected: exit 0, console summary says `PASS`, report marks the run `custom`, and JSON contains all six phases.

- [ ] **Step 6: Commit runner and regressions layout**

```bash
git add correctness-harness/src correctness-harness/tests/foundation.rs correctness-harness/regressions
git commit -m "test: run reproducible correctness profiles"
```

---

### Task 13: Repository Gate and Documentation

**Files:**
- Create: `scripts/run-correctness-harness.sh`
- Create: `correctness-harness/README.md`
- Modify: `README.md`

**Interfaces:**
- Produces: `scripts/run-correctness-harness.sh [pr|nightly|release] [runner arguments]`.
- Consumes: standalone harness CLI and Cargo formatting/testing commands.

- [ ] **Step 1: Write a failing shell-interface test**

Before the script exists, run:

```bash
scripts/run-correctness-harness.sh invalid
```

Expected: shell reports that the script does not exist. Record this RED result in the task transcript.

- [ ] **Step 2: Implement the safe single entry point**

```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROFILE="${1:-pr}"
if (($#)); then shift; fi
case "$PROFILE" in
  pr|nightly|release) ;;
  *) printf 'profile must be pr, nightly, or release\n' >&2; exit 2 ;;
esac

cargo fmt --manifest-path "$REPO_ROOT/correctness-harness/Cargo.toml" -- --check
cargo test --manifest-path "$REPO_ROOT/correctness-harness/Cargo.toml"
cargo run --release --manifest-path "$REPO_ROOT/correctness-harness/Cargo.toml" -- \
  --profile "$PROFILE" "$@"
```

Do not clean targets, remove files, or rewrite reports outside `target/correctness-harness/`.

- [ ] **Step 3: Document exact workflows and guarantee boundary**

Document these commands:

```bash
scripts/run-correctness-harness.sh pr
cargo run --manifest-path correctness-harness/Cargo.toml -- --profile pr --surface versioned --seed 123 --commands 250
cargo run --manifest-path correctness-harness/Cargo.toml -- --regression correctness-harness/regressions/versioned/example.json
```

Explain phase order, profile sizes, report location/schema, seed replay, minimized-trace promotion, API inventory ownership, how to add a new public method, and why passing testing provides strong regression assurance rather than a mathematical proof of absence of bugs.

- [ ] **Step 4: Run focused repository verification**

Run:

```bash
bash -n scripts/run-correctness-harness.sh
cargo fmt --manifest-path correctness-harness/Cargo.toml -- --check
cargo test --manifest-path correctness-harness/Cargo.toml
cargo clippy --manifest-path correctness-harness/Cargo.toml --all-targets -- -D warnings
cargo test --all-targets
scripts/run-correctness-harness.sh pr
git diff --check
```

Expected: every command exits 0; the PR report covers all inventory rows, contracts, regressions, canaries, fixed campaigns, representative faults, and concurrency schedules.

- [ ] **Step 5: Verify the diff contains no unrelated worktree changes**

Run: `git status --short` and `git diff --stat HEAD`

Expected: harness commits contain only `correctness-harness/`, `scripts/run-correctness-harness.sh`, the two approved design/plan docs, and the small README addition. Pre-existing unrelated modifications remain uncommitted and unchanged.

- [ ] **Step 6: Commit repository integration**

```bash
git add scripts/run-correctness-harness.sh correctness-harness/README.md README.md
git commit -m "docs: require core correctness gate"
```

---

## Final Verification Checklist

- [ ] `api_inventory.json` has exact set equality with discovered public methods for every explicit in-scope type.
- [ ] No inventory contract remains pending.
- [ ] Every mutation canary is caught by its intended invariant.
- [ ] Versioned histories match the immutable `BTreeMap` model and overlapping sync/async transcripts match.
- [ ] Indexed incremental state matches independent full extractor rebuilds across all projection modes.
- [ ] Proximity exact search matches scalar brute force for all canonical metrics; approximate results meet validity and honesty contracts.
- [ ] Fault, corruption, cancellation, durable reopen, and controlled concurrency suites pass.
- [ ] Saved regressions run before generated campaigns and failures emit minimized replayable JSON.
- [ ] Named profile constants and report schema match the approved specification.
- [ ] Harness builds on Rust 1.81 without a Tokio requirement.
- [ ] Root tests, harness tests, strict Clippy, formatting, and PR correctness gate pass.
- [ ] Unrelated worktree changes are preserved and absent from harness commits.
