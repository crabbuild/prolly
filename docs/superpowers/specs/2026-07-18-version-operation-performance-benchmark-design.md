# Version-Operation Performance Benchmark Design

## Status

Approved for specification on 2026-07-18. Implementation and the full performance run require review of this written specification.

## Objective

Extend the existing native Rust versus Dolt Go prolly-tree comparison to measure version-related operations under identical, deterministic, single-worker, in-memory workloads. The primary comparison covers operations with equivalent semantics in both implementations: full diff, range diff, patch generation and application, and three-way merge. A separate Rust-only section measures the higher-level `VersionedMap` lifecycle where Dolt Go does not expose an equivalent facade.

The benchmark must establish both performance and semantic parity. A timing is publishable only when both implementations consume the same logical workload and produce matching change counts, conflict counts, result cardinalities, and content digests.

## Goals

1. Compare equivalent Rust and Dolt Go version operations at 10K, 50K, 1M, 5M, and 10M records.
2. Measure equal-version, sparse-change, and heavy-change behavior at 0%, 1%, and 30% change density.
3. Cover append, random, and clustered edit locality where density is non-zero.
4. Measure clean, convergent, and conflicting three-way merges.
5. Measure the core Rust `VersionedMap` lifecycle separately and label it as Rust-only.
6. Run every scenario three times in a process-isolated, single-worker, in-memory configuration.
7. Record enough source, binary, workload, machine, and validation provenance to reproduce the run.
8. Produce machine-readable raw results and a concise Markdown report with medians, speedups, dispersion, and validation status.

## Non-Goals

- Comparing disk-backed or remote stores in this benchmark.
- Measuring multi-worker scaling.
- Comparing SQL-layer Dolt operations, repositories, commits, schemas, or working sets.
- Treating the Rust-only lifecycle section as a Go-versus-Rust comparison.
- Including fixture generation, tree construction, validation, process startup, or report generation in operation timings.
- Comparing proof generation, backup/restore, synchronization, export/import, garbage collection, or node reclamation.
- Guaranteeing byte-identical internal nodes across Rust and Dolt Go; parity is defined by logical ordered key/value content and operation results.

## Benchmark Layers

### Common native comparison

The common layer operates on immutable prolly-tree snapshots in each native implementation. It times only operations with matching logical semantics:

- full diff;
- key-range diff;
- structural patch generation;
- structural patch application;
- three-way merge with disjoint edits;
- three-way merge with convergent overlapping edits;
- three-way merge with conflicting overlapping edits and a deterministic resolver.

Rust uses the native `Prolly` tree APIs. Go uses Dolt's native `store/prolly/tree` or `store/prolly` APIs at the lowest public level that preserves the same operation semantics. Adapter work required to encode string keys and values is setup work and remains outside the timed regions.

### Rust lifecycle measurements

The lifecycle layer uses `VersionedMap<MemStore>` and reports Rust timings only:

- version publication;
- current-head resolution;
- current and historical snapshot resolution;
- historical point reads;
- historical range scans;
- version catalog listing;
- rollback to retained versions;
- retention pruning.

The report must keep lifecycle results in a separate section and must not infer a Dolt Go winner or loser for these operations.

## Deterministic Data Contract

### Base records

Each base tree contains exactly the requested number of records. Keys are fixed-width, lexicographically sortable strings. Values are deterministic pseudorandom byte strings shorter than 100 bytes. Generation uses a documented fixed seed and integer-only algorithms implemented identically in Rust and Go.

The workload contract has an explicit version string. Any change to key encoding, value generation, edit selection, range bounds, resolver policy, or digest calculation increments that version.

### Change density

The benchmark uses three densities:

- `0%`: both compared versions reference the same immutable root, exercising content-identity short-circuiting. Locality variants are omitted because they would be duplicates.
- `1%`: sparse changes expose structural-sharing and traversal behavior.
- `30%`: heavy changes retain continuity with the existing mutation benchmark.

For non-zero densities, the requested edit count is derived from the base record count using integer arithmetic. Every runner records the requested and effective counts.

### Edit locality and mix

Non-zero densities use three deterministic locality patterns:

- `append`: all edits add keys strictly after the base key range;
- `random`: edit locations are selected by a deterministic permutation across the keyspace;
- `clustered`: edits are concentrated into deterministic contiguous clusters using the same cluster-size rule in both languages.

Random and clustered workloads use 40% updates, 30% inserts, and 30% deletes. Remainders are assigned deterministically. Equal insert and delete shares keep result cardinality approximately stable while exercising every diff type. The append workload remains append-only because modifying or deleting existing records would no longer represent append locality.

### Range selection

Range diff uses one deterministic half-open key range spanning 10% of the logical union keyspace. The range is positioned by the workload contract to intersect the edited region, including the appended tail for append workloads. The range and expected in-range changes are generated before timing. Both implementations must report matching range-result counts and digests.

### Three-way branches

For every non-zero density and locality, the benchmark derives left and right branches from the same base:

- `disjoint`: left and right edit non-overlapping logical keys;
- `convergent`: both branches apply identical edits and values to the same logical keys;
- `conflicting`: both branches edit the same logical keys incompatibly. Update/update and add/add pairs use different deterministic values; a key assigned to deletion on the left is updated on the right. Append conflicts are add/add pairs with different values.

Conflicting merges use a deterministic prefer-left resolver in both implementations. The runners record discovered conflict count, resolved conflict count, merged cardinality, and merged-content digest. The `0%` case measures one no-op merge rather than repeating meaningless relationship variants.

## Timed Operations

### Full diff

Time creation and complete consumption of the diff iterator or callback stream. Every change is folded into an order-sensitive digest. Reporting includes total latency, changes per second, change count, and digest.

### Range diff

Time creation and complete consumption of diff results inside the predefined half-open range. Reporting uses the same fields as full diff plus range bounds in scenario metadata.

### Patch generation

Generate a structural patch from base to target and fully materialize the patch representation required for later application. Report total latency, logical changes represented, patch item count, and an order-sensitive patch digest.

Rust's structural patch API and Dolt's patch generator may encode patches differently. Cross-language validation therefore compares logical effects and logical patch operations, not serialized patch bytes.

### Patch application

Apply the pre-generated native patch to the base tree. Patch generation is outside this timed region. Validate the resulting tree against the target using cardinality, complete ordered-content digest, and a post-timing diff that must be empty.

### Three-way merge

Time the complete native merge, including conflict callbacks and construction of the result tree. Post-timing validation scans the result and checks expected content, counts, conflict totals, and digest. Report total latency and effective edited keys per second.

## Rust Lifecycle Contract

### History setup

Lifecycle read, listing, rollback, and pruning scenarios use a catalog of exactly 100 versions, including the initial base version. Setup starts with the requested base size, then publishes 99 small deterministic deltas that preserve structural sharing. Catalog creation is outside timed regions except in the explicit publication scenario.

### Version publication

Time publication of one `1%` or `30%` append, random, or clustered batch through `VersionedMap`. Report total publish latency, effective changes per second, resulting version ID, result cardinality, and content digest. Input generation and validation are not timed.

### Head and snapshot resolution

Use enough repeated resolutions to avoid treating timer granularity as signal. Head resolution validates the current version ID. Snapshot resolution covers the current snapshot and deterministic historical versions sampled across the 100-version catalog. Report nanoseconds per resolution.

### Historical point reads

Perform 100,000 deterministic reads distributed across retained historical versions with a fixed hit/miss mix. Resolve snapshots before the timed read loop so the result measures pinned historical reads rather than repeated manifest lookup. Fold hits, misses, keys, and values into a digest.

### Historical range scans

Scan pinned historical snapshots over deterministic ranges and consume every returned record. Report total latency, rows per second, and nanoseconds per returned row. Snapshot resolution and expected-result construction are outside timing.

### Version listing

Enumerate the complete 100-version catalog and consume every version record. Report total latency and nanoseconds per listed version. Validate ordering, uniqueness, head membership, and catalog digest after timing.

### Rollback

Alternate the head between two retained historical version IDs. Each measured rollback includes the atomic head update performed by `rollback_to`; post-operation ID validation is outside timing. Report total latency and nanoseconds per rollback.

### Retention pruning

Start each repetition with a fresh 100-version catalog, roll the head back to a deterministic older version outside timing, then retain the newest 10 versions plus that older current head and time `prune_versions`. The expected result is 11 retained and 89 removed versions. Report total latency, versions examined, removed count, retained count, and catalog digest. Node garbage collection is out of scope.

## Process and Storage Policy

- Storage is in-memory: Rust `MemStore` and Dolt's in-memory node store.
- `RAYON_NUM_THREADS=1` and `GOMAXPROCS=1` are set explicitly.
- Every implementation/scenario/repetition runs in a fresh process. A scenario may emit several independently timed operation rows when they share the same immutable base and derived trees.
- Base and derived trees are built in the same store used by the timed operation, so measurements intentionally represent warm in-memory version operations.
- Setup, deterministic input generation, expected-output calculation, and post-operation validation are outside timed regions.
- The benchmark uses release/optimized binaries copied into the result directory before execution.
- Each scenario runs three repetitions at every size, including 5M and 10M.

## Runner and Harness Architecture

### Native runners

Add a Rust common-operation runner and a Dolt Go common-operation runner. Each accepts explicit arguments for record count, density, locality, range policy, seed, and contract version. One invocation constructs the immutable base and derived versions once, then emits separate CSV rows for full diff, range diff, patch generation, patch application, and the applicable merge relationships. Timed regions do not overlap, and each operation receives fresh operation-local iterators or result builders.

Add a separate Rust lifecycle runner accepting record count, lifecycle scenario, density/locality where applicable, history depth, seed, and contract version. Read-only head, snapshot, historical-read, historical-scan, and listing measurements may share one prepared history process. Publication, rollback, and pruning use separate scenario invocations because they mutate lifecycle state.

The runners share language-local workload helpers so setup and validation code is not duplicated across operations. The logical algorithms and golden contract vectors remain intentionally duplicated across Rust and Go to prove that each implementation independently constructs the same workload.

### Orchestration

Add one shell entry point that:

1. refuses to overwrite an existing result directory;
2. builds optimized Rust and Go binaries;
3. copies the exact binaries into the result directory;
4. records Git revisions, relevant source hashes, binary SHA-256 hashes, toolchain versions, host information, workload policy, worker settings, and repetition counts;
5. runs the complete scenario matrix in process isolation;
6. records command identity, exit status, elapsed process time, and raw output in a manifest;
7. stops on any failed process or validation error;
8. invokes the summarizer only after the matrix is complete.

The Go runner directory may be outside the parent Rust repository's tracked files. Its source hash is therefore mandatory provenance rather than relying only on the Dolt commit ID.

### Result schema

Every raw result identifies at least:

- implementation and revision;
- contract version and seed;
- records, density, locality, operation, and merge relationship;
- history depth or range policy when applicable;
- elapsed nanoseconds and the operation-specific unit count;
- normalized nanoseconds per unit and units per second;
- logical result count and digest;
- base, target, patch, or merged cardinality where applicable;
- conflict counts where applicable;
- repetition number;
- validation status.

Operation-specific total latency remains available even when the normalized unit is a change, returned row, listed version, or lookup.

## Validation and Failure Behavior

The harness fails closed. A result is excluded from reporting and the run stops when any of these occurs:

- runner exit status is non-zero;
- contract version, seed, operation count, or scenario metadata differs;
- base or derived workload digests differ across languages;
- diff or range-diff count/digest differs;
- patch application does not reproduce the target;
- conflict discovery or resolution totals differ;
- merged cardinality or complete content digest differs;
- a lifecycle operation resolves the wrong version, returns unexpected content, or violates retention expectations;
- any raw result reports `validated=false`.

Validation performs complete ordered scans for final tree digests. Sampling alone is not sufficient for a publishable comparison.

## Reporting and Reproducibility

The summarizer produces one Markdown report and machine-readable summary CSV files.

The common comparison reports median total latency, normalized throughput, Rust/Go speedup, winner, and coefficient of variation for each scenario. It also aggregates winners by operation, density, locality, relationship, and size.

The Rust lifecycle section reports medians and dispersion without cross-language winner claims. The report highlights timing groups whose coefficient of variation exceeds 10% and treats exact nanosecond values from very short operations cautiously.

A reproducibility audit checks:

- the complete expected Cartesian matrix is present;
- all three repetitions exist;
- every result validated successfully;
- logical workload and result identities match across languages and repetitions;
- each implementation used one fixed revision and binary hash;
- winner direction is consistent across repetitions;
- median, p95, and maximum coefficients of variation are reported.

## Test Strategy

1. Add golden-vector tests for deterministic key/value generation, edit selection, range bounds, branch relationships, resolver decisions, and digests in both languages.
2. Add small Rust unit tests for every common and lifecycle operation.
3. Add small Go unit tests for every common operation.
4. Add a cross-language contract smoke test that compares raw outputs for 10K records at 0% and 1% density.
5. Add negative tests proving digest, count, conflict, and metadata mismatches stop the summarizer.
6. Run Rust formatting and the relevant release-mode binary tests.
7. Run Go formatting and runner package tests.
8. Run the orchestration script with a reduced smoke matrix.
9. Run the complete 10K, 50K, 1M, 5M, and 10M matrix with three repetitions.
10. Run the independent artifact and reproducibility audit before publishing conclusions.

## Implementation Isolation

Implementation occurs in a dedicated worktree and branch created from `main@42873d1561c3641c9091fe2a616567e790029762`:

- branch: `codex/version-performance-benchmark`;
- worktree: `/Users/haipingfu/CrabDB-worktrees/prolly-version-performance-benchmark`.

The primary `/Users/haipingfu/CrabDB/prolly` worktree currently contains unrelated modifications and an unresolved documentation conflict. The benchmark implementation must not modify, stage, resolve, or commit any of that primary-worktree state.

## Acceptance Criteria

1. Both common runners pass deterministic golden vectors and cross-language contract tests.
2. Every common scenario validates logical parity before it appears in the report.
3. Every Rust lifecycle scenario validates its version, content, and catalog invariants.
4. The smoke matrix completes from one documented command.
5. The full matrix contains exactly three successful repetitions per expected scenario at all five sizes.
6. Result artifacts contain source revisions and hashes, copied binary hashes, machine/toolchain details, raw outputs, a process manifest, summary CSV files, and a Markdown report.
7. The report separates common comparisons from Rust-only lifecycle results and uses operation-appropriate units.
8. An independent audit confirms matrix completeness, validation success, input identity, result identity, fixed binaries, and repetition dispersion.
9. No benchmark conclusions are published for operations whose logical outputs differ or whose result matrix is incomplete.
