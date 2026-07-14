# SQLite Performance Matrix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a reproducible SQLite-backed performance matrix comparing `fa7c219` with the current prolly-tree implementation across build, read, mutation, diff, and merge workloads under WAL+FULL and WAL+NORMAL.

**Architecture:** A cross-version-compatible benchmark executable prepares named-root SQLite fixtures and runs one isolated workload per process. An alternating shell runner builds both revisions, clones checkpointed fixtures, captures raw CSV and process metrics, and a standard-library Rust summarizer produces normalized CSV and a regression-first Markdown report.

**Tech Stack:** Rust 2021, `prolly-map`, `prolly-store-sqlite`, bundled `rusqlite`, POSIX shell, SQLite CLI, `/usr/bin/time -l`, CSV text.

## Global Constraints

- Compare unchanged original revision `fa7c219` with the current implementation using identical harness source.
- Measure 1K, 10K, 50K, 100K, 1M, and 10M records.
- Measure WAL+FULL and WAL+NORMAL separately; never aggregate profiles.
- Use five alternating process repetitions for every version/profile/size/workload tuple.
- Use deterministic keys, values, mutation sets, merge branches, and random seeds.
- “Cold” means a fresh prolly manager cache, not a flushed operating-system page cache.
- Retain failures and every positive delta; never extrapolate a missing tier.
- Do not change production tree or SQLite-store behavior to improve benchmark results.
- Do not merge to `main` while a material latency, memory, size, or I/O regression remains unexplained.

---

### Task 1: Deterministic Workload Model

**Files:**
- Create: `stores/prolly-store-sqlite/benches/sqlite_workload_support.rs`
- Create: `stores/prolly-store-sqlite/tests/sqlite_workload_support.rs`

**Interfaces:**
- Produces: `DurabilityProfile`, `Workload`, `BenchArgs`, `sample_count`, `merge_count`, `key`, `value`, `random_indexes`, `clustered_indexes`, `right_edge_indexes`, and `shuffled_ids`.
- Consumes: `PROLLY_SQLITE_WORKLOAD`, `PROLLY_SQLITE_RECORDS`, `PROLLY_SQLITE_PROFILE`, `PROLLY_SQLITE_VERSION`, `PROLLY_SQLITE_RUN`, and `PROLLY_SQLITE_DB`.

- [ ] **Step 1: Write deterministic-generator tests**

```rust
#[path = "../benches/sqlite_workload_support.rs"]
mod support;

#[test]
fn generated_sets_are_deterministic_unique_and_bounded() {
    assert_eq!(support::sample_count(1_000), 100);
    assert_eq!(support::sample_count(100_000), 1_000);
    assert_eq!(support::sample_count(10_000_000), 10_000);
    let random = support::random_indexes(100_000, 1_000, 0x6a09_e667_f3bc_c909);
    assert_eq!(random.len(), 1_000);
    assert!(random.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(random, support::random_indexes(100_000, 1_000, 0x6a09_e667_f3bc_c909));
    let clustered = support::clustered_indexes(100_000, 1_000);
    assert!(clustered.windows(2).all(|pair| pair[1] == pair[0] + 1));
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml --test sqlite_workload_support`

Expected: FAIL because the support module does not exist.

- [ ] **Step 3: Implement exact parsers and generators**

Use a xorshift generator with seed `0x6a09_e667_f3bc_c909`, collect unique random indexes in `BTreeSet`, and return sorted vectors. Define every workload name from the approved specification in `Workload::ALL_NAMES`.

```rust
pub fn sample_count(records: usize) -> usize {
    records.min((records / 100).max(100)).min(10_000)
}

pub fn merge_count(records: usize) -> usize {
    (sample_count(records) / 2).max(50)
}

pub fn key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

pub fn value(id: usize, generation: u8) -> Vec<u8> {
    format!("value-{id:020}-{generation:02}-payload").into_bytes()
}
```

- [ ] **Step 4: Run the support tests**

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add stores/prolly-store-sqlite/benches/sqlite_workload_support.rs stores/prolly-store-sqlite/tests/sqlite_workload_support.rs
git commit -m "bench: add deterministic SQLite workload model"
```

### Task 2: Fixture Preparation and Build Workloads

**Files:**
- Create: `stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs`
- Modify: `stores/prolly-store-sqlite/Cargo.toml`
- Modify: `stores/prolly-store-sqlite/tests/sqlite_workload_support.rs`

**Interfaces:**
- Consumes: Task 1 model and generators.
- Produces: harness-free `sqlite_workload_bench`, named base root `b"sqlite-workload-base"`, and one validated CSV row per invocation.

- [ ] **Step 1: Add a failing CSV-contract test**

Add a test asserting `CsvRow::header()` and `CsvRow::to_csv()` contain the same field count and preserve version/profile/workload names.

- [ ] **Step 2: Run the test**

Expected: FAIL because `CsvRow` is undefined.

- [ ] **Step 3: Declare the benchmark target and CSV contract**

```toml
[[bench]]
name = "sqlite_workload_bench"
harness = false
```

Define `CsvRow` with explicit scalar fields for tuple identity, operations, timing, validation, prolly metrics, tree stats, and SQLite file/node statistics. Escape error text and keep header/data order identical.

- [ ] **Step 4: Implement profile-aware SQLite opening**

```rust
SqliteStoreConfig {
    busy_timeout_ms: 5_000,
    enable_wal: true,
    synchronous_normal: matches!(args.profile, DurabilityProfile::Normal),
}
```

- [ ] **Step 5: Implement sorted fixture preparation**

Use `SortedBatchBuilder` for IDs `0..records`, time ingestion plus build and named-root publication, collect stats, then verify first/middle/last keys through a reopened manager before emitting `validated=true`.

- [ ] **Step 6: Implement shuffled batch build**

Use `BatchBuilder` and `shuffled_ids(records, seed)`; validate logical count and reopened probes. It writes a standalone database and must not overwrite a prepared fixture.

- [ ] **Step 7: Run build smoke commands**

```bash
CARGO_INCREMENTAL=0 cargo bench --manifest-path stores/prolly-store-sqlite/Cargo.toml --bench sqlite_workload_bench --no-run
PROLLY_SQLITE_WORKLOAD=sorted_stream_build PROLLY_SQLITE_RECORDS=1000 PROLLY_SQLITE_PROFILE=full PROLLY_SQLITE_VERSION=current PROLLY_SQLITE_RUN=1 PROLLY_SQLITE_DB=/tmp/prolly-sqlite-build-smoke.db cargo bench --manifest-path stores/prolly-store-sqlite/Cargo.toml --bench sqlite_workload_bench
```

Expected: one valid 1K build row and a reopenable named root.

- [ ] **Step 8: Commit**

```bash
git add stores/prolly-store-sqlite/Cargo.toml stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs stores/prolly-store-sqlite/benches/sqlite_workload_support.rs stores/prolly-store-sqlite/tests/sqlite_workload_support.rs
git commit -m "bench: add SQLite fixture build workloads"
```

### Task 3: Read and Mutation Workloads

**Files:**
- Modify: `stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs`
- Modify: `stores/prolly-store-sqlite/benches/sqlite_workload_support.rs`
- Modify: `stores/prolly-store-sqlite/tests/sqlite_workload_support.rs`

**Interfaces:**
- Consumes: cloned SQLite fixture with named base root.
- Produces: six read rows and five mutation rows.

- [ ] **Step 1: Add failing expected-state tests**

Test append IDs begin at `records`, updates preserve cardinality, deletes reduce cardinality, and random/clustered expected values use exact generations.

- [ ] **Step 2: Run tests**

Expected: FAIL with unresolved expected-state helpers.

- [ ] **Step 3: Implement cold and warm reads**

Cold reads open a new store and manager and time 1,000,000 lookups. Warm reads execute the exact sequence once before resetting metrics. Validate every value and consume returned bytes through `black_box`.

- [ ] **Step 4: Implement append, update, and delete workloads**

Use `append_batch` for suffix keys and `Prolly::batch` for updates/deletes. Reset metrics immediately before timing. Validate all changed keys, exact cardinality, representative unchanged probes, and reopened probes; publish the result root.

- [ ] **Step 5: Run all 1K read/mutation modes under FULL and NORMAL**

Expected: all 11 modes emit valid rows with correct operations and cardinalities.

- [ ] **Step 6: Commit**

```bash
git add stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs stores/prolly-store-sqlite/benches/sqlite_workload_support.rs stores/prolly-store-sqlite/tests/sqlite_workload_support.rs
git commit -m "bench: cover SQLite reads and mutations"
```

### Task 4: Diff and Merge Workloads

**Files:**
- Modify: `stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs`
- Modify: `stores/prolly-store-sqlite/benches/sqlite_workload_support.rs`
- Modify: `stores/prolly-store-sqlite/tests/sqlite_workload_support.rs`

**Interfaces:**
- Consumes: deterministic changed-key and branch-key sets.
- Produces: six diff rows and five merge rows.

- [ ] **Step 1: Add failing branch-set tests**

Assert disjoint branch sets never overlap, conflict sets are identical, append branches use separate suffix ranges, and expected cardinalities are exact.

- [ ] **Step 2: Run tests**

Expected: FAIL with unresolved branch helpers.

- [ ] **Step 3: Implement diff preparation, timing, and exact validation**

Prepare changed trees outside timing. A fresh manager times `diff`; compare every returned key and diff variant against an ordered expected vector. Include zero-change identical diff.

- [ ] **Step 4: Implement merge preparation, timing, and exact validation**

Prepare left/right trees outside timing. Resolve conflicts deterministically:

```rust
let resolver: Resolver = Box::new(|conflict| {
    Resolution::value(conflict.right.clone().expect("right conflict value"))
});
```

Validate every changed key, exact cardinality, representative unchanged keys, and reopen probes.

- [ ] **Step 5: Run all 1K diff/merge modes**

Expected: all 11 modes emit valid rows with exact diff counts and merge cardinalities.

- [ ] **Step 6: Commit**

```bash
git add stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs stores/prolly-store-sqlite/benches/sqlite_workload_support.rs stores/prolly-store-sqlite/tests/sqlite_workload_support.rs
git commit -m "bench: cover SQLite diff and merge workloads"
```

### Task 5: Cross-version Alternating Runner

**Files:**
- Create: `scripts/run_sqlite_workload_report.sh`

**Interfaces:**
- Consumes: current repository, baseline `fa7c219`, shared harness, and matrix overrides.
- Produces: raw CSV/time/stderr, `machine.txt`, and `run-manifest.csv`.

- [ ] **Step 1: Confirm the runner is absent**

Run: `SQLITE_BENCH_SIZES=1000 SQLITE_BENCH_RUNS=1 scripts/run_sqlite_workload_report.sh`

Expected: file-not-found failure.

- [ ] **Step 2: Implement strict identical builds**

Use `set -eu`; create a detached baseline worktree; copy the exact harness/support files and Cargo stanza; build isolated release binaries; fail on harness hash mismatch. Record revisions, dirty state, hashes, Rust/SQLite versions, filesystem, CPU, memory, and configured matrix.

- [ ] **Step 3: Implement preparation and checkpoint validation**

For every tuple, run sorted preparation through `/usr/bin/time -l`, require a valid row, run `PRAGMA wal_checkpoint(TRUNCATE)` and `PRAGMA integrity_check`, and require successful output.

- [ ] **Step 4: Implement safe clones and alternating order**

Prefer macOS `cp -c`, Linux `cp --reflink=auto`, then normal `cp`. Clone only the checkpointed main database. Alternate version and profile order. Record stdout, stderr, timing, and exit status even on failure.

- [ ] **Step 5: Add smoke overrides and validation**

Support `SQLITE_BENCH_SIZES`, `SQLITE_BENCH_RUNS`, `SQLITE_BENCH_PROFILES`, and `SQLITE_BENCH_WORKLOADS`. A 1K one-run FULL smoke must contain both versions and every requested valid workload.

- [ ] **Step 6: Commit**

```bash
git add scripts/run_sqlite_workload_report.sh
git commit -m "bench: add alternating SQLite workload runner"
```

### Task 6: Regression-first Report Generator

**Files:**
- Create: `src/bin/prolly-sqlite-report.rs`

**Interfaces:**
- Consumes: machine metadata, manifest, raw CSV, and timing files.
- Produces: `results.csv` and `report.md`.

- [ ] **Step 1: Add failing parser/classifier tests**

Cover CSV parsing, odd/even median, full ranges, lower-is-better deltas, throughput inversion, sub-1ms noise, 3% latency threshold, 5%+4MiB RSS threshold, 3%+1MiB size threshold, I/O deltas, and failed-manifest preservation.

- [ ] **Step 2: Run tests**

Run: `cargo test --bin prolly-sqlite-report`

Expected: FAIL until parser, aggregator, and classifiers exist.

- [ ] **Step 3: Implement parsing and aggregation**

Use the standard library. Reject mismatched operation counts, missing versions, mixed profiles, invalid successful rows, and duplicate tuple/run rows. Compute median/min/max and latency, throughput, RSS, fixture-size, node-payload, and prolly-I/O deltas.

- [ ] **Step 4: Implement regression-first rendering**

Render failures; latency, memory, size, and I/O regressions; gains; complete FULL and NORMAL matrices; structural/storage tables; methodology; machine metadata. Display every positive delta.

- [ ] **Step 5: Verify unit tests and deterministic smoke rendering**

Regenerate the 1K report twice and require byte-identical outputs.

- [ ] **Step 6: Commit**

```bash
git add src/bin/prolly-sqlite-report.rs
git commit -m "bench: summarize SQLite workload regressions"
```

### Task 7: Full Measurement Matrix and Audit

**Files:**
- Create/populate: `performance-results/sqlite-workloads-2026-07-14/raw/`
- Create: `performance-results/sqlite-workloads-2026-07-14/machine.txt`
- Create: `performance-results/sqlite-workloads-2026-07-14/run-manifest.csv`
- Create: `performance-results/sqlite-workloads-2026-07-14/results.csv`
- Create: `performance-results/sqlite-workloads-2026-07-14/report.md`

**Interfaces:**
- Consumes: Tasks 2–6.
- Produces: retained raw evidence and audited report.

- [ ] **Step 1: Run 1K smoke matrix**

Run both profiles, versions, every workload, and one repetition. Require validation and integrity checks before scaling.

- [ ] **Step 2: Run measured 1K–100K matrix**

Run five alternating repetitions for 1K, 10K, 50K, and 100K.

- [ ] **Step 3: Run measured 1M matrix**

Run both profiles, versions, every workload, and five repetitions. Audit any row whose range exceeds 15% of its median.

- [ ] **Step 4: Run measured 10M matrix**

Run both profiles, versions, every workload, and five repetitions. Do not omit shuffled build, merge, or delete workloads after seeing results.

- [ ] **Step 5: Generate and mechanically audit the report**

Require no missing tuple, matching harness hashes, valid zero-exit rows, explicit failure rows, retained nonempty stderr, and byte-identical regeneration.

- [ ] **Step 6: Diagnose each material regression**

Use targeted repetitions and relevant current-only cache or legacy-equivalent attribution. Retain attribution separately and never replace primary data.

- [ ] **Step 7: Commit retained evidence**

```bash
git add performance-results/sqlite-workloads-2026-07-14
git commit -m "perf: report SQLite workload comparison"
```

### Task 8: Final Verification and Handoff

**Files:**
- Modify only if verification exposes a benchmark defect.

**Interfaces:**
- Consumes: completed pipeline and report.
- Produces: evidence-backed merge-readiness assessment.

- [ ] **Step 1: Run benchmark tests**

```bash
cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml
cargo test --bin prolly-sqlite-report
```

- [ ] **Step 2: Run repository quality gates**

```bash
CARGO_INCREMENTAL=0 cargo test --all-features
CARGO_INCREMENTAL=0 cargo clippy --all-features --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --all-features --no-deps
cargo fmt --all -- --check
git diff --check
```

Expected: all commands exit zero.

- [ ] **Step 3: Audit evidence mechanically**

Check manifest statuses, invalid rows, missing tuples, stderr, regression rows, harness hashes, raw counts, and regeneration equality.

- [ ] **Step 4: Remove temporary artifacts**

Remove detached worktrees, isolated targets, database fixtures, WAL/SHM files, generated lockfiles, and smoke-only results. Preserve checked-in source and final evidence.

- [ ] **Step 5: Deliver the report**

Lead with remaining regressions, then gains, validation, durability differences, scale behavior, memory/storage/I/O trade-offs, and coverage limitations. Do not merge without user direction.

