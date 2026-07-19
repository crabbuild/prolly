# SQLite and Turso Local Performance Comparison Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build, validate, run, and report a reproducible local-only throughput and latency comparison between synchronous SQLite prolly and native asynchronous Turso prolly from 10K through 2M records.

**Architecture:** Add a non-published benchmark package with deterministic workload/model code, isolated fixture lifecycle code, and separate SQLite-sync and Turso-async runners. A Rust matrix harness writes resumable raw CSV rows; a strict Python summarizer validates completeness, aggregates three repetitions, calculates cross-adapter ratios, and writes the final report. A shell driver builds once, records provenance, runs smoke/full profiles, and invokes the summarizer.

**Tech Stack:** Rust 2021, `prolly-map`, `prolly-store-sqlite`, `prolly-store-turso`, Tokio, Serde, `csv`, `fs2`, Python 3 standard library, POSIX shell.

## Global Constraints

- Compare local files only; never enable Turso's `sync` feature or call `push()`/`pull()`.
- SQLite must use synchronous `Prolly<SqliteStore>` with `SqliteStoreConfig::default()`.
- Turso must use native `AsyncProlly<TursoStore>` with `TursoBackend::open()`.
- Full sizes are exactly `10_000,50_000,100_000,500_000,1_000_000,2_000_000` with three independent repetitions.
- Changes are `clamp(records / 100, 100, 10_000)` except the explicit 100-record/10-change smoke profile.
- Keys are exactly `key-{id:020}` (24 bytes); values are exactly `value-{id:020}-{generation:02}-payload` (37 bytes).
- Patterns are exactly append, deterministic random, and midpoint-clustered.
- APIs are exactly sequential individual put, one batch, eager diff, and conflict-free disjoint three-way merge.
- Fixture creation, fixture copying, changed-tree preparation, and merge-branch preparation are outside API timings.
- Every reported successful row must pass result validation and named-root reopen validation.
- Do not alter, resolve, stage, or discard the unrelated pre-existing cherry-pick conflict. Git commit steps remain blocked until the user resolves it; preserve implementation changes in the current worktree if Git refuses a commit.

---

## File Map

- Create `benchmarks/sqlite-turso-local/Cargo.toml`: non-published benchmark package and dependencies.
- Create `benchmarks/sqlite-turso-local/src/lib.rs`: module exports used by the binary and tests.
- Create `benchmarks/sqlite-turso-local/src/model.rs`: configuration, enums, workload generation, expected counts, row schemas, percentiles, and resume keys.
- Create `benchmarks/sqlite-turso-local/src/fixture.rs`: fixture directories, sidecar-safe cloning, byte accounting, and cleanup.
- Create `benchmarks/sqlite-turso-local/src/sqlite_runner.rs`: synchronous SQLite fixture and cell execution.
- Create `benchmarks/sqlite-turso-local/src/turso_runner.rs`: asynchronous Turso fixture and cell execution.
- Create `benchmarks/sqlite-turso-local/src/harness.rs`: matrix order, resume, guards, durable CSV output, and manifest compatibility.
- Create `benchmarks/sqlite-turso-local/src/main.rs`: environment/CLI entry point and exit behavior.
- Create `benchmarks/sqlite-turso-local/tests/local_smoke.rs`: 100-record local integration matrix.
- Modify `/Users/haipingfu/CrabDB/Cargo.toml`: register `prolly/benchmarks/sqlite-turso-local` in the parent workspace.
- Modify `/Users/haipingfu/CrabDB/Cargo.lock`: resolve benchmark-only dependencies.
- Create `scripts/summarize_sqlite_turso_local_comparison.py`: strict matrix validation, aggregation, CSV, and Markdown report.
- Create `scripts/tests/test_summarize_sqlite_turso_local_comparison.py`: summarizer unit tests.
- Create `scripts/run_sqlite_turso_local_comparison.sh`: release build, machine/manifest capture, smoke/full execution, resume, and summarization.
- Create `scripts/tests/test_run_sqlite_turso_local_comparison.py`: driver black-box tests with a fake benchmark executable.
- Create `docs/sqlite-turso-local-performance.md`: reproduction commands and methodology/limitation documentation.
- Create `performance-results/sqlite-turso-local-2026-07-18/`: generated full-run artifacts only after verification.

---

### Task 1: Scaffold the Benchmark Package and Deterministic Workload Contract

**Files:**
- Create: `benchmarks/sqlite-turso-local/Cargo.toml`
- Create: `benchmarks/sqlite-turso-local/src/lib.rs`
- Create: `benchmarks/sqlite-turso-local/src/model.rs`
- Create: `benchmarks/sqlite-turso-local/src/main.rs`
- Modify: `/Users/haipingfu/CrabDB/Cargo.toml`
- Modify: `/Users/haipingfu/CrabDB/Cargo.lock`

**Interfaces:**
- Produces: `Adapter::{SqliteSync,TursoAsync}`, `Api::{Put,Batch,Diff,Merge}`, `Pattern::{Append,Random,Clustered}`.
- Produces: `RunConfig`, `CellSpec`, `change_count`, `key`, `value`, `mutation_ids`, `merge_ids`, and stable enum parsing/display.
- Consumes: approved constants from the design specification.

- [ ] **Step 1: Register a non-published benchmark package**

Create `benchmarks/sqlite-turso-local/Cargo.toml` with this dependency boundary:

```toml
[package]
name = "prolly-sqlite-turso-local-bench"
version = "0.0.0"
edition = "2021"
rust-version = "1.88"
publish = false

[dependencies]
csv = "1.3"
fs2 = "0.4"
prolly = { package = "prolly-map", path = "../..", features = ["async-store"] }
prolly-store-sqlite = { path = "../../stores/prolly-store-sqlite" }
prolly-store-turso = { path = "../../stores/prolly-store-turso" }
serde = { version = "1.0", features = ["derive"] }
tokio = { version = "1.45", features = ["macros", "rt-multi-thread"] }
tempfile = "3.20"

[lints.rust]
unsafe_code = "forbid"
```

Add `"prolly/benchmarks/sqlite-turso-local"` to the parent workspace members. Run:

```sh
cargo metadata --manifest-path benchmarks/sqlite-turso-local/Cargo.toml --no-deps
```

Expected: the package is named `prolly-sqlite-turso-local-bench`, reports Rust 1.88, and does not enable `prolly-store-turso/sync`.

- [ ] **Step 2: Write failing model tests**

In `model.rs`, define tests before implementations:

```rust
#[test]
fn change_count_uses_approved_bounds() {
    assert_eq!(change_count(10_000), 100);
    assert_eq!(change_count(50_000), 500);
    assert_eq!(change_count(1_000_000), 10_000);
    assert_eq!(change_count(2_000_000), 10_000);
}

#[test]
fn keys_and_values_have_frozen_width_and_order() {
    assert_eq!(key(7).len(), 24);
    assert_eq!(value(7, 1).len(), 37);
    assert!(key(7) < key(8));
}

#[test]
fn random_and_clustered_inputs_are_deterministic_and_disjoint() {
    let first = mutation_ids(Pattern::Random, 10_000, 100, RANDOM_SEED);
    assert_eq!(first, mutation_ids(Pattern::Random, 10_000, 100, RANDOM_SEED));
    assert_eq!(first.iter().collect::<std::collections::BTreeSet<_>>().len(), 100);
    let (left, right) = merge_ids(Pattern::Clustered, 10_000, 100, RANDOM_SEED);
    assert!(left.iter().all(|id| !right.contains(id)));
}
```

Initially declare the called functions with `todo!()` and run:

```sh
cargo test --manifest-path benchmarks/sqlite-turso-local/Cargo.toml model::tests
```

Expected: FAIL from the first `todo!()` path.

- [ ] **Step 3: Implement the workload model**

Implement:

```rust
pub const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
pub const FULL_SIZES: &[usize] = &[10_000, 50_000, 100_000, 500_000, 1_000_000, 2_000_000];

pub fn change_count(records: usize) -> usize {
    (records / 100).clamp(100, 10_000).min(records)
}

pub fn key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

pub fn value(id: usize, generation: u8) -> Vec<u8> {
    format!("value-{id:020}-{generation:02}-payload").into_bytes()
}
```

Use a fixed xorshift generator plus a `HashSet` to retain unique random IDs in generation order. Append IDs start at `records`; clustered IDs form a midpoint interval. `merge_ids` must return two disjoint vectors of exactly `changes` IDs each, with non-overlapping append ranges or halves of one random/clustered selection.

- [ ] **Step 4: Add typed configuration and parsing tests**

Add `RunConfig` fields for output directory, adapters, sizes, runs, APIs, patterns, optional explicit changes, maximum seconds, minimum free bytes, keep fixtures, and worker threads. Test rejection of zero sizes/runs, unknown enum names, duplicate filters, and a smoke profile that explicitly selects 100 records/10 changes/one run.

Run:

```sh
cargo test --manifest-path benchmarks/sqlite-turso-local/Cargo.toml model::tests
cargo clippy --manifest-path benchmarks/sqlite-turso-local/Cargo.toml --all-targets -- -D warnings
```

Expected: all model tests and Clippy pass.

- [ ] **Step 5: Checkpoint the task**

```sh
git add benchmarks/sqlite-turso-local
git commit -m "bench: define SQLite Turso workload contract"
git -C /Users/haipingfu/CrabDB add Cargo.toml Cargo.lock
```

Expected in the current repository: the submodule commit may be refused because
an unrelated cherry-pick is unresolved. Do not commit the parent workspace files
until the submodule can record its benchmark commit. Do not alter that conflict;
record the refusal and keep these scoped changes intact.

---

### Task 2: Add Measurement Rows, Percentiles, Durable CSV, and Resume Keys

**Files:**
- Modify: `benchmarks/sqlite-turso-local/src/model.rs`
- Create: `benchmarks/sqlite-turso-local/src/measurement.rs`
- Modify: `benchmarks/sqlite-turso-local/src/lib.rs`

**Interfaces:**
- Produces: `RawRow`, `FixtureRow`, `CellKey`, `FixtureKey`, `nearest_rank`, `CsvSink<T>`, and `ResumeState`.
- Consumes: `Adapter`, `Api`, `Pattern`, `CellSpec`, and schema version `sqlite-turso-local-v1`.

- [ ] **Step 1: Write failing percentile and resume tests**

```rust
#[test]
fn nearest_rank_uses_one_based_ceiling() {
    let samples = vec![10, 20, 30, 40, 50];
    assert_eq!(nearest_rank(&samples, 0.50), Some(30));
    assert_eq!(nearest_rank(&samples, 0.95), Some(50));
    assert_eq!(nearest_rank(&[], 0.99), None);
}

#[test]
fn resume_rejects_duplicates_and_skips_exact_cells() {
    let row = RawRow::example();
    let state = ResumeState::from_rows(&[row.clone()]).unwrap();
    assert!(state.contains(&row.key()));
    assert!(ResumeState::from_rows(&[row.clone(), row]).is_err());
}
```

Run the focused test and confirm it fails before implementing the functions.

- [ ] **Step 2: Implement frozen row schemas**

Define serializable rows with these exact fields:

```rust
pub struct RawRow {
    pub schema: String,
    pub revision: String,
    pub dirty: bool,
    pub adapter: Adapter,
    pub records: usize,
    pub repetition: usize,
    pub api: Api,
    pub pattern: Pattern,
    pub configured_changes: usize,
    pub observed_changes: usize,
    pub total_ns: u128,
    pub operations_per_sec: f64,
    pub p50_ns: Option<u128>,
    pub p95_ns: Option<u128>,
    pub p99_ns: Option<u128>,
    pub max_ns: Option<u128>,
    pub db_bytes_before: u64,
    pub db_bytes_after: u64,
    pub expected_records: usize,
    pub observed_records: usize,
    pub validated: bool,
    pub error: String,
}
```

`FixtureRow` contains schema, revision, dirty, adapter, records, repetition, build nanoseconds, records/sec, database bytes, observed records, validated, and error. `CellKey` is the tuple `(adapter, records, repetition, api, pattern)`.

- [ ] **Step 3: Implement durable append and strict resume loading**

`CsvSink::open(path)` must create a header only for a new empty file, serialize one row, call `flush()` after every row, and propagate failures. `ResumeState::load` must reject wrong schemas, duplicate keys, successful rows with `validated=false`, and rows whose revision/configuration conflicts with the manifest.

Test CSV round-trip with an error containing commas, quotes, and newlines.

- [ ] **Step 4: Verify Task 2**

```sh
cargo test --manifest-path benchmarks/sqlite-turso-local/Cargo.toml measurement
cargo fmt --manifest-path benchmarks/sqlite-turso-local/Cargo.toml -- --check
cargo clippy --manifest-path benchmarks/sqlite-turso-local/Cargo.toml --all-targets -- -D warnings
```

Expected: percentile, CSV, duplicate, and resume tests pass with no warnings.

- [ ] **Step 5: Checkpoint the task**

Attempt a scoped commit named `bench: add resumable measurement records`; preserve the worktree if the unrelated cherry-pick still blocks commits.

---

### Task 3: Implement Sidecar-Safe Fixtures and the Native SQLite Runner

**Files:**
- Create: `benchmarks/sqlite-turso-local/src/fixture.rs`
- Create: `benchmarks/sqlite-turso-local/src/sqlite_runner.rs`
- Modify: `benchmarks/sqlite-turso-local/src/lib.rs`
- Add tests in: `benchmarks/sqlite-turso-local/src/fixture.rs`
- Add tests in: `benchmarks/sqlite-turso-local/src/sqlite_runner.rs`

**Interfaces:**
- Produces: `FixtureLayout`, `clone_fixture`, `directory_bytes`, `build_sqlite_fixture`, and `run_sqlite_cell`.
- `build_sqlite_fixture(spec, layout) -> Result<FixtureRow, BenchError>` persists root name `adapter-bench-base`.
- `run_sqlite_cell(spec, layout) -> Result<RawRow, BenchError>` uses synchronous `Prolly<SqliteStore>` exclusively.

- [ ] **Step 1: Write failing fixture clone tests**

Create a source directory containing `fixture.db`, `fixture.db-wal`, and a nested metadata file. Assert `clone_fixture` copies regular database sidecars, rejects a non-empty destination, and `directory_bytes` equals the sum of regular files. Confirm the focused test fails before implementation.

- [ ] **Step 2: Implement safe fixture lifecycle helpers**

`FixtureLayout` must place source and one transient cell clone below the configured output filesystem. Use explicit validated paths, `create_dir_all`, `copy`, and scoped `remove_dir_all` only for the exact generated cell directory. Never delete an output root or unresolved environment-variable path.

- [ ] **Step 3: Write a failing 100-record SQLite fixture test**

The test must build 100 sorted base mutations in chunks, publish `adapter-bench-base`, drop all handles, reopen the database, load the root, and validate first/middle/last values. It must assert `FixtureRow.validated` and `observed_records == 100`.

- [ ] **Step 4: Implement `build_sqlite_fixture`**

Open `SqliteStore::open_with_config(path, SqliteStoreConfig::default())`, construct `Prolly`, build deterministic append batches, publish the named root, validate, drop handles, and measure fixture bytes. Record build timing separately from later cell timing.

- [ ] **Step 5: Write failing SQLite cell tests for all APIs and patterns**

For one cloned 100-record fixture and 10 changes, assert:

```rust
for api in Api::ALL {
    for pattern in Pattern::ALL {
        let row = run_sqlite_cell(&spec(api, pattern), &clone).unwrap();
        assert!(row.validated, "{api}/{pattern}: {}", row.error);
        assert_eq!(row.observed_changes, expected_operations(api, 10));
    }
}
```

Put rows must contain p50/p95/p99/max; other API rows must contain `None` percentile fields.

- [ ] **Step 6: Implement native synchronous SQLite cells**

Use separate helpers for `run_put`, `run_batch`, `run_diff`, and `run_merge`. Time only the approved API call. Prepare diff trees and merge branches before `Instant::now()`. Publish the result root, close, reopen, reload, count entries, and validate changed/unaffected samples before setting `validated=true`.

- [ ] **Step 7: Verify Task 3**

```sh
cargo test --manifest-path benchmarks/sqlite-turso-local/Cargo.toml sqlite_runner
cargo test --manifest-path benchmarks/sqlite-turso-local/Cargo.toml fixture
cargo clippy --manifest-path benchmarks/sqlite-turso-local/Cargo.toml --all-targets -- -D warnings
```

Expected: fixture and 12 SQLite cell combinations pass locally.

- [ ] **Step 8: Checkpoint the task**

Attempt a scoped commit named `bench: add native SQLite benchmark runner`; leave the unrelated conflict untouched if blocked.

---

### Task 4: Implement the Native Async Turso Runner

**Files:**
- Create: `benchmarks/sqlite-turso-local/src/turso_runner.rs`
- Modify: `benchmarks/sqlite-turso-local/src/lib.rs`
- Add tests in: `benchmarks/sqlite-turso-local/src/turso_runner.rs`

**Interfaces:**
- Produces: `build_turso_fixture(spec, layout) -> Future<Result<FixtureRow, BenchError>>`.
- Produces: `run_turso_cell(spec, layout) -> Future<Result<RawRow, BenchError>>`.
- Consumes: the same `CellSpec`, mutation vectors, named root, fixture clone, row schema, and validators as SQLite.

- [ ] **Step 1: Write a failing 100-record Turso fixture test**

Use `#[tokio::test(flavor = "multi_thread")]`, `TursoBackend::open`, `TursoStore::new`, and `AsyncProlly`. Publish and reopen the same named root and validate first/middle/last values. Confirm failure while functions are unimplemented.

- [ ] **Step 2: Implement `build_turso_fixture` without sync features**

Build base chunks through `AsyncProlly::batch(...).await`; its append fast path handles sorted suffixes. Drop manager/backend handles before cloning. The package dependency must remain `prolly-store-turso = { path = "..." }` with no features.

- [ ] **Step 3: Write failing Turso cell tests for all APIs and patterns**

Mirror the SQLite assertions for 12 combinations. Verify local-only scope through
the dependency feature audit and by keeping all runner constructors limited to
`TursoBackend::open`; do not mutate process-wide credential environment
variables in parallel tests.

- [ ] **Step 4: Implement native asynchronous Turso cells**

Call `AsyncProlly::put`, `batch`, `diff`, and `merge` directly and await each timed future. Do not wrap Turso in a synchronous runtime bridge. Reuse identical IDs, mutations, expected counts, generation values, and validation samples from `model.rs`.

- [ ] **Step 5: Verify Task 4**

```sh
cargo test --manifest-path benchmarks/sqlite-turso-local/Cargo.toml turso_runner
if cargo tree --manifest-path benchmarks/sqlite-turso-local/Cargo.toml -e features | rg -q 'turso feature "sync"'; then exit 1; fi
cargo clippy --manifest-path benchmarks/sqlite-turso-local/Cargo.toml --all-targets -- -D warnings
```

Expected: all 12 Turso combinations pass locally, and the feature audit prints no enabled Turso sync feature.

- [ ] **Step 6: Checkpoint the task**

Attempt a scoped commit named `bench: add native async Turso benchmark runner`; preserve changes if Git remains blocked.

---

### Task 5: Implement Matrix Ordering, Guards, Resume, and the Smoke Integration Test

**Files:**
- Create: `benchmarks/sqlite-turso-local/src/harness.rs`
- Modify: `benchmarks/sqlite-turso-local/src/main.rs`
- Create: `benchmarks/sqlite-turso-local/tests/local_smoke.rs`

**Interfaces:**
- Produces: `run_matrix(config: RunConfig) -> Future<Result<RunStats, BenchError>>`.
- Produces: `RunStats { fixtures, measured, skipped, failed, stopped_by_guard }`.
- Consumes: adapter fixture/cell functions and durable CSV/resume state.

- [ ] **Step 1: Write failing matrix-order tests**

Assert repetition 1 orders SQLite then Turso, repetition 2 orders Turso then SQLite, and every full configuration produces exactly 36 fixture keys and 432 cell keys with no duplicates. Assert the smoke configuration produces 2 fixtures and 24 cells.

- [ ] **Step 2: Implement deterministic matrix enumeration**

Enumerate repetition, record count, alternating adapter order, API, then pattern. Skip only exact successful keys already present in resume state. A failed existing row must be rerunnable rather than silently skipped.

- [ ] **Step 3: Write failing guard and manifest compatibility tests**

Inject a clock and free-space probe into the harness. Assert it stops between cells when elapsed time is exceeded or available bytes fall below the threshold. Assert resume rejects a manifest with different schema, revision, seed, filters, durability description, or Tokio worker count.

- [ ] **Step 4: Implement guards and manifest state**

Use `fs2::available_space(output_dir)` and `Instant` between cells. Persist stop reason and end time to `run-manifest.txt`; flush CSV sinks first. Return a nonzero main exit on a guard stop or failed cell while preserving resumable output.

- [ ] **Step 5: Implement failed-row handling**

Catch adapter/copy/validation errors per cell, emit a row with `validated=false` and escaped `error`, increment `failed`, and continue only when configured for diagnostic continuation. Default full runs stop after recording the first failure.

- [ ] **Step 6: Add the local integration smoke matrix**

`tests/local_smoke.rs` must run 100 records, 10 changes, one repetition, both adapters, four APIs, and three patterns into a temporary output directory. Read the CSV back and assert 24 unique validated raw rows, 2 validated fixture rows, no credential requirement, and successful named-root reopen checks.

- [ ] **Step 7: Verify Task 5**

```sh
cargo test --manifest-path benchmarks/sqlite-turso-local/Cargo.toml
cargo run --release --manifest-path benchmarks/sqlite-turso-local/Cargo.toml -- --profile smoke --output target/sqlite-turso-smoke
```

Expected: all tests pass; the release smoke command reports 2 fixtures, 24 measured cells, 0 failed.

- [ ] **Step 8: Checkpoint the task**

Attempt a scoped commit named `bench: add resumable local adapter matrix`; do not touch the unrelated conflict if blocked.

---

### Task 6: Add Strict Aggregation and Markdown Reporting

**Files:**
- Create: `scripts/summarize_sqlite_turso_local_comparison.py`
- Create: `scripts/tests/test_summarize_sqlite_turso_local_comparison.py`

**Interfaces:**
- Produces: `load_rows`, `validate_matrix`, `summarize`, `write_summary_csv`, and `render_report`.
- CLI: `python3 scripts/summarize_sqlite_turso_local_comparison.py --input RAW --fixtures FIXTURES --output-dir DIR --sizes CSV --runs N [--allow-partial]`.

- [ ] **Step 1: Write failing strict-validation tests**

Build synthetic rows for two adapters and assert rejection of missing fields, wrong schema, duplicate primary keys, `validated=false`, mismatched operations/change counts, missing adapter pairs, missing repetitions, and incomplete matrices unless `--allow-partial` is set.

- [ ] **Step 2: Write failing aggregation and ratio tests**

For three repetitions with known timings, assert median/min/max latency, median throughput, put p50/p95/p99 medians, Turso/SQLite latency ratio, and Turso/SQLite throughput ratio. Include a test that lower latency ratio and higher throughput ratio are labeled as Turso-favorable.

- [ ] **Step 3: Implement strict loading and validation**

Use only Python's standard library. The required scenario set is the Cartesian product of requested sizes, four APIs, three patterns, and requested repetitions, paired across `sqlite-sync` and `turso-async`. Full defaults require 432 raw rows and 36 fixture rows.

- [ ] **Step 4: Implement summary and report rendering**

Write `summary.csv` with one row per size/API/pattern (72 full rows) and both adapter medians plus ratios. Render compact Markdown sections per API, fixture build context, validation counts, largest observed differences, scaling notes derived from data, and every limitation in the design. Never call a difference significant without statistical support; describe observed ratios.

- [ ] **Step 5: Verify Task 6**

```sh
python3 -m unittest scripts.tests.test_summarize_sqlite_turso_local_comparison -v
python3 -m py_compile scripts/summarize_sqlite_turso_local_comparison.py
```

Expected: validation and aggregation tests pass.

- [ ] **Step 6: Checkpoint the task**

Attempt a scoped commit named `bench: summarize SQLite Turso comparison`; retain changes if the repository conflict blocks it.

---

### Task 7: Add the Reproducible Driver and Documentation

**Files:**
- Create: `scripts/run_sqlite_turso_local_comparison.sh`
- Create: `scripts/tests/test_run_sqlite_turso_local_comparison.py`
- Create: `docs/sqlite-turso-local-performance.md`

**Interfaces:**
- Environment: `BENCH_OUT`, `BENCH_SIZES`, `BENCH_RUNS`, `BENCH_APIS`, `BENCH_PATTERNS`, `BENCH_ADAPTERS`, `BENCH_MAX_SECONDS`, `BENCH_MIN_FREE_GB`, `BENCH_KEEP_FIXTURES`, and `BENCH_TOKIO_WORKERS`.
- Default command runs the full approved local matrix; `BENCH_PROFILE=smoke` selects the smoke profile.

- [ ] **Step 1: Write a failing driver black-box test**

Use a temporary fake `cargo` executable that records arguments and writes valid 24-row smoke CSV fixtures. Assert the driver builds release once, never passes `--features sync`, records machine/manifest files, calls the benchmark with the requested output, invokes the summarizer, resumes an existing output, and propagates benchmark/summarizer failures.

- [ ] **Step 2: Implement the shell driver**

Use `set -eu`, resolve the repository root from the script location, validate numeric/filter inputs, create an explicit output directory, and run:

```sh
cargo build --release --manifest-path benchmarks/sqlite-turso-local/Cargo.toml
"$TARGET_DIR/release/prolly-sqlite-turso-local-bench" \
  --output "$BENCH_OUT" --sizes "$BENCH_SIZES" --runs "$BENCH_RUNS" \
  --apis "$BENCH_APIS" --patterns "$BENCH_PATTERNS" --adapters "$BENCH_ADAPTERS"
python3 scripts/summarize_sqlite_turso_local_comparison.py \
  --input "$BENCH_OUT/raw-results.csv" \
  --fixtures "$BENCH_OUT/fixture-results.csv" \
  --output-dir "$BENCH_OUT" --sizes "$BENCH_SIZES" --runs "$BENCH_RUNS"
```

Capture `git rev-parse HEAD`, scoped dirty state, `rustc -Vv`, `cargo -V`, `uname`, CPU/memory details available on the platform, filesystem/volume information, exact command/configuration, dependency tree, and timestamps. Do not print environment credentials.

- [ ] **Step 3: Document reproduction and interpretation**

`docs/sqlite-turso-local-performance.md` must include smoke/full/resume commands, output schemas, ratio direction, cache state, durability caveat, async-versus-sync caveat, local-only scope, expected 432/36 row counts, disk/time controls, and instructions for retaining a failed fixture.

- [ ] **Step 4: Verify Task 7**

```sh
python3 -m unittest scripts.tests.test_run_sqlite_turso_local_comparison -v
if command -v shellcheck >/dev/null 2>&1; then shellcheck scripts/run_sqlite_turso_local_comparison.sh; fi
BENCH_PROFILE=smoke BENCH_OUT=target/sqlite-turso-driver-smoke scripts/run_sqlite_turso_local_comparison.sh
```

Expected: black-box tests pass, ShellCheck is clean when installed, and the real local smoke produces a valid report without network access.

- [ ] **Step 5: Checkpoint the task**

Attempt a scoped commit named `bench: automate SQLite Turso comparison`; preserve the worktree if the unrelated cherry-pick prevents commits.

---

### Task 8: Review, Verify, Run the Full Matrix, and Publish Results

**Files:**
- Review: `benchmarks/sqlite-turso-local/**`
- Review: `scripts/run_sqlite_turso_local_comparison.sh`
- Review: `scripts/summarize_sqlite_turso_local_comparison.py`
- Create: `performance-results/sqlite-turso-local-2026-07-18/raw-results.csv`
- Create: `performance-results/sqlite-turso-local-2026-07-18/fixture-results.csv`
- Create: `performance-results/sqlite-turso-local-2026-07-18/summary.csv`
- Create: `performance-results/sqlite-turso-local-2026-07-18/report.md`
- Create: `performance-results/sqlite-turso-local-2026-07-18/machine.txt`
- Create: `performance-results/sqlite-turso-local-2026-07-18/run-manifest.txt`

**Interfaces:**
- Consumes all preceding benchmark, driver, and summarizer interfaces.
- Produces a complete evidence-backed comparison report with 432 validated raw rows and 36 validated fixture rows.

- [ ] **Step 1: Run an independent code and methodology review**

Request review for workload identity, timed-region boundaries, adapter-native API use, fixture isolation, validation strength, resume correctness, accidental network/sync enablement, unsafe cleanup paths, ratio direction, and claims supported by data. Fix every Critical and Important finding.

- [ ] **Step 2: Run the complete preflight verification**

```sh
cargo fmt --manifest-path benchmarks/sqlite-turso-local/Cargo.toml -- --check
cargo test --manifest-path benchmarks/sqlite-turso-local/Cargo.toml
cargo clippy --manifest-path benchmarks/sqlite-turso-local/Cargo.toml --all-targets -- -D warnings
cargo build --release --manifest-path benchmarks/sqlite-turso-local/Cargo.toml
python3 -m unittest scripts.tests.test_summarize_sqlite_turso_local_comparison scripts.tests.test_run_sqlite_turso_local_comparison -v
BENCH_PROFILE=smoke BENCH_OUT=target/sqlite-turso-final-smoke scripts/run_sqlite_turso_local_comparison.sh
```

Expected: all commands pass, smoke has 24 validated raw rows and 2 validated fixtures, and the dependency audit shows no Turso sync feature.

- [ ] **Step 3: Record a quiet-machine baseline and start the resumable full run**

Close unrelated high-load processes where permitted, keep the machine on power, and run serially:

```sh
BENCH_OUT=performance-results/sqlite-turso-local-2026-07-18 \
BENCH_SIZES=10000,50000,100000,500000,1000000,2000000 \
BENCH_RUNS=3 \
BENCH_APIS=put,batch,diff,merge \
BENCH_PATTERNS=append,random,clustered \
BENCH_ADAPTERS=sqlite-sync,turso-async \
scripts/run_sqlite_turso_local_comparison.sh
```

Expected: the command may run for hours; incremental rows and manifest updates make it safe to resume with the identical command after interruption.

- [ ] **Step 4: Validate matrix completeness and regenerate summaries**

```sh
python3 scripts/summarize_sqlite_turso_local_comparison.py \
  --input performance-results/sqlite-turso-local-2026-07-18/raw-results.csv \
  --fixtures performance-results/sqlite-turso-local-2026-07-18/fixture-results.csv \
  --output-dir performance-results/sqlite-turso-local-2026-07-18 \
  --sizes 10000,50000,100000,500000,1000000,2000000 --runs 3
```

Expected: exactly 432 validated raw rows, 36 validated fixtures, 72 summary rows, no failed/missing cells, and regenerated files are byte-stable.

- [ ] **Step 5: Audit the final report against raw evidence**

Manually recompute at least one small and one 2M scenario ratio from raw rows, confirm percentile direction for individual put, confirm every limitation is present, and ensure the report distinguishes observation from causal inference.

- [ ] **Step 6: Run final scoped hygiene checks**

```sh
git diff --check -- benchmarks/sqlite-turso-local scripts/run_sqlite_turso_local_comparison.sh scripts/summarize_sqlite_turso_local_comparison.py scripts/tests/test_run_sqlite_turso_local_comparison.py scripts/tests/test_summarize_sqlite_turso_local_comparison.py docs/sqlite-turso-local-performance.md docs/superpowers/specs/2026-07-18-sqlite-turso-local-performance-comparison-design.md docs/superpowers/plans/2026-07-18-sqlite-turso-local-performance-comparison.md
```

Expected: no whitespace errors in scoped files. A broad repository diff may still report the unrelated pre-existing conflict and must not be described as clean.

- [ ] **Step 7: Checkpoint the completed work when Git permits**

After the user resolves the unrelated cherry-pick, commit benchmark code and generated evidence in intentional commits. Until then, do not run cherry-pick continuation, conflict resolution, reset, checkout, or cleanup commands on the user's behalf.

