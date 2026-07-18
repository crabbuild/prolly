# SQLite-Backed Prolly Key-Pattern Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a reproducible end-to-end `Prolly<SqliteStore>` benchmark for append, random, and clustered writes and reads at 10K, 50K, 100K, 500K, and 1M records with 24-byte keys and 100-byte values.

**Architecture:** Add a standalone Rust benchmark crate so the new experiment does not change the historical SQLite harness. Separate deterministic workload modeling, durable CSV measurement, closed-fixture management, SQLite/prolly execution, matrix orchestration, and reporting into focused modules; drive them through one release binary and a provenance-capturing shell wrapper.

**Tech Stack:** Rust 2021, `prolly-map`, `prolly-store-sqlite`, `rusqlite`, `serde`, `csv`, `tempfile`, POSIX shell, Python 3 standard library.

## Global Constraints

- Measure end-to-end synchronous `Prolly<SqliteStore>`, not raw SQLite.
- Use base sizes `10000,50000,100000,500000,1000000` and three repetitions.
- Use exactly 24-byte keys and exactly 100-byte values.
- Use file-backed SQLite defaults: WAL, `synchronous=NORMAL`, 5,000 ms busy timeout, and `temp_store=MEMORY`.
- Use one-percent mutation samples clamped to 100 through 10,000.
- Label manager cache state explicitly and state that OS filesystem cache is uncontrolled.
- Validate every successful result and reopen every persisted mutation result.
- Do not alter or overwrite unrelated dirty-worktree files.

---

### Task 1: Deterministic Workload Model and CLI

**Files:**
- Create: `benchmarks/sqlite-prolly-patterns/Cargo.toml`
- Create: `benchmarks/sqlite-prolly-patterns/src/lib.rs`
- Create: `benchmarks/sqlite-prolly-patterns/src/model.rs`
- Create: `benchmarks/sqlite-prolly-patterns/src/cli.rs`
- Create: `benchmarks/sqlite-prolly-patterns/tests/model_cli.rs`

**Interfaces:**
- Consumes: public `prolly-map` and `prolly-store-sqlite` path dependencies.
- Produces: `RunConfig`, `Operation`, `Pattern`, `CacheState`, `CellSpec`, `key`, `value`, `mutation_ids`, `read_ids`, `range_bounds`, `change_count`, and `parse_args`.

- [ ] **Step 1: Write failing model and CLI tests**

```rust
use prolly_sqlite_pattern_bench::cli::parse_args;
use prolly_sqlite_pattern_bench::model::{
    change_count, key, mutation_ids, value, Pattern,
};

#[test]
fn record_widths_and_samples_are_exact() {
    assert_eq!(key(42).len(), 24);
    assert_eq!(value(42, 0).len(), 100);
    assert_eq!(value(42, 1).len(), 100);
    assert_ne!(value(42, 0), value(42, 1));
    assert_eq!(change_count(10_000), 100);
    assert_eq!(change_count(50_000), 500);
    assert_eq!(change_count(1_000_000), 10_000);
}

#[test]
fn patterns_are_deterministic_and_semantically_distinct() {
    let random = mutation_ids(Pattern::Random, 10_000, 100);
    assert_eq!(random, mutation_ids(Pattern::Random, 10_000, 100));
    assert_eq!(random.len(), 100);
    assert!(random.iter().all(|id| *id < 10_000));
    assert_eq!(mutation_ids(Pattern::Append, 10_000, 3), vec![10_000, 10_001, 10_002]);
    assert_eq!(mutation_ids(Pattern::Clustered, 10_000, 3), vec![4_998, 4_999, 5_000]);
}

#[test]
fn smoke_profile_can_be_selected() {
    let config = parse_args(["bench", "--profile", "smoke", "--output", "/tmp/sqlite-pattern-smoke"]).unwrap();
    assert_eq!(config.sizes, vec![100]);
    assert_eq!(config.runs, 1);
    assert_eq!(config.explicit_operations, Some(10));
}
```

- [ ] **Step 2: Run tests and verify the crate is absent**

Run: `cargo test --manifest-path benchmarks/sqlite-prolly-patterns/Cargo.toml --test model_cli`

Expected: FAIL because `benchmarks/sqlite-prolly-patterns/Cargo.toml` does not exist.

- [ ] **Step 3: Create the crate and deterministic model**

Implement these exact public shapes:

```rust
pub const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
pub const FULL_SIZES: &[usize] = &[10_000, 50_000, 100_000, 500_000, 1_000_000];

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub enum Operation { Put, Batch, PointRead, RangeScan }

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub enum Pattern { Append, Random, Clustered }

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub enum CacheState { NotApplicable, ColdManager, WarmManager }

pub fn key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

pub fn value(id: usize, generation: u8) -> Vec<u8> {
    let prefix = format!("value-{id:020}-{generation:02}-");
    let mut bytes = prefix.into_bytes();
    bytes.resize(100, b'x');
    bytes
}

pub fn change_count(records: usize) -> usize {
    (records / 100).clamp(100, 10_000).min(records)
}
```

Use fixed-seed xorshift sampling without replacement for random IDs. For point reads, map append to the existing right edge `records-count..records`, random to unique seeded IDs, and clustered to a centered interval. For range scans, return half-open key bounds and exact expected IDs for a contiguous random-start, centered, or right-edge interval.

Implement `RunConfig::full`, `RunConfig::smoke`, validation, and argument parsing for `--profile`, `--output`, `--sizes`, `--runs`, `--operations`, `--keep-fixtures`, `--revision`, and `--dirty|--clean`.

- [ ] **Step 4: Run focused tests**

Run: `cargo test --manifest-path benchmarks/sqlite-prolly-patterns/Cargo.toml --test model_cli`

Expected: PASS.

- [ ] **Step 5: Commit the model if the repository index is safe**

```sh
git add benchmarks/sqlite-prolly-patterns
git commit -m "bench: model SQLite prolly key patterns"
```

Expected: commit succeeds only when no unrelated unmerged index entries exist; otherwise leave the changes uncommitted and record that limitation.

---

### Task 2: Durable Measurements, Fixture Isolation, and Aggregation

**Files:**
- Create: `benchmarks/sqlite-prolly-patterns/src/measurement.rs`
- Create: `benchmarks/sqlite-prolly-patterns/src/fixture.rs`
- Create: `benchmarks/sqlite-prolly-patterns/src/report.rs`
- Create: `benchmarks/sqlite-prolly-patterns/tests/measurement_fixture.rs`

**Interfaces:**
- Consumes: `Operation`, `Pattern`, `CacheState`, and `RunConfig` from Task 1.
- Produces: `RawRow`, `FixtureRow`, `CellKey`, `CsvSink<T>`, `nearest_rank`, `FixtureLayout`, `clone_fixture`, `directory_bytes`, `remove_generated_dir`, `summarize`, and `write_report`.

- [ ] **Step 1: Write failing measurement and fixture tests**

```rust
use prolly_sqlite_pattern_bench::fixture::{clone_fixture, directory_bytes};
use prolly_sqlite_pattern_bench::measurement::nearest_rank;

#[test]
fn nearest_rank_uses_one_based_ceiling() {
    assert_eq!(nearest_rank(&[10, 20, 30, 40, 50], 0.50), Some(30));
    assert_eq!(nearest_rank(&[10, 20, 30, 40, 50], 0.95), Some(50));
    assert_eq!(nearest_rank(&[], 0.99), None);
}

#[test]
fn closed_fixture_clone_copies_all_regular_files() {
    let temp = tempfile::tempdir().unwrap();
    let source = temp.path().join("source");
    let destination = temp.path().join("destination");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("prolly.db"), b"database").unwrap();
    std::fs::write(source.join("prolly.db-wal"), b"wal").unwrap();
    clone_fixture(&source, &destination).unwrap();
    assert_eq!(directory_bytes(&destination).unwrap(), 11);
}
```

- [ ] **Step 2: Verify the new tests fail**

Run: `cargo test --manifest-path benchmarks/sqlite-prolly-patterns/Cargo.toml --test measurement_fixture`

Expected: FAIL with unresolved `fixture` and `measurement` modules.

- [ ] **Step 3: Implement rows, durable CSV, and safe fixture helpers**

Define `RawRow` with schema/revision/dirty provenance; records, repetition, operation, pattern, and cache state; configured/observed operations; total/mean/percentile latency; throughput; complete prolly metric counters; tree shape; DB/WAL/SHM/total bytes; expected/observed entries; validation; and error. Define `FixtureRow` with build latency, throughput, tree shape, database bytes, and validation.

`CsvSink::append` must call `flush()` and `sync_data()`. `clone_fixture` must reject symlinks and existing destinations. `remove_generated_dir` must require a descendant of the explicit benchmark `fixtures/` or `cells/` root before recursively deleting.

- [ ] **Step 4: Implement aggregation and Markdown reporting**

Group validated raw rows by `(records, operation, pattern, cache_state)`, require exactly `runs` rows per group, sort total latency and throughput, and emit median/min/max values. Write `summary.csv` and a compact `report.md` with mutation throughput, point-read latency/throughput, range-scan throughput, fixture build rate/size, machine scope, and all interpretation limits from the design.

- [ ] **Step 5: Run focused tests**

Run: `cargo test --manifest-path benchmarks/sqlite-prolly-patterns/Cargo.toml --test measurement_fixture`

Expected: PASS.

- [ ] **Step 6: Commit if safe**

```sh
git add benchmarks/sqlite-prolly-patterns/src benchmarks/sqlite-prolly-patterns/tests
git commit -m "bench: add durable SQLite pattern measurements"
```

---

### Task 3: SQLite Fixture and Workload Runner

**Files:**
- Create: `benchmarks/sqlite-prolly-patterns/src/sqlite_runner.rs`
- Create: `benchmarks/sqlite-prolly-patterns/tests/sqlite_smoke.rs`

**Interfaces:**
- Consumes: workload generators, row types, and fixture paths from Tasks 1 and 2.
- Produces: `build_fixture(spec, layout) -> Result<FixtureRow, String>` and `run_cell(spec, layout) -> Result<RawRow, String>`.

- [ ] **Step 1: Write a failing end-to-end smoke test**

```rust
use prolly_sqlite_pattern_bench::fixture::FixtureLayout;
use prolly_sqlite_pattern_bench::model::{enumerate_cells, FixtureSpec, RunConfig};
use prolly_sqlite_pattern_bench::sqlite_runner::{build_fixture, run_cell};

#[test]
fn every_smoke_cell_validates() {
    let temp = tempfile::tempdir().unwrap();
    let config = RunConfig::smoke(temp.path().to_path_buf());
    let layout = FixtureLayout::new(config.output.clone(), 100, 1);
    let fixture = build_fixture(&FixtureSpec::from_config(&config, 100, 1), &layout).unwrap();
    assert!(fixture.validated);
    for cell in enumerate_cells(&config, 100, 1) {
        layout.clone_for(&cell).unwrap();
        let row = run_cell(&cell, &layout).unwrap();
        assert!(row.validated, "{cell:?}");
        layout.remove_cell(&cell).unwrap();
    }
}
```

- [ ] **Step 2: Verify the smoke test fails**

Run: `cargo test --manifest-path benchmarks/sqlite-prolly-patterns/Cargo.toml --test sqlite_smoke -- --nocapture`

Expected: FAIL with unresolved `sqlite_runner`.

- [ ] **Step 3: Implement fixture construction and reopening**

Open `SqliteStore::open` in a fixture directory, create `SortedBatchBuilder<Arc<SqliteStore>>`, add `0..records` using fixed keys and generation-zero values, time only `build`, publish `b"sqlite-pattern-base"`, validate first/middle/last samples and exact `len`, close all handles, reopen, reload the named root, and validate again. Collect tree statistics after timing and file sizes after close.

- [ ] **Step 4: Implement put and batch mutations**

For `Put`, time every `Prolly::put`, retain individual nanoseconds, and carry the returned tree forward. For `Batch`, construct `Vec<Mutation::Put>` before timing and time one `Prolly::batch`. Reset manager metrics immediately before timing. Append uses generation one at new right-edge IDs; random and clustered update generation one at existing IDs. Validate cardinality and every changed value, publish `b"sqlite-pattern-result"`, close, reopen, reload, and validate before producing a successful row.

- [ ] **Step 5: Implement cold/warm point reads and range scans**

For cold point reads, open a fresh manager and immediately time all calls. For warm reads, execute and validate one untimed pass, reset metrics, then time the same IDs. Record per-call timings. For ranges, create half-open bounds before timing, eagerly collect the complete iterator during timing, then validate exact IDs and values. Both read operations keep base-tree cardinality unchanged.

- [ ] **Step 6: Run smoke and contract tests**

Run: `cargo test --manifest-path benchmarks/sqlite-prolly-patterns/Cargo.toml --test sqlite_smoke -- --nocapture`

Expected: PASS for all 15 smoke cells: six mutation cells, six point-read cells, and three range cells.

- [ ] **Step 7: Commit if safe**

```sh
git add benchmarks/sqlite-prolly-patterns/src/sqlite_runner.rs benchmarks/sqlite-prolly-patterns/tests/sqlite_smoke.rs
git commit -m "bench: execute SQLite-backed prolly workloads"
```

---

### Task 4: Matrix Orchestration, Resume Contract, and Documentation

**Files:**
- Create: `benchmarks/sqlite-prolly-patterns/src/harness.rs`
- Create: `benchmarks/sqlite-prolly-patterns/src/main.rs`
- Create: `benchmarks/sqlite-prolly-patterns/tests/matrix.rs`
- Create: `scripts/run_sqlite_prolly_pattern_benchmark.sh`
- Create: `docs/sqlite-prolly-pattern-benchmark.md`

**Interfaces:**
- Consumes: all prior task modules.
- Produces: executable `prolly-sqlite-pattern-bench`, resumable output directory, provenance files, summary, and report.

- [ ] **Step 1: Write failing matrix cardinality tests**

```rust
use prolly_sqlite_pattern_bench::harness::enumerate_matrix;
use prolly_sqlite_pattern_bench::model::RunConfig;

#[test]
fn full_matrix_has_expected_cardinality() {
    let config = RunConfig::full("results".into(), "revision".into(), false);
    let plan = enumerate_matrix(&config);
    assert_eq!(plan.fixtures.len(), 15);
    assert_eq!(plan.cells.len(), 225);
}

#[test]
fn smoke_matrix_has_fifteen_cells() {
    let plan = enumerate_matrix(&RunConfig::smoke("smoke".into()));
    assert_eq!(plan.fixtures.len(), 1);
    assert_eq!(plan.cells.len(), 15);
}
```

- [ ] **Step 2: Verify matrix tests fail**

Run: `cargo test --manifest-path benchmarks/sqlite-prolly-patterns/Cargo.toml --test matrix`

Expected: FAIL with unresolved `harness`.

- [ ] **Step 3: Implement serial orchestration and durable status**

Enumerate repetitions outermost, reverse pattern order on even repetitions, build one fresh fixture per size/repetition, clone it for each pending cell, run serially, append and sync each row, remove generated clones unless `--keep-fixtures`, and write `run-status.txt` as `running`, `complete`, or `failed: ...`. Validate an existing manifest byte-for-byte before resuming and skip only validated exact cell keys.

- [ ] **Step 4: Implement the binary and wrapper**

`main.rs` parses CLI arguments, invokes `run_matrix`, generates summary/report on success, and exits 2 for invalid arguments or 1 for benchmark failures. The shell wrapper resolves the repository root, captures `git rev-parse HEAD` and dirty state, builds the benchmark with `CARGO_INCREMENTAL=0 --release`, records `uname`, CPU, memory, filesystem, Rust/Cargo versions, resolved dependencies, and exact command, then executes the release binary. Environment controls are `SQLITE_PATTERN_OUT`, `SQLITE_PATTERN_SIZES`, `SQLITE_PATTERN_RUNS`, and `SQLITE_PATTERN_KEEP_FIXTURES`.

- [ ] **Step 5: Document execution and interpretation**

Document the full command, smoke command, output files, 225-row expectation, exact key/value widths, mutation cardinalities, cold/warm manager meaning, SQLite durability defaults, and uncontrolled OS-cache limitation.

- [ ] **Step 6: Run tests**

Run: `cargo test --manifest-path benchmarks/sqlite-prolly-patterns/Cargo.toml`

Expected: all unit and integration tests PASS.

- [ ] **Step 7: Commit if safe**

```sh
git add benchmarks/sqlite-prolly-patterns scripts/run_sqlite_prolly_pattern_benchmark.sh docs/sqlite-prolly-pattern-benchmark.md
git commit -m "bench: orchestrate SQLite prolly performance matrix"
```

---

### Task 5: Quality Gates and Full Measurement

**Files:**
- Create: `performance-results/sqlite-prolly-patterns-2026-07-18/`
- Modify only if defects are found: `benchmarks/sqlite-prolly-patterns/`, `scripts/run_sqlite_prolly_pattern_benchmark.sh`, `docs/sqlite-prolly-pattern-benchmark.md`

**Interfaces:**
- Consumes: release benchmark from Task 4.
- Produces: verified raw data, summary, report, manifest, status, and machine provenance.

- [ ] **Step 1: Run formatting**

Run: `cargo fmt --manifest-path benchmarks/sqlite-prolly-patterns/Cargo.toml -- --check`

Expected: exit 0. If it reports differences, run the same command without `--check`, inspect the diff, and rerun the check.

- [ ] **Step 2: Run strict static analysis**

Run: `cargo clippy --manifest-path benchmarks/sqlite-prolly-patterns/Cargo.toml --all-targets -- -D warnings`

Expected: exit 0 with no warnings.

- [ ] **Step 3: Run the release smoke profile**

Run: `scripts/run_sqlite_prolly_pattern_benchmark.sh --profile smoke --output performance-results/sqlite-prolly-patterns-smoke-2026-07-18`

Expected: `run-status.txt` is `complete`, one fixture row and 15 validated raw rows exist, and the report contains no missing cells.

- [ ] **Step 4: Run the full benchmark**

Run: `scripts/run_sqlite_prolly_pattern_benchmark.sh --profile full --output performance-results/sqlite-prolly-patterns-2026-07-18`

Expected: `run-status.txt` is `complete`, 15 fixture rows and 225 validated raw rows exist, and `summary.csv` contains 75 workload groups.

- [ ] **Step 5: Independently verify artifacts**

Run:

```sh
test "$(tail -n +2 performance-results/sqlite-prolly-patterns-2026-07-18/raw-results.csv | wc -l | tr -d ' ')" = 225
test "$(tail -n +2 performance-results/sqlite-prolly-patterns-2026-07-18/fixture-results.csv | wc -l | tr -d ' ')" = 15
test "$(cat performance-results/sqlite-prolly-patterns-2026-07-18/run-status.txt)" = complete
awk -F, 'NR > 1 && $NF != "true" { bad++ } END { exit bad != 0 }' performance-results/sqlite-prolly-patterns-2026-07-18/raw-results.csv
```

Expected: every command exits 0.

- [ ] **Step 6: Review the report against raw medians**

Choose one size from each scale tier (10K, 100K, and 1M), recompute median throughput for append batch, random put, clustered point-read warm, and right-edge range from `raw-results.csv`, and confirm exact agreement with `summary.csv` and `report.md` after display rounding.

- [ ] **Step 7: Commit benchmark code and results only if safe and requested**

```sh
git add benchmarks/sqlite-prolly-patterns scripts/run_sqlite_prolly_pattern_benchmark.sh docs/sqlite-prolly-pattern-benchmark.md performance-results/sqlite-prolly-patterns-2026-07-18
git commit -m "perf: measure SQLite-backed prolly key patterns"
```

Expected: skip the commit while unrelated unmerged index entries remain; do not stage or modify those entries.
