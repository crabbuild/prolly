# PostgreSQL Prolly Scale Performance Verification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a reproducible end-to-end PostgreSQL-backed Prolly benchmark at 1M and 10M logical records and produce validated per-operation metrics and a report.

**Architecture:** A standalone Rust benchmark crate generates deterministic fixtures and invokes the public async Prolly/PostgreSQL APIs. PostgreSQL fixture snapshot tables restore an equivalent base before every cell, while `pg_stat_statements` and Prolly metrics capture database-side and tree-side work. A shell runner owns Docker/provenance and a Python summarizer strictly validates and aggregates durable CSV rows.

**Tech Stack:** Rust 2021, Tokio, SQLx 0.8, `prolly-map`, `prolly-store-postgres`, Serde/CSV, PostgreSQL 16 with `pg_stat_statements`, Docker Compose, Bash, Python 3 standard library.

## Global Constraints

- Measure 1,000,000 and 10,000,000 records.
- Use 24-byte keys, 27-byte values, fixed seed `0x6a09e667f3bcc909`, and 10,000-key multi-key changes.
- Measure the current production code without modifying core Prolly or `prolly-store-postgres` behavior.
- Run a single client serially with `Config::default()` and the adapter's current SQLx defaults.
- Stop when free disk is below 3 GiB.
- Do not stage, reset, commit, or otherwise alter the checkout's pre-existing user changes or unresolved merge entry.
- All generated service state must use the dedicated `prolly-postgres-scale-bench` Compose project.

---

### Task 1: Deterministic workload model

**Files:**
- Create: `benchmarks/postgres-scale/Cargo.toml`
- Create: `benchmarks/postgres-scale/src/lib.rs`
- Create: `benchmarks/postgres-scale/src/model.rs`

**Interfaces:**
- Produces: `Pattern::{Append,Random,Clustered}`, `Operation`, `RunConfig`, `CellSpec`, `key(usize) -> Vec<u8>`, `value(usize,u8) -> Vec<u8>`, `pattern_ids(records,count,pattern,salt) -> Vec<usize>`, `merge_ids(...) -> (Vec<usize>,Vec<usize>)`.

- [ ] **Step 1: Add failing model tests**

  Tests assert exact key/value widths, lexicographic key order, 10,000 changes at both requested sizes, deterministic unique random IDs, centered contiguous clustered IDs, tail append IDs, and disjoint merge branches.

- [ ] **Step 2: Run the focused tests and confirm RED**

  Run: `cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml model::tests`

  Expected: compile failure because model functions and types do not exist.

- [ ] **Step 3: Implement the model with stable xorshift generation**

  Use the exact public signatures above. Random generation inserts IDs into a `BTreeSet` and returns stable ascending order for canonical batch inputs; a salt distinguishes left/right and operation inputs without changing the frozen seed.

- [ ] **Step 4: Run the focused tests and confirm GREEN**

  Run: `cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml model::tests`

  Expected: all model tests pass.

### Task 2: Durable measurement schema

**Files:**
- Create: `benchmarks/postgres-scale/src/measurement.rs`

**Interfaces:**
- Consumes: `Pattern`, `Operation`, `prolly::ProllyMetricsSnapshot`.
- Produces: `SCHEMA_VERSION`, `RawRow`, `PgMetrics`, `PhysicalSize`, `percentile(&[u128],f64)`, `CsvSink::append`, `CellKey`.

- [ ] **Step 1: Add failing measurement tests**

  Cover nearest-rank p50/p95/p99/max, empty samples, CSV round trips with commas/newlines, duplicate cell keys, finite throughput, and `total_ns / logical_operations` consistency.

- [ ] **Step 2: Run and confirm RED**

  Run: `cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml measurement::tests`

- [ ] **Step 3: Implement serializable rows and fsynced append-only CSV output**

  `RawRow` includes all fields frozen in the design: provenance, workload key, timing, Prolly counters, tree shape, PostgreSQL counters, sizes, validation, and error. `CsvSink::append` flushes and calls `sync_data()` after every row.

- [ ] **Step 4: Run and confirm GREEN**

  Run: `cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml measurement::tests`

### Task 3: PostgreSQL fixture and statistics controller

**Files:**
- Create: `benchmarks/postgres-scale/src/postgres.rs`

**Interfaces:**
- Consumes: `PostgresBackend::pool()`, dedicated benchmark database connection.
- Produces: `initialize_benchmark_schema`, `clear_all`, `snapshot_base`, `restore_base`, `reset_pg_stats`, `read_pg_metrics`, `read_physical_size`, `postgres_metadata`.

- [ ] **Step 1: Add ignored Docker integration tests**

  Given `PROLLY_STORE_POSTGRES_URL`, initialize the adapter schema and extension, insert sample node/hint/root rows, snapshot, mutate, restore, and assert byte-identical base tables. Reset statement statistics, query a Prolly table, and assert calls/block counters can be read.

- [ ] **Step 2: Run unit tests, then the ignored test against no service to verify its explicit skip/failure contract**

  Run: `cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml`

- [ ] **Step 3: Implement transactional snapshot/restore and metrics queries**

  Snapshot tables are `UNLOGGED` and live in schema `prolly_bench`. Restore uses one transaction with `TRUNCATE` plus `INSERT ... SELECT`, then `ANALYZE`. Statistics filter statement text for the three production Prolly tables and exclude benchmark metadata queries.

- [ ] **Step 4: Start the dedicated PostgreSQL service and confirm GREEN**

  Run: `PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55433/prolly cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml postgres::tests::snapshot_restore_round_trip -- --ignored`

### Task 4: End-to-end workload runners

**Files:**
- Create: `benchmarks/postgres-scale/src/workloads.rs`

**Interfaces:**
- Consumes: `CellSpec`, `PostgresBackend`, restored named root `benchmark/base`, deterministic model inputs.
- Produces: `build_fixture(...) -> Fixture`, `run_cell(...) -> RawRow`, operation-specific validation helpers.

- [ ] **Step 1: Add failing 1,000-record async tests for each operation**

  Tests cover build/reopen, one put for every pattern, 100-key batch, cold/warm get, `get_many`, tail/center range scan, full scan, exact diff keys, and disjoint merge values/count.

- [ ] **Step 2: Run the focused Docker tests and confirm RED**

  Run: `PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55433/prolly cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml workloads::tests -- --ignored --test-threads=1`

- [ ] **Step 3: Implement build and read operations**

  Build pre-generates mutations, resets statistics, times only `batch`, captures metrics immediately, then publishes/reopens and scans to validate exact count/order/checksum. Get records individual call samples; query times one `get_many`; scans consume iterators incrementally.

- [ ] **Step 4: Implement put, batch, diff, and merge operations**

  Every cell restores base first, opens a fresh manager, resets metrics/statistics immediately before its timed API, captures both immediately afterward, and validates outside the timer. Diff compares exact key sets; merge checks both branch sets and unchanged samples.

- [ ] **Step 5: Run the focused Docker tests and confirm GREEN**

  Run: `PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55433/prolly cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml workloads::tests -- --ignored --test-threads=1`

### Task 5: Matrix orchestration and resumability

**Files:**
- Create: `benchmarks/postgres-scale/src/cli.rs`
- Create: `benchmarks/postgres-scale/src/harness.rs`
- Create: `benchmarks/postgres-scale/src/main.rs`

**Interfaces:**
- Consumes: `RunConfig`, workload runners, `CsvSink`.
- Produces: executable `prolly-postgres-scale-bench` with `--profile smoke|full`, `--sizes`, `--runs`, `--operations`, `--patterns`, `--output`, `--revision`, `--dirty|--clean`, `--min-free-gb`, and `--url`.

- [ ] **Step 1: Add failing CLI/matrix tests**

  Assert the smoke profile is 1,000 records/100 changes/one run; full is 1M and 10M/10,000 changes/three runs; build/full-scan occur once; inapplicable scan/random cells are absent; cell keys are unique; resume skips only validated exact keys.

- [ ] **Step 2: Run and confirm RED**

  Run: `cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml cli::tests harness::tests`

- [ ] **Step 3: Implement CLI and serial matrix**

  The harness checks free space between cells, rotates pattern order by repetition, persists each row immediately, preserves partial output on failure, and refuses incompatible resume manifests.

- [ ] **Step 4: Run and confirm GREEN**

  Run: `cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml`

### Task 6: Docker runner and strict report generator

**Files:**
- Create: `benchmarks/postgres-scale/docker-compose.yml`
- Create: `scripts/run_postgres_scale_benchmark.sh`
- Create: `scripts/summarize_postgres_scale_benchmark.py`
- Create: `scripts/tests/test_summarize_postgres_scale_benchmark.py`

**Interfaces:**
- Runner produces all provenance files and invokes the release executable.
- Summarizer consumes `raw-results.csv` and `run-manifest.txt`, validates the required matrix, and produces `summary.csv` and `report.md`.

- [ ] **Step 1: Add failing Python summarizer tests**

  Test median/min/max aggregation, one-sample labeling, throughput arithmetic, missing/duplicate/failed rows, inapplicable cells, and compact Markdown tables.

- [ ] **Step 2: Run and confirm RED**

  Run: `python3 -m unittest scripts.tests.test_summarize_postgres_scale_benchmark -v`

- [ ] **Step 3: Implement Compose, runner, and summarizer**

  Compose binds `127.0.0.1:55433`, starts PostgreSQL with `shared_preload_libraries=pg_stat_statements`, and uses a named volume scoped by project. The runner waits for health, builds once in release mode, records SHA-256/dependencies/machine/PostgreSQL settings, invokes the harness, runs the strict summarizer, and tears down only on explicit `BENCH_CLEANUP=1`.

- [ ] **Step 4: Run tests and shell syntax checks**

  Run: `python3 -m unittest scripts.tests.test_summarize_postgres_scale_benchmark -v`

  Run: `bash -n scripts/run_postgres_scale_benchmark.sh`

  Run: `docker compose -p prolly-postgres-scale-bench -f benchmarks/postgres-scale/docker-compose.yml config`

### Task 7: Verification runs and performance report

**Files:**
- Create: `performance-results/postgres-scale-2026-07-18/**`
- Create: `docs/postgres-scale-performance.md`

**Interfaces:**
- Consumes: verified release binary and dedicated PostgreSQL service.
- Produces: smoke evidence, full raw data, strict summary, and user-facing conclusions.

- [ ] **Step 1: Run formatting, tests, lint, and release build**

  Run: `cargo fmt --manifest-path benchmarks/postgres-scale/Cargo.toml -- --check`

  Run: `cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml`

  Run: `cargo clippy --manifest-path benchmarks/postgres-scale/Cargo.toml --all-targets -- -D warnings`

  Run: `cargo build --release --manifest-path benchmarks/postgres-scale/Cargo.toml`

- [ ] **Step 2: Run the complete Docker smoke profile**

  Run: `BENCH_PROFILE=smoke BENCH_OUT=performance-results/postgres-scale-smoke-2026-07-18 scripts/run_postgres_scale_benchmark.sh`

  Expected: every workload validates and the strict summarizer reports a complete smoke matrix.

- [ ] **Step 3: Run 1M and 10M full profiles with durable output**

  Run: `BENCH_PROFILE=full BENCH_OUT=performance-results/postgres-scale-2026-07-18 scripts/run_postgres_scale_benchmark.sh`

  Expected: all required rows validate or the partial output and failure reason remain durable and are reported honestly.

- [ ] **Step 4: Independently regenerate and validate the report**

  Run: `python3 scripts/summarize_postgres_scale_benchmark.py --input performance-results/postgres-scale-2026-07-18/raw-results.csv --manifest performance-results/postgres-scale-2026-07-18/run-manifest.txt --output-dir performance-results/postgres-scale-2026-07-18`

- [ ] **Step 5: Write the checked-in methodology/results document**

  Copy only validated aggregates into `docs/postgres-scale-performance.md`, link raw evidence, state sample counts and all reporting limits, and highlight observed algorithmic behavior without extrapolating beyond the measured machine.

- [ ] **Step 6: Run fresh final verification**

  Re-run Task 7 Step 1 plus the strict summarizer, inspect `git diff --check`, verify every required result row has `validated=true`, and report exact commands/output counts before claiming completion.
