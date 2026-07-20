# PostgreSQL 1M / 30% Prolly Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing PostgreSQL scale harness to separate 300,000-key mutation workloads from 10,000-key read samples, then run and report a validated 1M-record baseline.

**Architecture:** Keep the standalone async Rust benchmark and its Docker PostgreSQL fixture isolation. Add one independent read-cardinality configuration, define merge cardinality as a configured total split evenly across disjoint branches, and preserve strict append-only raw results plus Python aggregation.

**Tech Stack:** Rust 2021, Tokio, SQLx 0.8, `prolly-map`, `prolly-store-postgres`, PostgreSQL 16, Docker Compose, Bash, Python 3 standard library.

## Global Constraints

- Initial size is exactly 1,000,000 records.
- Mutation total is exactly 300,000 keys; merge branches are 150,000 + 150,000.
- Read and bounded-scan sample size is exactly 10,000.
- Use append, random, and clustered patterns with seed `0x6a09e667f3bcc909`.
- Use one serial client and three repetitions except one-shot build/full scan.
- Write all benchmark artifacts below `performance-results/postgres/baseline`.
- Preserve unrelated working-tree changes and do not alter core Prolly behavior.

---

### Task 1: Separate read and mutation cardinality

**Files:**
- Modify: `benchmarks/postgres-scale/src/cli.rs`
- Modify: `benchmarks/postgres-scale/src/harness.rs`
- Modify: `benchmarks/postgres-scale/src/workloads.rs`

**Interfaces:**
- `RunConfig::read_samples: usize`
- CLI option `--read-samples N`
- `CellSpec::read_samples: usize`

- [ ] Add CLI and matrix tests asserting `changes=300`, `read_samples=100`, and merge branch changes of 150 in a 1,000-record baseline-shaped configuration.
- [ ] Run `cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml cli::tests harness::tests` and confirm the new assertions fail because the fields/options are absent.
- [ ] Implement parsing, validation, and propagation of `read_samples`.
- [ ] Route get, query, and bounded scan ID generation and logical-operation counts through `read_samples`; keep batch/diff on `changes`.
- [ ] Split merge's configured total evenly, reject odd merge totals, and retain exact validation of both branches.
- [ ] Re-run the focused tests and confirm they pass.

### Task 2: Freeze runner provenance and reporting

**Files:**
- Modify: `scripts/run_postgres_scale_benchmark.sh`
- Modify: `scripts/summarize_postgres_scale_benchmark.py`
- Modify: `scripts/tests/test_summarize_postgres_scale_benchmark.py`

**Interfaces:**
- Environment variable `BENCH_READ_SAMPLES`
- Manifest keys `read_samples` and `merge_changes_semantics`

- [ ] Add a summarizer test that requires the report to state mutation count, read sample count, and total merge semantics from the manifest.
- [ ] Run `python3 -m unittest scripts.tests.test_summarize_postgres_scale_benchmark -v` and confirm the new assertion fails.
- [ ] Add `BENCH_READ_SAMPLES`, manifest capture, and `--read-samples` forwarding to the runner.
- [ ] Pass manifest metadata into report rendering and document the workload cardinalities and limitations.
- [ ] Re-run Python tests and `bash -n scripts/run_postgres_scale_benchmark.sh`.

### Task 3: Verify the harness against Docker PostgreSQL

**Files:**
- Test: existing Rust/Python test suites and Docker Compose configuration

- [ ] Run all Rust unit tests with `cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml`.
- [ ] Run all summarizer unit tests.
- [ ] Validate Compose with `docker compose -p prolly-postgres-scale-bench -f benchmarks/postgres-scale/docker-compose.yml config`.
- [ ] Run a Docker-backed smoke matrix at 1,000 records, 300 mutations, 100 read samples, and three repetitions.
- [ ] Strictly summarize the smoke output and confirm every row is validated.

### Task 4: Execute the 1M baseline

**Files:**
- Create: `performance-results/postgres/baseline/*`

- [ ] Confirm sufficient disk, Docker resources, revision/dirty status, and no competing benchmark container.
- [ ] Run the release harness with `BENCH_SIZES=1000000`, `BENCH_RUNS=3`, `BENCH_CHANGES=300000`, `BENCH_READ_SAMPLES=10000`, and the full operation/pattern matrix.
- [ ] Monitor durable output, host load, disk, PostgreSQL health, and validation failures; resume only with identical configuration if interrupted.
- [ ] Run the strict summarizer and verify the exact matrix with no failed or duplicate rows.

### Task 5: Produce and verify the baseline handoff

**Files:**
- Modify: `performance-results/postgres/baseline/report.md`
- Create: `performance-results/postgres/baseline/README.md`

- [ ] Add a concise interpretation section identifying dominant costs, pattern sensitivity, PostgreSQL-vs-client time, tree I/O, and limitations without changing raw measurements.
- [ ] Document exact rerun commands for 1M and parameterized future 5M/10M runs.
- [ ] Verify CSV row counts, validation flags, manifest values, timestamps, hashes, and all expected artifact files.
- [ ] Re-run Rust tests, Python tests, shell syntax, Compose config, and strict summarization immediately before reporting completion.
