# MySQL Performance and PostgreSQL Comparison Implementation Plan

**Goal:** Make the Rust MySQL adapter scale through bounded set-based SQL and
provide reproducible end-to-end and service-level MySQL/PostgreSQL comparisons.

**Context:** The MySQL adapter currently loops over individual SQL statements
for batch reads and writes. The PostgreSQL adapter already provides chunked
set-based operations. The repository's `backend-comparison` crate already owns
deterministic end-to-end workloads, fail-closed evidence, paired statistics,
and PostgreSQL/DynamoDB orchestration. This plan extends those proven paths
instead of building a parallel benchmark stack.

**Execution style:** Implementation first with context. Production interfaces
and shared harness paths are implemented before focused regression and
integration tests are added. Existing conformance tests remain the semantic
backstop.

## 1. Harden `prolly-store-mysql`

**Files:**

- Modify `stores/prolly-store-mysql/src/lib.rs`
- Modify `stores/prolly-store-mysql/Cargo.toml`
- Modify `stores/prolly-store-mysql/tests/mysql_backend.rs`
- Modify `stores/prolly-store-mysql/README.md`

**Changes:**

- Add `MySqlBackendOptions`, option-aware constructors, and a 1,000-item
  default batch limit.
- Use SQLx `QueryBuilder` for bounded multi-row node/root upserts and deletes.
- Fetch ordered batch reads with bounded `IN` queries and reconstruct order,
  duplicates, and missing values client-side.
- Reduce repeated node and root operations with last-write-wins semantics.
- Add `prolly_root_locks`; acquire sorted lock identities before every root
  mutation or conditional publication.
- Keep every multi-chunk public operation atomic.
- Document batch and pool tuning plus the internal lock table.

## 2. Add MySQL to the shared comparison runner

**Files:**

- Modify `benchmarks/backend-comparison/Cargo.toml`
- Modify `benchmarks/backend-comparison/src/adapters.rs`
- Modify `benchmarks/backend-comparison/src/cli.rs`
- Modify `benchmarks/backend-comparison/src/evidence.rs`
- Modify `benchmarks/backend-comparison/src/runner.rs`
- Modify `benchmarks/backend-comparison/src/summary.rs`
- Add `benchmarks/backend-comparison/src/bin/mysql.rs`
- Add `benchmarks/backend-comparison/tests/mysql_smoke.rs`

**Changes:**

- Add `Backend::MySql` and MySQL connection parsing.
- Connect, initialize, clear, and run MySQL through the same generic public
  operation runner as PostgreSQL.
- Let summarization accept an explicit backend pair while retaining the
  PostgreSQL/DynamoDB default.
- Preserve the result schema's meaning and exact logical-outcome validation.

## 3. Add the SQL service-scale suite

**Files:**

- Add focused service modules under `benchmarks/backend-comparison/src/service/`
- Add service configuration files under
  `benchmarks/backend-comparison/workloads/`
- Add service runner binaries or a backend-selecting service binary

**Changes:**

- Share request generation, client/pool cells, tenant selection, operation
  mixes, HDR latency histograms, and validation between SQL backends.
- Sweep adapter batch size, logical batch size, clients, pool size, tenant
  count, and hot-root share.
- Record throughput, p50/p95/p99/p99.9/max, conflicts, retries, errors, Prolly
  counters, and available database diagnostics.
- Keep backend-specific code limited to pools, resets, and diagnostics.

## 4. Add controlled and external orchestration

**Files:**

- Add `scripts/run_mysql_postgres_comparison.sh`
- Add `scripts/tests/test_run_mysql_postgres_comparison.py`
- Extend `benchmarks/backend-comparison/docker-compose.yml`
- Update `benchmarks/BACKEND_COMPARISON.md`

**Changes:**

- Use pinned MySQL 8 and PostgreSQL 16 images with fresh local volumes.
- Copy and hash release runners, alternate backend order, exclude warmups, and
  require seven measured repetitions.
- Add explicit external mode with disposable-database acknowledgement.
- Redact URL credentials from captured commands and reports.
- Fail closed on dirty source, existing output, incomplete runs, or mixed
  evidence.

## 5. Verify and compare

**Commands:**

- `cargo fmt --all -- --check`
- `cargo test --manifest-path stores/prolly-store-mysql/Cargo.toml`
- `cargo clippy --manifest-path stores/prolly-store-mysql/Cargo.toml --all-targets -- -D warnings`
- `cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml`
- `python3 -m unittest scripts.tests.test_run_backend_comparison scripts.tests.test_run_mysql_postgres_comparison`
- Docker-backed MySQL adapter conformance and concurrency tests
- Controlled smoke comparison through `scripts/run_mysql_postgres_comparison.sh`

After the smoke matrix validates, run a larger local profile and save its raw
evidence and generated report. Interpret results only for the captured machine,
Docker allocation, database configuration, adapter revision, and workload.
