# Trustworthy backend benchmark implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a fail-closed, statistically defensible PostgreSQL and DynamoDB Local comparison and replace the invalid 10M result.

**Architecture:** A pure Rust workload crate generates byte-identical fixtures and expected outcomes. A second Rust crate owns one generic Prolly runner, thin backend connection binaries, evidence validation, statistics, and report generation. A shell orchestrator starts pinned local services, alternates backend order, records provenance, and invokes the Rust summarizer.

**Tech Stack:** Rust 2021, Tokio, Prolly async engine, PostgreSQL through SQLx, DynamoDB Local through the AWS Rust SDK, CSV, Serde, SHA-256, Docker Compose, Bash

## Global constraints

- Use contract version `backend-workload-v1`
- Use result schema `backend-comparison-v1`
- Use timed-scope version `public-prolly-operation-v1`
- Require one excluded warm-up and at least seven measured repetitions
- Reject dirty tracked worktrees, existing output directories, mixed provenance, missing rows, duplicate rows, and validation mismatches
- Generate identical keys, values, mutation order, query order, merge branches, and expected results for both backends
- Keep input generation, fixture setup, diagnostics, and complete validation outside timed regions
- Report a winner only when the paired bootstrap 95% confidence interval excludes parity and the median effect exceeds 5%
- Treat all DynamoDB figures as DynamoDB Local results
- Preserve unrelated untracked workspace directories

---

### Task 1: Shared deterministic workload contract

**Files:**
- Create: `benchmarks/backend-workload-contract/Cargo.toml`
- Create: `benchmarks/backend-workload-contract/src/lib.rs`
- Create: `benchmarks/backend-workload-contract/src/digest.rs`
- Create: `benchmarks/backend-workload-contract/src/oracle.rs`

**Interfaces:**
- Produces: `WorkloadSpec`, `Workload`, `MutationRecord`, `MergeBranches`, `ExpectedOutcomes`, `Digest`, `CONTRACT_VERSION`
- Produces: `Workload::generate(spec) -> Result<Workload, String>`
- Produces: `digest_entries`, `digest_mutations`, and `digest_diffs`

- [ ] **Step 1: Create the crate and failing golden-vector tests**

Add tests that require:

```rust
let spec = WorkloadSpec {
    records: 100,
    value_bytes: 27,
    changes: 10,
    samples: 8,
    concurrency: 4,
    seed: 0x6a09_e667_f3bc_c909,
};
let workload = Workload::generate(spec).unwrap();
assert_eq!(workload.base_entries[0].0, b"key-00000000000000000000");
assert_eq!(workload.base_entries[0].1.len(), 27);
assert_eq!(workload.query_ids.len(), 8);
assert_eq!(workload.batch_mutations.len(), 10);
assert_eq!(workload.merge.left.len(), 5);
assert_eq!(workload.merge.right.len(), 5);
assert!(workload.merge.left.iter().all(|item| !workload.merge.right.contains(item)));
assert_eq!(workload.contract_version, "backend-workload-v1");
assert_eq!(workload.workload_digest.to_hex().len(), 64);
```

Add oracle tests that apply mutations to `BTreeMap<Vec<u8>, Vec<u8>>` and compare exact batch, diff, and merged states.

- [ ] **Step 2: Run the tests and verify the crate fails to compile**

Run:

```bash
cargo test --manifest-path benchmarks/backend-workload-contract/Cargo.toml
```

Expected: failure because the contract types and generators do not exist.

- [ ] **Step 3: Implement deterministic generation and digest framing**

Use fixed-width keys, value bytes derived from the seed, identifier, and generation, and length-prefixed SHA-256 digest records. Generate random identifiers with the existing xorshift seed policy, then sort them. Interleave sorted merge identifiers by index parity.

Keep the crate independent of Prolly. Represent mutations as:

```rust
pub enum MutationRecord {
    Upsert { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}
```

Build complete expected `BTreeMap` states and logical diff records during generation.

- [ ] **Step 4: Run tests and formatting**

Run:

```bash
cargo fmt --manifest-path benchmarks/backend-workload-contract/Cargo.toml --check
cargo test --manifest-path benchmarks/backend-workload-contract/Cargo.toml
cargo clippy --manifest-path benchmarks/backend-workload-contract/Cargo.toml --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 5: Commit the contract**

```bash
git add benchmarks/backend-workload-contract
git commit -m "Add shared backend workload contract"
```

### Task 2: Common evidence and measurement model

**Files:**
- Create: `benchmarks/backend-comparison/Cargo.toml`
- Create: `benchmarks/backend-comparison/src/lib.rs`
- Create: `benchmarks/backend-comparison/src/cli.rs`
- Create: `benchmarks/backend-comparison/src/evidence.rs`
- Create: `benchmarks/backend-comparison/src/measure.rs`

**Interfaces:**
- Consumes: `backend_workload_contract::{Workload, WorkloadSpec, Digest}`
- Produces: `Backend`, `Operation`, `RunConfig`, `EvidenceRow`
- Produces: `measure(future) -> Result<Measured<T>, String>`
- Produces: `EvidenceRow::validate() -> Result<(), String>`

- [ ] **Step 1: Write failing evidence and timing-boundary tests**

Require rows to reject zero timings, incorrect throughput, empty digests, invalid roots, false validation, and unsupported schemas. Add an event-order test:

```rust
let events = Arc::new(Mutex::new(Vec::new()));
let measured = measure({
    let events = events.clone();
    async move {
        events.lock().unwrap().push("operation");
        Ok::<_, String>(7)
    }
}).await.unwrap();
events.lock().unwrap().push("validation");
assert_eq!(*events.lock().unwrap(), ["operation", "validation"]);
assert_eq!(measured.value, 7);
assert!(measured.elapsed_ns > 0);
```

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml evidence measure
```

Expected: failure because the evidence and measurement modules do not exist.

- [ ] **Step 3: Implement the schema and CLI**

`EvidenceRow` includes run ID, backend, repetition, operation, workload dimensions, source and binary hashes, contract and timing versions, elapsed nanoseconds, logical operations, throughput, root, observed count, workload digest, outcome digest, validation flag, and error text.

`RunConfig` accepts one backend, one repetition, explicit workload dimensions, connection settings, output path, run identity, source identity, and binary identity. It refuses unsupported dimensions and dirty provenance.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo fmt --manifest-path benchmarks/backend-comparison/Cargo.toml --check
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml
```

Expected: all focused tests pass.

- [ ] **Step 5: Commit the evidence boundary**

```bash
git add benchmarks/backend-comparison
git commit -m "Define backend comparison evidence"
```

### Task 3: One generic Prolly operation runner

**Files:**
- Create: `benchmarks/backend-comparison/src/runner.rs`
- Modify: `benchmarks/backend-comparison/src/lib.rs`
- Modify: `benchmarks/backend-comparison/Cargo.toml`

**Interfaces:**
- Consumes: `RunConfig`, `Workload`, and any `B: RemoteStoreBackend + Clone`
- Produces: `run_workload<B>(backend, config, workload) -> Result<Vec<EvidenceRow>, String>`
- Produces: `validate_tree`, `validate_query`, `validate_diff`, and `validate_merge`

- [ ] **Step 1: Add failing generic-runner tests with an in-memory remote backend**

Create a test backend implementing `RemoteStoreBackend` over locked `BTreeMap` values. Require all six operations, seven rows for one repetition, exact roots, and matching outcome digests. Inject wrong expected query bytes and assert that the runner returns an error without writing a validated row.

- [ ] **Step 2: Run the runner tests and verify failure**

Run:

```bash
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml runner
```

Expected: failure because `run_workload` and validators do not exist.

- [ ] **Step 3: Implement common setup, operations, and complete validation**

Convert `MutationRecord` to Prolly mutations only at the runner boundary. Build the base once per process. Construct batch, diff target, and merge branches outside their measured operations.

Time only:

```rust
measure(manager.batch(&manager.create(), base_mutations)).await
measure(manager.batch(&base, batch_mutations)).await
measure(manager.get_many(&base, &query_keys)).await
measure(concurrent_reads(&manager, &base, &query_keys, concurrency)).await
measure(manager.diff(&base, &diff_target)).await
measure(manager.merge(&base, &left, &right, None)).await
```

After each timer, scan the complete result with `scan_range(tree, b"", None, visitor)`, compare every key/value pair to the oracle, and compute the ordered outcome digest. Compare query positions and values exactly. Compare every diff record field exactly. Record the canonical root.

- [ ] **Step 4: Run tests, Clippy, and formatting**

Run:

```bash
cargo fmt --manifest-path benchmarks/backend-comparison/Cargo.toml --check
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml
cargo clippy --manifest-path benchmarks/backend-comparison/Cargo.toml --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 5: Commit the common runner**

```bash
git add benchmarks/backend-comparison
git commit -m "Run equivalent backend operations"
```

### Task 4: Thin PostgreSQL and DynamoDB Local binaries

**Files:**
- Create: `benchmarks/backend-comparison/src/bin/postgres.rs`
- Create: `benchmarks/backend-comparison/src/bin/dynamodb.rs`
- Create: `benchmarks/backend-comparison/tests/postgres_smoke.rs`
- Create: `benchmarks/backend-comparison/tests/dynamodb_smoke.rs`
- Modify: `benchmarks/backend-comparison/Cargo.toml`

**Interfaces:**
- Consumes: `parse_run_config`, `Workload::generate`, and `run_workload`
- Produces: `prolly-backend-postgres` and `prolly-backend-dynamodb`

- [ ] **Step 1: Add ignored Docker integration tests**

PostgreSQL setup initializes and truncates `prolly_nodes`, `prolly_hints`, and `prolly_roots`. DynamoDB setup uses a non-empty run and repetition prefix, initializes schema, and clears that namespace before and after the run.

Each ignored test runs 100 records, 10 changes, 10 queries, and validates all six output rows.

- [ ] **Step 2: Run tests and confirm the binaries are missing**

Run:

```bash
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml --test postgres_smoke
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml --test dynamodb_smoke
```

Expected: compile failure because the backend binaries do not exist.

- [ ] **Step 3: Implement adapter-only setup**

Both binaries parse the same workload and provenance arguments, construct the backend, call the same generic runner, and write one new CSV file. Refuse an existing output file.

PostgreSQL accepts `--url`. DynamoDB Local accepts `--endpoint`, `--table`, and adapter parallelism values. No adapter binary contains workload, timer, validator, or winner logic.

- [ ] **Step 4: Run unit tests and Docker integration tests**

Run:

```bash
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml
PROLLY_BACKEND_POSTGRES_TEST_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly \
  cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml --test postgres_smoke -- --ignored
PROLLY_BACKEND_DYNAMODB_TEST_ENDPOINT=http://127.0.0.1:8000 \
  cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml --test dynamodb_smoke -- --ignored
```

Expected: all pass while the local services run.

- [ ] **Step 5: Commit adapter binaries**

```bash
git add benchmarks/backend-comparison
git commit -m "Connect comparison runner to both backends"
```

### Task 5: Fail-closed Rust statistics and report generator

**Files:**
- Create: `benchmarks/backend-comparison/src/statistics.rs`
- Create: `benchmarks/backend-comparison/src/summary.rs`
- Create: `benchmarks/backend-comparison/src/bin/summarize.rs`
- Create: `benchmarks/backend-comparison/tests/summary_fixtures.rs`
- Modify: `benchmarks/backend-comparison/src/lib.rs`

**Interfaces:**
- Produces: `median`, `median_absolute_deviation`, `coefficient_of_variation`, and `paired_bootstrap_ratio_ci`
- Produces: `summarize_run(input, manifest, output) -> Result<(), String>`
- Produces: `prolly-backend-summarize`

- [ ] **Step 1: Write failing statistical golden tests**

Pin median, median absolute deviation, sample coefficient of variation, and a 10,000-resample paired bootstrap interval using a fixed xorshift seed. Require `winner` only when the interval excludes `1.0` and the median ratio differs by more than `5%`.

Add fixture tests that reject:

- Six rather than seven repetitions
- Missing or duplicate operations
- Mismatched workload or outcome digests
- Mismatched roots
- Mixed run, revision, binary, contract, schema, or timing versions
- Dirty, resumed, or failed manifests
- Incorrect stored throughput arithmetic

- [ ] **Step 2: Run tests and verify failure**

Run:

```bash
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml statistics summary
```

Expected: failure because statistics and summary functions do not exist.

- [ ] **Step 3: Implement validation, statistics, CSV, and Markdown output**

Use paired repetition numbers. Derive throughput from median elapsed time rather than taking the median of stored rates. Emit latency in milliseconds and throughput in operations per second under separate headings. Include repetitions, range, median absolute deviation, coefficient of variation, confidence interval, effect, and `PostgreSQL`, `DynamoDB Local`, or `inconclusive`.

Write output files only after the complete input passes. Use temporary files in the output directory and rename them after successful rendering.

- [ ] **Step 4: Run focused and full tests**

Run:

```bash
cargo fmt --manifest-path benchmarks/backend-comparison/Cargo.toml --check
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml
cargo clippy --manifest-path benchmarks/backend-comparison/Cargo.toml --all-targets -- -D warnings
```

Expected: all pass.

- [ ] **Step 5: Commit statistical reporting**

```bash
git add benchmarks/backend-comparison
git commit -m "Add fail-closed backend comparison statistics"
```

### Task 6: Fresh-run orchestration and provenance

**Files:**
- Create: `benchmarks/backend-comparison/docker-compose.yml`
- Create: `scripts/tests/test_run_backend_comparison.py`
- Rewrite: `scripts/run_backend_comparison.sh`
- Delete: `scripts/summarize_backend_comparison.py`
- Modify: `benchmarks/BACKEND_COMPARISON.md`

**Interfaces:**
- Consumes: the three release binaries from `benchmarks/backend-comparison`
- Produces: an immutable run directory containing `manifest.txt`, `raw-results.csv`, `comparison.csv`, `report.md`, logs, binaries, and hashes

- [ ] **Step 1: Write failing shell-driver tests**

Use temporary fake `git`, `docker`, `cargo`, backend binary, and summarizer commands. Require the driver to:

- Reject dirty tracked files
- Reject an existing output path
- Require at least seven repetitions
- Alternate backend order by repetition
- Run one excluded warm-up per backend
- Remove service volumes between invocations
- Record non-empty commit, tree, lockfile, binary, config, image, and command hashes
- Leave `status=failed` on interruption and call no summarizer

- [ ] **Step 2: Run tests and confirm current driver fails**

Run:

```bash
python3 -m unittest scripts.tests.test_run_backend_comparison -v
```

Expected: failures for overwrite, dirty-tree, repetition, alternation, and provenance requirements.

- [ ] **Step 3: Implement pinned Compose services and the fresh orchestrator**

Use immutable PostgreSQL and DynamoDB Local image references in the comparison-specific Compose file. Capture requested references plus resolved image IDs.

Build the release binaries once, copy them into the result directory, hash them, and run those copies. Create a unique run ID. Start a clean backend container for each warm-up and repetition, wait for health, run one backend binary, then stop and remove its volumes.

Write per-invocation CSV files, verify their headers match, and concatenate them into `raw-results.csv`. Mark the manifest complete only after every invocation succeeds. Invoke the Rust summarizer last.

- [ ] **Step 4: Run shell tests and static checks**

Run:

```bash
python3 -m unittest scripts.tests.test_run_backend_comparison -v
bash -n scripts/run_backend_comparison.sh
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml
```

Expected: all pass.

- [ ] **Step 5: Commit orchestration and documentation**

```bash
git add benchmarks/backend-comparison/docker-compose.yml benchmarks/BACKEND_COMPARISON.md scripts/run_backend_comparison.sh scripts/tests/test_run_backend_comparison.py
git add -u scripts/summarize_backend_comparison.py
git commit -m "Harden backend comparison orchestration"
```

### Task 7: Docker smoke verification and implementation commit

**Files:**
- Modify only if failures reveal defects in files from Tasks 1 through 6

**Interfaces:**
- Produces: a complete 10,000-record publishable smoke directory

- [ ] **Step 1: Run all focused static and unit gates**

Run:

```bash
cargo fmt --manifest-path benchmarks/backend-workload-contract/Cargo.toml --check
cargo clippy --manifest-path benchmarks/backend-workload-contract/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path benchmarks/backend-workload-contract/Cargo.toml
cargo fmt --manifest-path benchmarks/backend-comparison/Cargo.toml --check
cargo clippy --manifest-path benchmarks/backend-comparison/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path benchmarks/backend-comparison/Cargo.toml
python3 -m unittest scripts.tests.test_run_backend_comparison -v
bash -n scripts/run_backend_comparison.sh
```

Expected: all pass.

- [ ] **Step 2: Commit any verification fixes**

Stage only files in the approved benchmark scope, then commit with:

```bash
git commit -m "Fix backend comparison verification"
```

Skip this commit when no tracked files changed.

- [ ] **Step 3: Push the clean implementation**

Run:

```bash
git status --short
git push
```

Expected: only unrelated untracked directories remain and the branch push succeeds.

- [ ] **Step 4: Run the Docker smoke comparison**

Run:

```bash
BENCH_RECORDS=10000 \
BENCH_CHANGES=1000 \
BENCH_SAMPLES=1000 \
BENCH_RUNS=7 \
BENCH_OUT=performance-results/backend-comparison-smoke-2026-07-26 \
scripts/run_backend_comparison.sh
```

Expected: complete manifest, 84 measured rows, matching workload and outcome evidence, and a generated report.

- [ ] **Step 5: Re-run the summarizer and compare output bytes**

Copy `comparison.csv` and `report.md`, run the saved summarizer against saved raw inputs, and use `cmp` to require byte-identical regenerated output.

### Task 8: Clean 10M run, immutable results, and PR correction

**Files:**
- Delete or supersede: the invalid backend comparison result directory added by commit `c1c4f925`
- Create: `performance-results/backend-comparison-10m-<run-id>/`
- Update: pull request #50 description

**Interfaces:**
- Produces: verified 10M raw evidence and the corrected public performance comparison

- [ ] **Step 1: Confirm the implementation revision is clean**

Run:

```bash
git status --short
git rev-parse HEAD
```

Expected: no tracked changes and a committed implementation revision.

- [ ] **Step 2: Run the clean 10M comparison**

Run:

```bash
BENCH_RECORDS=10000000 \
BENCH_VALUE_BYTES=27 \
BENCH_CHANGES=10000 \
BENCH_SAMPLES=10000 \
BENCH_CONCURRENCY=32 \
BENCH_RUNS=7 \
scripts/run_backend_comparison.sh
```

Expected: the run completes with no failed validation, provenance, matrix, or statistical checks.

- [ ] **Step 3: Audit raw evidence and regeneration**

Require 84 measured rows, seven paired repetitions for six operations and two backends, matching workload and outcome digests, matching roots, and byte-identical summary regeneration.

- [ ] **Step 4: Commit immutable results**

```bash
git add performance-results benchmarks/BACKEND_COMPARISON.md
git commit -m "Record trustworthy 10M backend comparison"
git push
```

- [ ] **Step 5: Update PR #50**

Replace the mislabeled table and old claims. Link the design, implementation revision, result manifest, raw CSV, and reproduction command. Report milliseconds, operations per second, dispersion, confidence intervals, and only statistically supported winners.

- [ ] **Step 6: Verify the PR and branch**

Run:

```bash
gh pr view 50 --json url,title,body,headRefName,isDraft
git status --short
```

Expected: PR #50 points to the hardened result and only unrelated untracked directories remain.
