# Secondary Index Applications and Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship two production-shaped Rust `IndexedMap` examples, a repeatable latency/capacity harness, deterministic resource-limit probes, and a provenance-backed performance report produced on this machine.

**Architecture:** Keep the compact lifecycle example intact and add separate typed serde/JSON user-directory and task-queue examples. Split the custom Cargo benchmark into model, output, and scenario modules; emit a stable raw CSV contract that a standard-library Python summarizer and shell runner turn into checked-in evidence. Use focused Rust integration tests for configured-limit boundaries and atomic rejection behavior.

**Tech Stack:** Rust 1.81, `prolly-map`, serde/serde_json, Cargo custom benchmarks, Python 3 standard library, POSIX shell, Markdown/CSV.

## Global Constraints

- Keep `examples/secondary_index.rs` as the compact byte-oriented lifecycle tour.
- Do not add secondary-index semantics or change persisted formats.
- Use deterministic, side-effect-free, retry-safe extractors that return `SecondaryIndexError` for malformed JSON.
- Use `KeyBuilder` canonical segments and order-preserving integer encodings; do not concatenate composite fields with delimiters.
- The report profile defaults to a 600-second post-compilation budget and stops scheduling optional work after 540 seconds.
- The 1,000,000-record probe is optional; 1,000, 10,000, 100,000 and configured-limit probes are required.
- Report empirical capacity only as the largest tested successfully on this machine.
- Preserve unrelated working-tree changes, especially existing language-binding and performance-result work.

---

## File structure

- Create `examples/saas_user_directory.rs`: typed SaaS user records and seven application indexes.
- Create `examples/saas_task_queue.rs`: typed SaaS tasks and five queue/dashboard indexes.
- Create `tests/secondary_index_limits.rs`: deterministic public-API resource-limit and atomicity tests.
- Modify `benches/secondary_index_bench.rs`: profile parsing and benchmark orchestration.
- Create `benches/secondary_index_bench/model.rs`: deterministic benchmark keys, values, and definitions.
- Create `benches/secondary_index_bench/output.rs`: stable CSV rows and default-limit rows.
- Create `benches/secondary_index_bench/scenarios.rs`: isolated build, write, read, shape, and limit scenarios.
- Create `scripts/summarize_secondary_index_bench.py`: CSV validation, percentiles, summary CSV, and Markdown report.
- Create `scripts/test_summarize_secondary_index_bench.py`: standard-library unit tests for the summarizer.
- Create `scripts/run-secondary-index-bench.sh`: release build, provenance capture, timed run, and report production.
- Modify `README.md`, `docs/README.md`, `docs/secondary-index-design.md`, and `docs/performance.md`: discovery, usage, and reproduction guidance.
- Create `performance-results/secondary-index-2026-07-17/`: generated manifest, raw CSV, summary CSV, and report.

### Task 1: Configured resource-limit boundary tests

**Files:**
- Create: `tests/secondary_index_limits.rs`

**Interfaces:**
- Consumes: `SecondaryIndex::builder`, `SecondaryIndexLimits`, `IndexedMap::{ensure_index,put,edit,verify_index,export_current,snapshot}`, and `Error::IndexResourceLimitExceeded`.
- Produces: helpers `limits()`, `assert_limit(error, resource, limit, actual)`, and focused tests proving exact boundaries and unchanged indexed snapshots after rejected publication.

- [ ] **Step 1: Write failing limit-boundary tests**

Create tests that use small copied defaults with one field changed at a time. Include direct extractor tests for `term_bytes`, `projection_bytes`, `all_source_value_bytes`, `terms_per_record`, and `projected_bytes_per_record`; indexed write tests for `derived_mutations_per_transaction` and `projected_bytes_per_transaction`; build tests for `temporary_sort_bytes` and `build_entries`; and export tests for `bundle_nodes` and `bundle_bytes`.

Each publication-sensitive test captures this exact identity before the rejected operation:

```rust
fn snapshot_identity(indexed: &prolly::IndexedMap<'_, Arc<MemStore>>) -> prolly::IndexedSnapshotId {
    indexed.snapshot().unwrap().id().clone()
}

fn assert_limit(error: Error, resource: &'static str, limit: usize, actual: usize) {
    assert!(matches!(
        error,
        Error::IndexResourceLimitExceeded {
            resource: observed,
            limit: observed_limit,
            actual: observed_actual,
        } if observed == resource && observed_limit == limit && observed_actual == actual
    ));
}
```

For a rejected write, assert `snapshot_identity(&indexed)` equals the captured identity and that the rejected primary key is absent. For a rejected build, assert the source head is unchanged and `health().active_indexes` is empty.

- [ ] **Step 2: Run the focused tests and verify the intended failures**

Run:

```bash
cargo test --test secondary_index_limits
```

Expected: compilation or assertion failures until all exact public error resource names and boundary fixtures are aligned with the shipped implementation.

- [ ] **Step 3: Complete the minimal fixtures and assertions**

Use `SecondaryIndexLimits { changed_field: small_value, ..SecondaryIndexLimits::default() }` and deterministic callbacks. Ensure accepted-boundary cases call `extract`, `put`, `ensure_index`, `verify_index`, or `export_current` successfully before their rejected `limit + 1` counterparts.

- [ ] **Step 4: Verify focused and existing secondary-index tests**

Run:

```bash
cargo test --test secondary_index_limits
cargo test --test secondary_index
```

Expected: all tests pass with zero failures.

- [ ] **Step 5: Commit the isolated boundary-test deliverable**

```bash
git add tests/secondary_index_limits.rs
git commit -m "test(index): cover configured resource boundaries"
```

### Task 2: Typed SaaS user-directory example

**Files:**
- Create: `examples/saas_user_directory.rs`

**Interfaces:**
- Produces: `UserStatus`, `User`, `UserSummary`, `decode_user`, `encode_user`, `tenant_status_term`, `created_day_term`, and `registry`.
- Consumes: `KeyBuilder`, every projection mode, exact/range/paged queries, source joins, projections, and verification.

- [ ] **Step 1: Verify the example target is absent**

Run:

```bash
cargo run --example saas_user_directory
```

Expected: failure containing `no example target named 'saas_user_directory'`.

- [ ] **Step 2: Implement the typed model and canonical definitions**

Define a serde record with `tenant_id`, `email`, `status`, `display_name`, `tags`, and `created_at`. Register these exact names and projection roles:

```rust
const INDEX_NAMES: &[&[u8]] = &[
    b"by-status",
    b"by-tag",
    b"by-tenant-status",
    b"by-email-domain",
    b"by-created-day",
    b"by-status-summary",
    b"by-status-full",
];
```

Build composite terms with:

```rust
fn tenant_status_term(tenant: &str, status: UserStatus) -> Vec<u8> {
    KeyBuilder::new()
        .push_str(tenant)
        .push_segment(status.term())
        .finish()
}

fn created_day_term(day: u64) -> Vec<u8> {
    KeyBuilder::new().push_u64(day).finish()
}
```

`by-tag` emits lowercase zero-or-more terms; `by-email-domain` emits zero terms when no `@` is present; `by-status-summary` stores serialized `UserSummary`; and `by-status-full` uses `All`.

- [ ] **Step 3: Add the assertion-backed workflow**

Populate raw source records before activation, ensure all seven indexes, update an invited user to active through `IndexedMap::edit`, and insert a tagless user. Assert exact status/tag/domain queries, tenant composite lookup, half-open day range, decoded summary/full projections, ordered source records, a two-page cursor round trip, and seven valid verification results.

- [ ] **Step 4: Run and lint the example**

Run:

```bash
cargo run --example saas_user_directory
cargo clippy --example saas_user_directory -- -D warnings
```

Expected: the example prints its verified index/user count and Clippy exits successfully.

- [ ] **Step 5: Commit the user-directory example**

```bash
git add examples/saas_user_directory.rs
git commit -m "docs(index): add SaaS user directory example"
```

### Task 3: Typed SaaS task-queue example

**Files:**
- Create: `examples/saas_task_queue.rs`

**Interfaces:**
- Produces: `TaskState`, `Task`, `TaskSummary`, `queue_term`, `assignee_term`, `due_term`, `dashboard_term`, and `registry`.
- Consumes: sparse and multi-valued extraction, `Include`, ordered ranges, tenant-scoped pages, source joins, and pinned snapshots.

- [ ] **Step 1: Verify the example target is absent**

Run:

```bash
cargo run --example saas_task_queue
```

Expected: failure containing `no example target named 'saas_task_queue'`.

- [ ] **Step 2: Implement canonical task indexes**

Use fields `tenant_id`, `state`, `priority: u8`, `assignee: Option<String>`, `due_at: u64`, `title`, and `labels`. Register `by-queue`, `by-assignee`, `by-label`, `by-due`, and `by-dashboard`. Encode the queue as tenant/state plus an inverted priority segment (`u64::from(u8::MAX - priority)`) so higher priority sorts first. Prefix assignee and due terms with tenant segments. Store a serialized `TaskSummary` in `by-dashboard`.

- [ ] **Step 3: Add the assertion-backed queue workflow**

Create two tenants and at least four tasks. Assert sparse unassigned behavior, label fan-out, tenant-bounded due ranges, high-priority queue ordering, dashboard projection decoding, and source joins. Pin a snapshot, move one task from queued to running, then prove the old snapshot still returns the old queue membership while the new snapshot returns the new state.

- [ ] **Step 4: Run and lint the example**

Run:

```bash
cargo run --example saas_task_queue
cargo clippy --example saas_task_queue -- -D warnings
```

Expected: the example prints verified task/index counts and Clippy exits successfully.

- [ ] **Step 5: Commit the task-queue example**

```bash
git add examples/saas_task_queue.rs
git commit -m "docs(index): add SaaS task queue example"
```

### Task 4: Repeated benchmark harness and raw CSV contract

**Files:**
- Modify: `benches/secondary_index_bench.rs`
- Create: `benches/secondary_index_bench/model.rs`
- Create: `benches/secondary_index_bench/output.rs`
- Create: `benches/secondary_index_bench/scenarios.rs`

**Interfaces:**
- Produces: `Profile::{Smoke,Report,Focused}`, `Settings::from_env`, `CsvRow::header/write`, `run_build_scenarios`, `run_write_scenarios`, `run_read_scenarios`, `run_shape_scenarios`, and `run_limit_probes`.
- CSV columns: `record_type,scenario,projection,scale,batch,indexes,fanout,cardinality,sample,elapsed_ns,work_items,items_per_sec,source_nodes,index_nodes,catalog_nodes,projected_bytes,physical_upserts,physical_deletes,bundle_nodes,bundle_bytes,limit_name,limit,actual,outcome,verified`.

- [ ] **Step 1: Capture a failing smoke-contract check**

Run:

```bash
PROLLY_INDEX_BENCH_PROFILE=smoke PROLLY_INDEX_BENCH_SAMPLES=2 \
  cargo bench --bench secondary_index_bench > /tmp/prolly-secondary-index-before.csv
head -1 /tmp/prolly-secondary-index-before.csv | \
  rg '^record_type,scenario,projection,scale,batch,indexes,fanout,cardinality,sample,elapsed_ns,'
```

Expected: the `rg` command fails because the old benchmark emits `operation,scale,batch,...`.

- [ ] **Step 2: Implement settings and stable CSV output**

`Settings::from_env` maps `smoke` to scale `[1_000]`, two samples, small batches, and no optional million probe; `report` maps required scales `[1_000, 10_000, 100_000]`, at least five samples, and a 600-second budget; focused mode accepts `PROLLY_INDEX_BENCH_SCALES`, `PROLLY_INDEX_BENCH_SAMPLES`, and `PROLLY_INDEX_BENCH_SCENARIOS`. Reject zero scales/samples and unknown profiles with a clear stderr error and nonzero exit.

`CsvRow::write` emits one RFC-4180-safe row with no human prose on stdout. Limit-default and limit-probe rows use `record_type` to distinguish them from timed samples.

- [ ] **Step 3: Implement deterministic model helpers and isolated scenarios**

Build user values with fixed tenant/status/cardinality distributions. Measure:

- activation for independent `KeysOnly`, `Include`, and `All` definitions;
- plain/indexed writes at batches 1/10/100/1,000, including term-changing and projection-only updates;
- exact low/medium/high cardinality, tenant composite, range, one-page, `records`, and `projected` reads;
- 1/4/8/16/32 index activation and 1/8/64/256 term fan-out; and
- all default-limit rows plus small deterministic acceptance/rejection probes.

Warm each read/write operation once, clear or snapshot cumulative metrics immediately before samples, validate cardinality/content after every timed call, and use `std::hint::black_box` on returned data. Emit logical amplification counters and bundle sizes when the scenario produces them.

- [ ] **Step 4: Verify the smoke CSV contract**

Run:

```bash
PROLLY_INDEX_BENCH_PROFILE=smoke PROLLY_INDEX_BENCH_SAMPLES=2 \
  cargo bench --bench secondary_index_bench > /tmp/prolly-secondary-index-smoke.csv
python3 - <<'PY'
import csv
rows = list(csv.DictReader(open('/tmp/prolly-secondary-index-smoke.csv')))
assert rows
assert all(row['verified'] == 'true' for row in rows)
assert {'sample', 'limit_default', 'limit_probe'} <= {row['record_type'] for row in rows}
assert {'build', 'write', 'read', 'shape'} <= {row['scenario'].split('/')[0] for row in rows if row['record_type'] == 'sample'}
PY
```

Expected: benchmark and validation both exit successfully.

- [ ] **Step 5: Commit the benchmark harness**

```bash
git add benches/secondary_index_bench.rs benches/secondary_index_bench
git commit -m "bench(index): add repeated latency and capacity matrix"
```

### Task 5: Summarizer and provenance runner

**Files:**
- Create: `scripts/summarize_secondary_index_bench.py`
- Create: `scripts/test_summarize_secondary_index_bench.py`
- Create: `scripts/run-secondary-index-bench.sh`

**Interfaces:**
- Produces: Python `nearest_rank(values, percentile)`, `load_rows(path)`, `summarize(rows)`, `render_report(...)`, and CLI `RAW_CSV MANIFEST OUT_DIR`; shell CLI `run-secondary-index-bench.sh [smoke|report] [OUT_DIR]`.

- [ ] **Step 1: Write failing standard-library summarizer tests**

Test nearest-rank p50/p95/p99 selection, grouping by all scenario-shape dimensions, rejection of missing columns, rejection of `verified != true`, preservation of limit rows, summary CSV output, and report sections `Environment`, `Latency and throughput`, `Configured limits`, `Largest tested capacity`, `Amplification`, `Caveats`, and `Reproduce`.

Run:

```bash
python3 -m unittest scripts/test_summarize_secondary_index_bench.py -v
```

Expected: import failure because `summarize_secondary_index_bench.py` does not exist.

- [ ] **Step 2: Implement the summarizer**

Use only `argparse`, `csv`, `json`, `math`, `statistics`, `pathlib`, and collections from Python's standard library. Compute nearest-rank percentiles from sorted `elapsed_ns`; sum work to compute aggregate throughput; never average percentiles. Reject empty samples, malformed numeric fields, duplicate sample identities, and unverified rows. Write `summary.csv` and `report.md` atomically through sibling `.tmp` files followed by `Path.replace`.

- [ ] **Step 3: Implement the runner**

The shell script must use `set -eu`, create only its explicit output directory, record `manifest.json` through a Python standard-library snippet, run `cargo bench --bench secondary_index_bench` with the selected profile, tee raw stdout to `raw.csv`, keep stderr in `bench.stderr`, and invoke the summarizer. Record UTC timestamp, `uname`, architecture, CPU, memory, `rustc -Vv`, `cargo -V`, git revision/branch/dirty state, profile, budget, and exact command. Do not delete or overwrite an existing non-empty output directory.

- [ ] **Step 4: Verify tests and a smoke report**

Run:

```bash
python3 -m unittest scripts/test_summarize_secondary_index_bench.py -v
scripts/run-secondary-index-bench.sh smoke /tmp/prolly-secondary-index-report-smoke
test -s /tmp/prolly-secondary-index-report-smoke/raw.csv
test -s /tmp/prolly-secondary-index-report-smoke/summary.csv
test -s /tmp/prolly-secondary-index-report-smoke/report.md
```

Expected: all unit tests pass and all three generated files are nonempty.

- [ ] **Step 5: Commit reporting tooling**

```bash
git add scripts/summarize_secondary_index_bench.py scripts/test_summarize_secondary_index_bench.py scripts/run-secondary-index-bench.sh
git commit -m "bench(index): add reproducible report pipeline"
```

### Task 6: Documentation and measured report

**Files:**
- Modify: `README.md`
- Modify: `docs/README.md`
- Modify: `docs/secondary-index-design.md`
- Modify: `docs/performance.md`
- Create: `performance-results/secondary-index-2026-07-17/manifest.json`
- Create: `performance-results/secondary-index-2026-07-17/raw.csv`
- Create: `performance-results/secondary-index-2026-07-17/summary.csv`
- Create: `performance-results/secondary-index-2026-07-17/report.md`
- Create: `performance-results/secondary-index-2026-07-17/bench.stderr`

**Interfaces:**
- Produces: discoverable example commands, benchmark profile commands, result interpretation guidance, and machine-specific checked-in evidence.

- [ ] **Step 1: Add documentation links and exact commands**

Add both examples beside `secondary_index.rs` in the README example map. In `docs/secondary-index-design.md`, add an application-examples section linking each pattern. In `docs/performance.md`, document smoke/report/focused environment variables, CSV/report outputs, p50/p95/p99 interpretation, logical amplification counters, configured versus observed limits, and the latest report link. Update `docs/README.md` to expose the application and performance paths.

- [ ] **Step 2: Run the full report profile**

Run:

```bash
scripts/run-secondary-index-bench.sh report performance-results/secondary-index-2026-07-17
```

Expected: the runner completes required 1K/10K/100K and limit rows, optionally includes 1M if budget remains, and creates all listed files with only verified rows.

- [ ] **Step 3: Inspect report claims against raw evidence**

Run:

```bash
python3 scripts/summarize_secondary_index_bench.py \
  performance-results/secondary-index-2026-07-17/raw.csv \
  performance-results/secondary-index-2026-07-17/manifest.json \
  performance-results/secondary-index-2026-07-17
rg -n "universal|max(imum)? supported|guarantee" \
  performance-results/secondary-index-2026-07-17/report.md
```

Expected: regeneration succeeds; any `rg` matches are caveats denying universal claims, not unsupported claims.

- [ ] **Step 4: Run final verification**

Run:

```bash
cargo fmt --all -- --check
cargo test --test secondary_index_limits
cargo test --test secondary_index
cargo run --example saas_user_directory
cargo run --example saas_task_queue
python3 -m unittest scripts/test_summarize_secondary_index_bench.py -v
PROLLY_INDEX_BENCH_PROFILE=smoke cargo bench --bench secondary_index_bench
cargo clippy --bench secondary_index_bench --example saas_user_directory --example saas_task_queue -- -D warnings
git diff --check
```

Expected: every command exits zero with no test failures, Clippy warnings, formatting differences, or whitespace errors.

- [ ] **Step 5: Commit documentation and measured evidence**

```bash
git add README.md docs/README.md docs/secondary-index-design.md docs/performance.md performance-results/secondary-index-2026-07-17
git commit -m "docs(index): publish application and performance evidence"
```

## Plan self-review

- Every approved design requirement maps to Tasks 1–6.
- Examples, benchmark components, reporting tools, and docs have focused file ownership.
- Public names match the current `SecondaryIndexLimits`, `IndexedMap`, snapshot, projection, metrics, and bundle APIs.
- No new library feature or persistence change is required.
- The full report is generated only after the harness and summarizer pass smoke verification.
