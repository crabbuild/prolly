# Scale Performance Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce a reproducible, evidence-backed original-versus-improved performance report for 1K through 10M records under append-only, random, and clustered key workloads.

**Architecture:** Add one deterministic benchmark binary that compiles unchanged at the original commit and the improved worktree, emits validated CSV rows, and streams base construction. A shell driver builds isolated binaries, alternates process order, captures macOS peak RSS, and retains raw outputs; a small checked-in Rust summarizer converts raw rows into aggregate CSV and Markdown without external analysis dependencies.

**Tech Stack:** Rust 2021, `prolly-map`, `SortedBatchBuilder`, `MemStore`, POSIX shell, `/usr/bin/time -l`, CSV text.

## Global Constraints

- Measure 1,000, 10,000, 50,000, 100,000, 1,000,000, and 10,000,000 records.
- Compare untouched commit `fa7c219` with the current improved worktree.
- Use identical harness source, deterministic seeds, data, operation counts, release settings, and host.
- Do not change production behavior for benchmark results.
- Validate every timed workload before emitting its measurement.
- Retain failures and regressions; never extrapolate missing tiers.
- Do not add the prohibited external implementation name to production code.

---

### Task 1: Cross-Version Scale Harness

**Files:**
- Create: `benches/scale_workloads.rs`
- Modify: `Cargo.toml`
- Test: `benches/scale_workloads.rs` self-validation at 1,000 records

**Interfaces:**
- Consumes: `SCALE_RECORDS`, `SCALE_VERSION`, and shared APIs present at `fa7c219`.
- Produces: CSV rows with `version,records,workload,operations,total_ns,ns_per_op,validated,nodes_read,nodes_written,bytes_read,bytes_written,num_nodes,num_leaves,num_internal,height,tree_bytes`.

- [ ] **Step 1: Declare the benchmark target before the source exists**

```toml
[[bench]]
name = "scale_workloads"
harness = false
```

- [ ] **Step 2: Verify the target fails because the harness is absent**

Run: `cargo bench --bench scale_workloads --no-run`

Expected: failure reporting that `benches/scale_workloads.rs` does not exist.

- [ ] **Step 3: Implement deterministic record and index generators**

```rust
fn key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

fn value(id: usize, generation: u8) -> Vec<u8> {
    format!("value-{id:020}-{generation:02}").into_bytes()
}

fn sample_count(records: usize) -> usize {
    records.min((records / 100).max(100)).min(10_000)
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
```

Generate unique random indexes with a `BTreeSet`, then sort them. Generate the
clustered indexes as a contiguous range centered on `records / 2`.

- [ ] **Step 4: Implement streamed base construction and structural output**

Create `Arc<MemStore>`, feed keys in ascending order to `SortedBatchBuilder`,
call `build`, construct `Prolly`, and call `collect_stats`. Time only record
generation plus builder ingestion and `build`; validate `total_key_value_pairs`.

- [ ] **Step 5: Implement reads, mutations, diffs, and validation**

Use one warm pass before random and clustered reads. Build append mutations with
IDs starting at `records`, random updates with generation `1`, and clustered
updates with generation `2`. Apply each workload independently to the same base
tree. Reset manager metrics immediately before timing. Validate sampled reads,
logical counts, diff counts, and exact changed-key sets before emitting CSV.

- [ ] **Step 6: Verify RED becomes GREEN**

Run:

```sh
SCALE_RECORDS=1000 SCALE_VERSION=improved \
  cargo bench --bench scale_workloads
```

Expected: one CSV header, base-build/reads/mutations/diffs rows, every row marked
`validated=true`, and process exit 0.

- [ ] **Step 7: Verify unchanged source compiles at the original commit**

Copy the harness and target stanza into a detached `fa7c219` worktree and run
the same 1,000-record command with `SCALE_VERSION=original`. Expected: identical
column schema and all validation fields true.

### Task 2: Reproducible Alternating Runner

**Files:**
- Create: `scripts/run_scale_report.sh`
- Create: `performance-results/scale-2026-07-14/.gitkeep`
- Test: runner smoke mode with `SCALE_SIZES=1000 SCALE_RUNS=1`

**Interfaces:**
- Consumes: improved repository path, detached baseline worktree, two release binaries.
- Produces: `raw/<version>-<records>-<run>.csv`, matching `.time` and `.stderr` files, `machine.txt`, and `run-manifest.csv`.

- [ ] **Step 1: Run the absent driver to verify failure**

Run: `SCALE_SIZES=1000 SCALE_RUNS=1 scripts/run_scale_report.sh`

Expected: shell reports the file does not exist.

- [ ] **Step 2: Implement strict setup and isolated builds**

Use `set -eu`, resolve repository paths, create a temporary detached worktree at
`fa7c219`, copy only the harness and Cargo target declaration, and build both
with separate `CARGO_TARGET_DIR` values. Record `rustc -Vv`, OS, CPU count,
memory, git revisions, and binary SHA-256 values.

- [ ] **Step 3: Implement alternating process execution**

For odd repetitions run original then improved; for even repetitions reverse
the order. Invoke each through `/usr/bin/time -l`, redirecting stdout, stderr,
and timing separately. Append exit status and paths to `run-manifest.csv` even
when one command fails. Default sizes are `1000 10000 50000 100000 1000000
10000000`; default runs are three, with two runs for 10M.

- [ ] **Step 4: Verify smoke artifacts**

Run: `SCALE_SIZES=1000 SCALE_RUNS=1 scripts/run_scale_report.sh`

Expected: two successful manifest entries, two CSV files with all validations
true, two timing files containing `maximum resident set size`, and machine data.

### Task 3: Aggregation and Markdown Report

**Files:**
- Create: `src/bin/prolly-scale-report.rs`
- Modify: `Cargo.toml`
- Create during measured run: `performance-results/scale-2026-07-14/results.csv`
- Create during measured run: `performance-results/scale-2026-07-14/report.md`
- Test: unit tests inside `src/bin/prolly-scale-report.rs`

**Interfaces:**
- Consumes: raw harness CSV, timing files, and run manifest.
- Produces: per-version medians/ranges, percentage deltas, peak RSS aggregates, and Markdown tables with regression flags.

- [ ] **Step 1: Add failing parser and median tests**

Test parsing a representative row, median for odd/even vectors, improvement
direction for latency versus throughput, and preservation of a failed manifest
entry. Run `cargo test --bin prolly-scale-report`; expect unresolved functions.

- [ ] **Step 2: Implement parser, aggregation, and report rendering**

Use only the standard library. Reject mismatched record/workload operation
counts across versions. For latency, compute `(current - original) / original`;
negative is faster. Include min/median/max, absolute delta, percentage change,
and `noise-sensitive` when below the design thresholds. Add a dedicated
regressions section before gains.

- [ ] **Step 3: Verify aggregation tests and smoke report**

Run `cargo test --bin prolly-scale-report`, then run the binary against the 1K
smoke directory. Expected: tests pass, `results.csv` and `report.md` are created,
and every number in Markdown exists in normalized CSV.

### Task 4: Full Measurement Matrix

**Files:**
- Populate: `performance-results/scale-2026-07-14/raw/`
- Populate: `performance-results/scale-2026-07-14/results.csv`
- Populate: `performance-results/scale-2026-07-14/report.md`

**Interfaces:**
- Consumes: validated runner and aggregator.
- Produces: complete measured evidence for all six requested sizes.

- [ ] **Step 1: Run warmups through 100K**

Run the harness once per version for 1K, 10K, 50K, and 100K without adding
those files to measured aggregates. Expected: all validations true.

- [ ] **Step 2: Run three alternating repetitions through 1M**

Run the driver for 1K, 10K, 50K, 100K, and 1M with three measured repetitions.
Expected: 30 successful process records.

- [ ] **Step 3: Run two alternating 10M repetitions**

Run the driver for 10M with two repetitions. Monitor free memory and elapsed
time between runs. A resource failure remains a report result; do not substitute
an estimate.

- [ ] **Step 4: Generate aggregates and inspect outliers**

Generate the report, inspect any run more than 15% from its version/workload
median, and rerun only when there is evidence of external interference. Retain
both original and replacement raw files with an explanation in the report.

### Task 5: Final Verification and Cleanup

**Files:**
- Verify all source and report artifacts above.

**Interfaces:**
- Consumes: completed report artifacts.
- Produces: a clean, reproducible handoff.

- [ ] **Step 1: Run source gates**

Run `cargo fmt --all -- --check`, `cargo test --bin prolly-scale-report`,
`cargo test --all-features`, and `git diff --check`. Expected: all pass.

- [ ] **Step 2: Audit evidence completeness**

Check six sizes, both versions, every workload, expected repetitions, all
validation flags, matching operation counts, and captured peak RSS. Expected:
no silent omissions.

- [ ] **Step 3: Remove temporary baseline worktree and build targets**

Remove only temporary worktrees and isolated benchmark target directories.
Keep raw CSV, timing, machine metadata, aggregate CSV, report, harness, and
runner.

- [ ] **Step 4: Commit report artifacts**

Stage only the scale harness, runner, summarizer, design/plan, and performance
results. Commit with `perf: report scale workload comparison`.
