# SQLite Performance Matrix Design

## Status

Approved in conversation on 2026-07-14. This document defines the benchmark and reporting design only; it does not authorize production behavior changes or merging to `main`.

## Objective

Produce a reproducible, validation-first comparison between original revision `fa7c219` and the current prolly-tree implementation for SQLite-backed tree construction, reads, mutations, diff, and merge. Exercise append-only, randomly distributed, and clustered key patterns at 1K through 10M records under both WAL durability profiles:

- `FULL`: WAL enabled with SQLite `synchronous=FULL`.
- `NORMAL`: WAL enabled with SQLite `synchronous=NORMAL`.

The report must expose every measured regression, including latency, I/O, database size, and peak process memory. A latency gain must not hide a memory or storage regression.

## Non-goals

- Change production tree or SQLite-store behavior to improve benchmark results.
- Compare different machines, SQLite versions, compilers, harness sources, seeds, or datasets.
- Claim a cold operating-system page cache. “Cold” in this report means a fresh prolly manager cache.
- Extrapolate beyond 10M records.
- Treat binding correctness tests as binding performance measurements.

## Alternatives Considered

### Dedicated checked-in harness and report pipeline

This is the selected design. It preserves raw evidence, supports exact cross-version inputs, and can be rerun as a merge gate. It requires a benchmark target, runner, summarizer, and retained result artifacts.

### Temporary external harness

This minimizes repository changes but makes the result difficult to reproduce and audit. It is unsuitable for a merge decision.

### Extend the existing staged-append harness

This is the smallest code change, but the existing harness cannot isolate builds, updates, deletes, diff, merge, cache state, or workload-specific peak memory. It remains useful as a staged-append smoke benchmark but is not the comprehensive report.

## Components

### Cross-version SQLite workload harness

Add `stores/prolly-store-sqlite/benches/sqlite_workload_bench.rs` and declare it as a harness-free benchmark target in the SQLite adapter crate.

The same source file must compile unchanged at both revisions. It may use only APIs present at `fa7c219`. The runner copies that exact source and target declaration into a temporary detached baseline worktree, then records the source SHA-256 for both builds. A mismatch invalidates the report.

The harness has two modes:

1. `prepare`: construct a base database, publish its tree as a named root, validate it after reopening, checkpoint SQLite, and emit the build row.
2. `workload`: open an isolated copy of the prepared database, load the named base root, execute one workload group, validate it, and emit one or more measurement rows.

Separating preparation from workload execution prevents base-build time from contaminating operation latency and allows `/usr/bin/time -l` to capture workload-specific peak RSS.

### Alternating runner

Add `scripts/run_sqlite_workload_report.sh`.

For every size, durability profile, repetition, and workload group, the runner alternates original/current process order. It builds separate release binaries with isolated target directories and records binary hashes, compiler details, SQLite version, operating system, CPU count, memory, filesystem, free space, and revisions.

After a prepared database is closed and `PRAGMA wal_checkpoint(TRUNCATE)` succeeds, the runner creates workload fixtures from the main database file with a copy-on-write clone when supported:

- macOS: `cp -c`.
- Linux: `cp --reflink=auto`.
- Fallback: a normal file copy.

The selected copy method is recorded. WAL and shared-memory sidecars are not copied after the successful checkpoint; SQLite creates fresh sidecars when a workload fixture opens. Workload fixture files are removed after their process exits. The prepared base remains unchanged for the rest of that repetition.

### Standard-library report generator

Add `src/bin/prolly-sqlite-report.rs`. It reads the manifest, raw CSV, timing files, and machine metadata and produces:

- `performance-results/sqlite-workloads-2026-07-14/results.csv`
- `performance-results/sqlite-workloads-2026-07-14/report.md`
- retained raw stdout, stderr, and timing files

The report generator uses no external analysis dependency. Parser, aggregation, classification, and rendering behavior are unit-tested.

## Matrix

### Independent dimensions

- Revisions: original `fa7c219`, current implementation.
- Durability: WAL+FULL, WAL+NORMAL.
- Record counts: 1K, 10K, 50K, 100K, 1M, 10M.
- Repetitions: five measured repetitions for every tuple.
- Deterministic seed: one checked-in constant shared by both revisions.

Version and durability order alternate between repetitions. No run is silently discarded. A replacement run requires a recorded external-interference reason, and both the original and replacement remain in raw evidence.

### Operation counts

- Mutation and diff changes: 1% of records, with a minimum of 100 and maximum of 10,000.
- Merge changes: half the mutation count per branch, with at least 50 changes per branch.
- Read probes: 1,000,000 operations. For trees smaller than the probe count, deterministic IDs repeat.
- Right-edge probes: the newest `min(records, 10,000)` keys repeated to the read-operation count.

Every generated key set is unique where uniqueness is required, then sorted before constructing mutations so differences in client ordering do not accidentally become the compared variable. The shuffled-build workload uses a separately defined deterministic permutation.

## Workloads

### Tree construction

1. `sorted_stream_build`: add monotonically increasing keys through the streaming sorted builder and persist the resulting tree.
2. `shuffled_batch_build`: add the same logical records in deterministic shuffled order through the unsorted batch builder.

Both rows include ingestion, node construction, SQLite writes, and final named-root publication. Reopen validation and integrity checking occur after timing. The shuffled build is measured through 10M. A timeout, out-of-memory termination, disk-capacity failure, or other resource failure is retained rather than replaced with an estimate.

### Reads

1. `random_reads_cold_manager`
2. `random_reads_warm_manager`
3. `clustered_reads_cold_manager`
4. `clustered_reads_warm_manager`
5. `right_edge_reads_cold_manager`
6. `right_edge_reads_warm_manager`

Cold-manager rows use a newly opened store and manager with no decoded prolly nodes cached. The OS page cache is not flushed. Warm-manager rows first execute the exact probe sequence once outside the timed interval. Result bytes are consumed through `black_box`, and every returned value is checked.

### Mutations

1. `append_batch_upserts`: add keys strictly above the current maximum.
2. `random_batch_updates`: replace values at deterministic randomly distributed existing keys.
3. `clustered_batch_updates`: replace values in a contiguous region centered in the keyspace.
4. `random_batch_deletes`: delete deterministic randomly distributed existing keys.
5. `clustered_batch_deletes`: delete a contiguous region centered in the keyspace.

Each mutation workload starts from an isolated base fixture with a fresh manager. Timing includes prolly planning, SQLite reads, node construction, and durable SQLite writes. Post-timing validation checks all changed keys and exact logical cardinality.

### Diff

1. `identical_diff`
2. `append_sparse_diff`
3. `random_sparse_diff`
4. `clustered_sparse_diff`
5. `random_delete_diff`
6. `clustered_delete_diff`

The changed tree is prepared outside the diff interval in the same isolated fixture. A fresh manager performs the timed diff. Validation compares the exact changed-key set and change kind, not only the number of results.

### Merge

1. `append_disjoint_sparse_merge`
2. `random_disjoint_sparse_merge`
3. `clustered_disjoint_sparse_merge`
4. `random_conflict_resolved_merge`
5. `clustered_conflict_resolved_merge`

Left and right trees are prepared outside the timed merge interval from the same base. Disjoint workloads use non-overlapping keys. Conflict workloads update the same keys with different values and use a deterministic resolver that chooses the right value. Validation checks every changed key, exact cardinality, and representative unchanged keys.

## SQLite Profiles

Both profiles use:

- Bundled SQLite from the adapter dependency.
- WAL journal mode.
- A 5-second busy timeout.
- `temp_store=MEMORY`, matching the adapter configuration.
- A single benchmark process with no competing database writer.

The FULL profile sets `synchronous=FULL`. The NORMAL profile sets `synchronous=NORMAL`. Profile names are included in every output row and result key. Results from different profiles are never aggregated together.

## Measurement Schema

Every workload row contains:

- version, durability profile, record count, repetition, and workload
- logical operation count
- total nanoseconds, nanoseconds per operation, and operations per second
- validation status and error text
- prolly nodes read/written and bytes read/written
- node-cache hits, misses, and evictions
- result tree node, leaf, internal-node, entry-count, height, and encoded-byte statistics when applicable
- database, WAL, shared-memory, and total fixture bytes before and after the workload
- SQLite node-row count and summed node-payload bytes

The runner adds process wall time, exit status, peak RSS, involuntary context switches, page faults, stdout path, stderr path, and timing path. Peak RSS belongs to the workload process, including untimed scenario preparation when diff or merge needs derived trees; the report labels this limitation explicitly.

## Validation

A row is valid only if all applicable checks pass:

- Base and result logical counts are exact.
- Every sampled read returns its expected bytes.
- Every upsert and delete has the expected post-state.
- Diff keys and change kinds exactly match the deterministic expected set.
- Merge output exactly matches the deterministic expected map.
- The named result tree reopens from a new store and manager.
- Representative first, middle, last, changed, and unchanged keys validate after reopening.
- `PRAGMA integrity_check` returns `ok`.
- The process exits zero and reports workload status `ok`.

Invalid and failed rows remain in the manifest and the report’s failure section. They do not contribute to performance medians.

## Aggregation and Regression Classification

For every version/profile/size/workload tuple, report median and full measured range. Percentage change is `(current - original) / original`; lower is better for latency, I/O, size, and memory, while higher is better for throughput.

Classifications are:

- `material regression`: at least 3% slower and original/current median workload duration is at least 1 ms.
- `material gain`: at least 3% faster and the same duration floor is met.
- `noise-sensitive`: absolute latency change below 3%, either median duration below 1 ms, or broad range overlap that weakens attribution.
- `memory regression`: peak RSS grows by at least 5% and 4 MiB.
- `size regression`: total SQLite fixture bytes grow by at least 3% and 1 MiB.
- `I/O regression`: nodes or bytes read/written grow by at least 3%, reported even if latency improves.

All positive deltas remain visible regardless of classification. The report starts with failures and regressions, followed by gains and the complete matrix. It never summarizes a workload as regression-free when memory, size, or I/O crosses its own threshold.

## Diagnostic Follow-up

The primary report compares product defaults. If it exposes a material regression, a targeted follow-up may run the current implementation with a legacy-equivalent format or bounded/disabled node cache to separate format, caching, and algorithm effects. Attribution runs are labeled separately and never replace the primary comparison.

Initial falsifiable hypotheses are:

1. If right-edge canonical reuse is effective, append mutation and append diff gains will grow with tree size.
2. If the guarded value-update path removes routing overhead, random and clustered update regressions will disappear once batches exceed its eligibility threshold.
3. If decoded-node caching causes the observed 10M RSS increase, bounding or disabling the cache will remove most of the memory delta while increasing reads.
4. If persisted format metadata or cardinality tracking drives diff/merge overhead, latency and bytes-read deltas will correlate with node visits rather than SQLite synchronization mode.

## Reproducibility and Cleanup

The runner records:

- current and baseline revisions
- dirty-worktree state
- shared harness hash
- benchmark binary hashes
- Rust and SQLite versions
- machine and filesystem details
- all environment variables that affect the matrix
- run order, timestamps, exit status, and artifact paths

Temporary worktrees, build targets, databases, WAL files, shared-memory files, and fixture clones are removed after report generation and audit. Checked-in source, raw evidence, normalized results, machine metadata, and the Markdown report remain.

## Acceptance Criteria

The report is complete only when:

1. The exact shared harness compiles at both revisions.
2. All requested sizes, profiles, workloads, versions, and repetitions are represented or have an explicit retained failure.
3. Every successful row passes logical, reopen, and SQLite integrity validation.
4. Aggregates reproduce deterministically from raw artifacts.
5. Regressions in latency, memory, size, and I/O are listed before gains.
6. Source formatting, tests, lint, and diff hygiene pass.
7. No temporary benchmark artifact remains outside the retained result directory.
