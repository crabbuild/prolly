# Scale Performance Report Design

## Purpose

Measure the improved prolly-tree implementation against the untouched `fa7c219`
baseline across realistic scale and key-locality patterns. The report must retain
raw evidence, identify policy-driven effects separately from engine changes, and
must not extrapolate unmeasured results.

## Compared Implementations

- **Original:** detached worktree at commit `fa7c219`, built in release mode.
- **Improved:** the current `codex/canonical-prolly` worktree, built in release
  mode.
- Both binaries use the same Rust toolchain, dependency resolution, benchmark
  source, deterministic seeds, record representation, operation counts, and
  host machine.
- The primary comparison uses each implementation's product default. This
  intentionally includes the improved key-only boundary and prefix-compressed
  `Node` default.
- A secondary attribution run compares the original default with the improved
  key/value-boundary, prefix-compressed policy where the cross-version API
  permits it. It is reported separately and never substituted for the primary
  result.

## Dataset Matrix

Measure exactly these logical record counts:

- 1,000
- 10,000
- 50,000
- 100,000
- 1,000,000
- 10,000,000

Each dataset is constructed with `SortedBatchBuilder` from generated records so
the harness does not retain a second full input copy. Keys are fixed-width,
lexicographically ordered byte strings derived from unsigned record IDs. Values
have a fixed deterministic payload and equal size in both versions.

Each size/version runs in its own process. A process emits one CSV record per
workload and a final structure record. `/usr/bin/time -l` captures peak resident
memory for the complete process. A failure, timeout, signal, or allocation error
is retained in the raw report rather than replaced with a projected value.

## Workloads

The mutation sample count is `min(records, max(100, records / 100), 10_000)`.
Read workloads use at most 100,000 logical reads so large tiers remain bounded.
All random choices use a checked-in fixed seed and a simple deterministic
generator implemented in the harness.

### Base Construction

Stream all sorted records into a fresh in-memory store. Report wall time,
records/second, peak process RSS, root identity, tree height, node count, leaf
count, internal count, logical entries, and total serialized node bytes.

### Random Point Reads

Read existing keys distributed across the complete keyspace. Warm the path once
before measurement, then report nanoseconds/read and reads/second. Validate that
every expected value is returned.

### Clustered Reads

Read the same number of existing keys from a contiguous region centered near
the middle of the keyspace. Report nanoseconds/read and reads/second, and verify
the returned values.

### Append-Only Mutations

Append sorted keys strictly greater than the base maximum through the direct
append API. Report nanoseconds/mutation, mutations/second, node reads/writes,
serialized bytes written, and resulting structural statistics. Validate the
resulting logical count and sampled appended values.

### Random Mutations

Update existing keys distributed deterministically across the full keyspace in
one sorted batch. Report nanoseconds/mutation, mutations/second, tree/store work,
and resulting structural statistics. Validate sampled updated and untouched
values.

### Clustered Mutations

Update a contiguous run of existing keys around the middle of the keyspace in
one sorted batch. Use the same mutation count and value sizes as the random
workload. Report the same metrics and validations.

### Structural Diff

Diff the base tree against each append, random-update, and clustered-update
result. Report wall time, nanoseconds/change, changes/second, node reads, and
change count. Validate that the diff count and keys exactly match the mutations.

## Repetition and Ordering

- Run one unreported warmup for tiers through 100,000 records.
- Collect three measured runs for tiers through 1,000,000 records.
- Collect two measured runs for 10,000,000 records unless both runs complete
  with low variance and adequate remaining runtime, in which case collect a
  third.
- Alternate original and improved process order per repetition.
- Report medians, individual ranges, percentage change, and absolute delta.
- Treat results below 20 ns/operation or changes below 3% as timer/noise
  sensitive unless the direction is consistent across every repetition.

## Artifacts

- `benches/scale_workloads.rs`: cross-version benchmark harness.
- `scripts/run_scale_report.sh`: reproducible build and alternating run driver.
- `performance-results/scale-2026-07-14/raw/`: raw stdout, stderr, timing, and
  machine metadata.
- `performance-results/scale-2026-07-14/results.csv`: normalized measurements.
- `performance-results/scale-2026-07-14/report.md`: methodology, tables, gains,
  regressions, caveats, and conclusions.

Generated release binaries and detached worktrees are temporary and are not
included in source changes.

## Acceptance Criteria

- Every requested size has a measured result or an explicit captured failure.
- All workload outputs validate their logical results before reporting timing.
- Original and improved runs use identical workload code and inputs.
- Raw values are sufficient to reproduce every aggregate in the report.
- The report lists regressions as prominently as gains.
- No product implementation is changed merely to improve this report.
- The existing Rust test and formatting gates remain green.
