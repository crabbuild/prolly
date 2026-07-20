# SQLite and Turso Local Adapter Performance Comparison Design

## Goal

Build and run a reproducible local-only performance comparison between the
preferred prolly integration for SQLite and the preferred prolly integration
for native Turso Database. Measure throughput and latency from 10,000 through
2,000,000 records for append, deterministic random, and clustered key patterns
across individual put, batch, diff, and conflict-free three-way merge APIs.

The comparison is end to end rather than a raw SQL-engine microbenchmark:

- SQLite uses synchronous `Prolly<SqliteStore>`.
- Turso uses native asynchronous `AsyncProlly<TursoStore>` on Tokio.

The difference between prolly's synchronous and asynchronous execution paths is
therefore part of the measured production behavior and must be stated in every
report. The benchmark does not use Turso Cloud, the adapter's
`turso-cloud-sync` feature, credentials, `push()`, or `pull()`.

## Benchmark Matrix

The full matrix uses these base record counts:

- 10,000
- 50,000
- 100,000
- 500,000
- 1,000,000
- 2,000,000

Each workload changes `clamp(records / 100, 100, 10_000)` keys. Workload inputs
use a fixed seed and are identical between adapters and repetitions.

Keys use the fixed-width ASCII encoding `key-{id:020}`, which is 24 bytes for
the supported range. Values use `value-{id:020}-{generation:02}-payload`, which
is 37 bytes. This keeps record count and access pattern as the
independent variables rather than key or payload width.

Every adapter/record-count cell has three independent repetitions. Repetitions
use fresh database fixtures. Adapter execution order alternates by repetition
to reduce systematic thermal and ordering bias.

## Access Patterns

### Append

Append mutations create new keys immediately beyond the base tree's right edge.
For merge, the left branch gets the first append range and the right branch gets
the immediately following, disjoint append range.

### Random

Random mutations update deterministic unique indexes distributed across the
existing keyspace. A fixed xorshift seed generates unique indexes in a stable
pseudorandom order, so individual puts exercise random traversal order and both
adapters receive byte-identical inputs. Batch processing may sort those inputs
internally as part of the public API.

For merge, one deterministic set containing twice the per-branch change count
is split into disjoint left and right sets.

### Clustered

Clustered mutations update a contiguous interval centered in the existing
keyspace. For merge, one interval containing twice the per-branch change count
is divided into adjacent, disjoint left and right halves.

## Measured APIs

### Individual Put

Apply the workload one key at a time with `put()`, carrying each returned tree
into the next call. Record the duration of every call and the total sequence.
Report total throughput plus p50, p95, p99, and maximum per-key latency. Verify
the final tree contains the expected record count and values.

### Batch

Apply the complete workload in one `batch()` call. Report total call latency
and changed keys per second. Verify the resulting tree exactly as for individual
put.

### Diff

Construct the changed tree outside the timed region using `batch()`. Time only
`diff(base, changed)`. Report call latency, diffs returned per second, and the
exact diff count. Append, random, and clustered diff cells must each return the
configured change count.

### Merge

Construct disjoint left and right trees outside the timed region. Time only the
conflict-free `merge(base, left, right, None)` call. Each branch receives the
configured change count, so throughput uses twice that count. Verify the merged
tree contains both branches' values, retains sampled unaffected base values,
and has the expected record count.

## Fixture Lifecycle and Cache State

For each adapter, record count, and repetition:

1. Create a new local database on the same filesystem as the output directory.
2. Build the base tree in deterministic append batches outside all API timing.
3. Publish a named base root and validate sampled first, middle, and last keys.
4. Close all database and prolly handles.
5. Clone the closed fixture into an isolated working directory for every
   pattern/API cell, including database sidecar files.
6. Reopen the cloned database and load the named root with a new prolly manager.
7. Run and validate exactly one measured cell.
8. Close and remove the cell database unless fixture retention is enabled.

The fresh manager makes prolly's in-process node cache cold at the start of each
cell. The operating-system filesystem cache is not forcibly dropped because
portable and reliable cache dropping is not available. Fixture copying can
warm filesystem pages, so reports must label results as cold-manager,
uncontrolled-OS-cache measurements.

Fixture creation and copying are excluded from put, batch, diff, and merge
timings. Base fixture build time and resulting bytes are recorded separately as
context, not mixed into API measurements.

## Adapter Configuration

Both adapters use the same prolly tree configuration, mutation vectors, Tokio
worker count where relevant, filesystem, process priority, and release build.

SQLite uses `SqliteStore::open_with_config` with the adapter's default local
configuration: a 5,000 ms busy timeout, WAL enabled, and synchronous NORMAL.
Turso uses `TursoBackend::open` and `TursoStore` with native Turso 0.7 defaults.
The benchmark does not apply experimental journal or concurrency pragmas.

These settings compare the documented out-of-box local behavior of each
adapter. They do not claim identical fsync or journaling semantics. The run
manifest records the exact package versions and configurations.

## Metrics and Raw Result Schema

Each raw result row contains:

- benchmark schema version;
- git revision and dirty-state marker;
- adapter (`sqlite-sync` or `turso-async`);
- record count, repetition, API, and pattern;
- configured and observed change counts;
- total nanoseconds and operations per second;
- p50, p95, p99, and maximum per-operation nanoseconds where applicable;
- database bytes before and after the measured cell;
- expected and observed result record counts;
- validation status and escaped error text.

Percentiles use the nearest-rank definition over sorted individual put
durations. Batch, diff, and merge have one timed API call per independent
repetition; the summary reports median, minimum, and maximum call latency across
the three repetitions rather than inventing within-run percentiles.

The comparison summary reports Turso/SQLite latency ratios and Turso/SQLite
throughput ratios. Ratios always compare the same records, API, and pattern.
Lower latency ratios and higher throughput ratios favor Turso.

## Outputs

Every output directory contains:

- `raw-results.csv`: one durable row per measured repetition;
- `summary.csv`: median/minimum/maximum and cross-adapter ratios;
- `report.md`: methodology, validation state, compact comparison tables,
  scaling observations, and limitations;
- `machine.txt`: operating system, CPU, memory, filesystem, Rust, and Cargo
  details available without privileged access;
- `run-manifest.txt`: git state, dependency versions, command line, fixed seed,
  configurations, sizes, repetitions, start/end times, and benchmark schema;
- `fixture-results.csv`: untimed fixture-build duration, throughput, bytes, and
  validation for each adapter/size/repetition.

The runner defaults to a dated directory under
`performance-results/sqlite-turso-local-YYYY-MM-DD` and accepts an explicit
output directory for repeatability and resume.

Raw and fixture CSV files are flushed after every completed row. The runner can
resume an interrupted output directory by validating its schema and skipping
complete primary-key tuples. It never silently mixes different schema versions,
seeds, configurations, or git revisions.

## Execution Controls

The release benchmark supports filters for adapters, record counts, APIs,
patterns, and repetitions. Defaults select the approved full matrix. A smoke
profile explicitly overrides the normal change-count floor and uses 100 records,
10 changes, one repetition, and all APIs/patterns.

Elapsed-time and free-disk guards are checked between cells. Hitting a guard
flushes all results, records the reason in the manifest, exits nonzero, and
leaves the run resumable. Successful cells remove temporary clones; a
keep-fixtures option retains them for diagnosis.

The benchmark runs serially. It does not overlap SQLite and Turso work or issue
concurrent API calls, because the requested comparison concerns single-client
local adapter behavior.

## Validation and Failure Semantics

No successful result row is emitted until its output is validated. Validation
includes:

- reopening and loading the named base root;
- exact expected record count for append and update patterns;
- sampled first, middle, last, changed, and unaffected keys;
- exact diff count and changed-key membership;
- exact merged branch values and unaffected base samples;
- persistence of the measured result through a named root and reopen check.

An adapter, cloning, parsing, or validation failure emits a failed row with its
error, preserves all prior rows, prevents aggregation from treating the cell as
valid, and makes the overall runner exit nonzero. Reports show missing and
failed cells explicitly rather than averaging incomplete data.

## Testing Strategy

Unit tests cover:

- record-count-to-change-count clamping;
- fixed-width key ordering and value generations;
- deterministic unique random indexes;
- clustered indexes and disjoint merge branches;
- nearest-rank percentile calculation;
- raw CSV round trips and escaped errors;
- primary-key computation and resume filtering;
- aggregation, ratio direction, and incomplete-cell rejection.

Integration verification runs the 100-record smoke profile against local
SQLite and local Turso files, exercises all three patterns and four APIs,
checks every validation flag, and confirms that no network-related environment
variables or sync features are required.

Before the full matrix, formatting, unit tests, strict Clippy, release build,
and the smoke profile must pass. After the run, aggregation is regenerated from
raw rows and checked for the expected complete matrix cardinality.

## Reporting Limitations

The final report must state that:

- it compares preferred end-to-end prolly paths, not raw SQL engines;
- SQLite is synchronous while Turso is asynchronously executed on Tokio;
- durability defaults are documented but not asserted to be identical;
- the prolly manager cache is cold while the OS cache is uncontrolled;
- results describe the recorded machine, filesystem, code revision, and Turso
  beta version;
- local results do not predict Turso Cloud push/pull latency or throughput.
