# SQLite-Backed Prolly Key-Pattern Benchmark Design

## Goal

Measure end-to-end `Prolly<SqliteStore>` behavior as a persistent local key-value
map at 10,000, 50,000, 100,000, 500,000, and 1,000,000 base records. Compare
append, deterministic random, and clustered access patterns using fixed-width
24-byte keys and fixed-width 100-byte values.

The benchmark measures the prolly map and SQLite adapter together. It is not a
raw SQLite SQL benchmark or a direct `Store` trait microbenchmark.

## SQLite and Tree Configuration

Use a file-backed `SqliteStore` with the adapter defaults:

- 5,000 ms busy timeout;
- WAL journal mode;
- `synchronous=NORMAL`;
- `temp_store=MEMORY`.

Use the production default prolly tree configuration. Record the resolved
configuration and software revision in the run manifest.

## Records and Determinism

Keys use `key-{id:020}`, exactly 24 ASCII bytes. Values are deterministic,
exactly 100 bytes, and encode the record identifier and generation before a
stable padding suffix. Tests must assert both widths.

The base map contains identifiers `0..records` at generation zero and is built
in sorted order. Random identifiers come from a fixed seed and are unique.
Clustered identifiers form one contiguous interval centered in the existing
keyspace. All repetitions use byte-identical logical inputs.

## Matrix

Run three independent repetitions for each base size:

- 10,000;
- 50,000;
- 100,000;
- 500,000;
- 1,000,000.

The mutation count is one percent of the base size, clamped to 100 through
10,000 records. Every repetition starts with a fresh SQLite fixture. Each
measured workload receives an isolated clone of the closed fixture and opens a
fresh prolly manager.

## Fixture Build

Measure construction of the complete base map with `SortedBatchBuilder` and
publish a named root. Report build latency, records per second, resulting tree
shape, logical tree bytes, SQLite file bytes, and validation state. Fixture
build time is context and is excluded from mutation and read timings.

## Mutation Workloads

Measure each pattern through both repeated individual `put` calls and one
transactional `batch` call:

- **Append:** insert new identifiers immediately after the base map's right
  edge.
- **Random:** update unique existing identifiers distributed across the full
  keyspace.
- **Clustered:** update a contiguous interval centered in the existing
  keyspace.

Individual put timing records total throughput and per-call p50, p95, p99, and
maximum latency. Batch timing records total call latency and logical mutations
per second. Validate expected cardinality and all changed values, publish the
result root, close the database, reopen it, and validate persistence before
accepting a row.

## Point-Read Workloads

Measure deterministic point reads in three regions:

- random identifiers distributed across the full map;
- a centered contiguous cluster;
- a contiguous interval at the right edge.

Use up to 10,000 reads per workload. For cold-manager measurements, open a new
manager immediately before the timed sequence. For warm-manager measurements,
perform one untimed pass, reset metrics, and time an identical second pass.
The operating-system filesystem cache is uncontrolled in both cases and this
limitation must be stated in the report.

Record total throughput, per-call latency percentiles, node I/O, and manager
cache counters. Validate every returned value outside the timed region or with
precomputed expected bytes so correctness checking does not distort one pattern
differently from another.

## Range-Scan Workloads

Measure bounded scans for random, clustered, and right-edge regions. Each scan
returns the same configured number of existing records, up to the mutation
sample count. Time eager consumption of the complete range result, report rows
per second and total latency, and validate exact first/last keys, ordering,
cardinality, and values.

## Repetition, Cache, and Ordering Controls

Run cells serially. Alternate pattern order between repetitions so one pattern
does not always benefit from machine ordering or thermal state. Clone only
closed SQLite fixtures, including sidecar files. Start each cell with a fresh
manager; warm-manager point reads explicitly warm only that manager.

Do not claim a physically cold disk cache. Report results as fresh-manager or
warm-manager with uncontrolled OS cache.

## Outputs

Write a dated directory under `performance-results/` containing:

- `raw-results.csv`, one validated row per repetition and workload;
- `fixture-results.csv`, one row per base fixture;
- `summary.csv`, median/minimum/maximum by size and workload;
- `report.md`, compact scaling tables, observations, and limitations;
- `machine.txt`, available CPU, memory, OS, filesystem, Rust, and Cargo data;
- `run-manifest.txt`, exact command, revision, dirty state, seed, sizes,
  repetition count, key/value widths, and configurations;
- `run-status.txt`, so interrupted runs are visibly incomplete.

Flush raw output after every cell. A failed validation produces a durable failed
row and makes the complete run fail; summaries exclude failed or incomplete
cells and clearly identify missing data.

## Metrics

Every workload row records:

- size, repetition, operation, pattern, and cache state where applicable;
- configured and observed operation counts;
- total nanoseconds, nanoseconds per operation, and operations per second;
- p50, p95, p99, and maximum per-call latency where individual calls exist;
- prolly nodes and bytes read/written;
- manager cache hits, misses, and evictions;
- tree entries, nodes, leaves, internal nodes, height, and logical bytes;
- SQLite database, WAL, and shared-memory bytes;
- validation state and escaped error text.

Percentiles use the nearest-rank definition over sorted per-call durations.
Summary statistics use the median, minimum, and maximum of the three independent
repetitions; they do not invent within-run percentiles for one-call batches or
range scans.

## Testing and Verification

Unit tests cover fixed key/value widths, deterministic random identifiers,
clustered and right-edge ranges, mutation-count bounds, nearest-rank
percentiles, matrix cardinality, CSV round trips, and summary aggregation.

A smoke profile uses 100 base records, 10 operations, one repetition, and every
operation/pattern/cache-state combination. It must validate published roots and
reopen persistence without network access.

Before the full run, formatting, unit tests, strict Clippy, a release build, and
the smoke profile must pass. Afterward, regenerate the summary from raw rows and
verify the expected matrix cardinality.

## Interpretation Limits

The final report must state that results describe:

- end-to-end synchronous prolly behavior on the recorded machine;
- one local SQLite connection and serial client workload;
- WAL plus `synchronous=NORMAL`, not `FULL` durability;
- a fresh or warmed prolly manager but uncontrolled operating-system cache;
- fixed 24-byte keys and 100-byte values;
- the tested code revision and dirty worktree state.

The results do not predict concurrent writers, remote filesystems, other
durability modes, raw SQLite performance, or application-level serialization
and business logic.
