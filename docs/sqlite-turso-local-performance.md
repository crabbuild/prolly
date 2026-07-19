# SQLite and Turso Local Performance Comparison

This benchmark compares prolly's preferred production integrations for local
database files:

- SQLite: synchronous `Prolly<SqliteStore>` using `SqliteStoreConfig::default()`.
- Turso: native asynchronous `AsyncProlly<TursoStore>` on Tokio using
  `TursoBackend::open()`.

It is an end-to-end adapter comparison, not a raw SQL-engine microbenchmark.
It never enables Turso Cloud sync, reads credentials, or calls `push()` or
`pull()`.

## Run it

Run the complete release matrix from the prolly repository:

```sh
scripts/run_sqlite_turso_local_comparison.sh
```

The equivalent explicit environment interface is:

```sh
BENCH_OUT=performance-results/sqlite-turso-local-2026-07-18 \
BENCH_SIZES=10000,50000,100000,500000,1000000,2000000 \
BENCH_RUNS=3 \
BENCH_APIS=put,batch,diff,merge \
BENCH_PATTERNS=append,random,clustered \
BENCH_ADAPTERS=sqlite-sync,turso-async \
scripts/run_sqlite_turso_local_comparison.sh
```

The default output is
`performance-results/sqlite-turso-local-YYYY-MM-DD`. A quick local validation
uses the same APIs and patterns with 100 records and 10 changes:

```sh
scripts/run_sqlite_turso_local_comparison.sh \
  --profile smoke \
  --output performance-results/sqlite-turso-local-smoke
```

`BENCH_PROFILE=smoke` and `BENCH_OUT=...` provide the same smoke selection.
Other environment controls are `BENCH_MAX_SECONDS`, `BENCH_MIN_FREE_GB`,
`BENCH_KEEP_FIXTURES=1`, and `BENCH_TOKIO_WORKERS`. Explicit CLI flags override
the profile and output environment values; workload filters can also be passed
through as CLI flags.

The full profile uses 10K, 50K, 100K, 500K, 1M, and 2M base records, three
fresh repetitions, append/random/clustered inputs, and put/batch/diff/merge.
Changes per branch are 1% of the base size, bounded to 100–10K. Keys are fixed
at 24 bytes and values at 37 bytes.

Useful controls can be forwarded to the Rust runner:

```sh
scripts/run_sqlite_turso_local_comparison.sh \
  --output /path/on/the/benchmark-filesystem \
  --max-seconds 21600 \
  --min-free-gb 20 \
  --tokio-workers 8
```

Filters are comma-separated: `--adapters`, `--sizes`, `--apis`, and
`--patterns`. Other controls include `--runs`, `--changes`,
`--build-batch-size`, and `--keep-fixtures`.

## Measurement contract

Each adapter/size/repetition starts from a newly built, closed local fixture.
Every API/pattern cell receives a filesystem clone including sidecars and opens
a new prolly manager, so the manager's node cache begins cold. Fixture building,
copying, diff-tree preparation, and merge-branch preparation are outside the
timed region. The operating-system filesystem cache is deliberately not
controlled.

Individual `put` records total throughput plus nearest-rank p50, p95, p99, and
maximum per-key latency. Batch, eager diff, and conflict-free three-way merge
record one total call per independent repetition. Merge throughput counts both
disjoint branches. Every successful row is validated for exact changes, result
values and record count, then published and reopened before it is written.

Results are written durably after each cell and can be resumed by rerunning the
same command. `run-manifest.txt` prevents mixing revisions, filters, seeds,
worker counts, or other workload settings. Elapsed-time and disk-space guards
stop between cells and leave completed rows resumable.

The complete matrix contains exactly 432 validated raw rows and 36 validated
fixture rows; aggregation emits 72 paired summary rows. A failure remains as a
durable `validated=false` row. Rerunning the identical command retries that
primary key and atomically replaces the failed row only after the retry has a
new result. Set `BENCH_KEEP_FIXTURES=1` before a diagnostic run to retain cell
and source databases.

## Outputs and interpretation

- `raw-results.csv`: validated per-repetition measurements.
- `fixture-results.csv`: untimed fixture build context.
- `summary.csv`: per-cell median/min/max values and cross-adapter ratios.
- `report.md`: compact ratio table and limitations.
- `machine.txt`: OS, CPU/memory where available, filesystem, Rust, and Cargo.
- `dependencies.txt`: exact resolved Rust dependency tree.
- `run-manifest.txt` and `run-status.txt`: resume contract and current state.

The raw CSV schema is: `schema`, `revision`, `dirty`, `adapter`, `records`,
`repetition`, `api`, `pattern`, `configured_changes`, `observed_changes`,
`total_ns`, `operations_per_sec`, `p50_ns`, `p95_ns`, `p99_ns`, `max_ns`,
`db_bytes_before`, `db_bytes_after`, `expected_records`, `observed_records`,
`validated`, and `error`. Fixture rows contain schema/provenance and the
adapter/records/repetition key followed by `build_ns`, `records_per_sec`,
`database_bytes`, `observed_records`, `validated`, and `error`.

`turso_over_sqlite_latency_ratio` below 1 favors Turso.
`turso_over_sqlite_throughput_ratio` above 1 favors Turso. The durability
defaults are not asserted to have identical fsync semantics, and results from
local files do not predict Turso Cloud synchronization behavior.

