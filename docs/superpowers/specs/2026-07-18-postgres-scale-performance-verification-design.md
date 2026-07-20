# PostgreSQL Prolly Scale Performance Verification Design

## Goal

Measure the current Rust `AsyncProlly<RemoteProllyStore<PostgresBackend>>`
implementation end to end against PostgreSQL 16 at 1,000,000 and 10,000,000
logical records. The benchmark reports reproducible per-operation latency,
throughput, Prolly node and byte I/O, PostgreSQL work, tree shape, and physical
database size for build, put, get, multi-get query, range/full scan, diff, and
three-way merge operations.

The benchmark changes measurement infrastructure only. It does not tune or
modify the Prolly tree, remote-store abstraction, or PostgreSQL adapter.

## Selected Approach

Add a standalone, non-published Rust benchmark crate under
`benchmarks/postgres-scale`. It calls the public asynchronous Prolly and
PostgreSQL adapter APIs in release mode. A repository runner owns a dedicated
Docker Compose project and captures machine, container, dependency, and command
provenance. A strict summarizer validates raw CSV rows before producing median
tables and a Markdown report.

This is preferred to adapting the synchronous in-memory scale benchmark because
the measured async store and database calls must remain real. It is also
preferred to a raw SQL client benchmark because raw SQL would omit Prolly tree
construction, traversal, structural sharing, diff, and merge behavior.

## Environment and Isolation

- Use `postgres:16-alpine` in a dedicated Compose project and volume.
- Expose the container only on `127.0.0.1` using a benchmark-specific port.
- Enable `pg_stat_statements` at server startup and create the extension before
  measurement.
- Use one client process, SQLx's default pool configuration, `Config::default()`,
  the adapter's current schema, and PostgreSQL defaults unless recorded here.
- Run all cells serially. Do not overlap benchmark operations.
- Record the Git revision and dirty state, Rust/Cargo versions, host CPU and
  memory, Docker resource allocation, PostgreSQL version/settings, free disk,
  dependency graph, and release-binary hash.
- The current host has about 9.5 GiB free at design time. Stop before a run if
  less than 3 GiB remains, and delete only the dedicated benchmark container and
  volume after durable results have been copied into `performance-results`.

## Data and Workload Contract

Keys are fixed-width ASCII `key-{id:020}` (24 bytes). Values are fixed-width
ASCII `val-{id:020}-{generation:02}` (27 bytes). IDs are below 10 billion and
therefore preserve numeric ordering under lexicographic comparison.

Base sizes are exactly 1,000,000 and 10,000,000 logical records. A base is built
with one sorted public `batch()` call against an empty tree so build time covers
mutation preparation, canonical node construction, encoding, SQLx calls,
PostgreSQL writes, and commit. The generated mutation vector is outside the
timed region. The resulting tree is published and reopened before validation.

For multi-key mutation workloads, the change count is
`min(10_000, max(100, records / 100))`; both requested sizes therefore use
10,000 keys. Inputs use the fixed seed `0x6a09e667f3bcc909`.

- Append adds IDs `[records, records + changes)`.
- Random updates deterministic unique IDs distributed across the base keyspace.
- Clustered updates one contiguous 10,000-key interval centered in the base.
- Merge gives left and right disjoint change sets of 10,000 keys each. Append
  uses adjacent suffixes, random splits one deterministic 20,000-ID set, and
  clustered splits one centered 20,000-key interval.

Before each independent cell, restore `prolly_nodes`, `prolly_hints`, and
`prolly_roots` from an untimed SQL snapshot of the validated base fixture. This
ensures repeated cells insert the same new content-addressed nodes into an
equivalent database state rather than measuring conflict updates left by an
earlier repetition. Restore, `ANALYZE`, statistics reset, input generation, and
validation are outside the timed region.

## Measured Operations

### Build

Time the single public `batch()` call that creates the complete base tree and
its PostgreSQL transaction. Publish/reopen and full count validation are not in
the build timer. Build is measured once per size because it creates the fixture
used by every other cell; its one-call nature is explicit in the report.

### Put

For each pattern, time one public `put()` call from the restored base using a
fresh manager, repeat three times, and report the latency distribution across
independent calls. A single-key random and clustered put differ by key position;
the 10,000-key pattern distinction is measured by the batch workload below.

### Batch Put

Time one public `batch()` call applying the complete 10,000-key pattern. Report
total call latency and logical changed keys per second. Verify exact changed
values and expected record count.

### Get

Generate 10,000 tail/append hits, 10,000 deterministic random hits, and 10,000
clustered hits.

- `get_cold` clears the manager's decoded-node cache before each timed `get()`.
- `get_warm` first executes the sequence once outside the timer, then measures
  the same sequence with the manager cache retained.

Report total throughput, p50/p95/p99/max latency, hit count, cache hits/misses,
and nodes/bytes read. Cache clearing does not control PostgreSQL or host OS page
caches; the report labels these as cold-manager and warm-manager results.

### Query

Time one public `get_many()` call for 10,000 tail/append keys, 10,000 random
keys, and 10,000 clustered keys using a fresh manager. Report query latency,
keys per second, hits, and node/byte I/O. This defines “query” as the Prolly
multi-key lookup API, not an SQL predicate over encoded nodes.

### Scan

Time 10,000-entry bounded scans over the append/tail and clustered/center
regions and a full ordered scan of the entire tree, consuming keys and values
into a checksum without retaining all entries. A random-key range is not
defined because a range scan is ordered and contiguous; random-key retrieval is
covered by query. Report entries per second, exact observed count, and node/byte
I/O.

### Diff

Construct the changed tree outside the diff timer from a restored base, reset
manager metrics, then time `diff(base, changed)` for append, random, and
clustered changes. Verify exact diff count and key membership. Report diffs per
second and structural-read metrics.

### Merge

Construct disjoint left and right trees outside the merge timer from the same
base, reset metrics, then time `merge(base, left, right, None)`. Verify all
20,000 branch changes, sampled unchanged keys, and expected count. Report
changed keys per second plus read/write metrics.

## Repetitions and Ordering

Build and full scan run once per size. Put, batch, get, query, bounded scan,
diff, and merge run three independent repetitions. Pattern order rotates by
repetition. The report must never describe a single-call value as a percentile;
it reports the observed value directly and marks the sample count. Aggregation
uses the median, minimum, and maximum across repetitions.

## Metrics

Each raw row contains:

- schema version, revision, dirty flag, timestamp, size, repetition, operation,
  pattern, cache state, and sample count;
- logical operations, observed items, total nanoseconds, nanoseconds per logical
  operation, operations per second, and p50/p95/p99/max when individual call
  samples exist;
- Prolly nodes/bytes read and written, batch read/write counts, cache
  hits/misses/evictions, and tree nodes/leaves/internal nodes/height/live bytes;
- PostgreSQL statement calls/execution milliseconds, shared block hits/reads/
  dirties/writes, temp blocks/bytes, WAL bytes, commits/rollbacks, and database/
  table/index bytes before and after;
- validation status and escaped error text.

PostgreSQL statement statistics include only statements referring to
`prolly_nodes`, `prolly_hints`, or `prolly_roots`. Values are database-side
observations and are not substitutes for end-to-end wall time.

## Outputs

The dated output directory contains `raw-results.csv`, `summary.csv`,
`report.md`, `machine.txt`, `postgres.txt`, `dependencies.txt`,
`run-manifest.txt`, build logs, and the release-binary SHA-256. Raw rows are
flushed after every successful cell. The runner can resume only when the frozen
schema, revision, size list, seed, and configuration match.

## Validation and Failure Semantics

Before full scale, unit tests cover key ordering, deterministic pattern sets,
disjoint merge sets, percentile calculation, operation counts, CSV escaping,
and aggregation. A Docker-backed 1,000-record smoke run exercises every API and
pattern and verifies publish/reopen behavior.

A result row is successful only after exact count and sampled value checks. Diff
validates every returned key. Scans validate count, ordering, and checksum.
Merge validates both branches and unchanged samples. Any failed validation,
SQL error, disk guard, non-finite metric, duplicate cell, or incomplete required
matrix stops the run with durable partial output and a nonzero exit.

## Reporting Limits

The report states that results are machine-specific; PostgreSQL runs in Docker
Desktop on the same host; only manager cache state is controlled; build and full
scan have one sample per size; the workload is single-client; values are small
and fixed-width; query means `get_many`; and current PostgreSQL/SQLx/default
settings are measured rather than tuned production settings.
