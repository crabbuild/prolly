# PostgreSQL-backed Prolly scale performance

## Reproducible service and scale harness

The Rust harness now measures two complementary suites:

- `service`: closed-loop concurrent point reads, multi-reads, commits, diffs,
  and merges across independent and contended named roots. It sweeps client
  counts and SQLx pool sizes and includes pool wait in end-to-end latency.
  Each logical operation uses a fresh Prolly manager, deliberately preventing
  decoded node-cache reuse between requests while leaving PostgreSQL and host
  caches warm.
- `scale`: the existing serial large-tree matrix, now configurable for record
  count, value size, operations, mutation patterns, and repetitions.

Run the fast combined smoke profile:

```bash
scripts/run_postgres_scale_benchmark.sh \
  --config benchmarks/postgres-scale/workloads/smoke.toml \
  --suite both \
  --output performance-results/postgres-service-smoke
```

Run the default service-scale profile:

```bash
scripts/run_postgres_scale_benchmark.sh \
  --config benchmarks/postgres-scale/workloads/default.toml \
  --suite service \
  --output performance-results/postgres-service
```

The TOML file is the workload contract. Service controls include tree records
and value bytes, clients, pool sizes, tenant count, hot-root share, operation
mix, warmup and measurement duration, timeouts, CAS retries, retained versions,
and PostgreSQL adapter batch size. Scale controls include sizes, value bytes,
operations, patterns, changes, read samples, and disk guard. Common settings
can also be overridden on the Rust executable; run it with `--help` for the
complete list.

Each result directory contains the original and resolved TOML, a configuration
hash and revision manifest, machine and PostgreSQL fingerprints, dependency and
binary provenance, durable raw CSV rows, summary CSV files, and a generated
Markdown report. Interrupted service runs discard only incomplete
client/pool cells and resume completed cells with matching provenance.

Service raw rows include HDR p50/p95/p99/p99.9/max latency, attempted and
successful throughput, CAS conflict/retry counters, Prolly cache/node/store
counters, `pg_stat_statements`, I/O/WAL/transaction counters, and physical
database/table/index sizes. A baseline directory enables configured throughput,
p99, conflict, error, and SQL-statement regression gates:

```bash
scripts/run_postgres_scale_benchmark.sh \
  --config benchmarks/postgres-scale/workloads/default.toml \
  --suite service \
  --baseline performance-results/postgres-service-baseline
```

Material machine or PostgreSQL configuration mismatches reject regression
comparison by default. `--allow-environment-mismatch` makes the comparison
explicitly exploratory and skips its gates.

## 2026-07-25 verification evidence

The combined smoke evidence is in
[`performance-results/postgres-service-smoke`](../performance-results/postgres-service-smoke/).
It contains 20 concurrent service rows and 25 serial scale rows. All 45 rows
validated, a second invocation measured zero rows and skipped all 45 completed
rows, and a deliberately mismatched revision was rejected without changing the
existing run manifest.

On this host, the 1,000-record smoke workload produced 250.3 successful
operations/s with one client and 298.7 successful operations/s with four
clients sharing a two-connection pool. The four-client cell recorded 81 CAS
conflicts and no timeout, SQL, validation, or worker-panic errors. These short
smoke numbers validate the harness and are not capacity estimates.

The adapter integration test observed exactly three node `INSERT` statements
when publishing five unique nodes with a two-item chunk limit, equal to
`ceil(5 / 2)`. A forced failure in the third chunk left neither earlier-chunk
node in the table. The ordered read test returned five requested slots,
including a missing CID and a duplicate, across three two-item `UNNEST`
chunks. Sixteen same-root CAS contenders produced exactly one winner; a
separately locked root did not prevent another root from completing.

The smoke scale rows also show the removal of per-node publication statements.
An append batch wrote five nodes in one engine batch and used three total
Prolly-table statements; a random batch wrote nine nodes and used five total
statements. In the historical 1-million-record baseline, the corresponding
append and random batch rows wrote 2,295 and 7,659 nodes while recording 2,299
and 15,318 statements. The tree sizes differ, so these are statement
amplification observations rather than latency comparisons.

A representative 100,000-record saturation run is recorded in
[`performance-results/postgres-service-verification`](../performance-results/postgres-service-verification/).
Each of its eight cells used a five-second warmup and 20-second measurement:

| Clients | Pool | Successful ops/s | Conflicts | Retries | Worst operation/class p99 | PG statements/success |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 8 | 26.6 | 0 | 0 | 175 ms | 16.69 |
| 1 | 32 | 26.2 | 0 | 0 | 180 ms | 16.68 |
| 8 | 8 | 83.0 | 126 | 76 | 926 ms | 18.03 |
| 8 | 32 | 92.0 | 147 | 92 | 582 ms | 18.27 |
| 32 | 8 | 74.4 | 344 | 268 | 1,991 ms | 19.50 |
| 32 | 32 | 103.3 | 515 | 396 | 1,417 ms | 20.28 |
| 64 | 8 | 70.3 | 452 | 345 | 3,823 ms | 20.17 |
| 64 | 32 | 98.2 | 646 | 493 | 2,647 ms | 20.93 |

All eight cells completed with zero unexpected errors. The sweep demonstrates
both connection-pool saturation and deliberate head contention: throughput
peaked at 32 clients with a 32-connection pool, while retries and tail latency
rose as concurrency increased. The result remains specific to the captured
machine, Docker/PostgreSQL configuration, workload, and dirty revision.

## Historical serial baseline

Measured on 2026-07-18 at revision `42873d1561c3641c9091fe2a616567e790029762`
with a dirty working tree. The complete generated table, raw rows, PostgreSQL
counters, and environment metadata are in
[`performance-results/postgres-scale-2026-07-18`](../performance-results/postgres-scale-2026-07-18/).

## Result

The PostgreSQL-backed engine scales well for initial construction, ordered
append batches, warm point reads, and localized ordered reads. It does not
currently scale for random or clustered mutation: any mutation that is not a
strict append falls through to `rebuild_tree`, reads the entire base tree, and
writes a complete replacement tree.

At 10 million records, a 10,000-key append batch completed in **45.3 ms**
(220,849 updates/s). The same-sized random and clustered batches took
**92.0 s** and **93.3 s** (109 and 107 updates/s). A single non-append put also
took about **89–90 s**, showing that cost is driven by rebuilding the 10 million
record tree rather than by the number of mutations.

## Workload

- PostgreSQL 16 Alpine in a dedicated Docker Compose project and volume, with
  `pg_stat_statements` and `track_io_timing` enabled.
- Fixed 24-byte ordered keys and 27-byte values; deterministic random sampling.
- Base sizes of 1,000,000 and 10,000,000 records.
- Append, random, and centered clustered key patterns.
- 10,000 keys per batch, query, scan, and diff; merge applies disjoint 10,000-key
  changes on each side.
- Build and full scan run once per size. Other workload cells run three times
  and report the median.
- Serial, single-client, end-to-end public async API calls through SQLx and
  PostgreSQL. `cold-manager` clears the decoded Prolly-node cache; it does not
  drop PostgreSQL or host filesystem caches.

## Headline metrics

| Workload | 1M | 10M |
|---|---:|---:|
| Initial build | 3.621 s (276,197 records/s) | 33.482 s (298,667 records/s) |
| Full scan | 5.335 s (187,456 records/s) | 54.363 s (183,949 records/s) |
| Append batch, 10k | 40.98 ms (244,023/s) | 45.28 ms (220,849/s) |
| Random batch, 10k | 8.933 s (1,120/s) | 91.976 s (109/s) |
| Clustered batch, 10k | 9.042 s (1,106/s) | 93.280 s (107/s) |
| Single append put | 7.48 ms | 12.35 ms |
| Single random put | 9.214 s | 89.286 s |
| Single clustered put | 9.186 s | 89.855 s |
| Cold point get, 10k calls | 27.64–27.93 s (2.76–2.79 ms/get) | 27.33–28.48 s (2.73–2.85 ms/get) |
| Warm point get, 10k calls | 7.52–18.12 ms (0.75–1.81 us/get) | 7.67–26.86 ms (0.77–2.69 us/get) |
| Ordered `get_many`, 10k | 51–57 ms | 50–54 ms |
| Random `get_many`, 10k | 2.967 s | 6.753 s |
| Ordered range scan, 10k | 53–60 ms | 53–56 ms |

## Diff and merge

| Workload | 1M | 10M |
|---|---:|---:|
| Append diff, 10k changes | 61.4 ms | 53.2 ms |
| Clustered diff, 10k changes | 104.7 ms | 110.2 ms |
| Random diff, 10k changes | 6.395 s | 13.342 s |
| Append merge, 20k changes | 92.3 ms (216,625 changes/s) | 111.6 ms (179,255 changes/s) |
| Clustered merge, 20k changes | 8.905 s | 87.586 s |
| Random merge, 20k changes | 11.296 s | 100.711 s |

Append and clustered diffs remain localized because changed keys occupy a
contiguous region. Random diff visits much more of the tree. Merge inherits the
mutation behavior: append remains incremental, while random and clustered
merges rebuild.

## Storage and database work

The base trees contained 7,719 nodes at 1M and 76,974 nodes at 10M, both at
height 3. Their serialized node content was 30.2 MiB and 302.0 MiB. PostgreSQL's
post-build table/index sizes were approximately 9.4 MiB at 1M and 92.0 MiB at
10M; PostgreSQL TOAST compression and content-addressed storage make these
figures different from the logical serialized-byte total.

A 10M random or clustered batch read about 302 MiB and wrote about 302 MiB,
with roughly 153,953 PostgreSQL statements. Median database execution time was
only 4.7–4.8 seconds of the 92–93 second wall time. This indicates that the
dominant opportunity is eliminating full-tree materialization/rebuild and the
per-node statement count, not tuning an individual PostgreSQL statement.

## Interpretation and run note

Docker Desktop stopped once under host disk/swap pressure during the long run.
The dedicated PostgreSQL container was restarted, the unlogged fixture snapshot
was rebuilt, and the matrix resumed by skipping already validated cells. The
resume binary only changed untimed fixture restoration and disk-guard ordering;
the timed workload code was unchanged. The restart reset PostgreSQL/OS cache
warmth for later repetitions, so modest run-to-run differences should not be
overinterpreted. Raw min/max values are preserved in the generated report.

These results describe one Apple M2 Max host, Docker Desktop, PostgreSQL 16,
small fixed values, and a serial client. They are not concurrency, connection
pool, large-value, or remote-network measurements.
