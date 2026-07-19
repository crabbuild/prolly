# SQLite Sync vs Turso Async Local Prolly Comparison (Partial)

Schema: `sqlite-turso-local-v1`. Revision: `unified-ready` (dirty: `true`). Planned repetitions: 3.

**Partial evidence:** the run was interrupted; tables include only adapter-paired cells with matching completed repetitions and must not be treated as the final matrix.

Lower Turso/SQLite latency ratios favor Turso; higher throughput ratios favor Turso.
Validation: 18 measured cells and 6 base fixtures passed the frozen row contract.

| Records | API | Pattern | Turso/SQLite latency | Turso/SQLite throughput |
|---:|---|---|---:|---:|
| 10,000 | put | append | 6.509× | 0.154× |
| 10,000 | put | clustered | 9.155× | 0.109× |
| 10,000 | put | random | 5.481× | 0.182× |

## Fixture build context

| Adapter | Records | Median build ms | Median records/s | Median bytes |
|---|---:|---:|---:|---:|
| sqlite-sync | 10,000 | 5.592 | 1788335.5 | 483328 |
| turso-async | 10,000 | 9.812 | 1019117.0 | 515008 |

## Largest observed differences

- Largest latency-ratio departure from parity: 10,000 records, put/clustered at 9.155× Turso/SQLite.
- Largest throughput-ratio departure from parity: 10,000 records, put/clustered at 0.109× Turso/SQLite.
- These are observed ratios on this run; no statistical-significance claim is made.

## Observed scaling

The tables report each requested size independently. Compare rows within the same API and pattern; changes scale at 1% until the 10K cap, so operation counts are not proportional above 1M records.

## Method and limitations

- This compares preferred end-to-end prolly paths, not raw SQL engines: synchronous `Prolly<SqliteStore>` versus Tokio-driven asynchronous `AsyncProlly<TursoStore>`.
- All databases are local files. Turso Cloud sync, credentials, `push()`, and `pull()` are not used.
- Adapter durability defaults are recorded but are not asserted to provide identical fsync or journaling semantics.
- Each measured cell starts with a cold prolly manager; the operating-system filesystem cache is uncontrolled.
- Results describe the recorded machine, filesystem, code revision, and Turso beta version and do not predict Turso Cloud performance.
- Individual-put percentiles are available in `summary.csv`; batch, diff, and merge latency ranges are across independent repetitions.
