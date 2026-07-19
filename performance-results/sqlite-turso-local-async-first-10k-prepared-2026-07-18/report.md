# SQLite Sync vs Turso Async Local Prolly Comparison

Schema: `sqlite-turso-local-v1`. Revision: `async-first-prepared` (dirty: `true`). Planned repetitions: 1.

Lower Turso/SQLite latency ratios favor Turso; higher throughput ratios favor Turso.
Validation: 24 measured cells and 2 base fixtures passed the frozen row contract.

| Records | API | Pattern | Turso/SQLite latency | Turso/SQLite throughput |
|---:|---|---|---:|---:|
| 10,000 | batch | append | 1.262× | 0.792× |
| 10,000 | batch | clustered | 1.250× | 0.800× |
| 10,000 | batch | random | 1.342× | 0.745× |
| 10,000 | diff | append | 1.379× | 0.725× |
| 10,000 | diff | clustered | 1.472× | 0.679× |
| 10,000 | diff | random | 1.479× | 0.676× |
| 10,000 | merge | append | 2.551× | 0.392× |
| 10,000 | merge | clustered | 6.153× | 0.163× |
| 10,000 | merge | random | 1.522× | 0.657× |
| 10,000 | put | append | 5.205× | 0.192× |
| 10,000 | put | clustered | 7.163× | 0.140× |
| 10,000 | put | random | 3.339× | 0.300× |

## Fixture build context

| Adapter | Records | Median build ms | Median records/s | Median bytes |
|---|---:|---:|---:|---:|
| sqlite-sync | 10,000 | 7.348 | 1360960.8 | 483328 |
| turso-async | 10,000 | 11.287 | 885978.2 | 515008 |

## Largest observed differences

- Largest latency-ratio departure from parity: 10,000 records, put/clustered at 7.163× Turso/SQLite.
- Largest throughput-ratio departure from parity: 10,000 records, put/clustered at 0.140× Turso/SQLite.
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
