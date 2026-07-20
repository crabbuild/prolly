# SQLite Sync vs Turso Async Local Prolly Comparison

Schema: `sqlite-turso-local-v1`. Revision: `async-first` (dirty: `true`). Planned repetitions: 1.

Lower Turso/SQLite latency ratios favor Turso; higher throughput ratios favor Turso.
Validation: 24 measured cells and 2 base fixtures passed the frozen row contract.

| Records | API | Pattern | Turso/SQLite latency | Turso/SQLite throughput |
|---:|---|---|---:|---:|
| 10,000 | batch | append | 0.752× | 1.329× |
| 10,000 | batch | clustered | 1.250× | 0.800× |
| 10,000 | batch | random | 1.513× | 0.661× |
| 10,000 | diff | append | 1.581× | 0.632× |
| 10,000 | diff | clustered | 1.857× | 0.538× |
| 10,000 | diff | random | 1.564× | 0.639× |
| 10,000 | merge | append | 2.480× | 0.403× |
| 10,000 | merge | clustered | 7.146× | 0.140× |
| 10,000 | merge | random | 1.509× | 0.663× |
| 10,000 | put | append | 6.665× | 0.150× |
| 10,000 | put | clustered | 7.588× | 0.132× |
| 10,000 | put | random | 4.088× | 0.245× |

## Fixture build context

| Adapter | Records | Median build ms | Median records/s | Median bytes |
|---|---:|---:|---:|---:|
| sqlite-sync | 10,000 | 6.390 | 1564924.7 | 483328 |
| turso-async | 10,000 | 11.462 | 872476.6 | 515008 |

## Largest observed differences

- Largest latency-ratio departure from parity: 10,000 records, put/clustered at 7.588× Turso/SQLite.
- Largest throughput-ratio departure from parity: 10,000 records, put/clustered at 0.132× Turso/SQLite.
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
