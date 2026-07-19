# SQLite Sync vs Turso Async Local Prolly Comparison

Schema: `sqlite-turso-local-v1`. Revision: `async-first` (dirty: `true`). Planned repetitions: 1.

Lower Turso/SQLite latency ratios favor Turso; higher throughput ratios favor Turso.
Validation: 24 measured cells and 2 base fixtures passed the frozen row contract.

| Records | API | Pattern | Turso/SQLite latency | Turso/SQLite throughput |
|---:|---|---|---:|---:|
| 100 | batch | append | 3.102× | 0.322× |
| 100 | batch | clustered | 4.296× | 0.233× |
| 100 | batch | random | 6.781× | 0.147× |
| 100 | diff | append | 1.516× | 0.660× |
| 100 | diff | clustered | 2.125× | 0.471× |
| 100 | diff | random | 2.100× | 0.476× |
| 100 | merge | append | 5.659× | 0.177× |
| 100 | merge | clustered | 6.805× | 0.147× |
| 100 | merge | random | 6.856× | 0.146× |
| 100 | put | append | 8.652× | 0.116× |
| 100 | put | clustered | 10.619× | 0.094× |
| 100 | put | random | 12.436× | 0.080× |

## Fixture build context

| Adapter | Records | Median build ms | Median records/s | Median bytes |
|---|---:|---:|---:|---:|
| sqlite-sync | 100 | 0.530 | 188753.3 | 20480 |
| turso-async | 100 | 1.388 | 72039.6 | 74168 |

## Largest observed differences

- Largest latency-ratio departure from parity: 100 records, put/random at 12.436× Turso/SQLite.
- Largest throughput-ratio departure from parity: 100 records, put/random at 0.080× Turso/SQLite.
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
