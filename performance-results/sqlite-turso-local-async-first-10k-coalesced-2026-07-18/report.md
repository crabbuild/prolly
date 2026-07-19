# SQLite Sync vs Turso Async Local Prolly Comparison

Schema: `sqlite-turso-local-v1`. Revision: `async-first-coalesced` (dirty: `true`). Planned repetitions: 1.

Lower Turso/SQLite latency ratios favor Turso; higher throughput ratios favor Turso.
Validation: 24 measured cells and 2 base fixtures passed the frozen row contract.

| Records | API | Pattern | Turso/SQLite latency | Turso/SQLite throughput |
|---:|---|---|---:|---:|
| 10,000 | batch | append | 1.960× | 0.510× |
| 10,000 | batch | clustered | 1.787× | 0.559× |
| 10,000 | batch | random | 1.361× | 0.735× |
| 10,000 | diff | append | 5.275× | 0.190× |
| 10,000 | diff | clustered | 3.167× | 0.316× |
| 10,000 | diff | random | 1.605× | 0.623× |
| 10,000 | merge | append | 4.803× | 0.208× |
| 10,000 | merge | clustered | 14.678× | 0.068× |
| 10,000 | merge | random | 2.127× | 0.470× |
| 10,000 | put | append | 4.029× | 0.248× |
| 10,000 | put | clustered | 7.693× | 0.130× |
| 10,000 | put | random | 3.477× | 0.288× |

## Fixture build context

| Adapter | Records | Median build ms | Median records/s | Median bytes |
|---|---:|---:|---:|---:|
| sqlite-sync | 10,000 | 9.058 | 1104052.3 | 483328 |
| turso-async | 10,000 | 11.516 | 868322.5 | 515008 |

## Largest observed differences

- Largest latency-ratio departure from parity: 10,000 records, merge/clustered at 14.678× Turso/SQLite.
- Largest throughput-ratio departure from parity: 10,000 records, merge/clustered at 0.068× Turso/SQLite.
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
