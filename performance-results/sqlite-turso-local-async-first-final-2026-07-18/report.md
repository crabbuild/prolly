# SQLite Sync vs Turso Async Local Prolly Comparison

Schema: `sqlite-turso-local-v1`. Revision: `7d26dde15b2492a07b58fe908aeecf73978d3abd` (dirty: `true`). Planned repetitions: 3.

Lower Turso/SQLite latency ratios favor Turso; higher throughput ratios favor Turso.
Validation: 432 measured cells and 36 base fixtures passed the frozen row contract.

| Records | API | Pattern | Turso/SQLite latency | Turso/SQLite throughput |
|---:|---|---|---:|---:|
| 10,000 | batch | append | 2.069× | 0.483× |
| 10,000 | batch | clustered | 2.911× | 0.343× |
| 10,000 | batch | random | 26.126× | 0.038× |
| 10,000 | diff | append | 8.268× | 0.121× |
| 10,000 | diff | clustered | 3.495× | 0.286× |
| 10,000 | diff | random | 2.829× | 0.353× |
| 10,000 | merge | append | 13.154× | 0.076× |
| 10,000 | merge | clustered | 14.385× | 0.070× |
| 10,000 | merge | random | 2.012× | 0.497× |
| 10,000 | put | append | 8.178× | 0.122× |
| 10,000 | put | clustered | 9.090× | 0.110× |
| 10,000 | put | random | 5.636× | 0.177× |
| 50,000 | batch | append | 0.683× | 1.465× |
| 50,000 | batch | clustered | 3.194× | 0.313× |
| 50,000 | batch | random | 1.783× | 0.561× |
| 50,000 | diff | append | 4.227× | 0.237× |
| 50,000 | diff | clustered | 2.207× | 0.453× |
| 50,000 | diff | random | 2.005× | 0.499× |
| 50,000 | merge | append | 2.163× | 0.462× |
| 50,000 | merge | clustered | 1.252× | 0.799× |
| 50,000 | merge | random | 1.160× | 0.862× |
| 50,000 | put | append | 4.782× | 0.209× |
| 50,000 | put | clustered | 6.444× | 0.155× |
| 50,000 | put | random | 4.896× | 0.204× |
| 100,000 | batch | append | 0.572× | 1.749× |
| 100,000 | batch | clustered | 1.409× | 0.710× |
| 100,000 | batch | random | 2.036× | 0.491× |
| 100,000 | diff | append | 1.280× | 0.781× |
| 100,000 | diff | clustered | 1.756× | 0.569× |
| 100,000 | diff | random | 1.131× | 0.884× |
| 100,000 | merge | append | 2.077× | 0.481× |
| 100,000 | merge | clustered | 1.794× | 0.557× |
| 100,000 | merge | random | 1.360× | 0.735× |
| 100,000 | put | append | 3.372× | 0.297× |
| 100,000 | put | clustered | 4.081× | 0.245× |
| 100,000 | put | random | 4.122× | 0.243× |
| 500,000 | batch | append | 0.223× | 4.477× |
| 500,000 | batch | clustered | 0.191× | 5.223× |
| 500,000 | batch | random | 1.042× | 0.959× |
| 500,000 | diff | append | 0.560× | 1.784× |
| 500,000 | diff | clustered | 1.360× | 0.735× |
| 500,000 | diff | random | 0.618× | 1.618× |
| 500,000 | merge | append | 0.466× | 2.148× |
| 500,000 | merge | clustered | 1.449× | 0.690× |
| 500,000 | merge | random | 0.624× | 1.603× |
| 500,000 | put | append | 2.858× | 0.350× |
| 500,000 | put | clustered | 4.095× | 0.244× |
| 500,000 | put | random | 3.167× | 0.316× |
| 1,000,000 | batch | append | 0.296× | 3.376× |
| 1,000,000 | batch | clustered | 0.367× | 2.727× |
| 1,000,000 | batch | random | 1.245× | 0.803× |
| 1,000,000 | diff | append | 0.711× | 1.407× |
| 1,000,000 | diff | clustered | 1.061× | 0.943× |
| 1,000,000 | diff | random | 1.417× | 0.706× |
| 1,000,000 | merge | append | 1.151× | 0.869× |
| 1,000,000 | merge | clustered | 0.944× | 1.060× |
| 1,000,000 | merge | random | 1.052× | 0.951× |
| 1,000,000 | put | append | 3.325× | 0.301× |
| 1,000,000 | put | clustered | 3.293× | 0.304× |
| 1,000,000 | put | random | 4.372× | 0.229× |
| 2,000,000 | batch | append | 0.155× | 6.448× |
| 2,000,000 | batch | clustered | 0.363× | 2.755× |
| 2,000,000 | batch | random | 0.964× | 1.037× |
| 2,000,000 | diff | append | 0.482× | 2.073× |
| 2,000,000 | diff | clustered | 1.148× | 0.871× |
| 2,000,000 | diff | random | 2.528× | 0.396× |
| 2,000,000 | merge | append | 0.483× | 2.072× |
| 2,000,000 | merge | clustered | 0.927× | 1.079× |
| 2,000,000 | merge | random | 1.928× | 0.519× |
| 2,000,000 | put | append | 2.426× | 0.412× |
| 2,000,000 | put | clustered | 3.180× | 0.314× |
| 2,000,000 | put | random | 3.098× | 0.323× |

## Fixture build context

| Adapter | Records | Median build ms | Median records/s | Median bytes |
|---|---:|---:|---:|---:|
| sqlite-sync | 10,000 | 13.297 | 752044.6 | 483328 |
| sqlite-sync | 50,000 | 74.132 | 674476.4 | 2375680 |
| sqlite-sync | 100,000 | 101.826 | 982067.1 | 4747264 |
| sqlite-sync | 500,000 | 708.202 | 706012.8 | 23547904 |
| sqlite-sync | 1,000,000 | 826.390 | 1210082.5 | 47177728 |
| sqlite-sync | 2,000,000 | 1673.278 | 1195258.9 | 94461952 |
| turso-async | 10,000 | 13.641 | 733099.8 | 515008 |
| turso-async | 50,000 | 79.681 | 627502.2 | 2290728 |
| turso-async | 100,000 | 177.932 | 562011.3 | 4489296 |
| turso-async | 500,000 | 886.050 | 564302.3 | 22278224 |
| turso-async | 1,000,000 | 1259.116 | 794207.8 | 44539984 |
| turso-async | 2,000,000 | 2501.700 | 799456.3 | 89223248 |

## Largest observed differences

- Largest latency-ratio departure from parity: 10,000 records, batch/random at 26.126× Turso/SQLite.
- Largest throughput-ratio departure from parity: 10,000 records, batch/random at 0.038× Turso/SQLite.
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
