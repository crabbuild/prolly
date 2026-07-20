# SQLite Sync vs Turso Async Local Prolly Comparison

Schema: `sqlite-turso-local-v1`. Revision: `57e8ecfa4ba6348d9ecbdff055d42a84b6ebabf5` (dirty: `true`). Planned repetitions: 3.

Lower Turso/SQLite latency ratios favor Turso; higher throughput ratios favor Turso.
Validation: 432 measured cells and 36 base fixtures passed the frozen row contract.

| Records | API | Pattern | Turso/SQLite latency | Turso/SQLite throughput |
|---:|---|---|---:|---:|
| 10,000 | batch | append | 1.414× | 0.707× |
| 10,000 | batch | clustered | 2.300× | 0.435× |
| 10,000 | batch | random | 12.876× | 0.078× |
| 10,000 | diff | append | 5.851× | 0.171× |
| 10,000 | diff | clustered | 3.519× | 0.284× |
| 10,000 | diff | random | 2.771× | 0.361× |
| 10,000 | merge | append | 5.155× | 0.194× |
| 10,000 | merge | clustered | 8.824× | 0.113× |
| 10,000 | merge | random | 1.616× | 0.619× |
| 10,000 | put | append | 7.547× | 0.133× |
| 10,000 | put | clustered | 9.563× | 0.105× |
| 10,000 | put | random | 5.504× | 0.182× |
| 50,000 | batch | append | 0.795× | 1.257× |
| 50,000 | batch | clustered | 1.698× | 0.589× |
| 50,000 | batch | random | 1.358× | 0.736× |
| 50,000 | diff | append | 1.740× | 0.575× |
| 50,000 | diff | clustered | 2.187× | 0.457× |
| 50,000 | diff | random | 1.753× | 0.571× |
| 50,000 | merge | append | 2.514× | 0.398× |
| 50,000 | merge | clustered | 2.555× | 0.391× |
| 50,000 | merge | random | 1.240× | 0.807× |
| 50,000 | put | append | 6.390× | 0.156× |
| 50,000 | put | clustered | 4.997× | 0.200× |
| 50,000 | put | random | 5.600× | 0.179× |
| 100,000 | batch | append | 0.549× | 1.821× |
| 100,000 | batch | clustered | 1.669× | 0.599× |
| 100,000 | batch | random | 1.280× | 0.782× |
| 100,000 | diff | append | 1.279× | 0.782× |
| 100,000 | diff | clustered | 1.257× | 0.796× |
| 100,000 | diff | random | 1.465× | 0.682× |
| 100,000 | merge | append | 2.347× | 0.426× |
| 100,000 | merge | clustered | 1.370× | 0.730× |
| 100,000 | merge | random | 1.075× | 0.930× |
| 100,000 | put | append | 4.136× | 0.242× |
| 100,000 | put | clustered | 4.247× | 0.235× |
| 100,000 | put | random | 4.140× | 0.242× |
| 500,000 | batch | append | 0.272× | 3.679× |
| 500,000 | batch | clustered | 0.443× | 2.259× |
| 500,000 | batch | random | 1.194× | 0.838× |
| 500,000 | diff | append | 0.478× | 2.094× |
| 500,000 | diff | clustered | 1.885× | 0.530× |
| 500,000 | diff | random | 1.514× | 0.660× |
| 500,000 | merge | append | 0.587× | 1.703× |
| 500,000 | merge | clustered | 1.424× | 0.702× |
| 500,000 | merge | random | 1.075× | 0.930× |
| 500,000 | put | append | 3.233× | 0.309× |
| 500,000 | put | clustered | 4.322× | 0.231× |
| 500,000 | put | random | 4.194× | 0.238× |
| 1,000,000 | batch | append | 0.290× | 3.447× |
| 1,000,000 | batch | clustered | 0.419× | 2.388× |
| 1,000,000 | batch | random | 1.470× | 0.680× |
| 1,000,000 | diff | append | 0.770× | 1.299× |
| 1,000,000 | diff | clustered | 1.089× | 0.918× |
| 1,000,000 | diff | random | 1.451× | 0.689× |
| 1,000,000 | merge | append | 1.191× | 0.839× |
| 1,000,000 | merge | clustered | 1.366× | 0.732× |
| 1,000,000 | merge | random | 1.189× | 0.841× |
| 1,000,000 | put | append | 2.691× | 0.372× |
| 1,000,000 | put | clustered | 3.786× | 0.264× |
| 1,000,000 | put | random | 3.957× | 0.253× |
| 2,000,000 | batch | append | 0.240× | 4.173× |
| 2,000,000 | batch | clustered | 0.323× | 3.098× |
| 2,000,000 | batch | random | 1.201× | 0.832× |
| 2,000,000 | diff | append | 0.508× | 1.970× |
| 2,000,000 | diff | clustered | 1.084× | 0.922× |
| 2,000,000 | diff | random | 1.309× | 0.764× |
| 2,000,000 | merge | append | 0.360× | 2.775× |
| 2,000,000 | merge | clustered | 0.976× | 1.025× |
| 2,000,000 | merge | random | 1.025× | 0.975× |
| 2,000,000 | put | append | 2.447× | 0.409× |
| 2,000,000 | put | clustered | 3.033× | 0.330× |
| 2,000,000 | put | random | 3.213× | 0.311× |

## Fixture build context

| Adapter | Records | Median build ms | Median records/s | Median bytes |
|---|---:|---:|---:|---:|
| sqlite-sync | 10,000 | 8.198 | 1219815.8 | 483328 |
| sqlite-sync | 50,000 | 38.970 | 1283052.0 | 2375680 |
| sqlite-sync | 100,000 | 78.251 | 1277938.9 | 4747264 |
| sqlite-sync | 500,000 | 398.743 | 1253939.3 | 23547904 |
| sqlite-sync | 1,000,000 | 948.594 | 1054191.3 | 47177728 |
| sqlite-sync | 2,000,000 | 1703.865 | 1173801.9 | 94461952 |
| turso-async | 10,000 | 13.083 | 764336.1 | 515008 |
| turso-async | 50,000 | 60.154 | 831194.7 | 2290728 |
| turso-async | 100,000 | 125.320 | 797959.9 | 4489296 |
| turso-async | 500,000 | 621.507 | 804496.4 | 22278224 |
| turso-async | 1,000,000 | 1252.739 | 798250.8 | 44539984 |
| turso-async | 2,000,000 | 2552.997 | 783393.0 | 89223248 |

## Largest observed differences

- Largest latency-ratio departure from parity: 10,000 records, batch/random at 12.876× Turso/SQLite.
- Largest throughput-ratio departure from parity: 10,000 records, batch/random at 0.078× Turso/SQLite.
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
