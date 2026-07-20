# SQLite prolly scale baseline

All values below are medians of independent repetitions.

## Workload contract

- Sizes: 1000000 records; repetitions: 3.
- Mutations: 30% of each base; read/scan samples: 10000.
- Merge changes are the total across two equal, disjoint branches.

## Workloads

| Records | Operation | Pattern | Cache | Median ns/op | Median ops/s |
|---:|---|---|---|---:|---:|
| 1000000 | put | append | n/a | 19920042.0 | 50.2 |
| 1000000 | put | random | n/a | 27686500.0 | 36.1 |
| 1000000 | put | clustered | n/a | 25918125.0 | 38.6 |
| 1000000 | batch | append | n/a | 3918.7 | 255186.3 |
| 1000000 | batch | random | n/a | 8060.0 | 124069.4 |
| 1000000 | batch | clustered | n/a | 4754.6 | 210320.8 |
| 1000000 | get_cold | append | cold-manager | 173244.3 | 5772.2 |
| 1000000 | get_cold | random | cold-manager | 248650.1 | 4021.7 |
| 1000000 | get_cold | clustered | cold-manager | 179463.0 | 5572.2 |
| 1000000 | get_warm | append | warm-manager | 248.5 | 4023403.3 |
| 1000000 | get_warm | random | warm-manager | 726.8 | 1375941.7 |
| 1000000 | get_warm | clustered | warm-manager | 260.8 | 3833866.3 |
| 1000000 | query | append | n/a | 12392.0 | 80697.4 |
| 1000000 | query | random | n/a | 126205.5 | 7923.6 |
| 1000000 | query | clustered | n/a | 12001.6 | 83322.1 |
| 1000000 | scan | append | n/a | 16140.9 | 61954.2 |
| 1000000 | scan | random | n/a | 13113.9 | 76254.8 |
| 1000000 | scan | clustered | n/a | 11352.2 | 88088.5 |
| 1000000 | full_scan | append | n/a | 1659.3 | 602655.6 |
| 1000000 | diff | append | n/a | 562.1 | 1778995.4 |
| 1000000 | diff | random | n/a | 3070.4 | 325692.0 |
| 1000000 | diff | clustered | n/a | 1004.0 | 996026.4 |
| 1000000 | merge | append | n/a | 858.0 | 1165521.9 |
| 1000000 | merge | random | n/a | 119286.3 | 8383.2 |
| 1000000 | merge | clustered | n/a | 8.0 | 124337282.3 |

## Fixture context

| Records | Repetitions | Median build ms | Median records/s | Median database MiB |
|---:|---:|---:|---:|---:|
| 1000000 | 3 | 1236.357 | 808827.8 | 113.11 |

## Measurement boundaries

- Fixture cloning, diff/merge branch setup, validation, stats, publication, and reopen checks are outside timed intervals.
- Scans include full iterator consumption; cold point gets clear the manager cache before every lookup.
- Each workload cell uses an isolated clone of a closed SQLite fixture.

## Interpretation limits

- End-to-end synchronous `Prolly<SqliteStore>` on one local connection.
- SQLite uses WAL and `synchronous=NORMAL`; this is not `FULL` durability.
- Manager cache state is controlled, but the operating-system filesystem cache is not.
- Keys are 24 bytes and values are 100 bytes. Results do not predict concurrent writers, remote filesystems, or raw SQLite.
