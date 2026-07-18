# SQLite-backed prolly key-pattern benchmark

All values below are medians of independent repetitions.

## Workloads

| Records | Operation | Pattern | Cache | Median ns/op | Median ops/s |
|---:|---|---|---|---:|---:|
| 100 | put | append | n/a | 150395.8 | 6649.1 |
| 100 | put | random | n/a | 106433.4 | 9395.5 |
| 100 | put | clustered | n/a | 964787.5 | 1036.5 |
| 100 | batch | append | n/a | 36770.8 | 27195.5 |
| 100 | batch | random | n/a | 46720.9 | 21403.7 |
| 100 | batch | clustered | n/a | 49200.0 | 20325.2 |
| 100 | point-read | append | cold-manager | 15508.3 | 64481.6 |
| 100 | point-read | append | warm-manager | 583.3 | 1714383.7 |
| 100 | point-read | random | cold-manager | 15537.5 | 64360.4 |
| 100 | point-read | random | warm-manager | 554.1 | 1804728.4 |
| 100 | point-read | clustered | cold-manager | 14812.5 | 67510.5 |
| 100 | point-read | clustered | warm-manager | 575.0 | 1739130.4 |
| 100 | range-scan | append | n/a | 17579.1 | 56885.7 |
| 100 | range-scan | random | n/a | 16279.2 | 61428.1 |
| 100 | range-scan | clustered | n/a | 16391.7 | 61006.5 |

## Fixture context

Validated fixture rows: 1.

## Interpretation limits

- End-to-end synchronous `Prolly<SqliteStore>` on one local connection.
- SQLite uses WAL and `synchronous=NORMAL`; this is not `FULL` durability.
- Manager cache state is controlled, but the operating-system filesystem cache is not.
- Keys are 24 bytes and values are 100 bytes. Results do not predict concurrent writers, remote filesystems, or raw SQLite.
