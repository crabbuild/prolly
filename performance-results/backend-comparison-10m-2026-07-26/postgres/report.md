# PostgreSQL-backed Prolly performance

Revision `fb5fe0c14ad5574f429075bf5cf2c0bf252697c7` (dirty=true); 16 validated raw rows.

This is an end-to-end single-client measurement of the public async Prolly API over SQLx and PostgreSQL 16 in Docker Desktop. Latency is wall-clock time; PostgreSQL execution time is separately observed by `pg_stat_statements`.

## Workload cardinality

Batch and diff mutate 10,000 keys. Point get, multi-get, and bounded scan sample 10,000 keys or entries.
Merge treats 10,000 as the total change count: 5,000 changes per branch across two disjoint branches.
Random merge keys are interleaved across both branches so each branch spans the full base keyspace.

## 10,000,000 records

| Operation | Pattern | Cache | n | Median ms | Min–max ms | ns/op | ops/s | Nodes R/W | MiB R/W | PG calls / ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| batch | random | cold-manager | n=3 | 2484.215 | 2425.600–2584.167 | 248421 | 4025.4 | 9413/9413 | 65.79/65.79 | 165/705.854 |
| build | base | cold-manager | n=1 | 8724.541 | 8724.541–8724.541 | 872 | 1146192.1 | 0/76974 | 0.00/301.95 | 76/2837.645 |
| concurrent_query | random | cold-manager | n=3 | 2030.185 | 2012.252–2042.857 | 203018 | 4925.7 | 9479/0 | 66.99/0.00 | 9479/353.979 |
| diff | random | cold-manager | n=3 | 1800.253 | 1797.459–1848.361 | 180025 | 5554.8 | 18848/0 | 132.48/0.00 | 4699/434.768 |
| merge | random | cold-manager | n=3 | 4398.450 | 4375.254–4462.778 | 439845 | 2273.5 | 16896/5503 | 122.68/39.94 | 3163/689.202 |
| query | random | cold-manager | n=3 | 621.446 | 600.637–668.383 | 62145 | 16091.5 | 9397/0 | 66.56/0.00 | 12/148.324 |

## Interpretation limits

- Results describe the recorded machine, Docker Desktop allocation, code revision, PostgreSQL defaults, and fixed 24-byte keys/27-byte values.
- `cold-manager` clears or recreates the decoded Prolly node cache; PostgreSQL and host OS caches are not forcibly dropped.
- The workload is serial and single-client. It does not measure connection-pool or concurrent transaction scaling.
- `query` means the public Prolly `get_many` API. Random-key range scans are intentionally not defined.
- Build and full scan have n=1 per size; other full-profile cells normally have n=3.
- Database-side statement time excludes client/runtime/tree processing and must not be compared as if it were end-to-end latency.
