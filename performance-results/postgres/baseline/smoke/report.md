# PostgreSQL-backed Prolly performance

Revision `d6f33dc7351ba93f6117913a11cfb85e7d751696` (dirty=true); 25 validated raw rows.

This is an end-to-end single-client measurement of the public async Prolly API over SQLx and PostgreSQL 16 in Docker Desktop. Latency is wall-clock time; PostgreSQL execution time is separately observed by `pg_stat_statements`.

## Workload cardinality

Batch and diff mutate 300 keys. Point get, multi-get, and bounded scan sample 100 keys or entries.
Merge treats 300 as the total change count: 150 changes per branch across two disjoint branches.

## 1,000 records

| Operation | Pattern | Cache | n | Median ms | Min–max ms | ns/op | ops/s | Nodes R/W | MiB R/W | PG calls / ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| batch | append | cold-manager | n=1 | 5.626 | 5.626–5.626 | 18753 | 53323.9 | 2/6 | 0.00/0.01 | 8/0.146 |
| batch | clustered | cold-manager | n=1 | 7.690 | 7.690–7.690 | 25632 | 39013.8 | 6/5 | 0.03/0.02 | 11/0.295 |
| batch | random | cold-manager | n=1 | 11.317 | 11.317–11.317 | 37725 | 26507.8 | 9/9 | 0.03/0.03 | 18/0.462 |
| build | base | cold-manager | n=1 | 5.418 | 5.418–5.418 | 5418 | 184571.3 | 0/9 | 0.00/0.03 | 9/0.579 |
| diff | append | cold-manager | n=1 | 4.366 | 4.366–4.366 | 14554 | 68709.5 | 8/0 | 0.01/0.00 | 8/0.042 |
| diff | clustered | cold-manager | n=1 | 7.662 | 7.662–7.662 | 25541 | 39152.8 | 10/0 | 0.05/0.00 | 10/0.138 |
| diff | random | cold-manager | n=1 | 22.714 | 22.714–22.714 | 75713 | 13207.8 | 18/0 | 0.06/0.00 | 18/0.237 |
| full_scan | append | cold-manager | n=1 | 5.119 | 5.119–5.119 | 5119 | 195349.1 | 9/0 | 0.03/0.00 | 9/0.073 |
| get_cold | append | cold-manager | n=1 | 135.990 | 135.990–135.990 | 1359900 | 735.3 | 200/0 | 0.92/0.00 | 200/2.853 |
| get_cold | clustered | cold-manager | n=1 | 122.218 | 122.218–122.218 | 1222180 | 818.2 | 200/0 | 0.48/0.00 | 200/0.813 |
| get_cold | random | cold-manager | n=1 | 123.392 | 123.392–123.392 | 1233925 | 810.4 | 200/0 | 0.81/0.00 | 200/1.985 |
| get_warm | append | warm-manager | n=1 | 0.047 | 0.047–0.047 | 475 | 2107126.3 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | clustered | warm-manager | n=1 | 0.051 | 0.051–0.051 | 512 | 1952819.9 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | random | warm-manager | n=1 | 0.063 | 0.063–0.063 | 630 | 1588360.5 | 0/0 | 0.00/0.00 | 0/0.000 |
| merge | append | cold-manager | n=1 | 6.643 | 6.643–6.643 | 22145 | 45157.8 | 8/3 | 0.01/0.01 | 11/0.148 |
| merge | clustered | cold-manager | n=1 | 4.757 | 4.757–4.757 | 15856 | 63067.7 | 6/2 | 0.01/0.00 | 8/0.113 |
| merge | random | cold-manager | n=1 | 5.700 | 5.700–5.700 | 19000 | 52631.2 | 6/2 | 0.01/0.00 | 8/0.126 |
| put | append | cold-manager | n=1 | 3.246 | 3.246–3.246 | 3246166 | 308.1 | 2/2 | 0.00/0.00 | 4/0.049 |
| put | clustered | cold-manager | n=1 | 3.519 | 3.519–3.519 | 3519125 | 284.2 | 3/2 | 0.01/0.00 | 5/0.081 |
| put | random | cold-manager | n=1 | 3.447 | 3.447–3.447 | 3446584 | 290.1 | 3/2 | 0.01/0.00 | 5/0.073 |
| query | append | cold-manager | n=1 | 1.826 | 1.826–1.826 | 18260 | 54763.3 | 3/0 | 0.01/0.00 | 3/0.038 |
| query | clustered | cold-manager | n=1 | 1.313 | 1.313–1.313 | 13131 | 76156.6 | 2/0 | 0.00/0.00 | 2/0.008 |
| query | random | cold-manager | n=1 | 4.572 | 4.572–4.572 | 45719 | 21872.9 | 8/0 | 0.03/0.00 | 8/0.064 |
| scan | append | cold-manager | n=1 | 1.743 | 1.743–1.743 | 17430 | 57372.3 | 3/0 | 0.01/0.00 | 3/0.033 |
| scan | clustered | cold-manager | n=1 | 1.168 | 1.168–1.168 | 11676 | 85643.9 | 2/0 | 0.00/0.00 | 2/0.010 |

## Interpretation limits

- Results describe the recorded machine, Docker Desktop allocation, code revision, PostgreSQL defaults, and fixed 24-byte keys/27-byte values.
- `cold-manager` clears or recreates the decoded Prolly node cache; PostgreSQL and host OS caches are not forcibly dropped.
- The workload is serial and single-client. It does not measure connection-pool or concurrent transaction scaling.
- `query` means the public Prolly `get_many` API. Random-key range scans are intentionally not defined.
- Build and full scan have n=1 per size; other full-profile cells normally have n=3.
- Database-side statement time excludes client/runtime/tree processing and must not be compared as if it were end-to-end latency.
