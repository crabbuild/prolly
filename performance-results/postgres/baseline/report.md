# PostgreSQL-backed Prolly performance

Revision `d6f33dc7351ba93f6117913a11cfb85e7d751696` (dirty=true); 71 validated raw rows.

This is an end-to-end single-client measurement of the public async Prolly API over SQLx and PostgreSQL 16 in Docker Desktop. Latency is wall-clock time; PostgreSQL execution time is separately observed by `pg_stat_statements`.

## Workload cardinality

Batch and diff mutate 300,000 keys. Point get, multi-get, and bounded scan sample 10,000 keys or entries.
Merge treats 300,000 as the total change count: 150,000 changes per branch across two disjoint branches.
Random merge keys are interleaved across both branches so each branch spans the full base keyspace.

## 1,000,000 records

| Operation | Pattern | Cache | n | Median ms | Min–max ms | ns/op | ops/s | Nodes R/W | MiB R/W | PG calls / ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| batch | append | cold-manager | n=3 | 1122.344 | 1053.904–1523.130 | 3741 | 267297.7 | 4/2295 | 0.01/9.07 | 2299/98.495 |
| batch | clustered | cold-manager | n=3 | 2838.448 | 2695.482–2853.372 | 9461 | 105691.6 | 2213/2211 | 9.07/9.06 | 4424/137.743 |
| batch | random | cold-manager | n=3 | 9085.377 | 8721.700–10090.863 | 30285 | 33020.1 | 7659/7659 | 30.18/30.18 | 15318/432.427 |
| build | base | cold-manager | n=1 | 3425.351 | 3425.351–3425.351 | 3425 | 291940.9 | 0/7719 | 0.00/30.20 | 7719/309.215 |
| diff | append | cold-manager | n=3 | 1616.231 | 1515.709–1841.486 | 5387 | 185617.0 | 2299/0 | 9.08/0.00 | 2299/41.474 |
| diff | clustered | cold-manager | n=3 | 955.106 | 914.460–1165.408 | 3184 | 314101.3 | 4422/0 | 18.13/0.00 | 4422/108.780 |
| diff | random | cold-manager | n=3 | 3717.060 | 3379.561–5014.762 | 12390 | 80709.0 | 15324/0 | 60.37/0.00 | 15324/435.063 |
| full_scan | append | cold-manager | n=1 | 5167.524 | 5167.524–5167.524 | 5168 | 193516.3 | 7719/0 | 30.20/0.00 | 7719/131.198 |
| get_cold | append | cold-manager | n=3 | 29234.914 | 28100.572–55574.429 | 2923491 | 342.1 | 40000/0 | 189.84/0.00 | 40000/1044.001 |
| get_cold | clustered | cold-manager | n=3 | 30063.623 | 27320.146–31086.097 | 3006362 | 332.6 | 40000/0 | 221.93/0.00 | 40000/1177.566 |
| get_cold | random | cold-manager | n=3 | 28361.608 | 26465.241–50086.439 | 2836161 | 352.6 | 40000/0 | 164.77/0.00 | 40000/974.077 |
| get_warm | append | warm-manager | n=3 | 4.369 | 4.348–8.350 | 437 | 2288853.3 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | clustered | warm-manager | n=3 | 4.354 | 4.280–4.389 | 435 | 2296497.1 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | random | warm-manager | n=3 | 12.722 | 12.536–13.092 | 1272 | 786024.5 | 0/0 | 0.00/0.00 | 0/0.000 |
| merge | append | cold-manager | n=3 | 1462.564 | 1264.980–1561.640 | 4875 | 205119.2 | 1139/1130 | 4.57/4.54 | 2269/71.874 |
| merge | clustered | cold-manager | n=3 | 13.816 | 12.788–13.998 | 46 | 21713497.5 | 12/4 | 0.09/0.03 | 16/1.106 |
| merge | random | cold-manager | n=3 | 48250.270 | 47747.594–65837.300 | 160834 | 6217.6 | 22584/7528 | 90.45/30.15 | 30112/1061.690 |
| put | append | cold-manager | n=3 | 6.810 | 6.338–18.754 | 6809875 | 146.8 | 4/4 | 0.01/0.01 | 8/0.411 |
| put | clustered | cold-manager | n=3 | 9.632 | 7.975–42.861 | 9632209 | 103.8 | 6/4 | 0.04/0.03 | 10/0.722 |
| put | random | cold-manager | n=3 | 9.126 | 7.989–10.091 | 9125958 | 109.6 | 6/4 | 0.03/0.02 | 10/0.759 |
| query | append | cold-manager | n=3 | 62.799 | 61.969–67.508 | 6280 | 159237.4 | 86/0 | 0.31/0.00 | 86/1.765 |
| query | clustered | cold-manager | n=3 | 55.538 | 55.101–58.403 | 5554 | 180058.1 | 74/0 | 0.34/0.00 | 74/1.670 |
| query | random | cold-manager | n=3 | 3505.896 | 3092.879–3579.634 | 350590 | 2852.3 | 4498/0 | 24.52/0.00 | 4498/101.989 |
| scan | append | cold-manager | n=3 | 58.714 | 57.552–71.810 | 5871 | 170317.6 | 86/0 | 0.31/0.00 | 86/1.674 |
| scan | clustered | cold-manager | n=3 | 53.133 | 51.928–56.361 | 5313 | 188206.1 | 74/0 | 0.34/0.00 | 74/1.663 |

## Interpretation limits

- Results describe the recorded machine, Docker Desktop allocation, code revision, PostgreSQL defaults, and fixed 24-byte keys/27-byte values.
- `cold-manager` clears or recreates the decoded Prolly node cache; PostgreSQL and host OS caches are not forcibly dropped.
- The workload is serial and single-client. It does not measure connection-pool or concurrent transaction scaling.
- `query` means the public Prolly `get_many` API. Random-key range scans are intentionally not defined.
- Build and full scan have n=1 per size; other full-profile cells normally have n=3.
- Database-side statement time excludes client/runtime/tree processing and must not be compared as if it were end-to-end latency.
