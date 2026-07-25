# PostgreSQL-backed Prolly service performance

Revision `b3d69526b4ddf6993b611b46926ef748c0c14687` (dirty=true); 20 validated service rows.

This closed-loop workload measures end-to-end public Prolly operations. Latency includes PostgreSQL pool wait.

## Service saturation

| Clients | Pool | Attempted ops/s | Successful ops/s | Conflicts | Unexpected errors | PG statements/op | Prolly node reads/op |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2 | 250.3 | 250.3 | 0 | 0 | 5.064 | 4.197 |
| 4 | 2 | 302.7 | 298.7 | 81 | 0 | 5.807 | 4.558 |

## Operation latency

| Clients | Pool | Operation | Tenant class | Samples | Successful ops/s | p50 ms | p95 ms | p99 ms | p99.9 ms | Max ms | Conflict rate |
|---:|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2 | commit | hot | 32 | 15.9 | 7.762 | 10.306 | 11.166 | 11.166 | 11.166 | 0.0000 |
| 1 | 2 | commit | independent | 94 | 46.8 | 8.114 | 10.617 | 16.843 | 16.843 | 16.843 | 0.0000 |
| 1 | 2 | diff | hot | 5 | 2.5 | 2.084 | 2.701 | 2.701 | 2.701 | 2.701 | 0.0000 |
| 1 | 2 | diff | independent | 45 | 22.4 | 2.118 | 2.992 | 3.287 | 3.287 | 3.287 | 0.0000 |
| 1 | 2 | merge | hot | 17 | 8.5 | 12.460 | 15.335 | 15.335 | 15.335 | 15.335 | 0.0000 |
| 1 | 2 | merge | independent | 8 | 4.0 | 12.968 | 16.990 | 16.990 | 16.990 | 16.990 | 0.0000 |
| 1 | 2 | multi_read | hot | 17 | 8.5 | 1.964 | 2.466 | 2.466 | 2.466 | 2.466 | 0.0000 |
| 1 | 2 | multi_read | independent | 58 | 28.9 | 1.932 | 2.454 | 2.834 | 2.834 | 2.834 | 0.0000 |
| 1 | 2 | point_read | hot | 55 | 27.4 | 1.595 | 2.101 | 2.318 | 2.318 | 2.318 | 0.0000 |
| 1 | 2 | point_read | independent | 172 | 85.6 | 1.586 | 2.086 | 2.640 | 2.722 | 2.722 | 0.0000 |
| 4 | 2 | commit | hot | 38 | 18.9 | 31.064 | 61.145 | 61.178 | 61.178 | 61.178 | 0.3968 |
| 4 | 2 | commit | independent | 113 | 56.1 | 22.462 | 51.773 | 57.672 | 60.424 | 60.424 | 0.2981 |
| 4 | 2 | diff | hot | 5 | 2.5 | 4.932 | 6.910 | 6.910 | 6.910 | 6.910 | 0.0000 |
| 4 | 2 | diff | independent | 56 | 27.8 | 5.276 | 6.558 | 7.430 | 7.430 | 7.430 | 0.0000 |
| 4 | 2 | merge | hot | 22 | 8.4 | 30.294 | 35.193 | 35.455 | 35.455 | 35.455 | 0.2273 |
| 4 | 2 | merge | independent | 9 | 3.0 | 29.884 | 35.422 | 35.422 | 35.422 | 35.422 | 0.3333 |
| 4 | 2 | multi_read | hot | 21 | 10.4 | 6.779 | 8.286 | 9.003 | 9.003 | 9.003 | 0.0000 |
| 4 | 2 | multi_read | independent | 71 | 35.2 | 6.844 | 8.626 | 10.428 | 10.428 | 10.428 | 0.0000 |
| 4 | 2 | point_read | hot | 67 | 33.2 | 6.185 | 8.294 | 9.486 | 9.486 | 9.486 | 0.0000 |
| 4 | 2 | point_read | independent | 208 | 103.2 | 6.423 | 8.192 | 8.782 | 9.658 | 9.658 | 0.0000 |

## Regression verdict

- All configured service gates passed or no baseline was supplied.

## Interpretation limits

- Results apply to the recorded machine, PostgreSQL settings, pool sizes, workload, and revision.
- The generator is closed-loop; it measures saturation by concurrency rather than an external arrival-rate distribution.
- Each logical service operation uses a fresh Prolly manager, so decoded node-cache entries are not shared between operations; PostgreSQL and host caches remain warm.
- Scheduler and transaction interleaving are nondeterministic even though operation traces and data are seeded.
- PostgreSQL statement and WAL counters are cell-wide and are repeated on operation rows; the report divides them by total cell completions.


## Serial large-tree performance

Revision `b3d69526b4ddf6993b611b46926ef748c0c14687` (dirty=true); 25 validated raw rows.

This is an end-to-end single-client measurement of the public async Prolly API over SQLx and PostgreSQL 16 in Docker Desktop. Latency is wall-clock time; PostgreSQL execution time is separately observed by `pg_stat_statements`.

## Workload cardinality

Batch and diff mutate 100 keys. Point get, multi-get, and bounded scan sample 100 keys or entries.
Merge treats 100 as the total change count: 50 changes per branch across two disjoint branches.
Random merge keys are interleaved across both branches so each branch spans the full base keyspace.

## 1,000 records

| Operation | Pattern | Cache | n | Median ms | Min–max ms | ns/op | ops/s | Nodes R/W | MiB R/W | PG calls / ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| batch | append | cold-manager | n=1 | 2.943 | 2.943–2.943 | 29429 | 33980.4 | 2/5 | 0.00/0.00 | 3/0.076 |
| batch | clustered | cold-manager | n=1 | 3.666 | 3.666–3.666 | 36663 | 27275.2 | 3/2 | 0.01/0.00 | 4/0.145 |
| batch | random | cold-manager | n=1 | 6.492 | 6.492–6.492 | 64922 | 15403.1 | 9/9 | 0.03/0.03 | 5/0.394 |
| build | base | cold-manager | n=1 | 2.892 | 2.892–2.892 | 2892 | 345771.4 | 0/9 | 0.00/0.03 | 1/0.418 |
| diff | append | cold-manager | n=1 | 3.515 | 3.515–3.515 | 35147 | 28452.2 | 7/0 | 0.01/0.00 | 5/0.074 |
| diff | clustered | cold-manager | n=1 | 1.310 | 1.310–1.310 | 13102 | 76326.1 | 4/0 | 0.01/0.00 | 2/0.075 |
| diff | random | cold-manager | n=1 | 1.788 | 1.788–1.788 | 17883 | 55919.3 | 18/0 | 0.06/0.00 | 2/0.148 |
| full_scan | append | cold-manager | n=1 | 2.349 | 2.349–2.349 | 2349 | 425637.7 | 9/0 | 0.03/0.00 | 3/0.077 |
| get_cold | append | cold-manager | n=1 | 133.036 | 133.036–133.036 | 1330358 | 751.7 | 200/0 | 0.92/0.00 | 200/2.553 |
| get_cold | clustered | cold-manager | n=1 | 118.859 | 118.859–118.859 | 1188593 | 841.3 | 200/0 | 0.48/0.00 | 200/0.754 |
| get_cold | random | cold-manager | n=1 | 125.838 | 125.838–125.838 | 1258380 | 794.7 | 200/0 | 0.81/0.00 | 200/1.856 |
| get_warm | append | warm-manager | n=1 | 0.117 | 0.117–0.117 | 1167 | 857147.8 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | clustered | warm-manager | n=1 | 0.137 | 0.137–0.137 | 1367 | 731486.1 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | random | warm-manager | n=1 | 0.184 | 0.184–0.184 | 1841 | 543109.3 | 0/0 | 0.00/0.00 | 0/0.000 |
| merge | append | cold-manager | n=1 | 5.020 | 5.020–5.020 | 50198 | 19921.0 | 8/2 | 0.01/0.00 | 6/0.123 |
| merge | clustered | cold-manager | n=1 | 2.721 | 2.721–2.721 | 27206 | 36756.8 | 6/2 | 0.01/0.00 | 3/0.130 |
| merge | random | cold-manager | n=1 | 6.632 | 6.632–6.632 | 66321 | 15078.2 | 27/9 | 0.09/0.03 | 3/0.507 |
| put | append | cold-manager | n=1 | 3.799 | 3.799–3.799 | 3799417 | 263.2 | 2/2 | 0.00/0.00 | 3/0.119 |
| put | clustered | cold-manager | n=1 | 3.485 | 3.485–3.485 | 3484625 | 287.0 | 3/2 | 0.01/0.00 | 4/0.121 |
| put | random | cold-manager | n=1 | 3.540 | 3.540–3.540 | 3539791 | 282.5 | 3/2 | 0.01/0.00 | 4/0.136 |
| query | append | cold-manager | n=1 | 1.423 | 1.423–1.423 | 14233 | 70259.7 | 3/0 | 0.01/0.00 | 2/0.062 |
| query | clustered | cold-manager | n=1 | 1.364 | 1.364–1.364 | 13645 | 73289.2 | 2/0 | 0.00/0.00 | 2/0.037 |
| query | random | cold-manager | n=1 | 1.609 | 1.609–1.609 | 16095 | 62132.7 | 8/0 | 0.03/0.00 | 2/0.085 |
| scan | append | cold-manager | n=1 | 1.769 | 1.769–1.769 | 17686 | 56541.1 | 3/0 | 0.01/0.00 | 3/0.030 |
| scan | clustered | cold-manager | n=1 | 1.379 | 1.379–1.379 | 13795 | 72492.2 | 2/0 | 0.00/0.00 | 2/0.011 |

## Interpretation limits

- Results describe the recorded machine, Docker Desktop allocation, code revision, PostgreSQL defaults, and fixed 24-byte keys/27-byte values.
- `cold-manager` clears or recreates the decoded Prolly node cache; PostgreSQL and host OS caches are not forcibly dropped.
- The workload is serial and single-client. It does not measure connection-pool or concurrent transaction scaling.
- `query` means the public Prolly `get_many` API. Random-key range scans are intentionally not defined.
- Build and full scan have n=1 per size; other full-profile cells normally have n=3.
- Database-side statement time excludes client/runtime/tree processing and must not be compared as if it were end-to-end latency.
