# PostgreSQL-backed Prolly performance

Revision `b3d69526b4ddf6993b611b46926ef748c0c14687` (dirty=true); 71 validated raw rows.

This is an end-to-end single-client measurement of the public async Prolly API over SQLx and PostgreSQL 16 in Docker Desktop. Latency is wall-clock time; PostgreSQL execution time is separately observed by `pg_stat_statements`.

## 10,000,000 records

| Operation | Pattern | Cache | n | Median ms | Min–max ms | ns/op | ops/s | Nodes R/W | MiB R/W | PG calls / ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| batch | append | cold-manager | n=3 | 22.638 | 21.372–24.402 | 2264 | 441739.2 | 4/73 | 0.01/0.32 | 5/2.670 |
| batch | clustered | cold-manager | n=3 | 40.825 | 38.354–80.350 | 4082 | 244949.7 | 72/69 | 0.34/0.33 | 10/4.413 |
| batch | random | cold-manager | n=3 | 2873.239 | 2823.589–3119.846 | 287324 | 3480.4 | 9413/9413 | 65.79/65.79 | 165/695.397 |
| build | base | cold-manager | n=1 | 10751.592 | 10751.592–10751.592 | 1075 | 930094.8 | 0/76974 | 0.00/301.95 | 76/2503.618 |
| diff | append | cold-manager | n=3 | 54.225 | 54.151–55.873 | 5422 | 184418.1 | 77/0 | 0.33/0.00 | 73/1.816 |
| diff | clustered | cold-manager | n=3 | 13.358 | 13.089–19.328 | 1336 | 748622.1 | 138/0 | 0.66/0.00 | 35/2.653 |
| diff | random | cold-manager | n=3 | 1743.746 | 1723.757–1944.776 | 174375 | 5734.8 | 18848/0 | 132.48/0.00 | 4699/432.860 |
| full_scan | append | cold-manager | n=1 | 48288.503 | 48288.503–48288.503 | 4829 | 207088.6 | 76979/0 | 301.97/0.00 | 68352/1377.708 |
| get_cold | append | cold-manager | n=3 | 28112.500 | 27056.160–31985.240 | 2811250 | 355.7 | 40000/0 | 157.57/0.00 | 40000/838.809 |
| get_cold | clustered | cold-manager | n=3 | 28527.979 | 27514.061–28966.448 | 2852798 | 350.5 | 40000/0 | 233.30/0.00 | 40000/981.750 |
| get_cold | random | cold-manager | n=3 | 29476.286 | 28196.382–34357.819 | 2947629 | 339.3 | 40000/0 | 246.77/0.00 | 40000/1195.488 |
| get_warm | append | warm-manager | n=3 | 10.225 | 4.489–10.389 | 1022 | 978003.0 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | clustered | warm-manager | n=3 | 9.396 | 4.156–10.363 | 940 | 1064329.8 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | random | warm-manager | n=3 | 25.671 | 21.726–29.671 | 2567 | 389537.7 | 0/0 | 0.00/0.00 | 0/0.000 |
| merge | append | cold-manager | n=3 | 4230.976 | 4137.351–4665.899 | 423098 | 2363.5 | 646/4 | 3.08/0.01 | 640/51.627 |
| merge | clustered | cold-manager | n=3 | 6.800 | 6.051–7.851 | 680 | 1470696.4 | 12/4 | 0.06/0.02 | 5/0.657 |
| merge | random | cold-manager | n=3 | 4742.679 | 4704.543–5044.866 | 474268 | 2108.5 | 16896/5503 | 122.68/39.94 | 3251/731.303 |
| put | append | cold-manager | n=3 | 7.926 | 6.837–10.190 | 7926208 | 126.2 | 4/4 | 0.01/0.01 | 5/0.459 |
| put | clustered | cold-manager | n=3 | 12.753 | 12.045–14.516 | 12752750 | 78.4 | 7/4 | 0.03/0.02 | 8/0.634 |
| put | random | cold-manager | n=3 | 15.541 | 11.143–16.158 | 15540834 | 64.3 | 7/4 | 0.03/0.01 | 8/0.633 |
| query | append | cold-manager | n=3 | 21.960 | 17.622–22.996 | 2196 | 455369.9 | 71/0 | 0.30/0.00 | 17/1.394 |
| query | clustered | cold-manager | n=3 | 22.286 | 19.822–26.661 | 2229 | 448704.6 | 69/0 | 0.33/0.00 | 19/1.465 |
| query | random | cold-manager | n=3 | 742.092 | 730.749–1253.060 | 74209 | 13475.4 | 9397/0 | 66.56/0.00 | 156/186.471 |
| scan | append | cold-manager | n=3 | 45.006 | 40.372–48.006 | 4501 | 222193.0 | 71/0 | 0.30/0.00 | 56/1.686 |
| scan | clustered | cold-manager | n=3 | 27.126 | 26.731–28.555 | 2713 | 368643.8 | 69/0 | 0.33/0.00 | 28/1.474 |

## Interpretation limits

- Results describe the recorded machine, Docker Desktop allocation, code revision, PostgreSQL defaults, and fixed 24-byte keys/27-byte values.
- `cold-manager` clears or recreates the decoded Prolly node cache; PostgreSQL and host OS caches are not forcibly dropped.
- The workload is serial and single-client. It does not measure connection-pool or concurrent transaction scaling.
- `query` means the public Prolly `get_many` API. Random-key range scans are intentionally not defined.
- Build and full scan have n=1 per size; other full-profile cells normally have n=3.
- Database-side statement time excludes client/runtime/tree processing and must not be compared as if it were end-to-end latency.
