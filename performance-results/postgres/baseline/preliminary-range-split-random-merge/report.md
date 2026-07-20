# PostgreSQL-backed Prolly performance

Revision `d6f33dc7351ba93f6117913a11cfb85e7d751696` (dirty=true); 71 validated raw rows.

This is an end-to-end single-client measurement of the public async Prolly API over SQLx and PostgreSQL 16 in Docker Desktop. Latency is wall-clock time; PostgreSQL execution time is separately observed by `pg_stat_statements`.

## Workload cardinality

Batch and diff mutate 300,000 keys. Point get, multi-get, and bounded scan sample 10,000 keys or entries.
Merge treats 300,000 as the total change count: 150,000 changes per branch across two disjoint branches.

## 1,000,000 records

| Operation | Pattern | Cache | n | Median ms | Min–max ms | ns/op | ops/s | Nodes R/W | MiB R/W | PG calls / ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| batch | append | cold-manager | n=3 | 1050.058 | 1002.746–1141.585 | 3500 | 285698.4 | 4/2295 | 0.01/9.07 | 2299/90.814 |
| batch | clustered | cold-manager | n=3 | 2690.374 | 2635.008–3182.001 | 8968 | 111508.6 | 2213/2211 | 9.07/9.06 | 4424/124.834 |
| batch | random | cold-manager | n=3 | 8637.911 | 8501.049–8723.440 | 28793 | 34730.6 | 7659/7659 | 30.18/30.18 | 15318/440.468 |
| build | base | cold-manager | n=1 | 3106.792 | 3106.792–3106.792 | 3107 | 321875.4 | 0/7719 | 0.00/30.20 | 7719/296.067 |
| diff | append | cold-manager | n=3 | 1843.616 | 1602.127–5458.464 | 6145 | 162723.7 | 2299/0 | 9.08/0.00 | 2299/44.901 |
| diff | clustered | cold-manager | n=3 | 930.310 | 874.075–1088.764 | 3101 | 322473.2 | 4422/0 | 18.13/0.00 | 4422/98.941 |
| diff | random | cold-manager | n=3 | 3455.878 | 2979.619–3717.621 | 11520 | 86808.6 | 15324/0 | 60.37/0.00 | 15324/325.214 |
| full_scan | append | cold-manager | n=1 | 5170.721 | 5170.721–5170.721 | 5171 | 193396.6 | 7719/0 | 30.20/0.00 | 7719/131.237 |
| get_cold | append | cold-manager | n=3 | 29355.573 | 26721.726–36796.982 | 2935557 | 340.7 | 40000/0 | 189.84/0.00 | 40000/1091.746 |
| get_cold | clustered | cold-manager | n=3 | 28932.065 | 27524.796–29047.309 | 2893207 | 345.6 | 40000/0 | 221.93/0.00 | 40000/1050.592 |
| get_cold | random | cold-manager | n=3 | 33044.045 | 27922.456–41690.428 | 3304405 | 302.6 | 40000/0 | 164.77/0.00 | 40000/999.081 |
| get_warm | append | warm-manager | n=3 | 4.473 | 4.438–4.481 | 447 | 2235740.0 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | clustered | warm-manager | n=3 | 4.337 | 4.309–4.930 | 434 | 2305563.7 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | random | warm-manager | n=3 | 12.194 | 12.013–14.736 | 1219 | 820067.0 | 0/0 | 0.00/0.00 | 0/0.000 |
| merge | append | cold-manager | n=3 | 1343.315 | 1262.063–1462.805 | 4478 | 223328.1 | 1139/1130 | 4.57/4.54 | 2269/69.632 |
| merge | clustered | cold-manager | n=3 | 13.426 | 13.068–19.826 | 45 | 22344912.3 | 12/4 | 0.09/0.03 | 16/0.970 |
| merge | random | cold-manager | n=3 | 18.350 | 13.961–21.537 | 61 | 16348921.7 | 12/4 | 0.09/0.03 | 16/1.217 |
| put | append | cold-manager | n=3 | 6.286 | 5.803–7.547 | 6286292 | 159.1 | 4/4 | 0.01/0.01 | 8/0.407 |
| put | clustered | cold-manager | n=3 | 9.440 | 9.315–9.591 | 9440167 | 105.9 | 6/4 | 0.04/0.03 | 10/0.719 |
| put | random | cold-manager | n=3 | 8.653 | 7.682–19.556 | 8652625 | 115.6 | 6/4 | 0.03/0.02 | 10/0.736 |
| query | append | cold-manager | n=3 | 60.529 | 56.389–64.785 | 6053 | 165209.4 | 86/0 | 0.31/0.00 | 86/1.621 |
| query | clustered | cold-manager | n=3 | 59.990 | 57.233–83.822 | 5999 | 166694.9 | 74/0 | 0.34/0.00 | 74/1.764 |
| query | random | cold-manager | n=3 | 3193.989 | 3104.238–4431.518 | 319399 | 3130.9 | 4498/0 | 24.52/0.00 | 4498/93.787 |
| scan | append | cold-manager | n=3 | 61.989 | 59.976–98.847 | 6199 | 161320.1 | 86/0 | 0.31/0.00 | 86/1.839 |
| scan | clustered | cold-manager | n=3 | 55.280 | 51.446–100.377 | 5528 | 180897.3 | 74/0 | 0.34/0.00 | 74/1.639 |

## Interpretation limits

- Results describe the recorded machine, Docker Desktop allocation, code revision, PostgreSQL defaults, and fixed 24-byte keys/27-byte values.
- `cold-manager` clears or recreates the decoded Prolly node cache; PostgreSQL and host OS caches are not forcibly dropped.
- The workload is serial and single-client. It does not measure connection-pool or concurrent transaction scaling.
- `query` means the public Prolly `get_many` API. Random-key range scans are intentionally not defined.
- Build and full scan have n=1 per size; other full-profile cells normally have n=3.
- Database-side statement time excludes client/runtime/tree processing and must not be compared as if it were end-to-end latency.
