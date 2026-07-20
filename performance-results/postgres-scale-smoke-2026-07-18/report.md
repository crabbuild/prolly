# PostgreSQL-backed Prolly performance

Revision `42873d1561c3641c9091fe2a616567e790029762` (dirty=true); 25 validated raw rows.

This is an end-to-end single-client measurement of the public async Prolly API over SQLx and PostgreSQL 16 in Docker Desktop. Latency is wall-clock time; PostgreSQL execution time is separately observed by `pg_stat_statements`.

## 1,000 records

| Operation | Pattern | Cache | n | Median ms | Min–max ms | ns/op | ops/s | Nodes R/W | MiB R/W | PG calls / ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| batch | append | cold-manager | n=1 | 8.356 | 8.356–8.356 | 83557 | 11967.9 | 0/5 | 0.00/0.00 | 9/0.260 |
| batch | clustered | cold-manager | n=1 | 43.088 | 43.088–43.088 | 430882 | 2320.8 | 8/9 | 0.03/0.03 | 21/1.422 |
| batch | random | cold-manager | n=1 | 14.375 | 14.375–14.375 | 143750 | 6956.5 | 8/9 | 0.03/0.03 | 21/0.542 |
| build | base | cold-manager | n=1 | 30.314 | 30.314–30.314 | 30314 | 32987.9 | 0/9 | 0.00/0.03 | 10/0.760 |
| diff | append | cold-manager | n=1 | 10.460 | 10.460–10.460 | 104595 | 9560.7 | 7/0 | 0.01/0.00 | 7/0.084 |
| diff | clustered | cold-manager | n=1 | 4.229 | 4.229–4.229 | 42285 | 23648.8 | 4/0 | 0.01/0.00 | 4/0.042 |
| diff | random | cold-manager | n=1 | 16.166 | 16.166–16.166 | 161658 | 6185.9 | 18/0 | 0.06/0.00 | 18/0.212 |
| full_scan | append | cold-manager | n=1 | 7.807 | 7.807–7.807 | 7807 | 128096.3 | 9/0 | 0.03/0.00 | 9/0.116 |
| get_cold | append | cold-manager | n=1 | 246.063 | 246.063–246.063 | 2460626 | 406.4 | 200/0 | 0.92/0.00 | 200/4.741 |
| get_cold | clustered | cold-manager | n=1 | 200.787 | 200.787–200.787 | 2007872 | 498.0 | 200/0 | 0.48/0.00 | 200/2.174 |
| get_cold | random | cold-manager | n=1 | 228.001 | 228.001–228.001 | 2280015 | 438.6 | 200/0 | 0.81/0.00 | 200/3.843 |
| get_warm | append | warm-manager | n=1 | 0.072 | 0.072–0.072 | 719 | 1391304.3 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | clustered | warm-manager | n=1 | 0.063 | 0.063–0.063 | 632 | 1583105.1 | 0/0 | 0.00/0.00 | 0/0.000 |
| get_warm | random | warm-manager | n=1 | 0.076 | 0.076–0.076 | 763 | 1310032.2 | 0/0 | 0.00/0.00 | 0/0.000 |
| merge | append | cold-manager | n=1 | 9.441 | 9.441–9.441 | 47205 | 21184.0 | 6/2 | 0.01/0.00 | 12/0.143 |
| merge | clustered | cold-manager | n=1 | 21.655 | 21.655–21.655 | 108275 | 9235.8 | 16/9 | 0.06/0.03 | 29/0.619 |
| merge | random | cold-manager | n=1 | 28.528 | 28.528–28.528 | 142639 | 7010.7 | 18/9 | 0.06/0.03 | 31/0.697 |
| put | append | cold-manager | n=1 | 9.589 | 9.589–9.589 | 9588750 | 104.3 | 0/2 | 0.00/0.00 | 6/0.106 |
| put | clustered | cold-manager | n=1 | 17.363 | 17.363–17.363 | 17362625 | 57.6 | 8/9 | 0.03/0.03 | 21/0.568 |
| put | random | cold-manager | n=1 | 21.829 | 21.829–21.829 | 21829417 | 45.8 | 8/9 | 0.03/0.03 | 21/0.564 |
| query | append | cold-manager | n=1 | 2.831 | 2.831–2.831 | 28312 | 35321.1 | 3/0 | 0.01/0.00 | 3/0.054 |
| query | clustered | cold-manager | n=1 | 3.456 | 3.456–3.456 | 34559 | 28935.9 | 2/0 | 0.00/0.00 | 2/0.015 |
| query | random | cold-manager | n=1 | 19.443 | 19.443–19.443 | 194432 | 5143.2 | 8/0 | 0.03/0.00 | 8/0.399 |
| scan | append | cold-manager | n=1 | 7.214 | 7.214–7.214 | 72139 | 13862.2 | 3/0 | 0.01/0.00 | 3/0.127 |
| scan | clustered | cold-manager | n=1 | 2.200 | 2.200–2.200 | 22001 | 45452.8 | 2/0 | 0.00/0.00 | 2/0.012 |

## Interpretation limits

- Results describe the recorded machine, Docker Desktop allocation, code revision, PostgreSQL defaults, and fixed 24-byte keys/27-byte values.
- `cold-manager` clears or recreates the decoded Prolly node cache; PostgreSQL and host OS caches are not forcibly dropped.
- The workload is serial and single-client. It does not measure connection-pool or concurrent transaction scaling.
- `query` means the public Prolly `get_many` API. Random-key range scans are intentionally not defined.
- Build and full scan have n=1 per size; other full-profile cells normally have n=3.
- Database-side statement time excludes client/runtime/tree processing and must not be compared as if it were end-to-end latency.
