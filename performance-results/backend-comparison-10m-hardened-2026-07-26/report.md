# PostgreSQL vs DynamoDB Local

This local comparison uses 10000000 records, 27-byte values, concurrency 32, and 7 measured repetitions. Latency is lower; throughput is higher.

| Operation | Logical ops | PostgreSQL median | PostgreSQL ops/s | DynamoDB Local median | DynamoDB Local ops/s | DDB/PG 95% CI | Result |
|---|---:|---:|---:|---:|---:|---:|---|
| build | 10000000 | 10283.30 ms | 972450.31 | 9107.59 ms | 1097985.62 | 0.744–0.917 | DynamoDB Local |
| batch | 10000 | 3182.98 ms | 3141.71 | 3519.62 ms | 2841.22 | 0.894–1.128 | inconclusive |
| query | 10000 | 627.59 ms | 15933.94 | 894.58 ms | 11178.42 | 1.388–1.637 | PostgreSQL |
| concurrent_query | 10000 | 3446.97 ms | 2901.10 | 1928.13 ms | 5186.37 | 0.481–0.636 | DynamoDB Local |
| diff | 10000 | 1832.30 ms | 5457.62 | 2384.86 ms | 4193.12 | 1.229–1.380 | PostgreSQL |
| merge | 10000 | 5094.24 ms | 1963.00 | 5301.38 ms | 1886.30 | 1.010–1.105 | inconclusive |

## Dispersion

| Operation | PostgreSQL range | PostgreSQL MAD | PostgreSQL CV | DynamoDB Local range | DynamoDB Local MAD | DynamoDB Local CV |
|---|---:|---:|---:|---:|---:|---:|
| build | 10065.36–24789.20 ms | 217.94 ms | 42.56% | 8861.99–11895.99 ms | 156.34 ms | 11.14% |
| batch | 3125.89–4117.29 ms | 23.51 ms | 11.89% | 3410.27–3632.42 ms | 80.42 ms | 2.49% |
| query | 590.58–718.74 ms | 30.87 ms | 7.12% | 882.25–1109.23 ms | 7.77 ms | 9.02% |
| concurrent_query | 3351.57–4229.29 ms | 95.40 ms | 9.34% | 1877.76–2207.01 ms | 39.05 ms | 7.19% |
| diff | 1754.55–2016.21 ms | 39.69 ms | 4.77% | 2293.20–8742.76 ms | 44.31 ms | 73.65% |
| merge | 4974.31–5583.98 ms | 69.98 ms | 4.02% | 5154.95–6229.98 ms | 113.94 ms | 6.92% |

## Interpretation

- Each row uses byte-identical workloads and complete post-timing validation.
- A winner requires a paired bootstrap 95% confidence interval that excludes parity and a median effect above 5%.
- DynamoDB Local does not model Amazon DynamoDB network latency, throttling, partitions, capacity, or cost.
- These measurements compare local adapter implementations, not production services.

Run ID: `backend-136ed99fc880-20260727T002905Z-82013`.
