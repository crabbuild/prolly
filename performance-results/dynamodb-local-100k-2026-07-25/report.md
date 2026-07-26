# DynamoDB Local performance report

Medians are reported across completed repetitions. DynamoDB Local is a repeatability tool, not a predictor of AWS DynamoDB capacity or latency.

| Operation | Median time | Operations/s | Speedup |
|---|---:|---:|---:|
| batch | 768.33 ms | 13,015.2 | 1.43x |
| build | 415.87 ms | 240,461.3 | 1.67x |
| cas_conflict | 94.94 ms | 1,053.3 | 1.46x |
| concurrent_query | 3314.29 ms | 3,017.2 | — |
| diff | 433.69 ms | 23,057.7 | 1.02x |
| list_roots | 36.89 ms | 27,104.0 | 19.46x |
| merge | 227.48 ms | 87,919.1 | 1.02x |
| query | 428.35 ms | 23,345.3 | 1.01x |
| raw_batch_get | 77.38 ms | 32,310.0 | 2.52x |
| raw_batch_put | 207.05 ms | 12,074.1 | 2.17x |
