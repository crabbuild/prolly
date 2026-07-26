# DynamoDB Local performance report

Medians are reported across completed repetitions. DynamoDB Local is a repeatability tool, not a predictor of AWS DynamoDB capacity or latency.

| Operation | Median time | Operations/s | Speedup |
|---|---:|---:|---:|
| batch | 6478.18 ms | 1,543.6 | — |
| build | 8689.13 ms | 1,150,863.2 | — |
| cas_conflict | 134.87 ms | 741.5 | — |
| concurrent_query | 2153.66 ms | 4,643.3 | — |
| diff | 2691.32 ms | 3,715.6 | — |
| list_roots | 34.63 ms | 28,879.3 | — |
| list_roots_large_table | 27.83 ms | 35,935.7 | — |
| merge | 3471.33 ms | 2,880.7 | — |
| query | 1013.36 ms | 9,868.2 | — |
| raw_batch_get | 80.07 ms | 31,224.3 | — |
| raw_batch_put | 219.77 ms | 11,375.3 | — |
