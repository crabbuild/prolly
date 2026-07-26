# DynamoDB Local performance report

Medians are reported across completed repetitions. DynamoDB Local is a repeatability tool, not a predictor of AWS DynamoDB capacity or latency.

| Operation | Median time | Operations/s | Speedup |
|---|---:|---:|---:|
| batch | 6902.64 ms | 1,448.7 | 1.16x |
| build | 10795.31 ms | 926,327.8 | 1.95x |
| cas_conflict | 94.89 ms | 1,053.9 | 1.10x |
| concurrent_query | 2117.82 ms | 4,721.8 | 1.21x |
| diff | 2899.67 ms | 3,448.7 | 1.11x |
| list_roots | 31.39 ms | 31,856.9 | 1.09x |
| list_roots_large_table | 29.91 ms | 33,429.4 | 2386.78x |
| merge | 3268.13 ms | 6,119.7 | 1.25x |
| query | 980.64 ms | 10,197.5 | 1.09x |
| raw_batch_get | 74.84 ms | 33,405.7 | 1.07x |
| raw_batch_put | 178.50 ms | 14,005.4 | 1.20x |
