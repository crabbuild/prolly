# DynamoDB Local performance report

Medians are reported across completed repetitions. DynamoDB Local is a repeatability tool, not a predictor of AWS DynamoDB capacity or latency.

| Operation | Median time | Operations/s | Speedup |
|---|---:|---:|---:|
| batch | 8026.85 ms | 1,245.8 | 0.91x |
| build | 21081.71 ms | 474,344.7 | 0.62x |
| cas_conflict | 104.52 ms | 956.8 | 1.18x |
| concurrent_query | 2561.26 ms | 3,904.3 | 1.06x |
| diff | 3205.91 ms | 3,119.2 | 0.86x |
| list_roots | 34.27 ms | 29,183.0 | 1.42x |
| list_roots_large_table | 71397.64 ms | 14.0 | — |
| merge | 4073.34 ms | 4,910.0 | 0.96x |
| query | 1071.50 ms | 9,332.7 | 1.94x |
| raw_batch_get | 80.23 ms | 31,160.6 | 2.29x |
| raw_batch_put | 213.74 ms | 11,696.6 | 2.34x |
