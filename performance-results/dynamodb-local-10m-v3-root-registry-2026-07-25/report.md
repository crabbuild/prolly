# DynamoDB Local performance report

Medians are reported across completed repetitions. DynamoDB Local is a repeatability tool, not a predictor of AWS DynamoDB capacity or latency.

| Operation | Median time | Operations/s | Speedup |
|---|---:|---:|---:|
| batch | 6679.95 ms | 1,497.0 | 1.20x |
| build | 11024.75 ms | 907,050.4 | 1.91x |
| cas_conflict | 89.27 ms | 1,120.2 | 1.17x |
| concurrent_query | 1994.69 ms | 5,013.3 | 1.28x |
| diff | 2600.59 ms | 3,845.3 | 1.23x |
| list_roots | 49.56 ms | 20,177.7 | 0.69x |
| list_roots_large_table | 63.43 ms | 15,764.5 | 1125.55x |
| merge | 3301.32 ms | 6,058.2 | 1.23x |
| query | 971.42 ms | 10,294.2 | 1.10x |
| raw_batch_get | 74.31 ms | 33,641.2 | 1.08x |
| raw_batch_put | 170.04 ms | 14,702.4 | 1.26x |
