# DynamoDB Local performance report

Medians are reported across completed repetitions. DynamoDB Local is a repeatability tool, not a predictor of AWS DynamoDB capacity or latency.

| Operation | Median time | Operations/s | Speedup |
|---|---:|---:|---:|
| batch | 7281.05 ms | 1,373.4 | — |
| build | 13117.07 ms | 762,365.2 | — |
| cas_conflict | 123.30 ms | 811.0 | — |
| concurrent_query | 2721.66 ms | 3,674.2 | — |
| diff | 2752.35 ms | 3,633.3 | — |
| list_roots | 48.82 ms | 20,483.6 | — |
| merge | 3903.81 ms | 5,123.2 | — |
| query | 2073.78 ms | 4,822.1 | — |
| raw_batch_get | 184.05 ms | 13,583.2 | — |
| raw_batch_put | 500.64 ms | 4,993.6 | — |
