# PostgreSQL vs DynamoDB Local

Equivalent public Prolly API workloads over 10,000,000 records with 27-byte values and concurrency 32. Times are medians; lower is better.

| Operation | Logical ops | PostgreSQL | DynamoDB Local | Lower latency |
|---|---:|---:|---:|---:|
| build | 10,000,000 | 8724.54 ms | 8689.13 ms | Within 1% |
| batch | 10,000 | 2484.21 ms | 6478.18 ms | PostgreSQL by 2.61× |
| query | 10,000 | 621.45 ms | 1013.36 ms | PostgreSQL by 1.63× |
| concurrent_query | 10,000 | 2030.18 ms | 2153.66 ms | PostgreSQL by 1.06× |
| diff | 10,000 | 1800.25 ms | 2691.32 ms | PostgreSQL by 1.49× |
| merge | 10,000 | 4398.45 ms | 3471.33 ms | DynamoDB Local by 1.27× |

## Interpretation

- Both adapters use the same record count, value size, operation count, and cold Prolly manager state.
- Build has one measured sample because constructing the shared fixture is the measurement; other operations use the configured repetition count.
- DynamoDB Local is useful for repeatable adapter regression testing. It does not model AWS network latency, partitions, throttling, or capacity.
- PostgreSQL runs in Docker with its own process and buffer cache. These numbers compare local implementations, not production infrastructure.

Detailed backend reports are in `postgres/report.md` and `dynamodb/report.md`.

## Change from supplied baselines

Positive throughput change is an improvement. Merge is omitted when a legacy DynamoDB baseline used the former double-sized merge workload.

| Backend | Operation | Baseline | Current | Throughput change |
|---|---|---:|---:|---:|
| PostgreSQL | build | 10751.59 ms | 8724.54 ms | +23.2% |
| PostgreSQL | batch | 2873.24 ms | 2484.21 ms | +15.7% |
| PostgreSQL | query | 742.09 ms | 621.45 ms | +19.4% |
| PostgreSQL | diff | 1743.75 ms | 1800.25 ms | -3.1% |
| PostgreSQL | merge | 4742.68 ms | 4398.45 ms | +7.8% |
| DynamoDB Local | build | 10795.31 ms | 8689.13 ms | +24.2% |
| DynamoDB Local | batch | 6902.64 ms | 6478.18 ms | +6.6% |
| DynamoDB Local | query | 980.64 ms | 1013.36 ms | -3.2% |
| DynamoDB Local | concurrent_query | 2117.82 ms | 2153.66 ms | -1.7% |
| DynamoDB Local | diff | 2899.67 ms | 2691.32 ms | +7.7% |
