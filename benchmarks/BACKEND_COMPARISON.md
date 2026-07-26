# PostgreSQL and DynamoDB Local comparison

Run the equivalent backend workloads with one command:

```bash
./scripts/run_backend_comparison.sh
```

The default run uses 1 million records, 27-byte values, 10,000 sampled or
changed keys, and three repetitions. The runner starts each Docker service
separately, runs the public Prolly API workloads, stops that service, and emits:

- `postgres/report.md`
- `dynamodb/report.md`
- `comparison.csv`
- `report.md`

All important scale and concurrency dimensions are configurable:

```bash
BENCH_RECORDS=10000000 \
BENCH_CHANGES=10000 \
BENCH_SAMPLES=10000 \
BENCH_RUNS=3 \
BENCH_CONCURRENCY=64 \
BENCH_BATCH_GET_PARALLELISM=16 \
BENCH_BATCH_WRITE_PARALLELISM=16 \
BENCH_OUT=performance-results/backend-comparison-10m \
./scripts/run_backend_comparison.sh
```

`BENCH_CHANGES` must be even. Batch and diff apply that many changes; merge
splits the same total evenly across two non-conflicting branches. PostgreSQL
and DynamoDB Local are never kept active together, which avoids cross-backend
CPU and memory interference.

DynamoDB Local results are regression measurements, not predictions of AWS
DynamoDB latency or capacity behavior.

Set `BENCH_POSTGRES_BASELINE` and `BENCH_DYNAMODB_BASELINE` to prior result
directories to add a compatible-operation before/after section. Legacy
DynamoDB results remain usable for build, batch, query, and diff; merge is
automatically omitted because schema v4 measured twice as many merge changes.

## Byte-for-byte correctness

Run the deterministic cross-backend verifier separately from the timed
benchmark:

```bash
./scripts/run_backend_correctness.sh
```

It starts PostgreSQL and DynamoDB Local, applies identical shuffled builds,
duplicate mutations, deletes, inserts, disjoint merges, and conflicting merges,
then compares both adapters with an in-memory oracle. It requires exact equality
for ordered diff payloads, conflict payloads, logical records, canonical roots,
cold-reopened snapshot bundles, every stored CID, and every serialized node
byte. Override `PROLLY_CORRECTNESS_RECORDS` or
`PROLLY_CORRECTNESS_CHANGES_PER_KIND` to increase the deterministic fixture.
