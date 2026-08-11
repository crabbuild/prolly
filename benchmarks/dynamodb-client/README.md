# Versioned DynamoDB client benchmark

This is the release-oriented benchmark for the in-process Rust
`prolly-dynamodb-client`. Unlike `benchmarks/dynamodb-scale`, every timed sample
enters through the DynamoDB-compatible logical client.

The executable slice measures cold and warm `GetItem`; `Query`; `Scan`;
ten-item `BatchGetItem`, `BatchWriteItem`, and `TransactGetItems`; an extended
read shape up to 100 items within the 4-MiB transactional response envelope;
the 25-item `BatchWriteItem` limit; one-, ten-, and hundred-action
`TransactWriteItems`; `PutItem`; `UpdateItem`; `DeleteItem`;
exact-version `GetItem`; structural diff; version enumeration; indexed
`PutItem`; GSI `Query`; explicit 128-KiB blob write/read; CAS restore; and
two-index planning/activation/replacement/removal; 100-write bounded retention
planning/application; fenced GC planning/application; and synchronized
1/4/8-writer point-write contention. At the configured history depth it also
enumerates and deduplicates every version through bounded pages, reads the
oldest immutable item byte-for-byte, and diffs oldest-to-head. Every
advertised write-version count is checked
against returned applied-transition metadata. It records one durable CSV row
per logical request, including serialized HTTP request bytes, buffered-body or
HTTP `Content-Length` response bytes, an explicit response-byte completeness
flag, SDK executions, wire attempts, inferred SDK retries, per-API attempt
counts, and physical transaction actions. A cold sample opens a fresh logical
client before the timed request; client-open traffic is deliberately outside
that sample.

Logical byte columns use DynamoDB item-size terms (attribute-name plus raw
attribute-value bytes), separately for input and output items. The completeness
flag is false when the benchmark has not yet instrumented a logical response
shape, so absence is never presented as a zero-byte response.

This slice is not the complete release matrix. Production concurrency,
per-operation memory/CPU, consumed capacity, hosted AWS, deeper history cases,
and the 10K/1M size matrix remain explicit release gates in
`extensions/dynamodb/client/PERFORMANCE.md`.

Run with the provenance-capturing runner (Docker by default):

```bash
./scripts/run_dynamodb_client_benchmark.sh \
  --profile smoke --output performance-results/dynamodb-client-smoke
```

Set `PROLLY_BENCH_SKIP_DOCKER=1` and `--endpoint` to use an existing emulator.
When doing so, set `PROLLY_DYNAMODB_LOCAL_ARCHIVE` to capture its archive digest.
`BENCH_TRANSACTION_SHAPES` is a comma-separated set of logical atomic-write
action counts. It defaults to `1,10,100`; reduce it to `1,10` for near-400-KiB
items so the logical aggregate remains within the 4-MiB transaction envelope.
`BENCH_READ_BATCH_ITEMS` defaults to the largest value up to 100 whose full
items fit the same transaction-read envelope; the checked-in matrix binds this
to 100, 63, or 10 according to item size. `BENCH_HISTORY_DEPTH` defaults to 100
writes and accepts 10 or more. Depths up to 80 validate the exact terminal
retention plan; larger depths exercise the 80-removal bound and
`more_removable`.
`BENCH_CONCURRENCY_WRITERS` defaults to synchronized 1/4/8-writer shapes with
five writes per writer. `BENCH_CONCURRENCY_RETRY_LIMIT` defaults to the normal
client value of seven. The validator requires a one-writer baseline and rejects
superlinear physical-request amplification, so removing local write admission
cannot silently produce acceptable evidence.
Runner v14 also binds `BENCH_NODE_CACHE_MAX_BYTES` into the executable CLI,
run manifest, and validator. It defaults to the client's 64-MiB retained
serialized-node weight; zero disables caching. This is not a process-RSS cap.

The executable also has a focused `bulk` workload for the one-version import
path. It records latency, SDK/wire calls, request/response bytes, and physical
transaction actions, then verifies that the imported table has exactly one
version. For example, against an already-running DynamoDB Local instance:

```bash
cargo run --release --manifest-path benchmarks/dynamodb-client/Cargo.toml -- \
  --workload bulk --records 1000000 --value-bytes 1024 --samples 1 \
  --endpoint http://127.0.0.1:8000 \
  --output performance-results/dynamodb-client-bulk-1m
```

This path consumes primary-key-sorted records and creates one commit for the
entire import. It is distinct from compatible `BatchWriteItem`, which retains
one commit per accepted item action.

Use `--workload large` with the same size flags to profile an explicit
`WriteSession` commit into an existing empty table. The timed row is
`LargeWriteCommit`; table creation is outside that row, and the harness verifies
that the session adds exactly one version.
The result directory contains raw samples, a deterministic summary/report,
machine and run manifests, dependency graph, binary digest, build log, DynamoDB
artifact identity, raw BSD/GNU process timing, and normalized peak RSS. The raw
timing also supplies whole-process user/system CPU; it is not per-operation CPU
attribution. Results from DynamoDB Local are regression evidence only and must
never be described as AWS latency, throughput, capacity, or cost evidence.

Long isolated runs may set `BENCH_TEARDOWN=docker-volume` together with an
explicit project name beginning `prolly-dynamodb-client-bench-ephemeral-`.
That mode skips per-item namespace deletion and removes only the runner-owned
Compose project and volume after validation (and on failure). It is rejected
for external endpoints, shared/default project names, and any pre-existing
container under the requested ephemeral project name.

The resumable matrix runner packages the supported local size cases:

```bash
./scripts/run_dynamodb_client_benchmark_matrix.sh --profile smoke
./scripts/run_dynamodb_client_benchmark_matrix.sh \
  --profile qualification --output performance-results/dynamodb-client-matrix
```

`qualification` covers 10K items at 1, 16, 64, and near-400 KiB plus 1M items
at 1 KiB. It requires a clean worktree by default and binds resumed cases to
their exact revision, workload manifest, and node-cache ceiling. Matrix-v2
aggregate validation rejects extra/missing CSV columns, missing/duplicate or
reordered cases, provenance drift, noncanonical shapes, tampered completion
manifests, and mismatched result directories. Runner-owned DynamoDB Local is
removed after every case on success or failure. This is intentionally not the
hosted-AWS or history-depth matrix.

The separate history-depth matrix is resumable and fixes the qualification
shape so it cannot be silently narrowed:

```bash
./scripts/run_dynamodb_client_history_matrix.sh --profile smoke
./scripts/run_dynamodb_client_history_matrix.sh \
  --profile qualification \
  --output performance-results/dynamodb-client-history
```

Smoke covers depths 10, 100, and 1,000 once. Qualification covers depths 10,
100, 1,000, 10,000, and 100,000 with ten samples per case, requires a clean
worktree, validates every case against its exact run manifest, and records an
aggregate matrix manifest. `BENCH_HISTORY_DEPTHS` may narrow diagnostic smoke
runs but is rejected for qualification. These cases use the focused `history`
workload: timed append, complete version enumeration, oldest immutable read,
oldest-to-head diff, and bounded retention. Global GC remains in the `full`
workload with its own explicit graph limits; history qualification does not
silently increase those limits. Every history sample opens an independent
physical key-prefix namespace, so later samples do not inherit earlier logical
catalogs or histories.
History-matrix-v2 applies the same exact aggregate-schema, cache-policy,
completion-manifest, ordering, and failure-safe teardown checks.

Runner v13 makes `full` versus `history` and teardown mode part of the
fail-closed run manifest and requires `gc-reachability.csv` for every
full-workload sample. The artifact records the derived protected-tree ceiling,
retained/protected trees, live node/blob bytes, exact blob-node and leaf-value
scan work, and examined deletion candidates. The derived ceiling allocates four
tree slots per configured history version plus 10,000 setup/catalog roots, with
checked arithmetic; validation rejects missing, malformed, duplicate,
revision-mismatched, over-limit, or empty-graph evidence. This removes the
prior hard-coded 10,000-tree ceiling that made a full depth-10K run structurally
impossible.

Runner v15 additionally requires `cache-usage.csv` for every full or history
sample. Each row binds the revision, primary-client role, and configured byte
ceiling to a single-lock occupancy snapshot. Validation rejects missing or
duplicate samples, impossible pinned occupancy, configuration drift, and
unpinned serialized-node weight above the declared ceiling. Pinned hint weight
is reported separately because correctness-optional pins may temporarily
exceed the normal cache limit. This artifact measures retained cache weight,
not allocator overhead or process RSS; qualification must retain both this
evidence and the runner's peak-RSS observation.

The latest format-12 depth-1,000 history diagnostic enumerated 1,001 unique
versions, verified the oldest value and one-change diff, and applied the bounded
80-removal plan. Append took 35.552 seconds and 22,797 SDK executions with
46,956,544-byte peak RSS. It used 322,173,160 request bytes and 505,937,748
response bytes, down 16.1% and 32.8% from format 11. A full depth-1,000 run also
completed GC with 984 retained roots, 2,861 protected trees, 56 blob-scan
nodes, and 3,073 scanned values. The additional protected trees are the exact
detached historical manifests; their leaf values are excluded from generic
blob scanning. These dirty-worktree DynamoDB Local values are regression
evidence, not a production envelope; the clean full-size, 10K/100K history,
GC-scale, and hosted-AWS matrices remain to run.

A one-sample runner-v12 format-12 10K diagnostic passes all six history rows
and cleanup. Append takes 441.004 seconds and 286,898 SDK executions with
89,948,160-byte peak client RSS; enumeration returns all 10,001 versions in
230.768 milliseconds. Request/response bytes are 4.357/10.392 GB, down
15.0%/24.3% from format 11. Per-item namespace cleanup expands whole-run time
to 1,462.62 seconds. Treat this as correctness/memory evidence and an
amplification warning, not a production envelope. Do not extrapolate it into a
100K run on the current 31-GiB in-memory emulator; the measured call count,
memory trajectory, and cleanup cost still project to an unsafe multi-hour run.

A separate runner-v13 full depth-10K diagnostic now passes all 40 exact rows
and GC under the recorded 50,000-tree ceiling. It retains 9,984 named roots and
protects 29,861 trees comprising 29,969 live nodes (23,346,531 bytes), scans
170 blob-reachability nodes and 30,073 values, and preserves the one live
131,127-byte blob. GC plan/apply take 1.296/0.320 seconds and 321/32 SDK
executions. Whole-process peak RSS is 245,334,016 bytes. The runner removes its
isolated container and volume after validation, reducing the full wall time to
508 seconds. This closes the previously unexecutable local 10K GC diagnostic;
it does not close clean repeated, 100K, full-size, or hosted-AWS qualification.

The current write path inherits only committed immutable nodes between client
transactions, pins the required table/global roots in bounded batch reads, and
omits indexed-publication readback only for backends that explicitly guarantee
durable completion. DynamoDB qualifies because a successful write is durably
persisted and the adapter retries `UnprocessedItems` until none remain or
returns an error. Unknown backends retain the conservative readback. In the
same five-sample depth-100 DynamoDB Local history shape, this reduces append
work from 1,704 to 803 mean SDK executions (52.9%), from 6,689,080 to 291,579
mean response bytes (95.6%), and from 7,973,805 to 7,729,455 mean request bytes
(3.1%), with the same 900 transaction actions. Median local append time is
1.522 seconds versus 1.943 seconds, but this is regression evidence rather
than an AWS latency claim. A one-sample depth-1,000 rerun completed in 16.709
seconds and 8,003 SDK executions, down from 35.552 seconds and 22,797
executions; all 1,001 versions, oldest read/diff, and retention checks passed.
At depth 10,000, runner v14 with the new 64-MiB client default passes all six
rows in 246.989 seconds with 101,177 SDK executions, 3.430 GB request bytes,
0.917 GB response bytes, and 184,401,920-byte peak client RSS. Relative to the
earlier pre-optimization format-12 run this is 44.0% less append time, 64.7%
fewer executions, 21.3% fewer request bytes, and 91.2% fewer response bytes;
peak RSS is higher than the former transaction-local-cache result but bounded.
Against the 256-MiB committed-cache diagnostic, the 64-MiB setting cuts peak
RSS 66.2% for 2.4% more executions and 5.8% more one-sample local append time.
The disposable run completes in 320 seconds including a 70-second rebuild.
The subsequent runner-v15 rerun directly records 2,936 entries and 67,104,564
bytes of retained serialized-node weight—4,300 bytes below the manifest-bound
67,108,864-byte ceiling—with no pins. All six rows pass; append takes 279.640
seconds, the complete workload uses 101,004 SDK executions, and peak RSS is
183,484,416 bytes. Request/response totals are 3,560,311,102 and 1,579,639,834
bytes. These one-sample differences are local-emulator variance, not AWS
performance claims.
The 100K gate remains closed on this emulator: the improved 10K result still
projects material duration and storage pressure requiring qualification rather
than extrapolation.
