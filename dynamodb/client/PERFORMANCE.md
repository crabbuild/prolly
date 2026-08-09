# Versioned DynamoDB Performance Envelope

This document states hard limits and the measurement contract. DynamoDB Local
results are regression evidence only; they are not AWS latency, throughput, or
cost claims.

## Enforced limits

| Dimension | Current limit or behavior |
| --- | --- |
| Logical item | DynamoDB-compatible 400 KiB canonical item limit |
| Inline logical value | 64 KiB; larger canonical items use verified chunked blobs |
| Query/Scan evaluated data | 1 MiB per logical page, with DynamoDB limit-before-filter behavior |
| BatchGet | 100 keys and 16 MiB response envelope |
| BatchWrite | 25 requests; separate accepted transitions, not atomic as a batch |
| TransactWrite | 100 logical actions, 4 MiB logical aggregate, and provider root-action preflight |
| Version/diff page | at most 1,000 entries |
| Collected history/diff convenience | at most 10,000 entries |
| One retention apply | at most 80 version removals in one strict transaction |
| Worker page | bounded to the exported worker-page maximum; sequential stream delivery |
| Physical transaction | at most 100 DynamoDB transaction actions |
| Logical conflict retries | 7 after the first attempt by default; configurable 0-63 |
| Decoded-node cache | 64 MiB default retained serialized-node weight; optional simultaneous node-count ceiling; zero disables |

`client.capabilities()` is authoritative for values negotiated from the active
provider and format. A deployment must record it rather than copying this table
into application logic.

Cache limits bound ordinary retained entries, not transient working sets or
correctness pins. A pinned entry may temporarily exceed a configured ceiling,
and one decoded node, operation input, response, blob buffer, or AWS SDK buffer
may itself be material. Peak RSS therefore remains a workload-specific release
measurement rather than a direct synonym for the cache byte limit.
`client.cache_usage()` captures entry/weight totals and pinned portions under
one cache read lock. Runner v15 persists that gauge per sample and validates
ordinary retained weight against the exact manifest-bound ceiling.
A cache-disabled diagnostic records zero entries, bytes, and pins under a
zero-byte ceiling while all history checks pass, confirming that zero is an
enforced mode rather than an undocumented sentinel.

## Amplification model

A logical point read resolves table metadata and a Prolly head, reads a tree
path, and may fetch a blob manifest and chunks. A write prepares a new tree
path, index-source and synchronous index state, commit records, schema/index
pairings, and conditioned roots. Immutable prepublication can leave unreachable
nodes after a root conflict; fenced GC reclaims them later.

Consequently, native DynamoDB request-unit assumptions do not apply directly.
Every benchmark and production dashboard must report, per logical operation:

- latency distribution and logical request/response bytes;
- physical DynamoDB APIs, request counts, bytes, retries, and consumed capacity;
- node and blob reads/writes, tree height, and cache behavior;
- root conflicts, retry count, and orphan/prepublication amplification;
- physical transaction action count and durable versions/commits created;
- process CPU and peak/resident memory.

## Required benchmark matrix

The release benchmark covers cold and warm Get/Put/Update/Delete, Query, Scan,
batches, transactions, history, secondary indexes, large blobs, retention, and
GC. It uses 1 KiB, 16 KiB, 64 KiB, and near-400-KiB items; 10K and 1M item
tables; history depths from 10 through 100K; and both uniform and single-hot-
table writers. Transaction shapes are 1, 10, and 100 actions when the effective
physical action budget permits them.

The existing `benchmarks/dynamodb-scale` harness measures the underlying store
and Prolly engine. `benchmarks/dynamodb-client` exercises the logical facade:
cold/warm CRUD, Query/Scan, bounded batches and transactions, immutable reads,
diff/history, synchronous indexes, verified blobs, restore, retention, fenced
GC, and synchronized 1/4/8-writer contention. Runner v13 makes `full` and
`history` distinct fail-closed workloads, records namespace versus isolated
Docker-volume teardown, and requires a `gc-reachability.csv` artifact. Each GC
row records a checked, history-scaled protected-tree ceiling; the validator
binds it to revision/sample and rejects missing, malformed, duplicate,
over-limit, or empty protected-graph evidence. Raw request counts/bytes,
retries, transaction actions, machine and binary provenance, process CPU, and
peak RSS remain part of every run.
Runner v15 also records cache occupancy per sample. The v2 size and history
matrix coordinators bind the selected cache ceiling into every case and the
aggregate manifest, validate exact aggregate schemas and case ordering, support
fail-closed resume, and tear down runner-owned DynamoDB Local after each case
even when execution fails.

Format 12 removes the former 1,024-snapshot write ceiling. One bounded active
index coordinator is paired with a current-only per-table snapshot catalog.
Each compact catalog locator binds the exact indexed snapshot ID and a
content-addressed one-record tree containing the full immutable historical
manifest. This avoids rewriting full manifests along catalog paths while
preserving exact historical base/index identity. Commit catalog and table-log
named roots retain only their current trees. Ordered strongly consistent root
batches and transaction-pinned root reads remove redundant provider round
trips. A 1,100-write core contract passes and verifies 1,110 exact roots,
including 1,103 immutable version roots.

The latest isolated DynamoDB Local depth-1,000 format-12 history diagnostic
appended 1,000 versions in 35.552 seconds using 22,797 SDK executions,
322,173,160 request bytes, 505,937,748 response bytes, and 9,000 physical
transaction actions, with 46,956,544-byte peak RSS. Format 11 needed 41.423
seconds, 24,520 executions, 383,827,260 request bytes, and 752,415,229 response
bytes. The earlier monolithic layout needed
58,768-59,812 executions, 68.830-103.696 seconds, and
336,330,752-420,921,344-byte peak RSS. At depth 100, the accepted separated-root
format-11 layout's five-sample median append was 1.738 seconds with 1,827.8 mean
SDK executions and 10,662,073 mean request bytes. Format 12 uses 1,704 mean
executions and 7,973,805 mean request bytes with the same 900 transaction
actions; its 1.943-second local median is treated as emulator variance rather
than a latency claim. A combined audit-root experiment reduced latency and
calls but increased request bytes, so it was rejected.

The current implementation further reduces repeated-write amplification
without changing format 12. Transactions inherit only CID-validated nodes from
successful earlier commits; rollback and conflict state is never promoted.
Required global/table roots are pinned through ordered strongly consistent
batch reads. Indexed root readback is omitted only when the physical backend
explicitly guarantees that successful publication durably completed every
submitted immutable write. The DynamoDB adapter opts in because successful
writes are durably persisted and its batch loop retries exactly the returned
`UnprocessedItems` until empty, otherwise failing before root publication;
unknown backends retain readback.
This contract follows AWS's documented guarantees that a successful write
response means the write is durably persisted and that `BatchWriteItem`
returns each unprocessed operation for retry: [DynamoDB read consistency](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html),
[BatchWriteItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html).

On the same depth-100 history shape, five DynamoDB Local samples now use
exactly 803 SDK executions per 100 writes (299 `BatchGetItem`, 300
`BatchWriteItem`, 104 `GetItem`, and 100 `TransactWriteItems`), 7,729,455 mean
request bytes, 291,579 mean response bytes, and the unchanged 900 transaction
actions. Relative to the accepted format-12 baseline this is 52.9% fewer SDK
executions, 3.1% fewer request bytes, and 95.6% fewer response bytes. Median
append latency was 1.522 seconds versus 1.943 seconds, but both are dirty local
regression evidence. A one-sample depth-1,000 rerun passed all exact history
checks in 16.709 seconds with 8,003 executions, 306,624,570 request bytes, and
2,912,685 response bytes, versus 35.552 seconds, 22,797 executions,
322,173,160 request bytes, and 505,937,748 response bytes before this change.
The depth-10,000 runner-v14 rerun with the 64-MiB client default also passes all
six exact rows: append takes 246.989 seconds and 101,177 executions with
3,430,182,872 request bytes, 917,395,395 response bytes, 90,000 transaction
actions, and 184,401,920-byte peak client RSS. Against the earlier
pre-optimization format-12 depth-10,000 run, latency falls 44.0%, executions
64.7%, request bytes 21.3%, and response bytes 91.2%. A separate 256-MiB
committed-cache diagnostic used 98,763 executions and 233.487 seconds but
545,619,968-byte peak RSS. The selected 64-MiB default therefore cuts measured
RSS 66.2% for 2.4% more executions and 5.8% more one-sample local append time.
The cache ceiling measures retained serialized-node weight, not allocator or
process RSS; transient SDK buffers and decoded representations remain part of
the production memory gate. The runner-v15 contract now records that cache
occupancy directly for every sample and rejects unpinned weight above the
manifest-bound ceiling; peak RSS remains an independent required observation.
Disposable-volume teardown and a 70-second rebuild put total wall time at 320
seconds.

A runner-v15 depth-10,000 rerun closes the cache-observation gap: all six rows
pass, and the final single-lock gauge reports 2,936 entries, 67,104,564 retained
serialized-node bytes, and zero pins under the exact 67,108,864-byte manifest
ceiling. Append takes 279.640 seconds; the complete workload uses 101,004 SDK
executions, 3,560,311,102 request bytes, 1,579,639,834 response bytes, and
183,484,416-byte peak RSS. This is direct bound evidence from DynamoDB Local,
not an AWS latency or cost envelope.

Full-workload depth 1,000 format-12 GC completes within the reviewed graph
limits: 984 retained roots, 2,861 protected trees, 2,855 live nodes (2,836,855
bytes), 56 blob-scan nodes, 3,073 scanned values, and one live 131,127-byte
blob. Retention apply required 99 SDK executions; GC plan/apply required 49/23.
Detached manifest trees are retained exactly while their locator is present;
retention contracts prove removed locators remove those trees from the GC
protection set.
This replaces the obsolete 1,000,252-value failure. The per-table append-only
blob registry safely retains every blob introduced by a successful logical
write or import while allowing failed/prepublished orphans to be reclaimed.
It is intentionally conservative: a blob referenced only by removed history
may remain until a future explicitly audited exact registry compaction.

Runner v13 also completes the full depth-10K workload after replacing the
harness's obsolete fixed 10,000-tree ceiling with a checked, recorded 50,000-
tree bound. The plan retains 9,984 named roots, protects 29,861 trees, reaches
29,969 nodes (23,346,531 bytes), scans 170 blob nodes and 30,073 values, and
preserves one 131,127-byte blob. Plan/apply take 1.296/0.320 seconds and 321/32
SDK executions; whole-process peak RSS is 245,334,016 bytes. All 40 exact rows
validate, and runner-owned volume teardown reduces wall time to 508 seconds.
This is a dirty local GC-scale diagnostic, not clean repeated or hosted-AWS
qualification.

These are dirty-worktree DynamoDB Local regression measurements, not hosted-AWS
latency, throughput, capacity, cost, or production-envelope claims. Clean
repeated 10K, 100K, full size, hosted-AWS, and production concurrency matrices
remain open.

A runner-v12 pre-optimization format-12 10K history diagnostic also completed all six
exact operations and cleanup. Append created 10,000 versions in 441.004 seconds
with 286,898 SDK executions, 90,000 transaction actions, 4,356,947,497 request
bytes, 10,392,276,426 response bytes, and 89,948,160-byte peak client RSS.
Compared with format 11, append latency fell 31.5%, executions 1.8%, request
bytes 15.0%, and response bytes 24.3%; peak client RSS increased 4.6% but remains
bounded. Enumeration returned all 10,001 versions in 230.768 milliseconds and
33 calls; oldest read, oldest-to-head diff, and the exact 80-version retention
apply passed. Whole-run time fell from 2,123 to 1,462.62 seconds, with safe
per-item namespace cleanup still dominating after the timed rows. This proves
10K correctness and bounded client memory, not an acceptable production
latency/cost envelope. The newer result above materially improves this path but
does not authorize a 100K local run on the current 31-GiB in-memory emulator:
the remaining duration/storage pressure and the measured 184-MB client RSS
require an explicit qualification envelope rather than extrapolation.

## Current scale decision

Format 12 retains one optimistic head per logical table. This is the simplest
serializable publication boundary and is the only implemented layout. It has
bounded retries. One shared `Client`/`Database` instance admits point and
transaction writes one at a time before speculative tree work, eliminating
avoidable conflicts among its clones. Independently opened clients and other
processes still rely on provider CAS and bounded retries; there is no unmeasured
production write-rate promise.

Do not introduce implicit microbatching: it changes acknowledgement and
conflict semantics. The implemented admission preserves request isolation and
does not change the durable format. Partition-sharded roots require
a new durable format, snapshot manifest, cross-partition transaction design,
history semantics, shadow migration, and atomic cutover.

The decision to retain the single head or migrate is made only after AWS tests
identify the sustained conflict/latency threshold for representative hot-table
workloads. Until those results are published, deployments must load-test their
own peak writer count and treat exceeding the observed envelope as unsupported.
