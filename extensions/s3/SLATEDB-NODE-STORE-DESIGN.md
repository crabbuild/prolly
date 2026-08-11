# SlateDB-backed Prolly node store for the versioned S3 client

> Status: proposed
>
> Scope: adopt SlateDB 0.14.x as an optional, authoritative packed store for
> Prolly nodes while retaining the existing direct-S3 node store as the
> distributed default.
>
> Audience: maintainers implementing the S3 core, AWS-shaped client, storage
> format, migration, qualification, and operations changes.

**Baseline dependency:** SlateDB 0.14.1, the version currently resolved by the
S3 workspace lockfile. A later minor or major version must repeat the store
conformance, crash, dependency, and performance gates in this design.

**Related repository material:**

- [`../../docs/object-store-vcs-design.md`](../../docs/object-store-vcs-design.md)
- [`../../docs/performance.md`](../../docs/performance.md)
- [`../../stores/prolly-store-slatedb/README.md`](../../stores/prolly-store-slatedb/README.md)
- [`QUALIFICATION.md`](QUALIFICATION.md)
- [`OPERATIONS.md`](OPERATIONS.md)

## Decision

Add two explicit repository profiles. Never select one implicitly at runtime.

1. **Distributed direct-S3** remains the default. Every Prolly node is an
   immutable S3 object and independent clients coordinate branch publication
   with S3 compare-and-exchange.
2. **Packed SlateDB single-writer** stores `CID -> node bytes` in one SlateDB
   database backed by the same bucket. It reduces small-object and request
   amplification, but exactly one writer service may own the database path.
   Any number of checkpointed/read-only readers may be deployed.

The first release does not use SlateDB as a distributed branch-ref authority,
does not place user payload chunks in SlateDB, and does not claim that several
independent SDK processes may write one SlateDB database safely.

This is a repository-format feature, not a cache toggle. A repository records
its node-store profile at initialization and clients fail closed when they do
not support it.

## Why adopt SlateDB

The current S3 store maps each 32-byte node CID to one immutable object:

```text
.prolly/v1/nodes/sha256/aa/bb/<full-cid>
```

A one-key versioned write updates the objects, versions, and operations trees,
and a non-empty payload also builds a content chunk-index tree. At scale, a
point mutation rewrites a root-to-leaf path in each affected tree. The current
64 KiB qualification result is 51 object-plane calls for one ordinary write.

SlateDB can pack many immutable Prolly nodes into WAL and compacted SST files:

```text
.prolly/v1/node-store/slatedb-v1/
├── manifest/
├── wal/
├── compactions/
├── compacted/
└── gc/
```

Inside the database, nodes use one logical key family:

```text
node:<32-byte-cid> -> exact serialized Prolly node bytes
```

This primarily improves:

- sustained write throughput through batch/WAL amortization;
- warm point-read latency through SlateDB block and object-store caches;
- physical S3 object count by packing nodes into SSTs;
- request cost for node publication and lookup;
- recovery time from listing millions of individual node objects.

It does not eliminate the S3 objects used for chunks, content manifests,
commits, deltas, refs, reflogs, publication coordination, multipart state, or
GC state. Those remain under the existing canonical layout.

## Constraints discovered in the current implementation

The design must address these existing boundaries:

- `Repository<P>` hardcodes `AsyncProlly<ProllyObjectStore<P>>`.
- `ContentStore<P>` independently constructs the same direct-S3 node store for
  each content chunk-index tree.
- publication leases protect individual physical object paths before a commit
  becomes reachable;
- clone, fetch, repair, fsck, and GC discover nodes through the direct physical
  node layout;
- `format/v1.cbor` is create-once and old clients assume the direct node path;
- the bucket may have native S3 versioning enabled;
- SlateDB provides one fenced writer per database path, not distributed
  multi-writer transactions for unrelated client processes;
- the existing `prolly-store-slatedb` package exposes the synchronous `Store`
  contract by owning a Tokio runtime, while the S3 repository is async-first.

The adoption therefore requires a native asynchronous node-store integration.
The S3 client must not call the synchronous adapter from Tokio workers.

## Goals

- Preserve the current Prolly tree encoding and CIDs exactly.
- Preserve commit IDs, logical version IDs, deltas, refs, and S3-shaped API
  semantics.
- Store state-tree and content-index nodes through the same selected backend.
- Make successful publication mean every referenced node is remotely durable.
- Detect corruption by verifying `sha256(node_bytes) == CID` on every read.
- Keep large user payloads in the existing 8 MiB content-chunk layout.
- Support one million live files and at least ten million retained logical
  versions in the first production capacity tier.
- Provide deterministic clone, migration, repair, fsck, backup, and GC.
- Make writer topology and durability visible in the public configuration and
  physical-layout APIs.
- Preserve the existing direct-S3 repository behavior without performance or
  format regressions.

## Non-goals

- Using SlateDB as the branch/tag CAS authority.
- Supporting several independent writers against one SlateDB path.
- Replacing the Prolly trees with SlateDB's logical key-value model.
- Storing payload chunks, commits, deltas, refs, or reflogs inside SlateDB in
  the first release.
- Transparently converting an existing create-once repository in place.
- Making an unflushed local or in-memory write visible through a branch ref.
- Enabling S3 Express directory buckets; the current provider profile continues
  to require general-purpose or qualified S3-compatible buckets.

## Architecture

### Components

```text
AWS-shaped versioned S3 client
              │
              ▼
      Repository<P, N>
       │            │
       │            └── N: CanonicalNodeStore
       │                  ├── DirectS3NodeStore<P>
       │                  └── SlateDbNodeStore
       │
       ├── ContentStore<P, N>
       │      ├── chunks: direct immutable S3 objects
       │      ├── manifests: direct immutable S3 objects
       │      └── chunk-index nodes: N
       │
       ├── commits/deltas/reflogs: direct immutable S3 objects
       └── refs/workspaces/leases/GC: direct S3 CAS records
```

`N` is selected once from the immutable repository format. State trees and
content chunk-index trees must use the same `N`; otherwise closure traversal,
clone, repair, and GC would require ambiguous per-root routing.

### Canonical node-store trait

Introduce an S3-core-specific trait rather than adding SlateDB concepts to the
general Prolly crate:

```rust,ignore
pub trait CanonicalNodeStore:
    prolly::AsyncStore<Error = Error> + Clone + Send + Sync + 'static
{
    fn descriptor(&self) -> NodeStoreDescriptorV1;

    // A successful return means every preceding successful publication is
    // remotely durable and recoverable by a new process.
    async fn durability_barrier(&self) -> Result<()>;

    // Deterministic, bounded logical-node enumeration for fsck, clone and GC.
    async fn list_node_cids_page(
        &self,
        after: Option<Cid>,
        limit: usize,
    ) -> Result<NodeCidPage>;

    // Logical deletion. Physical reclamation may happen later through
    // compaction and native-version cleanup.
    async fn delete_node_cids(&self, cids: &[Cid]) -> Result<NodeDeleteReport>;

    fn metrics(&self) -> NodeStoreMetricsSnapshot;
}
```

The ordinary `AsyncStore::publish_nodes` method remains the fast publication
entry point. Both implementations must:

- reject keys that are not exactly 32 bytes;
- verify submitted bytes match their CID before writing;
- make duplicate publication idempotent;
- verify bytes on read before returning them;
- preserve ordered batch-read results;
- return success only after all entries in a publication have been accepted;
- report `guarantees_durable_publication() == true` only after the durability
  contract is proven by crash tests.

### Direct-S3 implementation

Rename or wrap the existing `ProllyObjectStore<P>` as
`DirectS3NodeStore<P>`. Its persisted paths and behavior do not change. The new
trait methods use bounded S3 listing and exact physical-version deletion.

This refactor is deliberately behavior-preserving and lands before SlateDB is
wired into the repository.

### SlateDB implementation

Add `SlateDbNodeStore` in the client crate behind a new
`slatedb-node-store` feature. It owns a `slatedb::Db` but does not own a nested
Tokio runtime.

```rust,ignore
pub struct SlateDbNodeStore {
    db: slatedb::Db,
    descriptor: NodeStoreDescriptorV1,
    writer_lease: SlateDbWriterLease,
    metrics: Arc<NodeStoreMetrics>,
}
```

Node publication maps one Prolly `NodePublication` to one SlateDB `WriteBatch`.
The write uses `WriteOptions { await_durable: true, .. }`. A repository may not
write a commit or branch ref until this remote-durability future succeeds.

Hints may use `hint:` keys, but hints are never included in commit identity,
clone closure, fsck correctness, or retention. The packed canonical store does
not use SlateDB `root:` records; bucket commits and S3 refs remain the only
authoritative roots.

### Writer ownership

The packed profile requires a durable, renewable writer lease outside SlateDB:

```text
.prolly/v1/node-store/writer-lease
```

The record uses S3 compare-and-exchange and contains:

```rust,ignore
pub struct SlateDbWriterLeaseV1 {
    repository: RepositoryId,
    writer_id: String,
    generation: u64,
    fencing_token: [u8; 32],
    expires_at_millis: u64,
    updated_at_millis: u64,
}
```

Rules:

- a writer acquires the lease before opening SlateDB writable;
- it renews before half the lease interval has elapsed;
- every publication verifies that its fencing token is still current;
- lease loss immediately disables new writes and branch publication;
- reopening after expiry acquires a higher generation and lets SlateDB's own
  manifest fencing reject a stale engine instance;
- read-only clients use `DbReader` and never acquire this lease;
- the lease does not make SlateDB multi-writer; it makes the single-writer
  constraint explicit and fail-closed.

Applications that need many independent writers keep the direct-S3 profile or
put a single writer service in front of the packed profile.

## Repository format

### Descriptor

Add an immutable node-store descriptor to the repository format:

```rust,ignore
pub enum NodeStoreDescriptorV1 {
    DirectS3 {
        layout_version: u16,
    },
    SlateDbPacked {
        layout_version: u16,
        relative_path: String,
        key_schema: u16,
        writer_topology: WriterTopologyV1,
    },
}

pub enum WriterTopologyV1 {
    SingleWriter,
}
```

Direct S3 is the default value when the field is absent so existing v1 fixtures
remain readable. SlateDB repositories set:

```text
required_capability_profile = 2
min_reader_version = 2
min_writer_version = 2
node_store = SlateDbPacked {
    layout_version: 1,
    relative_path: "node-store/slatedb-v1",
    key_schema: 1,
    writer_topology: SingleWriter,
}
```

The path is relative to the repository prefix and must reject `..`, empty path
segments, leading slashes, control bytes, and any collision with another
canonical family.

Old clients see the higher minimum reader/writer version and fail before any
physical write. New clients opening an old repository select direct S3.

### Why an in-place flip is forbidden

`format/v1.cbor` is create-once. Replacing its node-store descriptor could make
one client read direct node objects while another reads SlateDB SSTs. Therefore:

- existing repositories never switch their descriptor in place;
- migration creates a new repository prefix or bucket;
- a successful migration copies the complete reachable logical history and
  then applications change their configured prefix;
- direct nodes may be removed only after the rollback window expires and the
  migrated repository independently passes fsck.

## Physical layout

The packed profile adds these families:

| Relative path | Discipline | Portable clone | GC owner |
| --- | --- | --- | --- |
| `node-store/slatedb-v1/manifest/**` | SlateDB managed | No raw copy | SlateDB |
| `node-store/slatedb-v1/wal/**` | SlateDB managed | No raw copy | SlateDB |
| `node-store/slatedb-v1/compactions/**` | SlateDB managed | No raw copy | SlateDB |
| `node-store/slatedb-v1/compacted/**` | SlateDB managed | No raw copy | SlateDB |
| `node-store/slatedb-v1/gc/**` | SlateDB managed | No raw copy | SlateDB |
| `node-store/writer-lease` | Mutable CAS | No | repository |
| `node-store/gc-fence` | Mutable CAS | No | repository |

Portable clone operates on logical node CIDs and bytes, not on SlateDB's
physical WAL/SST files. Physical backup and restore must copy every physical
version under the SlateDB path as one consistent backup unit.

## Write protocol

For a one-object put in the packed profile:

```text
Client
  ├─► Payload S3: upload immutable chunks
  ├─► SlateDB: publish content-index nodes and await remote durability
  ├─► Payload S3: write the content manifest
  ├─► SlateDB: publish state-tree nodes and await remote durability
  ├─► Canonical S3: write delta, commit, and reflog
  ├─► Branch ref: compare-and-exchange the expected head
  └─◄ Return or reconcile the commit receipt
```

The mandatory ordering is:

1. Persist and verify payload chunks.
2. Publish the content chunk-index nodes and await remote durability.
3. Persist the content manifest that names the durable chunk-index root.
4. Publish every new state-tree node and await remote durability.
5. Store delta, commit, and reflog immutable objects.
6. Verify the writer lease and GC epoch have not changed.
7. Compare-and-exchange the branch ref.
8. Reconcile an ambiguous CAS outcome using the operation record and ref.

A ref must never point to a commit whose nodes are only in memory.

### Retries

If branch CAS loses:

- already durable node keys remain harmless immutable orphans;
- reload the winning head;
- rebuild affected paths;
- publish the new node batch durably;
- retry using the same operation ID and input digest;
- let later reachability GC remove abandoned nodes.

No retry may reuse an operation ID with different logical input.

### Batch commits

`commit_workspace` currently calls `engine.put` once per mutation. The SlateDB
store will pack each individual node publication, but this still constructs and
stores intermediate Prolly paths. A separate optimization must use Prolly's
sorted batch mutation/builder so one workspace publishes only its final node
set.

The adoption is considered performance-complete only after workspace commits
use a bounded bulk path for sorted mutations. SlateDB alone is not a substitute
for that engine improvement.

## Read protocol

1. Resolve the branch/tag/commit through canonical S3 records.
2. Read the selected root CID from the commit.
3. Query `node:<cid>` through `SlateDbNodeStore`.
4. Verify SHA-256 before decoding the node.
5. Use native ordered batch reads for siblings or proof traversal.
6. Read the content manifest and raw chunks directly from S3.

Reader modes:

- the writer process reads through its `Db` and sees the latest remotely
  durable state;
- replicas open `DbReader` at an explicit checkpoint when reading a fixed
  commit closure;
- ordinary readers may poll the latest manifest, but a missing referenced CID
  must trigger refresh-and-retry before reporting `MissingClosure`;
- caches may return bytes only after hash verification.

The implementation must expose warm/cold and memory/disk/object-store cache hit
metrics separately.

## Correctness invariants

1. **Hash identity:** a node is accepted or returned only when its bytes hash to
   its CID.
2. **Durable-before-visible:** branch refs never expose non-durable nodes.
3. **Single authority:** S3 commits and refs select roots; SlateDB contains node
   bytes but no competing branch-head authority.
4. **Immutable semantics:** publishing the same CID is idempotent; different
   bytes for an existing CID are corruption.
5. **Fail-closed format:** a client that cannot open the configured node store
   performs no repository write.
6. **Writer fencing:** a stale or expired packed-store writer cannot publish
   nodes or move refs.
7. **Snapshot stability:** a historical commit always resolves the same logical
   node closure until retention explicitly releases it.
8. **Cache independence:** optional local caches may be deleted without changing
   repository correctness.
9. **Bounded maintenance:** scans, clone, fsck, and GC use pages and restartable
   checkpoints rather than materializing every CID.
10. **Payload separation:** user payload bytes never enter SlateDB leaf values.

## Failure handling

| Failure point | Required result |
| --- | --- |
| Before node batch is durable | No ref movement; retry is safe |
| After nodes durable, before commit write | Unreachable nodes; later GC eligible |
| After commit write, before ref CAS | Unreachable commit and nodes; retry/reconcile |
| Ref CAS accepted, response lost | Operation reconciliation returns the committed receipt |
| Writer lease expires during build | Abort before ref CAS; durable work remains unreachable |
| SlateDB manifest fencing rejects writer | Close writer, reacquire repository writer lease, reopen, reconcile |
| Reader cannot find a referenced CID | Refresh manifest/checkpoint once, then report `MissingClosure` |
| CID returns corrupt bytes | Quarantine cache entry if applicable and report `CorruptNode`; never overwrite in place |
| Compactor stops | Reads/writes continue until configured L0 backpressure; alert on debt |
| Bucket versioning retains deleted SST | Logical deletion succeeds; native-version sweeper/lifecycle reclaims later |

The existing accepted-CAS outage and cancellation matrices must run against
both node-store profiles.

## Garbage collection and retention

SlateDB compaction does not know Prolly reachability. Since every Prolly node
has a distinct immutable CID, compaction cannot infer that an old node is dead.

GC remains repository-driven:

1. Acquire a repository-wide SlateDB GC fence by CAS.
2. Reject new writer-lease acquisition and wait for the current writer to
   acknowledge the fence at a safe boundary.
3. Discover retained commits from branches, tags, reflogs, pins, workspaces,
   multipart uploads, sync runs, and publication records.
4. Traverse every retained tree root through the selected node store.
5. Produce a deterministic sorted live-CID set or partitioned mark files.
6. Scan `node:` keys in bounded pages.
7. Write delete tombstones for unreachable CIDs older than the configured grace
   boundary.
8. Checkpoint progress after every page.
9. Release the GC fence.
10. Let SlateDB compaction reclaim tombstoned rows.
11. Reclaim noncurrent S3 versions of obsolete SlateDB objects through an
    exact-version sweeper or an explicitly qualified prefix lifecycle rule.

The first packed-store release must not enable node deletion until this fence
protocol and crash recovery pass. Append-only node retention is the safe
intermediate behavior.

### Native S3 versioning policy

The repository currently rejects lifecycle rules that can delete canonical
history unexpectedly. Packed mode needs a narrower policy distinction:

- commits, deltas, chunks, manifests, refs, and reflogs retain the existing
  repository policy;
- `node-store/slatedb-v1/` may use a repository-qualified noncurrent-version
  expiration because SlateDB owns physical SST lifecycle;
- no current SlateDB object may be expired by bucket lifecycle;
- the backup rollback window must be longer than the noncurrent-version grace;
- provider attestation records the accepted prefix-specific policy digest.

Until prefix-policy qualification exists, use the exact-version maintenance
sweeper and leave provider lifecycle disabled.

## Clone, fetch, repair, and backup

### Logical clone and fetch

Clone and fetch remain format-independent at the commit/tree level:

1. walk the selected commit closure;
2. read each source CID through the source `CanonicalNodeStore`;
3. verify the bytes;
4. batch-publish missing CIDs through the destination store;
5. await destination remote durability;
6. copy immutable non-node objects;
7. move the destination ref through expected-head CAS.

This supports direct-S3 to SlateDB, SlateDB to direct-S3, and SlateDB to
SlateDB transfer without copying internal SST files.

### Repair

Repair copies only a missing CID from a qualified, identity-matching source.
Corrupt-present entries are never overwritten. Repair must write all missing
nodes durably before a repaired commit is declared healthy.

### Physical backup

Physical backup of a packed store requires a SlateDB checkpoint plus a complete
physical-version inventory of every object reachable from that checkpoint.
Copying only current SSTs without the matching manifest/checkpoint is invalid.

Restore opens the copied database read-only, verifies the checkpoint, traverses
all repository roots, then enables writer lease acquisition.

## Migration

### Supported migration

Migration is clone-to-new-prefix:

```text
.prolly/v1/                 existing direct-S3 repository
.prolly-packed/v1/          new SlateDB-packed repository
```

Steps:

1. Initialize the destination with the packed capability profile and the same
   logical repository identity through an explicit migration initializer.
2. Pin the source branch/tag set at a migration checkpoint.
3. Traverse and publish all reachable nodes into destination SlateDB.
4. Copy chunks, manifests, commits, deltas, and retained reflogs.
5. Await the SlateDB durability barrier.
6. Create destination refs with destination-local reflogs.
7. Run repository-wide fsck from both source and destination.
8. Enter dual-read verification; writes continue only on the source.
9. Stop source writers, copy the final incremental closure, and fsck again.
10. Change the application repository prefix and enable the packed writer.
11. Keep the source prefix read-only through the rollback window.

If packed-only commits are created after cutover, rollback is a reverse
packed-to-direct incremental migration: freeze the packed writer, copy and
verify the new closure into the direct repository, advance its refs through
expected-head CAS, and only then redirect clients. Pointing clients at the
stale source prefix would lose visible history and is forbidden.

Commit and root CIDs remain unchanged because their logical bytes do not change.
Repository initialization and destination-local reflog records are the only
expected physical differences.

### Unsupported migration

- rewriting `format/v1.cbor` in place;
- copying SlateDB SST objects without a checkpoint;
- opening one database path writable from source and destination processes;
- deleting direct nodes before packed-store fsck and rollback expiry;
- silently falling back to direct S3 when the SlateDB profile cannot open.

## Public configuration and API

Initialization makes topology explicit:

```rust,ignore
let client = VersionedS3Client::builder()
    .aws_client(aws)
    .bucket("archive")
    .repository_prefix(".prolly-packed/v1")
    .node_store(NodeStoreConfiguration::SlateDbPacked(
        SlateDbNodeStoreOptions::builder()
            .writer_id("ingest-primary")
            .local_cache_path("/var/cache/prolly/archive")
            .local_cache_bytes(20 * GIB)
            .memtable_bytes(512 * MIB)
            .block_cache_bytes(1024 * MIB)
            .l0_sst_bytes(256 * MIB)
            .max_unflushed_bytes(2 * GIB)
            .compression(Compression::Zstd)
            .build()?,
    ))
    .initialize()
    .await?;
```

Opening an existing repository normally derives the storage profile from the
format. Caller options tune local resources but cannot override the canonical
profile:

```rust,ignore
let client = VersionedS3Client::builder()
    .aws_client(aws)
    .bucket("archive")
    .repository_prefix(".prolly-packed/v1")
    .slatedb_runtime(SlateDbRuntimeOptions::reader(...))
    .open()
    .await?;
```

Expose:

```rust,ignore
client.node_store_profile() -> NodeStoreProfile
client.node_store_status().await -> Result<NodeStoreStatus>
client.node_store_metrics() -> NodeStoreMetricsSnapshot
client.flush_node_store().await -> Result<()>       // writer only
client.create_node_checkpoint().await -> Result<NodeCheckpoint>
client.verify_node_checkpoint(checkpoint).await -> Result<VerificationReport>
```

`physical_layout()` reports the SlateDB-managed families and states that their
internal paths are not stable application APIs.

### Error contract

Packed mode uses the existing structured repository error envelope and adds
specific classifications where callers need different recovery behavior:

| Condition | Public classification | Retry guidance |
| --- | --- | --- |
| Client does not understand the descriptor | `UnsupportedRepositoryFormat` | Upgrade client; do not retry |
| Another packed writer owns the lease | `WriterLeaseConflict` | Retry after lease expiry or contact owner |
| Current writer was fenced | `WriterFenced` | Stop writes, reopen, then reconcile |
| SlateDB or object store temporarily unavailable | `NodeStoreUnavailable` | Retry same operation ID |
| L0/memory backpressure deadline exceeded | `SlowDown` | Back off and retry same operation ID |
| Referenced CID absent after refresh | `MissingClosure` | Repair from a qualified source |
| Bytes do not hash to requested CID | `CorruptNode` | Quarantine caches; do not overwrite canonical entry |
| Remote durability deadline exceeded | `OutcomeUnknown` | Reopen and reconcile before retrying publication |

The AWS-shaped adapter maps only errors that have a faithful S3 analogue.
Writer fencing and unsupported format remain structured adapter errors rather
than being disguised as `NoSuchKey`.

## Observability

Metrics must distinguish logical repository work from SlateDB and provider
work:

- logical node gets, hits, misses, batches, puts, deletes, and bytes;
- nodes per publication and duplicate-CID suppression;
- memory-cache, local-disk-cache, and remote block hit rates;
- WAL batches, WAL bytes, durable-wait latency, and flush latency;
- manifest polls and refresh latency;
- L0 SST count, sorted-run count, compaction backlog, bytes read/written, and
  write amplification;
- SlateDB object-store GET/HEAD/PUT/LIST/DELETE calls and retries;
- writer lease generation, remaining TTL, renew failures, and fencing events;
- GC scanned/live/deleted CIDs, tombstone bytes, compaction reclaim bytes, and
  native-version reclaim bytes;
- complete S3-shaped operation latency and request count outside SlateDB.

Every benchmark row records repository profile, SlateDB version, object-store
provider, bucket versioning, compression, cache sizes, commit width, working-set
size, cold/warm state, and whether durability was awaited.

## Performance and capacity targets

The targets below are release gates, not promises derived from SlateDB's public
benchmarks.

### One-million-file qualification tier

Assume one million unique live keys, ten million logical versions, metadata
values under 1 KiB, payload chunks stored outside SlateDB, and a same-region
general-purpose S3 bucket.

| Area | Initial target |
| --- | --- |
| Live Prolly state | at least 1 million objects across three state trees |
| Retained history | at least 10 million object versions |
| Node value size | verify full range through the 16 MiB Prolly hard maximum |
| Warm point read | p95 <= 5 ms from process/local cache |
| Cold point read | p95 <= 200 ms in the qualification region |
| One-writer commit | p95 <= current direct-S3 baseline |
| 100-key atomic commit | at least 5x files/second over 100 separate direct commits |
| Bulk import | at least 10,000 files/second excluding payload upload |
| Node-store provider calls | at least 70% fewer per 100-key commit |
| Metadata storage amplification | <= 2.5x after compaction quiesces |
| Temporary compaction headroom | <= 2x the pre-compaction physical bytes |
| Restart | writable reopen <= 30 seconds at the 1M/10M tier |
| Reader recovery | checkpointed read-only open <= 10 seconds |
| Memory | configurable and stable below 4 GiB for the qualification profile |
| Local disk cache | optional; qualification profile uses 20-50 GiB |

The bulk target requires the sorted Prolly builder. It is not an acceptance
criterion for a naive loop of one million ordinary `PutObject` calls.

### Resource starting point

For the first 1M-file deployment:

- 4 vCPU and 2-4 GiB memory for the writer/compactor process;
- 512 MiB memtable budget;
- 1 GiB memory block/meta cache;
- 20 GiB local object-store cache, expandable to 50 GiB;
- 128-256 MiB L0 SST target;
- 1-2 GiB maximum unflushed data;
- four compaction workers initially;
- object storage sized for payload plus at least 100 GiB of metadata/history
  and 2x temporary metadata headroom until measurements replace this budget.

Backpressure must trigger before memory or L0 limits are exceeded. The client
returns a retryable `SlowDown`-class error with telemetry rather than allowing
unbounded memory growth.

## Security and operations

- Scope the writer role to the repository prefix and SlateDB sub-prefix.
- Scope read-only replicas without ref, lease, WAL, manifest, or compaction
  write permissions.
- Encrypt caches and object storage according to the existing repository
  policy; benchmark KMS request amplification separately.
- Do not include credentials, keys, payloads, or node bytes in metrics/logs.
- Pin SlateDB and `object_store` dependency versions in release evidence.
- Run the dependency advisory gate for the all-feature client.
- Back up the format descriptor, writer-lease policy, checkpoint, provider
  attestation, bucket lifecycle, and native-version inventory together.
- Alert before writer-lease expiry, on manifest fencing, sustained L0 growth,
  compaction failure, cache corruption, missing closure, or durability waits
  above the configured deadline.

## Phased execution plan

Each phase has an independent rollback boundary. Do not enable packed mode for
production data until Phase 8 completes.

### Phase 0: measurement contract and architecture decision

#### Context

The existing 51-call write result measures the whole S3-shaped operation, not
only Prolly-node traffic. Without a node-specific baseline, adoption could add
compaction complexity while failing to improve the dominant cost.

#### Work

- Add node-store metrics around direct-S3 `get`, `publish_nodes`, batch reads,
  bytes, latency, and provider operations.
- Run 1-, 100-, 10,000-, and 1,000,000-key state-tree workloads.
- Measure one-file commits, 100-key workspaces, sorted bulk builds, random
  updates, historical reads, and fsck.
- Record cold and warm results and native S3 object/version counts.
- Freeze this document as an ADR after reviewing the single-writer decision.

#### Acceptance criteria

- Checked-in machine-readable baseline covers all listed workloads.
- Whole-operation and node-store request counts reconcile exactly.
- The report identifies the proportion of commit latency and cost attributable
  to nodes.
- Maintainers approve direct S3 as the distributed default and SlateDB as an
  explicit single-writer profile.

#### Rollback

Metrics are additive. Remove no direct-S3 behavior.

### Phase 1: injectable canonical node-store abstraction

#### Context

Repository and content code currently construct `ProllyObjectStore<P>`
directly. SlateDB cannot be introduced safely while those paths can diverge.

#### Work

- Add `CanonicalNodeStore` and bounded CID-page types.
- Refactor `Repository<P>` and `ContentStore<P>` to share one injected store.
- Adapt the existing direct-S3 implementation without changing paths.
- Route state trees, content-index trees, fsck, clone, repair, and GC through
  the abstraction.
- Preserve compile-time monomorphization or use one internal enum; do not put a
  non-object-safe async trait on the public API accidentally.

#### Acceptance criteria

- All existing S3 unit, contract, RustFS, fault, qualification, and fixture
  tests pass unchanged in direct mode.
- Canonical physical snapshots before and after the refactor are byte-identical.
- The 64 KiB cost matrix remains within 2% latency and exactly equal request
  count/storage growth under controlled local conditions.
- One store instance is demonstrably shared by state and content-index trees.
- No SlateDB dependency enters `prolly-s3-core`.

#### Rollback

Revert the abstraction; the persisted format is unchanged.

### Phase 2: experimental async SlateDB node store

#### Context

The existing synchronous adapter owns a Tokio runtime and is unsuitable inside
the async S3 client. This phase proves the raw `CID -> bytes` contract before
changing repository formats.

#### Work

- Add the `slatedb-node-store` client feature.
- Implement native async get, ordered batch get, publish, durable wait, scan,
  logical delete, close, checkpoint, and metrics.
- Use one SlateDB `WriteBatch` per Prolly publication.
- Verify CID/bytes on every boundary.
- Add in-memory object-store tests and isolated RustFS tests under a temporary
  noncanonical prefix.
- Add writer ownership records and fencing tests.

#### Acceptance criteria

- The complete async store conformance suite passes.
- Kill-after-every-write-boundary tests never acknowledge a node that is absent
  after reopen.
- Duplicate CID publication is idempotent and corrupt bytes are rejected.
- Batch results preserve input order, including misses and duplicate requests.
- A second writer cannot open or publish to the same path.
- Multiple read-only readers can open and verify a checkpoint.
- Dependency, MSRV, Clippy, and clean-downstream gates pass with and without the
  feature.

#### Rollback

Disable/remove the feature. No canonical repository uses it yet.

### Phase 3: repository format and public API

#### Context

Old clients derive direct node paths. Packed repositories must prevent those
clients from opening rather than failing later with missing nodes.

#### Work

- Add `NodeStoreDescriptorV1` with the absent-field direct-S3 default.
- Introduce capability profile 2 and reader/writer version 2.
- Add golden fixtures for both profiles and independent codec verification.
- Add builder initialization options and format-derived open behavior.
- Extend `physical_layout`, compatibility manifest, error taxonomy, and
  documentation.

#### Acceptance criteria

- Existing v1 fixtures remain byte-identical and readable.
- A legacy client rejects the packed fixture before making a physical write.
- A new client opens old direct repositories without an override.
- Conflicting caller options cannot override the persisted profile.
- Invalid or colliding SlateDB paths are rejected deterministically.
- Initialization races converge on one identical descriptor.

#### Rollback

Packed repositories are development-only. Direct repositories remain on the
old profile and require no rollback.

### Phase 4: repository publication and recovery

#### Context

The correctness boundary is remote node durability before immutable commit and
ref publication. Existing physical-path protection cannot represent nodes
inside SST files directly.

#### Work

- Wire the packed store into state and content-index engines.
- Enforce durability barriers before commit storage and ref CAS.
- Add writer-lease verification and GC-epoch checks at publication boundaries.
- Initially retain all node keys; do not enable packed-node deletion.
- Port ordinary put/delete/copy, multi-delete, workspace, multipart completion,
  merge, restore, reset, fetch, and push.
- Extend operation reconciliation for SlateDB manifest refresh/reopen.

#### Acceptance criteria

- Exhaustive prewrite fault matrices pass for both profiles.
- Accepted-ref/lost-response tests return exactly one logical commit.
- Cancellation after node durability and before/after ref CAS is recoverable.
- No injected crash yields a ref whose reachable node is absent after a fresh
  process opens the database.
- Lease expiry or fencing prevents ref movement.
- Repository-wide fsck passes after every successful workflow and restart.

#### Rollback

Before packed-only commits exist, stop packed writers and switch applications
back to the untouched source direct repository. After packed-only commits
exist, perform the reverse logical sync described in the migration section
before switching. Never reinterpret the packed prefix as direct mode.

### Phase 5: batch path, reads, and performance

#### Context

SlateDB packs storage operations, but the workspace loop still persists
intermediate Prolly paths. The performance goal requires final-node batch
construction and native batch reads.

#### Work

- Add sorted/bounded workspace mutation building for objects and versions.
- Publish one final node batch per affected tree where feasible.
- Implement native parallel/batched SlateDB reads and shared immutable buffers.
- Configure memory, metadata, block, and local object-store caches.
- Add cold/warm benchmark modes and cache-loss drills.
- Tune WAL flush interval, L0 size, backpressure, compression, and compaction.

#### Acceptance criteria

- The 100-key commit meets the 5x files/second target.
- Node-store provider operations fall at least 70% versus direct S3 for the
  same 100-key logical commit.
- Warm point reads meet p95 <= 5 ms in the qualification environment.
- Cold reads meet p95 <= 200 ms without returning stale data.
- Compaction reaches steady state during a six-hour mixed workload.
- Memory remains below the configured 4 GiB ceiling.
- Deleting every local cache leaves canonical reads and fsck correct.

#### Rollback

Disable bulk and cache tuning independently; the packed format remains
readable through the conservative general paths.

### Phase 6: reachability GC and native-version reclamation

#### Context

Logical node deletion and physical SST reclamation are separate. Native S3
versioning can retain deleted SlateDB objects after logical GC.

#### Work

- Implement the writer/GC fence and restartable logical mark/sweep.
- Add bounded `node:` scans and delete batches.
- Integrate SlateDB compaction completion and reclaim metrics.
- Add exact physical-version cleanup for obsolete internal SlateDB objects.
- Qualify optional prefix-specific noncurrent-version lifecycle policy.
- Update backup retention and provider-attestation schemas.

#### Acceptance criteria

- Live nodes, active publications, workspaces, uploads, pins, refs, tags, and
  retained reflogs survive every GC race fixture.
- Known unreachable nodes disappear logically and physically after compaction
  and native-version cleanup.
- Interrupted mark, sweep, compaction, and exact-version cleanup resume safely.
- Publication either observes the GC fence or holds a writer epoch that GC
  recognizes; there is no unprotected interval.
- Storage returns within 10% of the expected compacted footprint after the
  test's rollback window.
- Provider lifecycle cannot delete a current or logically retained object.

#### Rollback

Disable deletion and retain nodes indefinitely. This consumes storage but
preserves correctness.

### Phase 7: migration, clone, repair, and physical backup

#### Context

Operational adoption requires a reversible path from existing repositories and
format-independent data movement.

#### Work

- Implement direct-to-packed and packed-to-direct logical clone.
- Add resumable migration checkpoints and final incremental cutover.
- Generalize missing-node repair to both stores.
- Add packed-store checkpointed physical backup and restore.
- Extend fsck to compare logical closure independently of physical layout.

#### Acceptance criteria

- A one-million-file direct repository migrates without changing commit IDs,
  tree roots, object version IDs, payloads, or historical reads.
- Source and destination produce equivalent branch/tag/log/list/diff results.
- Migration resumes after process and provider restarts without duplicate refs.
- Missing-node repair works in both directions; corrupt-present nodes are never
  overwritten.
- Physical restore opens at the recorded checkpoint and passes full fsck before
  writer lease acquisition.
- Reverse packed-to-direct synchronization and rollback to the source
  repository are documented and rehearsed without losing packed-only commits.

#### Rollback

Point applications back to the retained source prefix. Destroy the destination
only through exact-version cleanup after incident evidence is captured.

### Phase 8: production qualification and release

#### Context

The packed profile changes durability, concurrency, backup, and GC. Unit tests
and a short local benchmark do not establish production readiness.

#### Work

- Run the 1M-live/10M-version capacity tier on RustFS and AWS S3.
- Run 24-hour ingest/read/history/GC/compaction soak.
- Run the complete active-outage, credential-rotation, backup/restore, and
  dependency/release-evidence suites.
- Test KMS and non-KMS buckets separately.
- Publish an operator runbook, dashboards, alerts, sizing worksheet, and cost
  model.
- Sign the exact core/client packages, lockfile, format fixtures, provider
  attestations, and qualification results.

#### Acceptance criteria

- Every performance and capacity target in this document passes or is revised
  through an explicit reviewed decision with evidence.
- The 24-hour run has no missing closure, corrupt node, unbounded L0 growth,
  unreconciled operation, stale-writer publication, or unexplained physical
  version.
- AWS qualification confirms request mix, transmitted retries, latency,
  storage amplification, compaction cost, and KMS cost.
- Restore and direct-profile rollback rehearsals both complete successfully.
- Documentation states the single-writer requirement at every packed-mode
  configuration entry point.
- Release evidence is operator-key signed; local ephemeral signatures are not
  accepted.

#### Rollback

Keep packed mode feature-gated and unsupported for production. Direct S3
remains the supported distributed profile.

## Open decisions that must be resolved in Phase 0

1. Whether the first packed deployment is an embedded single writer or a
   dedicated writer service. The storage contract is the same, but service
   ownership changes failure recovery and SDK deployment guidance.
2. Whether the writer lease reuses publication lease infrastructure or receives
   a separate schema and renewal loop. A separate schema is recommended because
   its lifetime and scope are database-wide.
3. Whether physical native-version reclamation is implemented by the existing
   exact-version GC engine or a dedicated SlateDB namespace sweeper.
4. Whether Zstd, LZ4, or no block compression is the default. Choose from the
   target workload benchmark, not generic compression ratios.
5. Whether the direct-to-packed migration preserves the repository ID. It is
   recommended for an administrative storage migration, but the initializer
   must require an authenticated source proof to prevent identity injection.

None of these decisions permits relaxing durable-before-visible publication or
the single-writer topology.

## Alternatives considered

### Replace the three Prolly state trees with ordinary SlateDB rows

Rejected. It would remove deterministic Prolly roots, change commit identity,
and make diff, merge, proofs, clone, and historical closure depend on SlateDB's
internal snapshots. At that point the system would be a SlateDB-backed object
catalog rather than the designed Prolly versioned repository.

### Use SlateDB only as a disposable node cache

Retained as the recommended distributed-mode enhancement. It improves warm
reads and derived scans without changing authority, but it does not reduce
canonical node PUTs or the number of direct S3 node objects. It therefore does
not satisfy the packed-store cost and object-count goals on its own.

### Open one SlateDB database from every SDK client

Rejected. SlateDB fences to one writer per database path. An S3 CAS lease makes
ownership explicit but does not turn the database into a distributed
multi-writer engine.

### Use one SlateDB database per branch

Rejected for the initial release. It loses cross-branch node deduplication,
complicates merge and clone, and makes commits that reference shared history
depend on several database lifecycles.

### Shard nodes by CID prefix across several SlateDB databases

Deferred. It can increase writer throughput but a commit would span several
independent manifests and durability barriers. It is unnecessary for the
one-million-file tier and should be considered only after a single database is
measured as the bottleneck.

### Store payload chunks in SlateDB

Rejected. Large immutable chunks already match S3's strengths. Packing them
would increase compaction bandwidth, cache pollution, and recovery scope while
removing efficient direct range access.

## External references

- [SlateDB crate documentation](https://docs.rs/slatedb/0.14.1/slatedb/)
- [SlateDB architecture overview](https://slatedb.io/docs/design/overview/)
- [SlateDB physical files](https://slatedb.io/docs/design/files/)
- [SlateDB performance tuning](https://slatedb.io/docs/operations/tuning/)
- [SlateDB compaction](https://slatedb.io/docs/design/compaction/)
- [SlateDB FAQ and durability behavior](https://slatedb.io/docs/get-started/faq/)

## Completion definition

SlateDB adoption is complete only when:

- direct S3 remains byte-for-byte compatible;
- packed mode is explicit in the immutable format and public API;
- every referenced node is remotely durable before ref visibility;
- writer fencing, crash recovery, clone, repair, fsck, GC, backup, restore, and
  migration are proven against a natively versioned bucket;
- the 1M-live/10M-version tier meets its bounded performance and resource gates;
- full operation-level S3 cost is measured, not inferred only from SlateDB
  logical calls;
- the operator documentation makes the packed profile's single-writer limit
  impossible to overlook.
