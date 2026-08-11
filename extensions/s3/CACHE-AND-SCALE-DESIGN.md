# Prolly S3 cache and unbounded-cardinality design

Status: proposed scale architecture. This design is not part of the frozen
Prolly S3 Protocol v1 and is not implemented yet.

## Decision

Prolly S3 will support **unbounded repository cardinality**: the protocol will
not impose a fixed maximum number of files, commits, or refs. Every foreground
operation must nevertheless have bounded memory, bounded concurrency, and a
resumable work budget.

“Unlimited” cannot mean infinite storage, zero-cost global queries, or infinite
history traversed in one request. Capacity remains constrained by object-store
quotas, account throughput, retention cost, identifier space, and elapsed time.
The useful promise is that growing the repository does not require loading or
rewriting all repository metadata and does not eventually hit a format-level
cardinality ceiling.

![Cache and scale architecture](diagram/prolly-s3-cache-scale-architecture.svg)

## Goals

- Support billions of live file keys and substantially more retained versions.
- Support continuously growing commit history and ref namespaces.
- Keep repository open independent of repository cardinality.
- Make exact key lookup logarithmic in the Prolly tree size and independent of
  total commit count.
- Bound RAM, local disk, S3 calls, concurrency, and CPU for every request.
- Keep the existing three-call foreground write path.
- Let caches fail open without changing repository correctness.
- Make scans, history walks, indexing, and garbage collection resumable.

## Non-goals

- A single response containing every key, commit, or ref.
- Constant-time arbitrary history or global aggregation queries.
- Unbounded throughput on one hot branch. Publication for a branch is still a
  serialized, fenced compare-and-exchange.
- Treating a local cache or derived index as authoritative state.
- Claiming billion-object readiness before the qualification gates pass.

## Scale model

The logical object and version maps remain Prolly trees. Their immutable nodes
are addressed by content ID (CID), so one verified node can be reused across
snapshots and safely cached. With a representative fanout near 128, one billion
entries needs about five index levels; the exact height depends on encoded key
sizes and chunk boundaries.

Repository-wide work is divided into four independent dimensions:

| Dimension | Authoritative representation | Scale strategy |
|---|---|---|
| Files and logical versions | Immutable Prolly trees | Logarithmic lookup; streamed range scans |
| Commits | Immutable commit envelopes | Direct ID lookup; segmented skip index for graph walks |
| Refs | One conditional object per ref | Direct lookup; derived sharded catalog for listing |
| Node locations | Immutable index pages | Lazy prefix-sharded lookup; generation-aware cache |

No healthy foreground lookup uses `ListObjects`, loads a complete checkpoint,
or scans commit envelopes.

## Read path and cache hierarchy

The core crate defines an asynchronous `NodeCache` interface. A client adapter
may implement it with [Foyer](https://github.com/foyer-rs/foyer) as a hybrid
memory-and-disk cache. The repository engine remains usable without Foyer.

```text
pending write set
    -> L1 memory cache
    -> L2 local-disk cache
    -> node-location cache
    -> sharded node index
    -> ranged GET from immutable commit envelope
    -> verify CID and SHA-256
    -> admit to L1/L2
```

Concurrent misses for the same cache key are singleflighted. A cold lookup
therefore fetches each distinct node at most once per process at a time; a warm
lookup performs no metadata-node S3 reads. Fetching the file body remains one
version-qualified S3 `GetObject`.

### Cache keys

Node bytes use this logical cache key:

```text
(repository_namespace, protocol_version, tree_format_digest, cid)
```

The repository namespace prevents cross-tenant disclosure. Protocol and tree
format prevent an old decoder from consuming incompatible bytes. A deployment
may deliberately share verified nodes across repositories only when its trust
and encryption policy permits it.

Node-location entries use:

```text
(repository_namespace, node_index_generation, cid)
```

Including the generation prevents compaction or garbage collection from
leaving a stale physical range mapping in cache.

### Admission and eviction

- Insert only after length, SHA-256, and CID verification.
- Admit roots and internal nodes with higher priority than scan-only leaves.
- Admit the requested node, not every node in its containing commit envelope.
- Use capacity eviction, not TTL, for immutable node bytes.
- Use a scan-resistant policy such as S3-FIFO for the disk tier.
- Use short, generation-scoped negative caching only for confirmed misses.
- Treat cache corruption or I/O failure as a miss; evict and refetch.
- Never use cached ref state to authorize a mutation.

Recommended initial per-process budgets are 256–512 MiB for memory and
10–50 GiB for local disk, exposed as configuration rather than protocol
constants. Operators size them from the working-set and hit-rate metrics.

### Cached values

| Value | Cache policy | Authority check |
|---|---|---|
| Prolly node bytes | Memory + disk, immutable | Verify digest on admission and disk hit |
| Commit envelopes | Bounded memory/disk, immutable | Verify `CommitId` and envelope digest |
| Node-index pages | Memory + disk by generation | Verify page digest from parent |
| Node locations | Bounded memory by generation | Resolve from verified index page |
| Branch/tag refs | Tiny in-process cache only | Fresh conditional ref read for publication |
| File payload | Application/CDN decision | Version-qualified key plus committed checksum |

The existing whole-pack cache becomes an optional secondary optimization. It
must not be the primary cache because one hot node should not retain or fetch a
large envelope.

## Sharded node index v2

Protocol v1 points to one checkpoint containing every known node location.
Opening that checkpoint is O(total nodes) in bytes and memory, so it cannot be
the billion-node design.

Version 2 replaces it with a small mutable head and immutable, content-addressed
pages:

![Sharded node index](diagram/prolly-s3-sharded-node-index.svg)

1. `node-index/head` identifies a generation and manifest digest.
2. The manifest maps CID prefixes to immutable shard roots.
3. Each shard is a shallow sorted tree of fixed-target-size pages.
4. Leaf entries map a CID to commit envelope, byte range, length, and digest.
5. A lookup fetches only one manifest path and one leaf page, then caches them.

Pages target 4–16 MiB before compression; actual sizing is selected by load
tests. Prefixes split when a page exceeds its target. Sparse manifest levels
avoid eagerly creating empty shards.

Checkpoint construction is background work. It merges new commit-envelope
indexes into affected copy-on-write pages and conditionally advances the head.
The commit envelope remains a self-describing correctness fallback, so a lost
checkpoint update delays lookup optimization but cannot lose committed data.
No index-maintenance S3 call is added to the three-call foreground write.

Readers pin a head generation for the duration of a lookup. Old generations
remain readable until a grace period covers active readers and cache leases.

## Files and versions

Exact lookup follows one root-to-leaf Prolly path. Range listing returns a
bounded page and an opaque continuation cursor containing the pinned commit,
tree root, last key, and traversal state. It never assembles all keys in RAM.

Large file bodies continue to be whole S3 objects or provider-native multipart
uploads. Repository scale does not reintroduce content chunking.

A single file with extreme version count remains distributed through the
composite-key version tree. If profiling shows a pathological hot-key history,
immutable per-key history segments may be added as a derived accelerator; they
must not become a second authority.

## Commits and graph traversal

Commits stay immutable and directly addressable by `CommitId`. Repository open
does not enumerate commits.

The current fixed history traversal ceiling becomes a `TraversalBudget` with:

- maximum visited commits;
- maximum decoded bytes;
- maximum elapsed time;
- cancellation and concurrency limits.

When a budget is exhausted, log, ancestry, merge-base, diff, and reachability
operations return an opaque continuation cursor instead of treating repository
size as an error.

Background workers build immutable commit-graph segments with generation
numbers and binary-lifting ancestor pointers. A commit can reference ancestors
at distances 1, 2, 4, 8, and so on, allowing long first-parent walks and common
ancestor searches to skip history. Missing segments fall back to bounded graph
walking, preserving correctness.

## Refs

Each ref retains its deterministic S3 object and independent CAS token. Exact
lookup and publication therefore remain O(1) in ref count, and unrelated refs
do not contend on a global head.

Listing millions or billions of refs uses a derived Prolly catalog keyed by
normalized ref name. The catalog is partitioned by tenant and prefix and is
updated asynchronously from authoritative ref mutations. List responses are
paged and disclose catalog freshness. A repair worker reconciles catalog
entries against ref mutation records.

The catalog must never authorize writes. A caller that selects a catalog result
still loads the authoritative ref object before mutation. Ref creation and
deletion remain correct if catalog maintenance is delayed.

## Write path

Foreground publication remains:

1. write the whole payload version;
2. write one immutable commit/node envelope;
3. conditionally publish the branch ref.

New nodes are placed in the pending/L1 cache while the commit is built. After
the commit envelope is durable, they are eligible for L2 admission. The
background indexer consumes commit-envelope summaries, creates new index pages,
and advances the node-index head. Cache warming and index compaction add no
foreground S3 requests.

The fenced writer serializes only publication for a single branch. Scale-out
uses independent branches or repository partitions; a branch with one mutable
head has a finite maximum commit rate.

## Garbage collection at billion scale

A GC implementation cannot hold all reachable commits, nodes, or physical
versions in one process. It uses an epoch-based, partitioned mark-and-sweep:

1. Snapshot ref, pin, and reflog roots into a GC epoch.
2. Persist a sharded work queue and per-partition mark runs.
3. Workers traverse with bounded memory and checkpoint their cursors.
4. Merge sorted mark runs into immutable reachability manifests.
5. Produce exact `(key, VersionId)` deletion partitions.
6. Revalidate roots and the epoch before deleting each bounded batch.
7. Rate-limit deletion and persist every completed partition.

Workers are restartable and idempotent. A later epoch may supersede an earlier
one, but no sweep deletes from an epoch whose roots can no longer be validated.
The node-index generation advances away from reclaimed envelopes before their
grace period ends.

## Backpressure and isolation

All potentially large activities have independent resource pools:

| Activity | Bound |
|---|---|
| Foreground node fetch | Singleflight keys, requests, bytes, deadline |
| Payload reads/writes | Requests, bytes in flight, per-tenant quota |
| Cache fill | Admission bytes and disk write bandwidth |
| Index compaction | Pages, bytes, S3 request rate |
| History traversal | Commits, bytes, duration, continuation cursor |
| Ref/key listing | Page size and cursor lifetime |
| GC | Partitions, request rate, deletion batch, epoch deadline |

Tenant/repository quotas stop one scan or cache-warming workload from evicting
all useful data or exhausting the S3 request pool.

## Observability

Metrics are tagged by repository and operation, with bounded-cardinality labels:

- L1/L2 node hit rate, bytes, eviction, corruption, and fetch coalescing;
- node-index pages and S3 calls per lookup;
- Prolly tree depth and decoded bytes per operation;
- commit-graph skips versus fallback visits;
- foreground S3 calls, latency, throttling, and retry count;
- hot-branch publication queue time and conflict rate;
- index-generation lag, compaction debt, and orphan-envelope count;
- GC queue age, marked bytes, swept versions, and safety revalidations.

Logs may include sampled CIDs and commit IDs, but metrics must not label by CID,
key, ref, or commit because those would themselves create unbounded telemetry.

## Qualification gates

“Production ready at scale” requires reproducible tests at these checkpoints:

| Tier | Live keys | Retained versions/commits | Refs |
|---|---:|---:|---:|
| A | 1 million | 10 million | 10,000 |
| B | 100 million | 1 billion | 1 million |
| C | 1 billion | Workload-defined multi-billion | 10 million |

For each tier, record repository-open cost, cold/warm p50/p95/p99 lookup,
requests per operation, cache hit rate, hot-branch throughput, index catch-up,
restart recovery, GC progress, throttling, and monthly request/storage cost.
AWS tests must cover expected regions, key distributions, payload sizes, burst
traffic, cache loss, disk corruption, partial index publication, and worker
crashes. Synthetic scale must use real encoded pages and realistic key sizes.

Tier C is a qualification target, not a protocol maximum. Larger deployments
continue by adding storage, cache, workers, and repository partitions without a
format migration, subject to measured provider and operational limits.

## Migration and implementation phases

Protocol v1 remains frozen. Scale metadata lives under versioned v2 paths or a
negotiated capability; readers never reinterpret v1 bytes as v2.

1. Add the `NodeCache` interface, verified node-level admission,
   singleflight, metrics, and an optional audited Foyer adapter.
2. Add sharded node-index v2 with lazy pages, generation-aware location cache,
   background construction, and v1 fallback.
3. Add paged traversal APIs, durable continuation cursors, and commit-graph
   skip segments.
4. Add the derived sharded ref catalog and reconciliation.
5. Replace in-memory GC planning with epoch-based partitioned GC.
6. Run the qualification matrix before increasing the supported scale claim.

Foyer is currently denied by the dependency policy because it was previously
removed. Adoption requires an intentional dependency/security review, pinned
feature set, MSRV and license validation, corruption tests, and all workspace
quality gates. The engine-level cache interface should land independently so a
different cache implementation remains possible.

## Consequence

This design can remove fixed repository cardinality limits; it cannot remove
physical limits or make unbounded work instantaneous. The API contract is:

> Repository growth does not require whole-repository loading or rewriting.
> Every operation is bounded, pageable or resumable, and correctness never
> depends on a cache or derived index.
