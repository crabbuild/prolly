# Prolly S3 architecture

## Purpose

Prolly S3 is a thin repository layer over a versioned S3 bucket. The client is
the authoritative writer. It stores each file as one immutable payload and uses
Prolly trees to track names, metadata, commits, branches, and history.

There is one durable format and one public client. The implementation does not
dual-write, migrate, or negotiate legacy repository protocols.

![Architecture](diagram/prolly-s3-architecture.svg)

## Data model

- **Repository** — one logical history namespace under a reserved prefix.
- **Payload** — immutable whole-file bytes addressed by SHA-256.
- **Object version** — logical metadata binding a path to a payload or delete
  marker.
- **Bucket commit** — immutable parents, root manifests, author, message,
  operation identity, and authority stamp.
- **Branch ref** — mutable pointer to a publication event.
- **Publication event** — immutable link from the previous event to a commit.
- **Authority lease** — branch-local writer generation used to fence stale
  writers.
- **Advisory indexes** — rebuildable journal, operation, node, graph, and ref
  catalogs derived from authoritative refs and immutable events.

## Write path

![Write path](diagram/prolly-s3-write-path.svg)

1. Validate provider attestation and branch authority.
2. Upload the whole payload at its immutable derived key. Identical content is
   reused.
3. Apply one or many logical mutations to the Prolly tree.
4. Persist new tree nodes, root manifests, commit, and publication event.
5. Conditionally replace the branch ref. This compare-and-swap is the commit
   point.
6. Reconcile ambiguous outcomes by operation ID before fencing the writer.
7. Advance rebuildable indexes asynchronously.

Failed branch CAS attempts can leave unreachable immutable candidates. They are
never visible from an authoritative ref.

Commit sessions execute steps 2–4 incrementally and perform step 5 once. This
is the default ingestion architecture because publication costs are amortized
across all files in the batch.

## Read path

A current read loads the branch ref, publication event, commit, root manifest,
and the Prolly path for the key, then fetches the immutable payload. A
historical read starts at an explicit commit and follows the same immutable
path. Verified nodes and payload bindings are safe to cache.

Journal-derived location indexes map content IDs to node-pack ranges. Missing
advisory data is rebuilt from the linked publication journal; normal reads do
not recover by listing the bucket.

## Concurrency

Authority and publication are branch-scoped. Independent branches can publish
in parallel. Writers targeting the same branch share one ref CAS lane because
the branch has one linear history.

An authority stamp contains scope and generation. A takeover installs a higher
generation only after its barrier conditions hold. Stale writers validate
authority before uploading and are fenced before producing new payload cost.
Lease renewal is maintained while a writable client is alive.

## Merge

Merge is structural and restartable:

1. discover merge bases with a bounded persisted frontier;
2. build an immutable merge plan;
3. expose paginated changes and conflicts;
4. publish one two-parent commit through the target branch ref CAS;
5. clean up only mutable administration cursors.

The merge never materializes complete snapshots in memory.

## Durable namespace

The default prefix is `.prolly`. Important subtrees are:

```text
.prolly/
├── format/
├── authority/
├── refs/heads/
├── refs/tags/
├── payloads/
├── commits/sha256/
├── publications/sha256/
├── journal-index/
├── operation-index/
├── ref-catalog/
├── staging/
└── administration/
```

See [the path specification](spec/prolly-s3/paths.md) for exact templates.

## Safety properties

- Immutable records are content-addressed and verified after load.
- A commit is visible only through a successful branch ref conditional write.
- Retrying one operation ID with different input is rejected.
- Delete markers are logical records; they do not create mutable payload keys.
- Mutable control records retain bounded provider versions.
- Caches and indexes may be discarded without losing repository truth.

## Deliberate limits

There is no chunked payload format and no multipart ingestion. One file must fit
the repository and provider single-PUT limit. Garbage collection and
cross-repository transfer are not production APIs. Immutable unreachable data
therefore accumulates and must be included in storage planning.

The design has no fixed file, commit, or ref count in its logical structures,
but real deployments are bounded by provider quotas, request cost, latency,
throttling, local cache capacity, index lag, and same-branch publication
contention.
