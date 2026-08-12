# Prolly S3 operations

## Deployment contract

- Enable bucket versioning before repository initialization.
- Route all managed-key mutations through Prolly S3 writer services.
- Assign each branch to exactly one writer identity at a time; different
  services may own different branches.
- Give reader processes `.read_only(true)` clients.
- Keep `.prolly/v1/` reserved from application keys.
- Disable lifecycle expiry for managed current and noncurrent versions.
- Store provider-attestation and pagination HMAC keys in a secret manager.
- Use independent keys and rotate them with an overlap window.

## IAM capabilities

The writer needs object-version reads, whole-object writes, physical multipart,
version listing for recovery/GC, exact-version deletion for explicit GC, and
conditional metadata writes under `.prolly/v1/`. Readers need exact-version
gets plus repository metadata reads.

Do not grant a service permission to mutate managed user keys except through
this client. IAM and service ownership are part of the exclusive-wrapper
invariant.

## Start and reopen

Use `initialize()` once. It qualifies the provider and creates format v1. Use
`open()` afterward. A writable client lazily acquires and maintains authority
for the branches and system scopes it mutates; a read-only open acquires none.

Provider qualification is signed and bound to endpoint, region, bucket,
capabilities, and expiry. Expired or mismatched attestations fail closed.

## Monitoring

Track separately:

- logical operation latency and errors;
- object-plane SDK calls from `s3_operation_metrics()`;
- Smithy wire attempts and provider throttling;
- publication queue depth and wait time from `performance_snapshot()`;
- shard-authority renewal latency, ambiguity, and fencing events;
- unreachable physical versions and commit envelopes;
- GC candidate bytes, exact-version deletes, and failures;
- node-index checkpoint age and rebuild fallbacks.
- branch-ref physical version count and compaction failures.

The four-call write budget counts SDK operations, not internal HTTP retry
attempts. Alert on either dimension independently.

Tune `max_parallel_payload_writes` to the service's connection pool and S3
request-rate budget. Bound in-process metadata with `max_cached_commits`,
`max_cached_branches`, and `max_cached_node_pack_bytes`; do not disable these
bounds in a long-running writer. Alert when publication wait consumes a
material part of end-to-end latency or max queue depth grows continuously.

## Bulk ingestion and cache warm-up

Use `Client::ingest_objects` as the default path for imports and backfills. It
publishes at most 100 whole files per commit and also obeys
`max_staged_batch_bytes`. Tune the count downward when file sizes or commit
latency require it; use multipart for one file larger than the staged bound.

Configure a Foyer node cache on writers and traversal-heavy readers. Successful
publications write committed immutable nodes through to the cache. Reopen the
same cache directory after a restart, with only one process owning that
directory. On a new or empty host, call `prewarm_internal_node_cache` for the
production snapshot before accepting list, diff, or history traffic. It fetches
only the three state roots and internal descendants; it does not enumerate user
objects or fetch leaf nodes. The injected `NodeCache` may be a local persistent
Foyer cache or a tenant-isolated shared remote cache.

Every recurring mutable control object is bounded by
`mutable_control_versions_to_retain` (100 by default). The same Module covers
writer and shard-authority leases, branch and tag refs, pins, advisory-index
heads, and GC checkpoints. Branch refs compact on persisted generation
boundaries; lower-rate controls compact before every update. The compactor
revalidates the current storage token after the complete version listing and
exact-deletes only obsolete VersionIds. Restarting a writer forces compaction
before its first update, so the bound does not depend on process-local state.

The older branch-specific maintenance entry point remains available when an
operator wants a smaller recovery depth. Keep `s3:ListBucketVersions` and
version-qualified `s3:DeleteObject`/`s3:DeleteObjects` permissions available.
Never set branch-ref retention above the repository-wide control bound.

Protocol v1 still stores history as versions of the original user key. Do not
claim unbounded hot-key support on a provider with a finite or unknown per-key
limit. Protocol v2 uses immutable derived payload keys and qualifies finite
providers only when their per-key limit exceeds the mutable-control bound plus
two versions of safety headroom.

Protocol-v2 refs point at immutable publication events. Each event links to the
previous event for the same branch, so an indexer opens the ref once, persists
the returned journal cursor, and resumes without listing commits or refs. The
cursor is anchored to its original head; a concurrently advancing branch does
not change that traversal. Publishing the event adds one immutable write to the
v2 foreground path. A failed competing ref CAS can leave an unreachable event;
retain it until the v2 concurrent-GC gate is enabled.

Initialize `SegmentedOperationIndexV2` when a v2 branch is created, and advance
it continuously from the publication journal. The default idempotency window
is the smaller of one million ref generations and seven days. Applications
must retain and reuse operation IDs only within the configured window. The
mutable per-branch head stays under the repository control-version bound;
immutable sorted segments merge geometrically, so lookup reads a bounded
number of levels plus the explicitly bounded unindexed journal tail. Excessive
lag fails closed with `HistoryLimitExceeded` and must use resumable rebuild.

Initialize `JournalDerivedIndexesV2` in the same branch-creation workflow and
advance it continuously. It checkpoints the branch publication event together
with node-locator and commit-graph Prolly roots in one CAS. Normal advancement
follows immutable journal links and never lists the commit or ref namespaces.
If initialization is missed or lag exceeds the configured bound, it fails
closed instead of hiding an unbounded scan in foreground maintenance. Protocol
v1's older `advance_node_index_v2`, `advance_commit_graph_v2`, and
`advance_ref_catalog_v2` scan epochs remain compatibility/rebuild tools; do not
use them as protocol-v2 steady-state maintenance.

Native protocol v2 enumerates branches and tags through 16 event-driven
catalog shards. Branch index maintenance records the latest authoritative ref
generation after advancing its journal indexes. Branch/tag create and delete
also update the catalog before returning. Catalog reads perform point GETs and
must not call `ListObjectsV2`; monitor any ref-namespace LIST as repair or
administrative traffic. If a crash leaves a published ref absent from the
catalog, page `repair_branch_catalog_page` or `repair_tag_catalog_page` with a
bounded limit and persist the returned continuation between invocations.

```rust
use prolly::TreeFormat;
use prolly_s3_core::JournalDerivedIndexesV2;

let indexes = JournalDerivedIndexesV2::new(
    plane.clone(),
    ".prolly/v2",
    repository_id,
    TreeFormat::default(),
)?;

// Do this immediately after creating each branch, then on a background loop.
let report = indexes.advance(&publisher, "main", now_millis).await?;
println!(
    "indexed {} publications / {} commits / {} nodes",
    report.indexed_publications,
    report.indexed_commits,
    report.indexed_nodes,
);
```

Partitioned GC no longer holds the repository publication barrier while it
discovers roots, marks commits/nodes/versions, scans candidates, or consumes
dirty roots. Starting an epoch activates one durable coordinator. Each live
branch/tag/pin CAS then writes an ordered dirty-root event before its control
CAS. GC briefly takes the barrier to freeze a dirty sequence watermark and for
each bounded exact-delete batch; it never restarts the full root scan because a
writer advanced. After sweep, the coordinator is cleared and journal objects
are exact-deleted in restartable batches. Only one GC epoch may be active.

## Conflict and outage handling

- A stale `expected_head` or branch-ref CAS returns a conflict. Re-read and let
  the application decide whether to retry or merge.
- Never automatically rebase an atomic commit.
- Reuse a stable `OperationId` after an ambiguous response.
- Stop writes when authority renewal is ambiguous or the fence is lost.
- Perform takeover only after establishing that the previous writer cannot
  continue publishing.

## Whole-history administration

Do not build repository-wide jobs with `list_branches`, `list_tags`,
`list_retention_pins`, `list_reflog`, or an in-memory commit closure. Their
legacy convenience forms remain bounded compatibility APIs. Production jobs
page branches, tags, pins, and tag/branch reflogs with their `*_page` methods.

For a clone, fsck, repair, or export, start a `CommitClosureCursor` with the
first bounded root page, attach later root pages with
`extend_commit_closure`, and persist the cursor returned by every
`commit_closure_page`. The cursor stays constant-size because its stack and
visited set are immutable Prolly state. Each page has separate step and emitted
commit limits and emits parents before children. Persist copied-object and
source-to-destination commit mappings before persisting the next cursor. After
the final result/ref CAS, repeatedly call `cleanup_commit_closure`; abandoned
jobs require the same explicit bounded cleanup.

The built-in physical clone/fetch/push/repair pipeline consumes at most 256
commits per page and persists each successful source-to-destination mapping.
Incremental transfers use point GETs against those mappings and never LIST the
destination commit namespace. A mapping whose destination commit was reclaimed
is exact-deleted and rebuilt; monitor stale-mapping rebuilds because frequent
occurrences indicate transfer/GC retention policies are misaligned.

Workflow engines can checkpoint the same pipeline explicitly with
`start_physical_transfer`, `extend_physical_transfer`, and
`physical_transfer_page`. Canonically serialize only the returned
`PhysicalTransferCursor`, never the input cursor after a successful page. Use
`physical_transfer_mapping` to resolve final ref targets, publish those refs,
then clean up `cursor.closure` in bounded pages.

Deep verification follows the same ownership rule. Page branch and tag roots,
call `start_resumable_fsck`/`extend_resumable_fsck`, then persist each cursor
returned by `resumable_fsck_page`. Its phases durably deduplicate commit IDs,
content-addressed node CIDs, and physical version records. Live objects stream
through disk-backed spools; a delete-marker search checkpoints its provider
continuation after every LIST page. The compatibility `fsck` and `fsck_commit`
methods drive this cursor to completion with bounded memory and clean up their
internal job state automatically. External workflows must clean up
`cursor.closure` after success or abandonment.

## Backup and restore

A raw cross-bucket copy is not a valid repository restore: S3 assigns new
provider `VersionId` values while copied commits still reference the source
IDs. The client fails closed when those stale bindings are read.

For a portable backup, use `clone_to` to create a complete logical repository
in a versioned archive bucket. Restore by opening that archive read-only and
cloning it to the destination; both hops replay history and bind every logical
version to the destination provider's exact ID. Open the result read-only, run
`fsck`, and only then call `takeover_branch_writer` for each branch that the
restored service will own, using the previous writer ID, authority generation,
and auditable credential/process-isolation evidence.

A provider-native physical snapshot is usable in place only when the restore
mechanism explicitly guarantees preservation of every opaque `VersionId` and
delete marker. A current-key-only copy is always insufficient.

## Garbage collection

Use only repository GC for managed versions. For growing repositories, use the
v2 epoch workflow rather than the in-memory v1 dry run:

1. Advance node index v2 through a complete scan epoch.
2. Start the epoch with a conservative grace period.
3. Call bounded `advance_gc_epoch` steps until `Ready`.
4. Call bounded `sweep_gc_epoch` steps until `Completed`.
5. Continue advancing instead of sweeping whenever a write or restart returns
   the epoch to `CatchUpDirtyRoots`; cleanup is also advanced in bounded steps.

GC v2 requires the fenced `system/gc` authority scope. It marks reachable CIDs and the
actual envelopes supplying them before deletion, then names every exact
physical `VersionId`. Never bypass a `MissingCapability`, dirty-root catch-up, or
writer-fence error. Export marked-node, candidate, deleted, skipped-reachable,
and restart counts.

## Local RustFS

```bash
docker compose -f extensions/s3/docker-compose.rustfs.yml up -d

extensions/s3/scripts/verify_rustfs_aws_cli.sh

PROLLY_S3_RUSTFS=1 \
  cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test rustfs_repository -- --nocapture
```

Local credentials in the Compose file are development-only. RustFS verifies
behavior and request shape; it does not substitute for AWS release testing.
