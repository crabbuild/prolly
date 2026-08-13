# Prolly S3 API guide

The application-facing type is `prolly_s3_client::Client`. This guide describes
the public client surface in version 0.1.0 and separates ordinary application
operations from administrative maintenance.

## Choose the right workflow

| Goal | Recommended API |
|---|---|
| Create or reopen a repository | `Client::builder`, then `initialize` or `open` |
| Write one interactive file | `put_object` or `put_object_with_metadata` |
| Retry a write after an ambiguous response | `put_object_with_operation` with the same `OperationId` and input |
| Atomically write many files | `begin_commit`, or `put_objects` for bulk loading |
| Read current or historical data | `get_object`, `get_object_at` |
| Read metadata or a byte range | `head_object`, `get_object_range` |
| Copy without re-uploading the payload | `copy_object` |
| Delete one or many files | `delete_object`, `delete_objects` |
| List current or historical objects | `list_objects`, `list_objects_at`, `list_objects_delimited` |
| Inspect object-version history | `list_object_versions`, `list_versions_prefix`, `list_versions_at` |
| Select a branch, tag, or commit | `checkout`, `checked_out_ref`, `branch` |
| Name or retain a commit | `create_tag`, `tag`, `delete_tag`, `create_retention_pin` |
| Compare or inspect history | `log_bounded`, `diff_bounded`, `commit` |
| Inspect or recover ref movement | `open_reflog`, `read_reflog_page`, `recover_branch` |
| Restore logical state as new history | `start_restore`, `advance_restore` |
| Move a branch administratively | `reset_branch` |
| Merge branches | `start_merge`, `advance_merge`, `publish_merge` |
| Check repository integrity | `start_fsck`, `advance_fsck` |
| Reclaim unreachable immutable data | `start_gc`, `advance_gc`, `sweep_gc` |
| Synchronize only one logical snapshot | `start_repair_from`, `start_clone_from`, `start_fetch_from`, `start_push_to` |
| Preserve a complete source commit DAG | `start_history_clone_from`, `start_history_fetch_from`, `start_history_push_to` |
| Verify a logical backup | `start_backup_verification`, `advance_backup_verification` |
| Inspect cache and provider requests | `prewarm_node_cache`, `node_cache_snapshot`, `s3_operation_metrics` |

## Client identity and checkout

- `bucket` and `repository_id` return the physical bucket and immutable
  repository identity.
- `checkout` accepts unqualified names, `refs/heads/...`, `refs/tags/...`, a
  typed `CheckoutRef`, or a `CommitId`.
- `checked_out_ref` reports the resolved `CheckedOutRef`. `branch` returns
  `Some(name)` for an attached branch and `None` for a detached tag/commit.
- `CheckedOutRef::target` returns the immutable target for tag/commit
  checkouts; attached branches remain live and therefore return `None`.
- `head` returns the attached branch head or detached tag/commit target.
- `fenced_branches` reports branches this process can no longer publish.

`Client` clones share repository state, caches, authority maintenance, and S3
request metrics.

## Provisioning and opening

Start with `Client::builder` (the `builder` constructor). The builder exposes:

| Configuration | Methods |
|---|---|
| AWS transport and location | `aws_client`, `bucket`, `repository_prefix`, `default_branch` |
| Writer identity and fencing | `writer`, `authority_lease_duration`, `read_only` |
| Provider qualification | `provider_identity`, `attestation_signer`, `provider_attestation`, `provider_attestation_validity`, `provider_per_key_version_limit` |
| Immutable-node caching | `node_cache`, `max_cached_node_pack_bytes`, `max_cached_node_locations`, `max_cached_node_bytes` |
| Index maintenance | `background_index_maintenance`, `journal_index_max_unindexed_events`, `operation_index_limits` |
| Mutable-control retention | `mutable_control_version_retention` |

Call `initialize` once for a new prefix. Call `open` for an existing prefix.
All processes opening one repository must agree on its canonical format and
provider profile. Use a stable workload identity for `writer`.

## Object operations

### Writes

- `put_object` writes bytes with default headers and metadata.
- `put_object_with_metadata` adds S3-shaped logical headers and user metadata.
- `put_object_with_operation` accepts a caller-stable operation identity for
  ambiguous-response reconciliation.
- `copy_object` creates a new logical version while reusing an immutable
  payload binding.
- `delete_object` publishes one delete marker.
- `delete_objects` publishes many delete markers atomically.

A standalone or larger logical file is one immutable content-addressed payload
object. Bulk staging combines non-empty files up to 4 KiB into deterministic
immutable segments capped at 4 MiB. Tree bindings carry each logical checksum
and inclusive extent, and reads use byte ranges; empty and larger files retain
the direct representation.

### Reads

- `get_object` reads the selected branch head.
- `get_object_at` reads an explicit immutable snapshot.
- `head_object` returns logical metadata without the body.
- `get_object_range` reads an inclusive byte range from an explicit snapshot.

### Listings

- `list_objects` returns `(snapshot, objects, truncated)`.
- `list_objects_at` holds the supplied snapshot stable across pages.
- `list_objects_delimited` returns objects plus S3-style common prefixes.
- `list_objects_page` returns a snapshot-bound opaque cursor and resumes with a
  direct tree seek; `stream_objects` lazily consumes those pages with bounded
  memory. On detached tag or commit checkouts, page one is seeded from that
  immutable target rather than the branch's current head.
- `list_object_versions` lists versions for one logical key.
- `list_versions_prefix` and `list_versions_at` scan version history across a
  logical key prefix.

When an object listing is truncated, pass the last returned logical key as
`after`. Version-prefix pages use the opaque byte cursor returned in each
`VersionSummary`.

## Atomic commit sessions and bulk writes

`put_objects` is the concise in-memory bulk path. It divides `PutObjectInput`
values into durable atomic batches, uploads each checkpoint window with bounded
concurrency, and returns one receipt per published batch. `put_object_stream`
accepts a fallible `Stream` plus `BulkWriteOptions` for bounded-memory ingestion
from an unbounded source. Completed checkpoint windows remain resumable after
cancellation or a source/object failure.

For explicit control, call `begin_commit`. `CommitSessionBuilder` supports:

- `message` for the audited commit description;
- `expires_after` for durable staging expiry;
- `checkpoint_every` for remote checkpoint frequency;
- `ephemeral` for minimum-request, non-resumable staging;
- `start` to obtain a `CommitSession`.

A `CommitSession` exposes `id`, `operation`, `base_commit`, `staged_objects`,
and `is_durable`. Stage mutations with `put_object`,
`put_object_with_metadata`, `put_stream`, `put_stream_with_metadata`, and
`delete_object`. Use `checkpoint`, then `publish` or `abort`.

Persist the batch ID before acknowledging upstream progress. After a process
failure, call `resume_commit` with the batch ID; verified payload bindings are reused.
Expired durable sessions are removed in bounded pages with
`cleanup_expired_commit_sessions`.

Commit descriptors remain bounded as batches grow. Deltas of up to 128 logical
changes are embedded directly; larger deltas are stored in a content-addressed
Prolly tree and referenced by root and count. Resume, reconciliation, diff, and
publication preserve the same atomic semantics for both representations.

## Branches, tags, and retention

- `create_branch` creates a branch at an explicit commit or current head. It
  inherits immutable node-location and commit-graph roots from the checked-out
  source branch, so branch creation does not download or rebuild the snapshot.
- `delete_branch` requires the expected head.
- `create_tag`, `tag`, and `delete_tag` manage immutable-history names.
- `create_retention_pin`, `retention_pin`, `delete_retention_pin`, and
  `list_retention_pins_page` manage durable GC roots.

Branch and tag catalogs are derived, bounded indexes. Enumerate them with
`list_branch_catalog_page` and `list_tag_catalog_page`.

## History, diff, and recovery

- `commit` loads and validates one immutable commit.
- `log` and `diff` are convenience APIs for small bounded results.
- `log_bounded` accepts `TraversalBudget`; `diff_bounded` uses an opaque
  structural cursor and prunes identical subtrees. Diff resolves compact commit
  descriptors and fetches only nodes on the structural frontier.
- `open_reflog` captures a stable immutable journal snapshot;
  `read_reflog_page` reads newest-to-oldest pages from it.
- `reset_branch` is a CAS-protected administrative ref move.
- `recover_branch` selects the previous target named by a reflog event.
- `start_restore` and `advance_restore` materialize an old snapshot as new,
  auditable commits without discarding intervening history.

## Structural merge

Call `start_merge`, persist the returned cursor, and repeatedly call
`advance_merge`. If the graph has multiple best bases, inspect
`merge_bases_page` and call `select_merge_base`. Inspect planned work with
`merge_changes_page` and `merge_conflicts_page`. Only `publish_merge` moves the
target branch. `cleanup_merge` exact-deletes job-scoped plan data in bounded
pages after publication or abandonment.

Merge planning shares the immutable node cache with foreground reads and
advances structural-diff frontiers in bounded batches. Unchanged subtrees are
pruned without loading full commit node packs.

## Integrity and garbage collection

### Fsck

`start_fsck(false)` validates metadata and immutable structure.
`start_fsck(true)` additionally downloads and hashes reachable payload bytes.
Advance either mode with `advance_fsck`, persisting the cursor after every
page.

### GC

Start with `start_gc(grace_millis)`, advance marking and discovery with
`advance_gc`, then delete bounded exact-version batches with `sweep_gc`.
Retention pins are roots. The grace period must exceed the longest possible
unpublished upload, commit session, merge, repair, or transfer.

Concurrent GC coordinates all writer handles inside the authoritative process.
Quiesce separately running writer processes before GC.

## Repair, clone, fetch, push, and backup

There are two deliberately different transfer families:

1. Snapshot synchronization uses `start_repair_from` and
   `advance_repair_from`. Compatibility aliases are `start_clone_from`,
   `start_fetch_from`, and `start_push_to`. These recreate only the selected
   logical state.
2. History synchronization uses `start_history_transfer_from` and
   `advance_history_transfer_from`, then `publish_history_transfer`.
   Convenience starters are `start_history_clone_from`,
   `start_history_fetch_from`, and `start_history_push_to`.
   `history_transfer_mapping` resolves a source commit to its destination ID.

History transfer preserves parent topology, including merges, but commit and
object-version IDs change because repository identity and payload bindings are
destination-local.

After either transfer, use `start_backup_verification` and
`advance_backup_verification` to compare logical keys and downloaded content.

## Index and authority administration

These methods are operational controls, not normal foreground request paths:

- `advance_branch_indexes`, `branch_index_health`, and
  `wait_for_branch_indexes` inspect or catch up one branch.
- `start_branch_index_rebuild`, `advance_branch_index_rebuild`, and
  `cleanup_branch_index_rebuild` rebuild journal-derived indexes from a stable
  immutable cursor.
- `start_operation_index_rebuild` and `advance_operation_index_rebuild`
  rebuild bounded operation-ID reconciliation state.
- `repair_branch_catalog_page` and `repair_tag_catalog_page` repair derived ref
  catalogs by bounded physical namespace scans.
- `takeover_branch_writer` is a fencing operation. Isolate the previous writer
  before supplying its expected identity and generation.

## Cache and observability

- `node_cache_snapshot` returns immutable-node cache counters.
- `prewarm_node_cache` traverses both state trees for one snapshot.
- `s3_operation_metrics` returns provider operation and wire-attempt counters.
- `reset_s3_operation_metrics` atomically returns and resets those counters.

Metrics are process-local. Export them before process termination and correlate
them with provider request IDs and service-side metrics.

Journal-derived node indexes are built from compact commit descriptors and
node-pack tables of contents. Payload sections are range-fetched only when a
referenced node is actually read.

## Error and consistency model

The branch ref conditional write is the commit point. Payloads, nodes, commits,
and publication events are immutable candidates until that ref CAS succeeds.
Reads at an explicit `CommitId` remain stable.

Errors expose a stable `ErrorCode`, retry advice, optional operation ID, and
provider code/message/request ID. Retry ambiguous writes only with identical
input and the same operation ID. A precondition, authority, or ref conflict
must be reconciled rather than blindly retried.

## Runnable examples

All examples create an isolated repository in local RustFS:

| Scenario | Example |
|---|---|
| CRUD, metadata, ranges, copy, listing, history | [`basic_object_workflow.rs`](client/examples/basic_object_workflow.rs) |
| Durable atomic batches and streamed input | [`atomic_batch_and_streaming.rs`](client/examples/atomic_batch_and_streaming.rs) |
| Branch, bounded diff, merge, log, reflog | [`branch_diff_merge.rs`](client/examples/branch_diff_merge.rs) |
| Historical restore, reset, reflog recovery | [`restore_and_recovery.rs`](client/examples/restore_and_recovery.rs) |
| Commit-DAG transfer and backup verification | [`history_transfer_and_backup.rs`](client/examples/history_transfer_and_backup.rs) |
| Deep fsck, cache, metrics, retention, GC | [`integrity_gc_and_observability.rs`](client/examples/integrity_gc_and_observability.rs) |

See [the client guide](client/README.md#runnable-scenario-examples) for setup and
commands.
