# Prolly S3 operations

The stable upgrade/recovery contract, exported OpenTelemetry instruments, and
reference production alerts are defined in [GA-CONTRACT.md](GA-CONTRACT.md).

## Provisioning

1. Create a dedicated S3 or S3-compatible bucket.
2. Enable bucket versioning before initialization.
3. Reserve a repository prefix, normally `.prolly`.
4. Grant the client object read/write, exact-version read, conditional write,
   range read, version listing for qualification, and bucket-versioning access.
5. Keep application users from writing inside the reserved prefix.

Run `Client::builder().initialize()` once. Reopen with identical bucket,
prefix, provider identity, signer trust, and writer identity.

## Writer identity and takeover

Use stable workload identities, not hostnames that change on every restart.
Writable clients renew branch authority automatically. Alert on:

- fenced branches;
- renewal failures;
- frequent takeovers;
- ref CAS conflict rate;
- ambiguous-write reconciliation failures.

A takeover is an operational fencing action. Isolate or revoke the previous
writer before raising the generation.

## Ingestion

Commit sessions are the default:

- choose a checkpoint interval that limits lost staging work;
- persist the batch ID before accepting source-side completion;
- resume the same batch after process failure;
- publish once per useful unit of atomicity;
- use ephemeral sessions only for replayable jobs.

Very small interactive changes may use `put_object`. Persist an `OperationId`
for any request that crosses a durable job boundary.

## Index maintenance

Background branch-index maintenance is enabled by default. Monitor
`branch_index_health` and its lag generation. If repair is needed:

1. start a journal index rebuild;
2. advance it in bounded event pages;
3. build the operation index from the same stable journal cursor;
4. atomically install rebuilt heads;
5. clean up mutable cursor state.

Do not treat advisory index heads as repository truth. Refs and linked
publication events are authoritative.

## Cache operations

Use a local persistent Foyer directory per trust domain. It may be deleted at
any time. Size it for upper nodes plus the expected hot working set. Monitor
hit ratio, bytes, eviction, validation failures, and provider range reads.

Prewarm current branch roots and upper levels after deployment or failover when
tail latency matters.

## Integrity checks

`start_fsck` creates a snapshot-bound durable job. Every `advance_fsck` page
updates its repository checkpoint with a monotonic generation. After process
loss, reopen the repository, catch up the source branch index if necessary,
then call `resume_fsck(job)`. Never continue a locally saved cursor after
another worker has advanced the job; stale generations fail with `RefConflict`.

Keep the completed report for audit, then call `start_fsck_cleanup(job)` and
advance its cursor in bounded pages. Cleanup removes the distinct-payload work
tree, commit-closure work tree, older checkpoint versions, and finally the
current checkpoint. It is permitted only after completion and can restart from
the beginning after process loss. Metadata mode verifies bindings and provider
metadata; deep mode downloads and hashes each distinct complete payload object.

## Backups

Provider bucket replication or object backup must preserve all repository
objects and versions under the prefix. History transfer can recreate a commit
DAG in another repository, with new destination-local commit/version IDs and
payload bindings. Test restore into an isolated bucket and run deep logical
backup verification before declaring a backup usable.

## Storage retention

Use bounded `start_gc`, `advance_gc`, and `sweep_gc` jobs to reclaim unreachable
immutable commit, direct-node, and payload versions. The repository durably
checkpoints every returned page. Set the grace period longer than the maximum
duration of any unpublished commit, merge, repair, or transfer. Retention pins
are GC roots.

GC closes a durable repository-wide publication-admission epoch. Every branch
or tag CAS owns an expiring publication ticket, including writers in separate
processes. Marking waits for pre-epoch tickets to finish or expire; new
publications receive `PreconditionFailed` until cleanup reopens admission.
Alert on tickets that approach the authority-lease duration. Never delete
payload, commit, node, publication, index, ticket, or administration keys
manually.

Expired mutable commit-session checkpoints can be removed through
`cleanup_expired_commit_sessions`.

## Metrics and alerts

Collect:

- S3 calls and bytes by operation;
- p50/p95/p99 current and historical reads;
- single-write and batch publication latency;
- files and bytes per commit;
- CAS retries and reconciled operation IDs;
- authority renewal/takeover/fence events;
- journal and operation-index lag;
- cache hit ratio and cold reads;
- physical versus live logical storage.

Set budgets for request amplification, AWS cost, throttling, and latency based
on the intended key count and traffic. The local RustFS gate proves semantics
and reproducibility, not AWS production economics.

## Incident rules

- **Ambiguous write:** retry identical input with the same operation ID.
- **Writer fenced:** stop writes; inspect authority and takeover history.
- **Index behind:** keep refs intact and run bounded rebuild.
- **Cache corrupt:** remove the cache and reopen; investigate storage hardware.
- **Provider versioning suspended:** stop writes and restore the prerequisite.
- **Manual prefix mutation:** isolate the repository and run qualification;
  do not guess which derived objects are safe to remove.
