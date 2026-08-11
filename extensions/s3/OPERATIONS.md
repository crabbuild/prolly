# Prolly S3 operations

## Deployment contract

- Enable bucket versioning before repository initialization.
- Route all managed-key mutations through one writer service.
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

Do not grant a second service permission to mutate managed user keys. IAM and
service ownership are part of the exclusive-writer invariant.

## Start and reopen

Use `initialize()` once. It qualifies the provider and creates format v1. Use
`open()` afterward. A writable open acquires and maintains the writer lease;
a read-only open does not.

Provider qualification is signed and bound to endpoint, region, bucket,
capabilities, and expiry. Expired or mismatched attestations fail closed.

## Monitoring

Track separately:

- logical operation latency and errors;
- object-plane SDK calls from `s3_operation_metrics()`;
- Smithy wire attempts and provider throttling;
- publication queue depth and wait time from `performance_snapshot()`;
- lease renewal latency, ambiguity, and fencing events;
- unreachable physical versions and commit envelopes;
- GC candidate bytes, exact-version deletes, and failures;
- node-index checkpoint age and rebuild fallbacks.

The three-call write budget counts SDK operations, not internal HTTP retry
attempts. Alert on either dimension independently.

Tune `max_parallel_payload_writes` to the service's connection pool and S3
request-rate budget. Bound in-process metadata with `max_cached_commits`,
`max_cached_branches`, and `max_cached_node_pack_bytes`; do not disable these
bounds in a long-running writer. Alert when publication wait consumes a
material part of end-to-end latency or max queue depth grows continuously.

## Conflict and outage handling

- A stale `expected_head` or branch-ref CAS returns a conflict. Re-read and let
  the application decide whether to retry or merge.
- Never automatically rebase an atomic commit.
- Reuse a stable `OperationId` after an ambiguous response.
- Stop writes when lease renewal is ambiguous or the fence is lost.
- Perform takeover only after establishing that the previous writer cannot
  continue publishing.

## Backup and restore

A raw cross-bucket copy is not a valid repository restore: S3 assigns new
provider `VersionId` values while copied commits still reference the source
IDs. The client fails closed when those stale bindings are read.

For a portable backup, use `clone_to` to create a complete logical repository
in a versioned archive bucket. Restore by opening that archive read-only and
cloning it to the destination; both hops replay history and bind every logical
version to the destination provider's exact ID. Open the result read-only, run
`fsck`, and only then call `takeover_writer` with the previous writer ID, lease
generation, and auditable credential/process-isolation evidence.

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
   the epoch to `DiscoverRoots`.

GC v2 requires the authoritative writer lease. It marks reachable CIDs and the
actual envelopes supplying them before deletion, then names every exact
physical `VersionId`. Never bypass a `MissingCapability`, root-restart, or
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
