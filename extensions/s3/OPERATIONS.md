# Native-versioned S3 operations

## Deployment contract

- Enable bucket versioning before repository initialization.
- Route all managed-key mutations through one writer service.
- Give reader processes `.read_only(true)` clients.
- Keep `.prolly/v1/` reserved from application keys.
- Disable lifecycle expiry for managed current and noncurrent versions.
- Store provider-attestation and pagination HMAC keys in a secret manager.
- Use independent keys and rotate them with an overlap window.

## IAM capabilities

The writer needs object-version reads, whole-object writes, native multipart,
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
- writer queue depth and wait time;
- lease renewal latency, ambiguity, and fencing events;
- unreachable native versions and node packs;
- GC candidate bytes, exact-version deletes, and failures;
- node-index checkpoint age and rebuild fallbacks.

The four-call write budget counts SDK operations, not internal HTTP retry
attempts. Alert on either dimension independently.

## Conflict and outage handling

- A stale `expected_head` or branch-ref CAS returns a conflict. Re-read and let
  the application decide whether to retry or merge.
- Never automatically rebase an atomic commit.
- Reuse a stable `OperationId` after an ambiguous response.
- Stop writes when lease renewal is ambiguous or the fence is lost.
- Perform takeover only after establishing that the previous writer cannot
  continue publishing.

## Backup and restore

A physical backup must retain every object version and delete marker for both
managed user keys and `.prolly/v1/`. A current-key-only copy is insufficient.
Restore repository metadata and native versions together, then open read-only,
run `fsck`, and only then allow writer takeover.

Cross-bucket logical clone is different from physical restore: it copies
reachable content and rebinds provider `VersionId` values, producing
destination-local commit IDs.

## Garbage collection

Use only repository GC for managed versions. Review a dry run, retain required
reflogs and pins, apply a conservative grace period, and pace exact-version
deletes. Abort if reachability or provider listing is incomplete.

## Local RustFS

```bash
docker compose -f extensions/s3/docker-compose.rustfs.yml up -d

PROLLY_S3_RUSTFS=1 \
  cargo test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --test rustfs_repository -- --nocapture
```

Local credentials in the Compose file are development-only. RustFS verifies
behavior and request shape; it does not substitute for AWS release testing.
