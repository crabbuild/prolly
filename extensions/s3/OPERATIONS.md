# Versioned S3 operations and recovery runbook

This runbook applies to the v1 Rust client adapter. The physical S3 bucket is
authoritative; SlateDB is disposable. A branch ref compare-and-exchange is the
only logical visibility point. Never repair canonical objects through raw
path-level overwrite or path-only delete.

## Deployment preflight

1. Reserve one repository prefix, normally `.prolly/v1`, and deny unrelated
   applications access beneath it.
2. Enable native bucket versioning for defense-in-depth ref recovery. Logical
   object versions remain independent of native S3 versions.
3. Ensure no lifecycle expiration or default Object Lock retention targets the
   repository prefix. Server-side default encryption is allowed.
4. Give each process a stable writer identity and give every deployment the
   same cursor-verification key ring during its TTL-plus-skew overlap window.
5. Separate provider qualification, runtime, maintenance, and recovery roles.
6. Run provider qualification before initialization. Ordinary `open` must load
   a signed, matching, unexpired attestation and performs no write.
7. Run `cargo deny --manifest-path extensions/s3/Cargo.toml --config extensions/s3/deny.toml check
   advisories` against the candidate lockfile. Reject every unapproved advisory
   and verify the lockfile contains no legacy Rustls 0.21 transport. The sole
   policy exception is the reason-bearing, unmaintained-only `serde_cbor`
   advisory; removing it requires a backward-compatible repository-format
   migration and fixture update.

For local RustFS:

```bash
docker compose --env-file extensions/s3/.env.example -f extensions/s3/docker-compose.rustfs.yml up -d
docker inspect prolly-rustfs --format '{{.State.Health.Status}}'
```

The expected durable mount is `/Volumes/Workspace/prolly-data:/data`.

## Permission boundaries

Scope bucket actions to the configured bucket and object actions to the exact
repository prefix. Scope optional SlateDB access separately to
`.prolly-cache/<repository-id>/<writer-id-hex>/`.

| Role | Required S3 policy actions | Must not receive by default |
| --- | --- | --- |
| Runtime reader/writer | `s3:ListBucket`, `s3:GetObject`, `s3:PutObject`; KMS decrypt/data-key permissions when a customer-managed key is used | `s3:DeleteObject`, `s3:DeleteObjectVersion`, bucket policy/versioning changes |
| Provider qualifier | Runtime actions plus `s3:GetBucketVersioning`, `s3:GetLifecycleConfiguration`, `s3:GetBucketObjectLockConfiguration`, `s3:ListBucketVersions`, and exact cleanup permission appropriate to the bucket profile | Bucket lifecycle/Object Lock mutation |
| Maintenance/GC | Runtime read/list actions plus `s3:GetObjectVersion`, `s3:ListBucketVersions`, `s3:DeleteObject` for qualified unversioned storage and `s3:DeleteObjectVersion` for exact versioned deletion | `s3:PutBucketVersioning`, lifecycle mutation, governance-retention bypass |
| Recovery operator | Runtime actions plus `s3:GetObjectVersion` and `s3:ListBucketVersions`; add exact-version delete only for an independently approved cleanup | Lifecycle mutation and path-only bulk deletion |
| Provisioner | Bucket creation, encryption, ownership, public-access, policy, and versioning administration | Routine application credentials |

AWS maps `ListObjectsV2` to `s3:ListBucket`, `ListObjectVersions` to
`s3:ListBucketVersions`, selected-version reads to `s3:GetObjectVersion`, and
selected-version deletes to `s3:DeleteObjectVersion`. Review the current
[AWS S3 operation-to-policy-action table](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-with-s3-policy-actions.html)
before issuing release credentials.

RustFS `1.0.0-beta.10` has a narrower IAM action model than AWS for native
versions: a prefix-scoped grant of `s3:ListBucket` also permits
`ListObjectVersions`, and `s3:GetObject` also permits a selected-version read.
Explicitly denying the version-read action also denies ordinary reads, while an
explicit `ListBucketVersions` deny does not stop version listing. Therefore the
local RustFS runtime role is effectively recovery-read capable. It still denies
cross-prefix access, physical deletion (including selected-version deletion),
and bucket-versioning mutation. Use a dedicated bucket or trust boundary where
native-version metadata/history must be hidden from routine readers, and do not
claim AWS-equivalent IAM separation until the real-AWS drill proves it.

## Normal health checks

Run these against a reopened client:

1. Verify `provider_profile()` succeeds and record the attestation ID.
2. Record `head_commit()` and `repository_id()`.
3. Run `fsck_commit(head)` for a bounded current-closure check; schedule full
   `fsck()` explicitly rather than on client open.
4. Snapshot `s3_operation_metrics()` around representative operations. These
   count object-plane SDK calls and body bytes, not Smithy-internal retries.
5. Alert on `ProviderNotQualified`, `Corrupt*`, `MissingClosure`, sustained
   `RefConflict`, `OutcomeUnknown`, or a `Running` GC fence without an owner.

## Ambiguous mutation outcome

1. Do not create a new operation ID.
2. Call `reconcile_operation(original_operation_id)`.
3. If it returns a receipt, report that receipt as success.
4. If no receipt exists and the original request body is replayable or already
   staged, retry the identical request with the same operation ID.
5. Reject any different input with that operation ID; it must return
   `IdempotencyConflict`.
6. Escalate a repeated `OutcomeUnknown` with the operation ID and provider
   request metadata. Never report a definite failure when CAS acceptance is
   still ambiguous.

## Mistaken ref movement

Prefer logical reflog recovery:

1. Stop the actor continuing the mistaken movement.
2. Record the current head as `expected_head`.
3. Inspect `list_reflog()` and identify the exact audited entry whose old or
   new target is required.
4. Run `fsck_commit(target)`.
5. Call `recover_branch(reflog_entry_id, expected_head, reason)`.
6. Verify the new head, read representative objects, and run `fsck()`.

If reflog recovery is unavailable but native bucket versioning is enabled:

1. Call `list_native_branch_ref_versions()`; do not use a raw S3 overwrite.
2. Select a record by commit target, writer, operation, generation, and physical
   timestamp. A provider-native version ID is diagnostic input, not a logical
   object version.
3. Call `recover_branch_from_native_version(version_id, expected_head, reason)`.
   The SDK decodes the canonical ref, rejects tombstones, fscks the complete
   target closure, writes a new reflog entry, and performs expected-head CAS.
4. Verify current reads and full fsck. The older native version remains intact.

The executable versioned-bucket drill is
`rustfs_reflog_ref_recovery_drill_restores_a_mistaken_reset`.

## Missing or corrupt immutable closure

For a missing object:

1. Fence writes to the affected branch.
2. Run `fsck_commit(head)` and retain its report.
3. Open a separately qualified source with the same repository identity.
4. Run `repair_missing_from(source)`. It may copy only missing reachable
   immutable members.
5. Rerun fsck before releasing the write fence.

For a present corrupt immutable, stop. The repair API deliberately refuses to
overwrite it. Preserve all native versions and logs, quarantine access to the
repository, identify a known-good native version or backup, and perform an
audited restore into a fresh namespace. Never hide corruption with an in-place
write at a content-addressed path.

## SlateDB loss or corruption

1. Keep serving canonical reads from S3.
2. Stop the process owning the affected writable cache path.
3. Preserve the quarantine/checkpoint data for diagnosis when practical.
4. Delete or move only the owner-derived `.prolly-cache` path, never the
   canonical repository prefix.
5. Reopen `SlateDbAdvisoryIndex::open_owned` with the same repository/writer
   identity and run `rebuild_advisory_index()` until completed.
6. Compare heads and logical results with canonical S3 before restoring normal
   cache use.

The local destructive-cache rehearsal uses an isolated owner-derived cache
path and proves the canonical physical-version snapshot is unchanged:

```bash
PROLLY_S3_RUSTFS=1 cargo +1.94.1 test --manifest-path extensions/s3/Cargo.toml \
  -p prolly-s3-client --all-features --test rustfs_repository \
  rustfs_complete_slatedb_cache_loss_rebuilds_from_canonical_s3 -- \
  --nocapture --test-threads=1
```

## GC approval, execution, and abort

1. Run resumable mark with a grace period at least twice the publication lease.
2. Wait for a completed mark record and load its immutable plan.
3. Review candidate counts, bytes, kinds, cutoff, retained roots, and exact
   physical version IDs. Archive the approval record.
4. Start `sweep_gc_batch` with a bounded batch and configured rate limit.
5. Continue with the same plan ID until complete. A running sweep fences all ref
   publication across worker loss.
6. If the worker disappears, first prove no worker can still issue a delete.
   Load the current run generation, then call `abort_gc_run(plan, generation,
   reason)`. A stale generation must fail.
7. Run full fsck after completion or abort.

On a versioned bucket, every destructive request must include the recorded
native `versionId`. AWS documents that a path-only delete creates a delete
marker, whereas a selected-version delete permanently removes that version;
see [DeleteObject](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html).

## Provider outage and restart

1. Stop new work at the admission layer; do not disable qualification checks.
2. Retain operation IDs for every in-flight mutation.
3. After provider health returns, reopen normally using the existing signed
   attestation. Requalify only if the endpoint/bucket identity or attestation
   validity requires it.
4. Reconcile every ambiguous operation, then fsck affected heads.
5. Verify both reads and a new conditional publication before restoring traffic.

For local RustFS, execute:

```bash
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_restart_drill.sh
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_active_outage_drill.sh
```

Docker health alone is insufficient: RustFS may temporarily return a 503 while
IAM state is still loading. The restart drill requires authenticated S3
readiness; the active-outage drill requires four consecutive authenticated
probes before reopening or reconciling. The active drill accepts a real ref
CAS, loses its response, restarts RustFS, and reconciles the exact logical
operation. It covers ordinary put, two-parent merge, multipart completion,
atomic workspace publication, atomic multi-delete, restore, administrative
reset, and branch tombstone. Operation-bearing replay must add no second bucket
commit and may grow only publication coordination. Ref-only reset and branch
deletion replay must add no physical version or commit. Run the matrix serially
against an isolated provider and preserve release evidence with a new
`PROLLY_S3_CHAOS_EVIDENCE_DIR`.

## Backup and restore

Backups must include every physical object version beneath the repository
prefix plus bucket versioning/encryption/policy configuration and the signing
key inventory. A raw current-key-only copy is insufficient for native ref
recovery.

Restore into a new empty prefix or bucket, qualify the target provider
independently, and use clone/fetch closure validation where repository identity
must be preserved. Before cutover, verify format compatibility, provider
attestation, repository ID, expected heads/tags, representative historical
versions, and full fsck. Never point writers at a partially restored prefix.

The local physical-version rehearsal is:

```bash
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_backup_restore_drill.sh
```

It quiesces a generated source repository, lists the physical inventory before
and after capture, archives every selected version body and user-metadata map
under immutable unique keys, represents delete markers in a canonical CBOR
manifest, hashes each body and the manifest, and rejects a changing source. It
then replays each key's noncurrent revisions before its latest revision into a
fresh versioned bucket, independently qualifies that bucket, and verifies
repository identity, branch/tag heads, a logical historical read, native ref
revision count, a post-restore publication, and full fsck. Cleanup enumerates
and removes exact native versions from all three generated buckets.

This rehearsal records the source versioning state but does not replace a
production backup of bucket encryption, policy, Object Lock/lifecycle
configuration, audit records, or cursor/provider-attestation signing-key
inventory. Archive those controls separately and require the source inventory
to remain unchanged for the entire data capture. An online copy without a
quiescence or snapshot boundary is not an accepted backup.

## Credential and signing-key rotation

Rotate AWS credentials through the caller-owned AWS SDK provider and confirm a
new conditional publication before revoking the old principal. Rotate cursor
HMAC keys by retaining the old verification key for maximum cursor TTL plus
clock skew. Rotate provider-attestation signing keys only after every writer can
verify both old and new keys; requalification writes a new immutable
attestation and ordinary open remains read-only.

For local RustFS, the repeatable drill creates a generated prefix-only runtime
policy and two disposable users, publishes with the old user, overlaps and
proves the new user, disables the old user, verifies terminal
`PermissionDenied`, reopens with the administrator, rereads all payloads, runs
fsck, and removes both identities and the policy:

```bash
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_iam_drill.sh
```

The drill uses the digest-pinned disposable `minio/mc` image and never prints
generated secrets. RustFS reports disabled access keys as HTTP 400
`InvalidRequest / ErrAccessKeyDisabled`; the adapter normalizes that exact
provider response to non-retryable `PermissionDenied`.

## Required release evidence

Attach dated outputs from:

```bash
bash extensions/s3/scripts/check_clean_downstream.sh
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_restart_drill.sh
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_active_outage_drill.sh
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_iam_drill.sh
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_backup_restore_drill.sh
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_contention_matrix.sh
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_cost_matrix.sh
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_slatedb_http_correlation.sh
PROLLY_S3_RUSTFS=1 bash extensions/s3/scripts/run_rustfs_rolling_upgrade.sh
PROLLY_S3_RUSTFS=1 \
PROLLY_S3_RELEASE_SIGNING_KEY=/secure/path/release-ed25519-private.pem \
  bash extensions/s3/scripts/run_signed_release_rehearsal.sh
PROLLY_S3_RUSTFS=1 \
PROLLY_S3_SOAK_SECONDS=86400 \
PROLLY_S3_SOAK_RUN_ID=release-YYYYMMDD \
PROLLY_S3_SOAK_EVIDENCE_DIR=/Volumes/Workspace/prolly-build/versioned-s3/soak-evidence/release-YYYYMMDD \
  bash extensions/s3/scripts/run_rustfs_soak.sh
```

Also attach real-AWS qualification, wire-level retry/request telemetry,
release-topology cost and SlateDB HTTP-correlation measurements, IAM simulator
or sandbox evidence, backup restore rehearsal, mixed-version upgrade results,
and signed artifacts. The signed release runner must report a clean source and
`operator-supplied` signer; an ephemeral or dirty local rehearsal is not a
production attestation. Local RustFS evidence alone is not a production
attestation.

The soak runner refuses an existing evidence directory, preserves a failed
run, pins one prebuilt test executable by SHA-256, and independently verifies
elapsed time, exact workflow/fsck records, provider identity and restart count,
mount, source/toolchain identity, memory, provider-data growth, per-workflow
physical footprint, and build growth. Archive all three generated files:
`soak.log`, `verification.log`, and `checksums.sha256`.
