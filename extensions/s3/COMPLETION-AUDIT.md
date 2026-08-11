# Versioned S3 completion audit

This matrix is the requirement-by-requirement completion record for the local
RustFS delivery. It complements `QUALIFICATION.md`: that file contains the
measured evidence, while this file states which requirement each item proves.
No row is satisfied by design intent alone.

The local objective is a working bucket-level versioned S3 repository through
the adapted Rust SDK, backed by RustFS at the requested persistent data path.
It is deliberately distinct from production promotion on AWS. The latter
remains Phase 8 release evidence and is never inferred from local RustFS.

Status meanings:

- **Proven**: direct current-source, test, runtime, or signed-rehearsal evidence
  exists.
- **Running**: the exact final binary is accumulating required elapsed
  evidence; the row is not complete.
- **External release gate**: required before a production claim, but cannot be
  represented as local RustFS evidence.

## User objective

| Requirement | Status | Authoritative evidence |
| --- | --- | --- |
| Repeatable RustFS service using its S3-compatible API | **Proven** | `docker-compose.rustfs.yml`; live provider suite and runtime identity in `QUALIFICATION.md` |
| Durable RustFS data under `/Volumes/Workspace/prolly-data` | **Proven** | Compose bind mount and live container inspection record `/Volumes/Workspace/prolly-data:/data` |
| Bucket-level Git-like history rather than independent native object versions | **Proven** | Canonical commit/tree/ref model in `prolly-s3-core`; deterministic corpus, multi-process histories, rolling binaries, and live fsck evidence |
| Adapted, familiar Rust S3 client API | **Proven** | `prolly-s3-client`; compile-tested examples; compatibility manifest and manifest-parity tests |
| Current and historical object reads, logical versions, delete markers, and stable pagination | **Proven** | Unit/corpus tests plus live RustFS AWS-shaped and native differential suites |
| Atomic multi-object commits | **Proven** | Commit-session implementation, concurrency tests, and accepted-CAS outage workflow |
| Independent writers without process-local correctness | **Proven** | Conditional S3 ref CAS, 32-way CAS qualification, and independent-process branch/tag/merge contention |
| Branch, tag, log, diff, merge, restore, reset, and reflog administration | **Proven** | Maintenance matrix, multi-process suite, active-outage matrix, and backup/restore drill |
| Copy and restart-resumable multipart uploads | **Proven** | S3-shaped matrix, independent-process completion, resumable sync, and multipart outage reconciliation |
| Same-bucket SlateDB metadata without making it authoritative | **Proven** | Optional `slatedb-index` implementation, complete cache-loss/rebuild drill, canonical namespace snapshot, and body-blind transport correlation |
| Bounded streaming and retry without rereading caller bodies | **Proven** | Deterministic failure tests, 10,000-operation corpus, and 160 MiB RSS probe |
| Retention, fsck, exact-version GC, and recoverability | **Proven** | Fault-injection tests, maintenance matrix, lease fencing, IAM drill, and physical backup/restore rehearsal |
| Operational and release instructions | **Proven** | `README.md`, `OPERATIONS.md`, required CI workflow, dependency-security gate, and signed package rehearsal |
| Corrected final-source 24-hour RustFS stability run | **Incomplete** | `soak-evidence/local-20260809-24h-cleanup-v2-screen-final/soak.log` ended at iteration 37 after the local Docker daemon stopped and the provider endpoint became unreachable. A new uninterrupted run is required. |

The screen-supervised
`local-20260809-24h-cleanup-v2-screen-final` run completed 36 valid iterations,
then failed during iteration 37 when the local Docker daemon stopped and
`127.0.0.1:9000` refused connections. Its evidence remains preserved as an
incomplete diagnostic run and contributes no elapsed time to the release gate.

The earlier terminal-owned
`local-20260809-24h-cleanup-v2-final` evidence ended after 5,343 valid seconds
when its terminal session was interrupted. It remains preserved as incomplete
diagnostic evidence and contributes no elapsed time to the screen-supervised
final run.

## Declared v1 done criteria

| Criterion from the technical design | Status | Evidence class |
| --- | --- | --- |
| Familiar supported call chains compile with declared AWS types | **Proven** | Examples, downstream crates, and exact package pair |
| Every accepted operation and field is capability-described; others fail closed | **Proven** | Machine-readable compatibility manifest, parity test, and negative-field tests |
| Logical object versions and bucket commits are distinct and deterministic | **Proven** | Strong ID types, canonical fixtures, independent CBOR/ID verifier, and corpus |
| A visible ref always has a complete immutable hash-valid closure | **Proven** | Immutable-first publication, injected boundary failures, multi-process fsck, and outage reconciliation |
| Initialization converges and ordinary open is physically read-only | **Proven** | Partial-init tests and exact live native-version before/after snapshots |
| Provider behavior is qualified rather than trusted from configuration | **Proven locally** | Signed-capability model and live RustFS conformance; AWS remains an external release gate |
| Concurrent current, historical, selected-version, and paged reads stay correct | **Proven** | Corpus, stable-token tests, 32-way CAS, and live independent-process suite |
| Retry and ambiguous outcomes do not duplicate logical mutations | **Proven** | Operation records, deterministic reconciliation tests, and eight live accepted-CAS outage workflows |
| Unknown-length bodies are consumed once into bounded chunks | **Proven** | Poll-count/failure tests and resource probe |
| Multipart completion and explicit sessions publish atomically | **Proven** | Failure-boundary, concurrency, restart, and outage tests |
| Workspaces/uploads survive restart and terminal results reconcile | **Proven** | Reopened resumable sync and independent-process multipart recovery |
| Full history administration is deterministic and multi-process safe | **Proven** | Maintenance, rolling-binary, and independent-process matrices |
| SlateDB loss affects performance only | **Proven** | Complete live cache deletion/rebuild with unchanged canonical snapshot |
| Clone, fsck, retention, GC, sweep, and recovery are bounded and rehearsed | **Proven** | Cross-repository, maintenance, fault, IAM, and backup/restore matrices |
| GC protects active leases and exact-deletes native physical versions | **Proven** | Lease-fencing tests and live exact-version deletion/sweep evidence |
| Resource use and provider amplification are measured and regression-bounded | **Proven locally** | Published cost matrices, checked-in request budgets, fail-closed 1/8/32 contention tiers, RSS probe, and qualification baselines; AWS release evidence remains external |
| Open starts no hidden worker | **Proven** | API/lifecycle implementation audit and read-only namespace snapshot |
| Clean downstream packages compile at declared toolchains/dependencies | **Proven** | Rust 1.89 core, Rust 1.94.1 client, newest-line check, offline exact-pair package rehearsal |
| Upgrade and rollback are negotiated and tested | **Proven** | New→legacy→new packaged rolling rehearsal and fail-closed future-format fixtures |

## Production promotion gates

These rows remain intentionally open even when the local RustFS objective is
complete. They prevent a local development result from being mislabeled as a
production AWS release.

| Gate | Status | Required evidence |
| --- | --- | --- |
| AWS general-purpose buckets, versioning disabled and enabled | **External release gate** | Dated `aws_qualification` records from isolated release buckets |
| Complete AWS-native differential behavior | **External release gate** | Signed conditions/ranges/pagination/error matrix |
| Release-topology outage, cost, contention, IAM, recovery, and backup drills | **External release gate** | Dated topology-specific evidence and approved limits |
| Clean-source packages signed by the controlled operator key | **External release gate** | Final closed artifact set, manifest, signature, and rollback record |

## Final close procedure

After the active soak exits successfully:

1. Run the independent soak verifier with a minimum of 86,400 seconds.
2. Recompute and verify every generated evidence checksum.
3. Confirm the exact test binary digest matches the start record.
4. Confirm every iteration has exactly two workflow, test, cleanup, and fsck
   results, with no failures or residue.
5. Confirm zero RustFS restarts and all memory, per-iteration data, total data,
   workflow-footprint, and build-growth limits.
6. Re-run formatting, strict lint, dependency security, compatibility parity,
   and the ordinary non-corpus test suite if source code changed after the
   qualified binary was built.
7. Update `QUALIFICATION.md` with the terminal measurements and change the
   final local row above from **Running** to **Proven**.
8. Audit the worktree for placeholders, generated residue, and unrelated
   modifications before making the local-completion claim.
