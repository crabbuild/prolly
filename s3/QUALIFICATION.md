# Versioned S3 qualification record

This record distinguishes executable local evidence from production-release
evidence that requires an AWS account, elapsed soak time, and operator drills.
It is not a production attestation.

## Local qualification — 2026-08-09

- Host: Apple arm64 development workstation. The broad workspace/RustFS
  requalification used the unoptimized test profile with debug information
  disabled to bound generated artifact size; the final 82-case advisory-slice
  rerun used the default debug test profile on the external build volume. The
  separately reported corpus and RSS observations retain their stated
  debug-fixture context.
- RustFS: `rustfs/rustfs:1.0.0-beta.10`, OCI index digest
  `sha256:60f4f2f41ce95216f8cac676e69f9d90c0bfec458a3bc7fd7fb9b7c2452ac57a`,
  healthy loopback service on ports 9000/9001, with
  `/Volumes/Workspace/prolly-data:/data`.
- Toolchains: core check passed on Rust 1.89.0; all-feature workspace and
  minimal client checks passed on exact Rust 1.94.1. Separate locked
  downstream crates also passed on Rust 1.89.0 for core and Rust 1.94.1 for
  minimal/AWS-only and SlateDB client surfaces.
- Static quality: workspace/all-target/all-feature Clippy passed with
  `-D warnings`; formatting, compatibility JSON, compatibility-manifest parity,
  and the independent Python canonical-CBOR/ID verifier passed.
- Dependency security: `aws-sdk-s3` default features are disabled and only its
  modern `default-https-client`, HTTP 1.x, Tokio, and SigV4a features are
  enabled. The resolved graph contains Rustls 0.23.43 and rustls-webpki 0.103.13
  and no Rustls 0.21/rustls-webpki 0.101 legacy path. SlateDB default features
  are disabled and only its AWS object-store builder is enabled; consequently the
  unused Foyer/Bincode/Paste cache path and the unrelated
  `prolly-store-slatedb` adapter are absent. `cargo deny` rejects all
  vulnerability and unsoundness advisories. Its one exact, reason-bearing
  exception is RUSTSEC-2021-0127, an unmaintained-only notice for the canonical
  v1 `serde_cbor` codec; replacement is a durable-format migration, not a safe
  patch-level upgrade.
- Automated cases: 98 unit/integration/fixture/qualification-harness test
  entries passed with the expensive deterministic corpus excluded. Opt-in
  provider entries are accounted for separately below rather than treating a
  flag-disabled return as live evidence. The separate
  10,000-operation versioned/unversioned memory-plane corpus also passed after every
  operation, across repeated reopen points, final canonical commit comparison,
  version comparison, and fsck on the final audited source with Rust 1.89.0
  (1,102.53 seconds).
- Live RustFS: all 11 normally enabled provider scenarios passed, including
  32-way CAS,
  exact native-version deletion, independent-process ref/tag/merge and
  multipart completion contention, reopened resumable sync, SlateDB
  quarantine/rebuild, AWS-shaped conditions/ranges/listing, a dedicated
  physically versioned reflog/reset recovery drill, bounded hot-branch
  contention, and GC fencing. The 20 flag/helper-gated binary entries are not
  counted as default live scenarios; restart, eight-workflow active outage,
  1/8/32 contention, ordinary throughput, resource, and S3-shaped cost probes
  were each run explicitly through their dedicated flags/scripts.
- Native differential: raw RustFS and the adapter agree on payload/checksum,
  closed/open/suffix ranges, date conditions, ordinary range errors, and
  delimiter grouping. RustFS beta.10 returns 416 before evaluating a failing
  `If-Match` on an unsatisfiable range; the adapter deliberately returns 412 in
  accordance with RFC 9110's precondition-before-Range order. This provider
  deviation is pinned in the compatibility manifest.
- Recovery: fault tests passed for durable sync checkpoints, fixed-time GC mark
  restart records, CAS-checkpointed GC sweep, cache rebuild checkpoints,
  operation reconciliation, cancellation, and immutable prewrite boundaries.
  Fsck injection now covers missing and corrupt reachable nodes, chunks,
  content manifests, commits, and deltas, plus missing ref targets and
  identity-checked missing-object repair.
- Complete advisory-cache loss: a live physically versioned RustFS drill wrote
  canonical main and feature histories, closed SlateDB, deleted every current
  object under the owner-derived cache prefix, verified native cache delete
  markers, recreated the database, rebuilt both heads, reread both payloads,
  and fscked. Exact canonical physical-version snapshots were identical before
  cache deletion and after rebuild.
- Namespace/open safety: `Client::physical_layout()` exposes all 21 stable path
  families without provider I/O. The live RustFS test snapshots every native
  version and delete marker before and after ordinary `open` and proves the
  physical namespace is byte-for-byte unchanged.
- Upgrade negotiation: future `min_reader_version`, `min_writer_version`, and
  required-capability-profile fixtures fail with
  `UnsupportedRepositoryFormat`, and physical-version snapshots prove rejected
  opens perform no write. Profile 1 remains an omitted/default trailing field,
  preserving the original v1 marker bytes; a legacy-marker fixture opens with
  no physical mutation.
- Rolling binaries: `scripts/run_rustfs_rolling_upgrade.sh` compiled one current
  codec and one codec with the appended profile field physically absent. New,
  legacy, then new writers exchanged commits through a physically versioned
  RustFS bucket; both binaries read and fscked the combined history. Future
  reader, writer, and capability requirements were then tested separately;
  both binaries returned `UnsupportedRepositoryFormat`, failed closed, and
  exact native-version snapshot digests were unchanged around every rejected
  open. The legacy format decoder distinguishes a canonical appended field
  from corrupt CBOR; the harness no longer accepts `CorruptCommit` as an
  upgrade-negotiation result.
- Signed packaged rolling rehearsal: `scripts/run_signed_release_rehearsal.sh`
  packaged both workspace crates, extracted them, and compiled the client
  against that exact extracted core in offline mode, preventing substitution
  of an older registry sibling. It then compiled both rolling binaries from
  the same `.crate` archives, performed the live new→legacy→new exchange, and
  observed six exact `UnsupportedRepositoryFormat` rejections. Its closed
  twelve-artifact evidence set adds the exact-pair verification log plus the
  dependency-audit policy/result to the crates, lockfile,
  compatibility/canonical fixtures, package inventories, rolling log, and
  public key. The Ed25519 signature and every
  artifact digest reverified; the local manifest SHA-256 was
  `259fb064b4615e79ec433ab9d4feb47f55e384f1fa6c611753f74f18ac18a39b`.
  This run deliberately records `source_state=dirty` and
  `signer_mode=ephemeral-local-rehearsal`, so it proves the release workflow but
  is not the clean, operator-key-signed release record.
- Wire retry telemetry: the public `S3WireAttemptInterceptor` counts SDK
  executions, actual before-transmit hooks, completed attempts, response status
  classes, and attempts without a response. A deterministic local server
  returned 503 then 200; the fixture observed exactly one execution, two wire
  transmissions, one retry, one server-error response, and one success. The
  refreshed ordinary RustFS probe observed 1,134 executions/transmissions for
  writes and 100 for reads, with zero SDK retry transmissions.
- S3-shaped cost matrix: 17 measured object rows covered put/head/current and
  historical get/object and version listing/copy/single and atomic
  multi-delete/multipart create-upload-list-complete-abort-part-copy, plus a
  two-object atomic commit. Every row reported logical bytes, latency, issued
  call mix, actual transmissions/retries, transferred body bytes, and exact
  physical-version storage growth through a separate uninstrumented accounting
  client; all rows completed with zero SDK retries and final fsck passed.
- Repository maintenance cost matrix: 24 additional rows ran in an isolated
  physically versioned bucket and covered head/log, branch/tag/retention-pin
  administration, reflog/native-ref history, diff/merge planning/merge,
  restore/reset, commit and repository fsck, plus a GC reclaim plan and actual
  exact-version sweep of a known unreachable content closure. Every row
  reported issued calls, actual transmissions, transferred bytes, latency, and
  physical-version storage growth; all rows completed with zero SDK retries.
- Cross-repository cost matrix: four rows covered qualified clone, ordinary
  fetch, resumable fetch, and push. Clone exposed 71 object-plane calls plus
  three explicitly classified provider control-plane qualification requests;
  fetch/resumable-fetch/push issued 84/111/130 object-plane calls in the
  measured fixture. Both source and destination metrics were aggregated and
  every row completed with zero SDK retries and final fsck on both sides.
- Advisory rebuild cost matrix: one row rebuilt nine branch heads while
  quarantining one corrupt cache entry. Canonical authority discovery issued
  10 SDK calls/transmissions with zero retries; the separate SlateDB
  `object_store` decorator observed 19 API calls (13 puts, four gets, two
  lists), 5,545–5,625 uploaded bytes, and 164 returned bytes across the
  serialized and focused reruns. Authoritative plus cache physical-version
  storage grew by the same 5,545–5,625 bytes. A dedicated body-blind proxy run
  then correlated the complete SlateDB test lifecycle: 125 `object_store` API
  calls mapped one-to-one to 125 provider HTTP attempts (91 GET, nine HEAD,
  25 PUT), all with unique RustFS request IDs. Responses were 84 successes and
  41 expected discovery misses; there were no redirects, authorization errors,
  throttles, server errors, response-less attempts, or transport retries.
- Soak harness: the final-source smoke evidence at
  `/Volumes/Workspace/prolly-build/versioned-s3/soak-evidence/local-20260809-cleanup-v2-cadence-smoke-final`
  passed one complete iteration and two independent-process workflows in exactly 10 seconds.
  The pinned test-binary SHA-256 was
  `6fb926ddd75e11523bf34aa38acc0f2cf5ecb53fe2199f8c23018f9427d9e169`;
  every workflow emitted an exact physical footprint and final successful
  fsck, then exact-deleted 239 physical versions and proved zero remained.
  Independent verification observed 1,764 KiB provider-data growth, 4 KiB
  build growth, 206,045,184-byte peak RustFS memory, unchanged restart count,
  and a 37,209-byte maximum workflow footprint. The harness starts its
  deadline after baseline scans, invokes one prebuilt binary directly, and
  always executes at least one iteration; the required 24-hour run remains open.
- Provider restart: `scripts/run_rustfs_restart_drill.sh` passed both phases.
  It fscked durable state, restarted `prolly-rustfs`, waited for Docker health
  and authenticated S3 readiness, reopened and verified the pre-restart
  payload, published after restart, and fscked again. The refreshed run took 13
  seconds and recorded 196 KiB of data-directory growth. This stronger gate was
  added after a chaos run observed Docker `healthy` while RustFS still returned
  `503 Service not ready: waiting for iam`.
- Active provider outage: `scripts/run_rustfs_active_outage_drill.sh` passed an
  eight-workflow matrix: ordinary put, two-parent merge, multipart completion,
  atomic two-key workspace publication, atomic multi-delete, restore,
  administrative reset, and branch tombstone. Each scenario discarded an
  accepted ref-CAS response and restarted RustFS before repository control
  returned. Readiness required four consecutive authenticated S3 probes. The
  41-second run performed 375 first-attempt SDK calls with zero wire retries;
  eight restarts consumed 30.232 seconds and the provider directory grew 1,296
  KiB. Operation-bearing workflows reconciled exactly one bucket commit and
  bounded any replay growth to publication coordination. Reset and branch
  deletion reconciled as ref-only operations and an identical replay created
  zero physical versions and zero commits. Payloads, delete markers, merge
  parents, reflog counts, and exact operation/ref/tombstone outcomes matched;
  every final fsck passed. The evidence-preserving runner and independent
  verifier reject missing, duplicate, or weakened scenario results.
- IAM and credential rotation: `scripts/run_rustfs_iam_drill.sh` passed in 23
  seconds with 328 KiB provider-data growth. A generated prefix-scoped policy
  allowed adapter read/write/fsck while five negative probes denied
  cross-prefix put/list, current and selected-version physical deletion, and
  bucket-versioning mutation. A second identity published successfully before
  the old identity was disabled; the revoked identity then failed adapter open
  as non-retryable `PermissionDenied`. Administrator verification reread all
  three payloads, found exactly four commits, confirmed native bucket
  versioning remained enabled, and fscked. Both users and the custom policy were
  removed. RustFS beta.10 exposed two action-model deviations: `ListBucket`
  also authorized native-version listing, and `GetObject` authorized a
  selected-version read. Its HTTP 400 disabled-key response is normalized by
  the adapter to terminal permission denial.
- Physical backup and restore: `scripts/run_rustfs_backup_restore_drill.sh`
  passed in 16 seconds. It captured a stable 112-version source inventory into
  111 archive bodies plus one delete-marker record, totaling 21,769 body
  bytes, and stored a canonical manifest with SHA-256
  `8f08461afaaf61389c2439341062525a9cf7d9212de67a357a5452035040d57e`.
  Replaying the archive reconstructed all 112 physical revisions in a fresh
  versioned bucket. Independent target qualification then proved the same
  repository identity, main/feature/tag history, retained logical historical
  read, and three native main-ref revisions; a new publication and full fsck
  passed. Exact-version cleanup removed the source, archive, and restore
  buckets. The provider directory grew 2,940 KiB including RustFS metadata and
  deleted-version reclamation bookkeeping.

## Measured development baselines

These are host-specific observations, not service-level objectives.

| Workload | Observation |
| --- | --- |
| Deterministic engine corpus | Final audited source on Rust 1.89.0: 10,000 paired logical mutations in 1,102.53 s; 9.07 paired mutations/s; retained dual-store history used roughly 2.5 GiB RSS near completion |
| RustFS ordinary sequential | Refreshed instrumented run, 20 × 64 KiB: 10.061 s writes (1.988 puts/s), 0.188 s reads (106.469 gets/s). Writes issued 1,134 object-plane SDK calls (56.70/op), uploaded 1.182× logical bytes, and downloaded 1.175×; reads issued 100 calls (5/op) and downloaded 1.016×. The Smithy interceptor observed the same 1,134/100 wire transmissions and zero SDK retries. |
| RustFS S3-shaped cost matrix | Serialized runner observations: 64 KiB put: 51 calls and roughly +69.6 KiB of physical-version bytes; head/list/list-versions: 2 calls each, no storage growth; current/historical 64 KiB get: 5 calls and about 1.016× download each; zero-copy copy: 44 calls; delete: 39 calls; two-key delete: 47 calls; multipart create/upload/list-parts/list-uploads 2/10/2/4 calls; complete varied from 61–71 calls as generated catalog/tree identifiers crossed chunk boundaries; abort: 4 calls; part-copy: 13 calls; two-object 128 KiB atomic commit varied from 74–84 calls and approximately 1.125–1.128× upload. Zero SDK retries throughout. |
| RustFS repository maintenance matrix | Serialized 24-row physically versioned run: head/log/list-branches 1/4/3 calls; branch create/delete 11/4; tag create/list/delete 11/2/4; retention-pin create/list/delete 11/2/3; diff/merge-bases/plan-merge 2/8/13; merge/restore/reset 63/58/18; reflog/native-ref versions 7/7; commit/repository fsck 21/28. A known 1 KiB unreachable content closure produced a 16-call GC plan and a 29-call exact-version sweep (three deletes, −969 physical bytes), followed by successful fsck. Zero SDK retries throughout. |
| RustFS cross-repository matrix | Qualified clone: 71 object-plane calls plus three provider control-plane calls, 1.053× upload and 1.042× download relative to 13,433 immutable bytes; ordinary fetch/resumable fetch/push: 84/111/130 calls and +6,982/+7,888/+898 physical bytes. Zero SDK retries; source and destination fsck passed. |
| RustFS SlateDB advisory rebuild | Nine canonical branch heads plus one corrupt cache entry: 10 canonical SDK calls/transmissions, zero retries, and 1.277–1.299 s elapsed. Across the serialized and race-hardened reruns, the separately counted SlateDB stack issued 19 `object_store` API calls (13 puts, four gets, two lists), uploaded 5,545–5,625 bytes, returned 164 bytes, and added the same 5,545–5,625 physical-version bytes across authority and cache prefixes. A body-blind dedicated proxy correlated the full lifecycle one-to-one: 125 API calls and 125 HTTP attempts (91 GET, nine HEAD, 25 PUT), 125 unique RustFS request IDs, 84 successes, and 41 expected discovery misses with no unexpected response class. |
| RustFS accepted-CAS active-outage matrix | Eight serial workflows covered ordinary put, two-parent merge, multipart completion, atomic workspace publication, atomic multi-delete, restore, administrative reset, and branch tombstone. Every accepted ref-CAS response was discarded before a RustFS restart, followed by four consecutive authenticated readiness probes. The 41 s run issued 375 first-attempt SDK calls with zero wire retries; eight restarts used 30.232 s and provider data grew 1,296 KiB. Operation-bearing workflows reconciled one bucket commit with coordination-only replay growth; ref-only reset/delete replay created zero physical versions or commits. Exact operation/ref/tombstone state, payloads, delete markers, merge parents, reflogs, and fsck passed. |
| RustFS IAM/credential rotation | Generated prefix-only runtime policy; five denied cross-prefix/delete/versioning probes; old/new overlap publication; old-key disable mapped to non-retryable `PermissionDenied`; three payload rereads, four-commit history, enabled bucket versioning, fsck, and removal of two users plus one policy. Completed in 23 s with +328 KiB provider data. RustFS beta.10 aliases native-version list/read to ordinary list/get grants, so the local runtime role is recovery-read capable. |
| RustFS physical backup/restore | Stable 112-version source inventory archived as 111 hashed bodies plus one delete-marker record and a canonical hashed manifest (21,769 body bytes). Replay reconstructed 112 versions in a fresh versioned bucket; independent qualification preserved repository identity, main/feature/tag and logical historical reads, three native ref revisions, post-restore publication, and fsck. Three generated buckets were exact-version cleaned. Completed in 16 s with +2,940 KiB provider-directory growth. |
| RustFS signed packaged rolling rehearsal | Cargo produced the exact `prolly-s3-core` and `prolly-s3-client` archives; an offline extracted-pair check prevented registry sibling substitution before current and field-absent legacy binaries were built from those archives. The mandatory dependency gate excluded legacy TLS and unused Foyer/Bincode/Paste paths and allowed only the documented unmaintained canonical-codec advisory. New→legacy→new publication, dual fsck, six exact `UnsupportedRepositoryFormat` rejections, and unchanged physical snapshots passed. The final-source signed twelve-artifact set includes the dependency policy/result and reverified with manifest SHA-256 `259fb064b4615e79ec433ab9d4feb47f55e384f1fa6c611753f74f18ac18a39b`. Local signer/source state was explicitly ephemeral/dirty. |
| RustFS hot branch, 1 writer | p50/p95/p99/max 265.583 ms; 51 issued object-plane calls/write |
| RustFS hot branch, 8 writers | p50 2,447.503 ms; p95/p99/max 3,901.975 ms; 170 issued calls/write |
| RustFS hot branch, 32 writers | p50 30,452.888 ms; p95 37,216.516 ms; p99/max 37,351.528 ms; 579.5 issued calls/write |
| RustFS streamed multipart | Final-source 160 MiB upload + streamed read in 29.57 s; 107,921,408-byte maximum RSS; prior 8,830,976-byte no-op baseline retained as measurement context |

The single-node RustFS development profile is supported through 32 concurrent
hot-branch writers. A 128-writer pressure attempt encountered provider 503s and
did not converge within 3.5 minutes even with idempotent reconciliation, so 128
is not a qualified tier for this profile. The checked-in probe now has a hard
per-tier deadline and higher tiers are opt-in.

## Gates intentionally still open

Do not call this build production-qualified until all of the following evidence
is attached to a dated, signed release record:

1. Run `client/tests/aws_qualification.rs` against isolated AWS general-purpose
   buckets with native versioning disabled and enabled, plus real unsupported
   bucket/access-point identifiers.
2. Run and publish the complete native-S3 differential matrix for conditions,
   range precedence, delimiter pagination, and error mapping.
3. Complete the 24-hour multi-process soak and repeat or extend the verified
   eight-workflow accepted-CAS outage matrix on the release topology, retaining
   resource-growth and ambiguous-outcome accounting.
4. Repeat all cost and contention measurements, including the body-blind
   SlateDB HTTP correlation drill, on the release topology; qualify a higher
   supported maximum than this development profile if required. The local
   RustFS transport correlation is complete but is not release-topology
   evidence.
5. Repeat least-privilege IAM on AWS to prove distinct native-version actions;
   rehearse reflog/native-version recovery, GC approval and abort,
   backup/restore including external bucket controls and signing inventories,
   cache loss, and RustFS/AWS outage procedures in the release environment.
   The local RustFS credential-rotation and physical-version restore drills are
   complete, but the measured action aliases are not AWS IAM evidence and local
   manifests are not signed release backups.
6. Produce the final package, compatibility, cost, and rollback evidence from
   a clean source revision using the controlled operator release key. The local
   signed packaged rolling rehearsal is complete, but its explicit ephemeral
   key and dirty-source labels cannot be promoted into release evidence.

The phase plan in `plans/020-versioned-s3-client-adapter.md` remains the source
of truth for the exact acceptance criteria and rollback boundaries.
