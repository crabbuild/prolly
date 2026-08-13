# Prolly S3 enterprise-readiness audit

Audit date: 2026-08-13

Baseline: `ff6beb10` (`origin/main` at audit start)

Scope: `extensions/s3/core`, `extensions/s3/client`, protocol documents,
qualification tests, CI gates, and runnable examples

## Verdict

Prolly S3 is suitable for a controlled production pilot after workload-specific
AWS qualification. It is not yet justified to label it universally
“enterprise production ready.” The core durability design and deterministic
test coverage are strong, but enterprise promotion still depends on evidence
that cannot be established by source inspection or RustFS alone: AWS latency,
cost, throttling, recovery, multi-process operations, and representative-scale
results.

No data-loss defect was found in the audited deterministic paths. The audit did
find release-process and documentation gaps; the accompanying changes make
RustFS integration required in pull requests, align the AWS qualification
contract, update `API.md`, and add runnable scenario examples.

## Readiness by area

| Area | Assessment | Evidence or remaining condition |
|---|---|---|
| Immutable data model | Ready | Content-addressed payloads/nodes, canonical commit identities, exact-version reads and deletes |
| Atomic publication | Ready | Branch ref CAS is the commit point; ambiguous outcomes reconcile by operation ID |
| Writer fencing | Ready with runbook | Branch-scoped leases, renewal, takeover barrier, fenced-branch reporting |
| History and merge | Ready | Bounded log/diff/reflog, restartable restore and structural merge, DAG-preserving transfer |
| Integrity and repair | Ready | Metadata/deep fsck, logical repair, downloaded-content backup verification |
| Garbage collection | Ready pending live fault injection | Bounded exact-version sweep plus durable cross-process publication admission and expiring tickets |
| Local provider compatibility | Ready | Eight live RustFS integration tests and all six runnable examples passed against the pinned image |
| AWS compatibility | Not yet evidenced here | Operator-owned general-purpose versioned bucket qualification is required |
| Performance and scale | Not generally qualified | 10K and AWS SLO gates exist but require recorded workload-specific runs |
| Security and supply chain | Strong baseline | Unsafe Rust forbidden, locked dependencies, strict Clippy, dependency policy checks |
| Disaster recovery | Conditional | Logical transfer/verification exists; replication and isolated restore drills remain operator requirements |
| API documentation | Updated | Public client surface is categorized in `API.md`; scenario examples compile as Cargo targets |
| API stability | Pre-1.0 | Crate version is 0.1.0; enterprises need a compatibility/versioning policy before broad adoption |

## Strengths verified in source

- Logical files are immutable whole-file payload objects. Prolly state and
  commits reference provider-version bindings instead of overwriting user keys.
- Branch publication, authority, tags, catalogs, operation indexes, and journal
  indexes have explicit state machines and bounded continuations.
- Same-branch publication is serialized and CAS-protected; independent branches
  use independent authority and publication lanes.
- Durable commit sessions checkpoint staged metadata and reuse already uploaded
  payloads after restart.
- Historical transfer maps the source commit closure parent-first and preserves
  merge topology with destination-local identities.
- Deep fsck hashes reachable bodies. Backup verification downloads and compares
  logical content on both repositories.
- GC marks branch/tag/pin roots, catches dirty roots before sweep, and deletes
  exact physical versions rather than issuing unqualified deletes.
- Core and client crates forbid unsafe Rust. Production-source `expect` calls
  inspected by this audit follow immediately established invariants; no
  `todo!` or `unimplemented!` remains in the extension.

## Findings corrected by this audit

### Required RustFS tests previously self-skipped

The ordinary test command compiled `rustfs_repository`, but every provider test
returned success without doing work unless `PROLLY_S3_RUSTFS=1` was set. CI now
starts the digest-pinned RustFS image, waits for health, runs the integration
suite with the flag enabled, emits diagnostics on failure, and tears it down.

### AWS qualification instructions could run zero useful tests

The guide used `PROLLY_S3_AWS_QUALIFICATION`, `PROLLY_S3_AWS_BUCKET`, and
`PROLLY_S3_AWS_REGION`, while the harness read legacy names. The guide also
passed `--ignored` to a non-ignored matrix test. The harness now accepts the
documented names (with legacy fallbacks), and the commands invoke the intended
tests.

### Public API documentation was incomplete

The previous task table omitted public listing, transfer lifecycle, catalog,
index-rebuild, takeover, cleanup, observability, and commit-session methods.
`API.md` now covers normal, transfer, and administrative surfaces and explains
the difference between snapshot synchronization and history synchronization.

### Runnable coverage was too narrow

The previous repository had one combined demo. Six standalone Cargo examples
now cover ordinary object operations, durable batching and streaming, branch
merge, restore/reflog recovery, history transfer/backup verification, and
fsck/cache/metrics/retention/GC.

### S3 range reads compared incompatible checksums

The live basic-object example exposed a provider-adapter defect: an S3 range
response was hashed as a byte slice, then compared with the immutable
whole-object checksum. The adapter now retains the whole-object digest stored
on immutable payload metadata while the repository still checks the provider
ETag and exact version binding. A RustFS assertion exercises a historical range
read so the memory provider can no longer mask this behavior.

## Evidence collected in this audit

- Eight non-ignored `rustfs_repository` tests passed against the pinned,
  versioned RustFS container; the 10K provider gate remains explicitly ignored
  in ordinary runs.
- All six standalone examples completed against RustFS, including deep fsck,
  history transfer with a merge commit, backup verification, and exact-version
  GC.
- The workspace test suite, strict Clippy, formatting, dependency policy,
  protocol conformance, declared MSRVs, minimal features, and clean downstream
  consumers passed.
- The ignored 10K-key sparse structural merge gate passed when explicitly
  enabled. Its paired 4K first-parent-history gate remained CPU-bound beyond a
  15-minute debug-build qualification limit and was terminated; deep-history
  construction throughput therefore remains an open performance blocker.
- The exact 10K-commit RustFS gate at 32-way client concurrency also remained
  active beyond the 15-minute qualification limit and was terminated without
  an operation error. Because the harness emits no intermediate commit count,
  neither completion nor a partial throughput figure can be claimed; this is
  an unresolved hot-branch performance and qualification-observability gap.
- The AWS performance test was verified to fail closed when its operator-owned
  bucket and explicit SLO inputs were absent.

## Conditions blocking a universal enterprise-ready claim

1. **AWS SLO evidence is environment-specific.** Run the qualification and
   performance gates in every supported region, storage/lifecycle profile, IAM
   policy, and expected concurrency tier. Record p50/p95/p99, throughput,
   requests per operation, retries, throttling, and cost.
2. **Representative cardinality is not proven.** The 10K sparse-merge gate
   passed, but the 10K RustFS commit and 4K graph gates did not complete inside
   this audit's 15-minute limit. Even passing those regression tests would not
   prove millions or billions of files, commits, or refs.
3. **Cross-process GC needs provider fault evidence.** The protocol now closes
   durable publication admission and drains expiring per-publication tickets,
   but crash/timeout races still require live multi-process fault injection on
   every supported provider.
4. **Large-file multipart needs provider evidence.** Streamed files at or above
   64 MiB now use bounded multipart upload through the 5 TiB repository limit,
   but abort cleanup, retry cost, and throughput must be qualified on RustFS
   and AWS before promotion.
5. **Disaster recovery needs provider-level drills.** History transfer and
   logical verification do not prove that bucket replication, lifecycle,
   encryption-key recovery, retention, or regional failover are configured
   correctly.
6. **Operational integrations are library-level.** Metrics and provider request
   identifiers are exposed, but alerting, dashboards, tracing export, audit-log
   retention, and on-call procedures belong to the embedding service.
7. **The public crate is pre-1.0.** Publish a compatibility policy and migration
   strategy before committing multiple enterprise teams to the API and wire
   format.

## AWS substrate checks

The architecture's use of conditional writes and exact-version deletion aligns
with current AWS S3 general-purpose bucket behavior:

- AWS documents `If-None-Match`/`If-Match` preconditions and 409/412 conflict
  outcomes for conditional `PutObject`: [PutObject API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html).
- Permanently deleting one version requires its `versionId`, which matches the
  GC exact-delete contract: [S3 Versioning workflow](https://docs.aws.amazon.com/AmazonS3/latest/userguide/versioning-workflows.html).
- AWS states that scaling across prefixes is gradual and workload dependent,
  reinforcing the need for representative qualification rather than an
  “unlimited” claim: [S3 performance design patterns](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html).

Directory buckets are not a supported substitute: S3 Versioning and the same
version-ID semantics are not available there.

## Enterprise promotion checklist

- [ ] Required CI, including live RustFS integration, passes from a clean commit.
- [ ] Dependency, license, strict Clippy, MSRV, and downstream checks pass.
- [ ] AWS functional qualification passes in the intended bucket profile.
- [ ] AWS performance gate passes explicit latency, throughput, and request
      amplification SLOs at 1/8/32 concurrency.
- [ ] 10K commit and representative key-count/commit-count gates are recorded.
- [ ] GC is exercised with the real writer-quiescence procedure and retention
      policy.
- [ ] Cross-region or cross-account restore is performed into an isolated
      repository and verified deeply.
- [ ] IAM denies non-client mutation under the repository prefix and grants
      exact-version reads/deletes only to the maintenance role.
- [ ] Encryption, key recovery, lifecycle, replication, object lock, and legal
      hold interactions are explicitly tested or prohibited.
- [ ] Cache-loss cold start, throttling, network fault, ambiguous response,
      takeover, and authority-renewal incidents meet recovery objectives.
- [ ] Dashboards, alerts, audit retention, capacity thresholds, and on-call
      procedures are approved.

Only after these deployment-specific checks pass should an operator call that
particular Prolly S3 deployment enterprise production ready.
