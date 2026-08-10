# Versioned S3 implementation

This workspace is the executable development preview for the bucket-level
repository in
[`plans/020-versioned-s3-client-adapter.md`](../plans/020-versioned-s3-client-adapter.md).
It is an in-process Rust client adapter, not an S3 HTTP proxy: callers provide
an `aws_sdk_s3::Client`, then use familiar S3-shaped builders against a logical
bucket whose complete state is published as immutable Prolly commits.

The exact implemented surface and intentional exclusions are recorded in
[`compatibility-v1.json`](compatibility-v1.json). S3 remains authoritative.
SlateDB is optional and rebuildable; it never owns branch publication.

See [`QUALIFICATION.md`](QUALIFICATION.md) for the dated local evidence and the
AWS/soak/operational gates that remain open before a production claim.
See [`COMPLETION-AUDIT.md`](COMPLETION-AUDIT.md) for the explicit mapping from
the user objective and declared v1 done criteria to that evidence.
See [`OPERATIONS.md`](OPERATIONS.md) for IAM boundaries, ambiguity handling,
reflog/native-ref recovery, cache rebuild, GC approval/abort, provider outage,
backup/restore, and credential/key rotation procedures.
Language-neutral canonical CBOR/ID examples are checked in at
[`fixtures/canonical-v1.json`](fixtures/canonical-v1.json) and independently
round-tripped by the dependency-free Python verifier beside it.

Dependency qualification uses the AWS SDK's modern default HTTPS client with
its Rustls 0.23/AWS-LC transport. The SDK's legacy `rustls` feature is
deliberately disabled because it also enables the obsolete Rustls 0.21 stack.
SlateDB defaults are also disabled: only its AWS object-store builder is
enabled for same-bucket endpoints, while the adapter does not require SlateDB's
optional Foyer cache or the separate `prolly-store-slatedb` engine adapter.
Run `cargo deny --manifest-path s3/Cargo.toml --config s3/deny.toml check
advisories` before every candidate build. The policy denies every vulnerability
and unsoundness advisory and documents one exact unmaintained-codec exception:
`serde_cbor` is part of the v1 canonical durable format, so replacing it must be
a separately versioned, fixture-backed migration rather than a dependency-only
change.

## Safety model

- Payload chunks, content manifests, Prolly nodes, deltas, commits, reflogs,
  uploads, and workspaces are durable before a branch becomes visible.
- Every publishing mutation owns a renewable CAS lease whose immutable
  protection chain records staged physical objects and the proposed commit.
  Expired or abandoned leases cannot move a branch.
- A conditional write of one branch ref is the visibility point.
- Every object mutation has an `ObjectVersionId`; every atomic bucket snapshot
  has a separate `CommitId`; physical provider ETags are only CAS tokens.
- Reads and pagination can be pinned to an immutable commit.
- Unknown official-input fields fail with `UnsupportedParameter`.
- No maintenance or GC worker starts during `open`. GC requires an explicit,
  persisted dry-run followed by a separate sweep. Each destructive batch owns
  a durable publication fence, rechecks branch/tag heads and the current
  retained set, checkpoints progress with CAS, then deletes exact physical
  versions. An interrupted `Running` sweep fails closed without an automatic
  timeout: ref publication remains blocked until an operator proves that no
  delete worker survives and performs a generation-checked, reason-bearing
  abort.

## Local RustFS

The checked-in Compose profile pins RustFS beta.10 by its multi-architecture
OCI manifest digest and persists its data at the requested host location,
`/Volumes/Workspace/prolly-data` by default.

```bash
mkdir -p /Volumes/Workspace/prolly-data
docker compose --env-file s3/.env.example \
  -f s3/docker-compose.rustfs.yml up -d --wait
curl --fail http://127.0.0.1:9000/health
```

The defaults listen only on loopback. Compose resolves the data directory to
`/Volumes/Workspace/prolly-data`; repository data and optional SlateDB objects
can live in the same physical bucket under disjoint prefixes. The credentials
in `.env.example` are local-only and must not be reused on a reachable
deployment.

Verify the live endpoint, credentials, bucket access, byte round trip, and
exact-version cleanup using only the AWS CLI's `s3api` commands:

```bash
bash s3/scripts/verify_rustfs_aws_cli.sh
```

The verifier defaults to `prolly-versioned-s3-demo`. Override
`PROLLY_RUSTFS_ENDPOINT`, `PROLLY_RUSTFS_ACCESS_KEY`,
`PROLLY_RUSTFS_SECRET_KEY`, `PROLLY_RUSTFS_REGION`, or
`PROLLY_RUSTFS_BUCKET` as needed. Its unique probe key is outside the Prolly
repository namespace. It removes the exact probe version after a successful
check and makes a best-effort cleanup attempt if verification fails.

Stop the server without deleting data:

```bash
docker compose -f s3/docker-compose.rustfs.yml down
```

Run the complete bucket-versioning example against the loopback service:

```bash
PROLLY_RUSTFS_ENDPOINT=http://127.0.0.1:9000 \
PROLLY_RUSTFS_ACCESS_KEY=prollyadmin \
PROLLY_RUSTFS_SECRET_KEY=prolly-local-secret-change-me \
CARGO_TARGET_DIR=/Volumes/Workspace/prolly-build/versioned-s3 \
  cargo +1.94.1 run --manifest-path s3/Cargo.toml \
  -p prolly-s3-client --example rustfs_versioned_bucket
```

The example creates the physical bucket if necessary, idempotently initializes
the repository, writes two logical versions, reads the first commit through an
immutable snapshot, lists the current key, computes an exact-commit diff, and
fscks the reachable repository. Its fixed signing keys are local demonstration
credentials and must not be copied into a deployment.

## Minimal client

```rust,no_run
use std::{sync::Arc, time::{Duration, Instant}};
use aws_sdk_s3::primitives::ByteStream;
use prolly_s3_client::{
    Client, HmacAttestationSigner, HmacTokenSigner, ProviderIdentity,
};

# async fn example(aws: aws_sdk_s3::Client) -> prolly_s3_client::Result<()> {
let client = Client::builder()
    .aws_client(aws)
    .bucket("assets")
    .repository_prefix(".prolly/v1")
    .default_branch("main")
    .writer("asset-service")
    .gc_delete_rate_limit_per_second(100)
    .provider_identity(ProviderIdentity::aws_region("us-west-2"))
    .attestation_signer(Arc::new(HmacAttestationSigner::single(
        "provider-key-2026-08",
        vec![9_u8; 32],
    )?))
    .token_signer(Arc::new(HmacTokenSigner::single(
        "cursor-key-2026-08",
        vec![7_u8; 32],
    )?))
    .initialize()
    .await?;

let written = client
    .put_object()
    .bucket("assets")
    .key("images/logo.svg")
    .body(ByteStream::from_static(b"<svg/>"))
    .content_type("image/svg+xml")
    .logical_retry_limit(2)
    .deadline(Instant::now() + Duration::from_secs(30))
    .send()
    .await?;

let historical = client.at(written.snapshot).await?;
let body = historical
    .get_object()
    .bucket("assets")
    .key("images/logo.svg")
    .send()
    .await?
    .output
    .body;
# Ok(())
# }
```

Use `Client::builder().open().await` after initialization, supplying the same
provider identity and an attestation verification key ring. Initialization is
idempotent: it reuses a matching nonexpired attestation or runs an isolated
behavioral probe and persists a signed endpoint/bucket-bound record. `open`
performs no probe writes. Listing likewise requires a shared, restart-stable
token signer when pagination is possible.

Per-call deadlines bound the adapter operation through response creation.
Expiry before I/O returns `ErrorCode::Timeout`. If a publishing write reaches
its deadline after work starts, the adapter returns
`ErrorCode::OutcomeUnknown`, `RetryAdvice::ReconcileOperation`, and the stable
operation ID; call `client.reconcile_operation(id)` before deciding whether to
retry. `logical_retry_limit` overrides only the repository's ref-conflict
retry budget for that call and never changes AWS SDK transport retries.

`HmacTokenSigner::single` retains one cursor key indefinitely. Production key
rotation should use `HmacTokenSigner::managed` with `HmacTokenKey::retired` for
verification keys and preserve `HmacTokenKey::removed` tombstones after secret
removal. Client construction rejects removal until `cursor_ttl` plus
`cursor_clock_skew` has elapsed since retirement. The defaults are a 15-minute
TTL and five-minute skew; TTL is capped at 24 hours and skew at 15 minutes.

## Atomic bucket commit

```rust,no_run
# use aws_sdk_s3::primitives::ByteStream;
# async fn example(client: &prolly_s3_client::Client) -> prolly_s3_client::Result<()> {
let mut tx = client
    .begin_commit()
    .message("publish site")
    .start()
    .await?;
tx.put_object()
    .bucket(client.bucket())
    .key("index.html")
    .body(ByteStream::from_static(b"home"))
    .stage()
    .await?;
tx.put_object()
    .bucket(client.bucket())
    .key("app.js")
    .body(ByteStream::from_static(b"app"))
    .stage()
    .await?;
let receipt = tx.publish().await?;
# Ok(())
# }
```

A workspace is durable and can be resumed with
`client.resume_commit(workspace_id)`. Publication requires the exact original
base commit and never silently rebases.

## History administration

`Client` exposes explicit `merge_bases`, `plan_merge`, `merge`, `restore`,
`reset_branch`, `list_reflog`, and `recover_branch` operations. Merge is a
three-way object merge: current objects follow base/ours/theirs rules while
version and operation trees take validated unions. Multiple best bases require
an explicit choice, and same-key divergence fails unless `MergePolicy::Ours`
or `MergePolicy::Theirs` is selected. Restore creates fresh logical versions
as a child of the expected head; it never discards intervening history.

Large histories use `log_page(start, after, limit)` and
`diff_page(from, to, after_key, limit)`. Both bind traversal to immutable
commit IDs and use exclusive cursors; diff pages consume the structural Prolly
stream without materializing the complete change set. Multipart expiry is an
explicit maintenance call, `expire_multipart_uploads(limit)`—no background
worker starts during client open.

On a physically versioned provider,
`list_native_branch_ref_versions` exposes validated administrative recovery
records. `recover_branch_from_native_version` rejects tombstones, fscks the
selected target closure, and restores it through the normal expected-head,
reflog-producing reset path; it never overwrites a ref with raw provider bytes.

Maintenance is likewise explicit:

```rust,no_run
# use std::time::Duration;
# async fn example(client: &prolly_s3_client::Client) -> prolly_s3_client::Result<()> {
let dry_run = client.plan_gc(Duration::from_secs(2 * 60 * 60), 100_000).await?;
// Inspect and durably approve dry_run.plan.id before destructive execution.
let mut report = client.sweep_gc_batch(dry_run.plan.id, 1_000).await?;
while !report.complete {
    report = client.sweep_gc_batch(dry_run.plan.id, 1_000).await?;
}
# Ok(())
# }
```

Long-running planners may use
`plan_gc_resumable(run_id, grace, max_candidates)`. Its durable mark record
fixes the planning time and request identity. A replacement worker supplies the
same operation ID; it recomputes canonical reachability and CAS-publishes a
completed record containing the immutable plan ID. `gc_mark_run` reconciles a
lost response, and `load_gc_plan` retrieves the exact candidates for review.
No running or partial mark record can be swept.

If a sweep worker is confirmed dead while its run remains `Running`, load the
run and call `client.abort_gc_run(plan, run.generation, reason)`. Never abort a
worker that may still issue an exact-version delete; the durable fence is the
barrier between that worker and new publication.

The grace must be at least twice the configured publication-lease duration.
`gc_delete_rate_limit_per_second` is bound into each run when it starts; zero
means unlimited and configured values must be between 1 and 1,000. Pacing
occurs while the run remains `Running`, so new ref publication stays fenced
between exact-version deletes.
Retained roots include active refs, tags, explicit retention pins, reflogs
inside their configured recovery window, workspaces, multipart uploads, and
unexpired publication protection chains. Terminal ref records are tombstoned
so older native S3 values never become current again. V1 GC only reclaims
unreachable immutable data; it does not rewrite reachable history. It also
retains every native physical ref version indefinitely. This is conservative
recovery safety; a later bounded-ref-history collector requires a separate
audited plan.

Multipart support includes create, streamed part upload, full/ranged
`UploadPartCopy`, paged active-upload listing, list parts, idempotent complete,
abort, and explicit expiry cleanup. Terminal upload manifests remain durable
for outcome reconciliation even though they disappear from the active catalog.

`Client::clone_to` copies the portable immutable repository and creates refs
last with destination-local CAS. The target bucket is behaviorally qualified
and receives its own signed provider attestation; source attestations, probes,
leases, uploads, workspaces, cache data, and GC state are not copied. The v1
preview supports clone into an empty portable namespace. The returned
`QualifiedClone::target_s3_metrics` reports target qualification and copy
object-plane costs; combine it with the source client's interval metrics and a
shared `S3WireAttemptInterceptor` to account for the complete clone, including
the three provider control-plane qualification requests. `fetch_from` imports
portable immutable state without moving a local branch and returns the remote
head for inspection or merge. `push_to` requires matching repository identity,
copies immutable state first, and moves the destination branch only through an
explicit expected-head CAS with a destination-local reflog and lease.
Fetch and push traverse only the selected commit's reachable closure, so an
unreachable object that happens to use a canonical physical prefix is not
copied. `fsck_commit` checks one closure incrementally;
`repair_missing_from` copies missing immutable members from a qualified,
identity-matching repository and verifies the repaired closure. A corrupt
present immutable is never overwritten in place.

For large transfers, `fetch_from_resumable(source, run_id, max_objects)` pins
the source head in a destination-local CAS checkpoint and copies a bounded
sorted-path batch. Reopening a client and supplying the returned operation ID
continues after the last checkpointed path; `sync_run` reconciles worker loss
or a lost response. A later source-branch update does not alter an existing
run's closure.

## Physical layout

`Client::physical_layout()` returns the configured bucket and repository prefix
plus every stable physical path family, its write discipline, and whether clone
or GC manages it. This is a local inspection operation and issues no S3 request.

All paths are below the configured repository prefix. Immutable families are
`nodes/`, `chunks/`, `content-manifests/`, `deltas/`, `commits/`, reflogs,
multipart catalog snapshots, publication protection segments, and GC plans.
Mutable coordination records under `refs/`, `multipart/uploads/`, `workspaces/`,
publication leases, `retention/pins/`, `sync/runs/`, `gc/mark-runs/`, and
`gc/runs/` use conditional compare-and-exchange. Signed provider attestations
live under `providers/`; isolated qualification scratch data lives under
`probes/` and is not part of ordinary `open`. Logical user keys are encoded in
the Prolly trees and cannot collide with internal physical paths.

For a same-bucket SlateDB cache, initialize/open the canonical client first,
then call `SlateDbAdvisoryIndex::open_owned(object_store, repository_id,
writer_id)`. It derives `.prolly-cache/<repository-id>/<hex-writer-id>` and
persists an owner record, preventing accidental writable-path sharing across
repository/writer identities. `Client::rebuild_advisory_index` replaces all
heads from canonical refs. Corrupt entries are moved into the cache quarantine
namespace before removal. A durable running/completed rebuild checkpoint makes
a restart repeat the canonical rebuild safely and reports that it resumed.
Deleting the entire path must affect performance only.

## Workspace checks

```bash
cargo check --manifest-path s3/Cargo.toml --workspace --all-features
cargo test --manifest-path s3/Cargo.toml -p prolly-s3-core
PROLLY_S3_RUSTFS=1 \
  PROLLY_RUSTFS_ENDPOINT=http://127.0.0.1:9000 \
  PROLLY_RUSTFS_ACCESS_KEY=prollyadmin \
  PROLLY_RUSTFS_SECRET_KEY=prolly-local-secret-change-me \
  cargo test --manifest-path s3/Cargo.toml \
    -p prolly-s3-client --test rustfs_repository --all-features
```

Clean consumer builds are separate nested crates rather than workspace members.
They compile the AWS-independent core on Rust 1.89.0 and the client on exact
Rust 1.94.1 with minimal/AWS-only and SlateDB feature sets:

```bash
bash s3/scripts/check_clean_downstream.sh
```

Their checked-in lockfiles prevent qualification from silently substituting a
newer dependency graph.

The rolling-upgrade fixture builds two independent executables: the current
codec and a legacy v1 codec in which the appended capability-profile field is
physically absent. It interleaves new/legacy/new writes through a physically
versioned RustFS bucket, verifies the combined history from both binaries, and
then proves future reader, writer, and profile requirements reject both opens
without changing the native-version snapshot:

```bash
PROLLY_S3_RUSTFS=1 bash s3/scripts/run_rustfs_rolling_upgrade.sh
```

Builds default to
`/Volumes/Workspace/prolly-build/versioned-s3/rolling-upgrade`; set
`PROLLY_S3_ROLLING_BUILD_ROOT` to choose another location. The fixture uses a
unique repository prefix and retains it as qualification evidence.

The signed release rehearsal packages and verifies both workspace crates, then
builds the rolling binaries from the `.crate` archives rather than checkout
paths. It first enforces the dependency-advisory and forbidden-graph policy,
then signs the policy, its result, and the exact package evidence as one closed
evidence set with Ed25519; any unsigned extra file is rejected. A real release
requires a clean tree and an operator-controlled private key:

```bash
PROLLY_S3_RUSTFS=1 \
PROLLY_S3_RELEASE_SIGNING_KEY=/secure/path/release-ed25519-private.pem \
  bash s3/scripts/run_signed_release_rehearsal.sh
```

For a non-release local workflow rehearsal only, explicitly set both
`PROLLY_S3_RELEASE_ALLOW_DIRTY=1` and
`PROLLY_S3_ALLOW_EPHEMERAL_SIGNING=1`. The manifest records those weaker states
and the temporary private key is destroyed; they cannot be treated as release
attestation.

The opt-in 160 MiB multipart streaming/RSS probe uses the same environment plus
`PROLLY_S3_RESOURCE_TEST=1`. For meaningful macOS RSS, build the test once and
run the emitted `target/debug/deps/rustfs_repository-*` binary directly under
`/usr/bin/time -l`; measuring `cargo test` includes Cargo's own high-water RSS.
The opt-in single-writer ordinary-operation baseline uses
`PROLLY_S3_BENCHMARK=1` and prints payload size, elapsed time, put/get
operations per second, every object-plane AWS SDK call by operation, and body
bytes uploaded/downloaded. `Client::s3_operation_metrics()` reads the shared
counters and `reset_s3_operation_metrics()` starts a measurement interval.
These counters cover calls issued by the object plane. To distinguish SDK
executions from actual HTTP transmissions and retries, attach the public
`S3WireAttemptInterceptor` when constructing the caller-owned AWS client:

```rust,no_run
use prolly_s3_client::S3WireAttemptInterceptor;

# fn example(builder: aws_sdk_s3::config::Builder) {
let wire = S3WireAttemptInterceptor::new();
let aws = aws_sdk_s3::Client::from_conf(builder.interceptor(wire.clone()).build());
let before = wire.reset(); // reset only while no request is in flight
let after = wire.metrics();
assert!(after.transmissions >= after.executions);
# let _ = (aws, before);
# }
```

The interceptor records no credentials, headers, keys, or bodies. Its
deterministic 503→200 fixture proves that one SDK execution reports two
transmissions and one retry. Provider-side telemetry remains necessary to
correlate requests accepted beyond the client connection boundary. Record the
RustFS image, build profile, host, and dataset alongside benchmark output; the
number is evidence for that environment, not a provider-independent
performance promise.

Hot-branch contention is qualified on this single-node RustFS profile at 1, 8,
and 32 concurrent writers:

```bash
PROLLY_S3_RUSTFS=1 bash s3/scripts/run_rustfs_contention_matrix.sh
```

Every writer uses an explicit operation ID, reconciles ambiguous outcomes, and
retries only the same idempotent input. The probe requires every write to
converge and fscks the final repository. It enforces a 180-second tier deadline.
The default supported maximum is 32; set `PROLLY_S3_CONTENTION_MATRIX` explicitly
to probe higher tiers without treating them as qualified.

The S3-shaped operation cost matrix reports elapsed time, logical bytes,
object-plane calls by physical operation, actual wire transmissions/retries,
uploaded/downloaded bytes, and exact physical-version storage growth. Its 17
object rows cover CRUD, historical reads, listings, copy, multi-delete, the
multipart lifecycle, and a two-object atomic commit. A second 24-row matrix runs
in an isolated physically versioned bucket and covers branch/tag/pin
administration, log/reflog/native-ref history, diff/merge/restore/reset, fsck,
and an actual exact-version GC plan/sweep over a known orphan:

```bash
PROLLY_S3_RUSTFS=1 bash s3/scripts/run_rustfs_cost_matrix.sh
```

The maintenance matrix reuses `prolly-versioned-s3-costs` and isolates each
run by repository prefix. Override that bucket with
`PROLLY_S3_COST_VERSIONED_BUCKET`; the harness creates it if needed and enables
native versioning before repository initialization.

Four additional cross-repository rows cover qualified clone, ordinary fetch,
resumable fetch, and push. One SlateDB advisory-rebuild row separately reports
canonical repository S3 calls and SlateDB object-store API calls while physical
storage accounting covers both the authoritative and cache prefixes. The
runner therefore emits 46 measured rows in total.

Storage accounting uses a second uninstrumented SDK client, so its
`ListObjectVersions` traffic is excluded from each measured row. Results are a
topology-specific cost baseline, not an SLO. Correlate SlateDB's logical
`object_store` calls with provider-observed HTTP attempts through the dedicated
body-blind proxy (it records no URLs, keys, headers, credentials, or bodies):

```bash
PROLLY_S3_RUSTFS=1 bash s3/scripts/run_rustfs_slatedb_http_correlation.sh
```

The verifier permits only successful responses and the expected 404 discovery
misses, requires a unique RustFS request ID for every attempt, enforces HTTP
method lower bounds from the API counters, and rejects response-less attempts
or any other response class. Repeat this correlation on the release topology;
the local RustFS result is a development baseline rather than an SLO.

The RustFS suite creates one shared test bucket and isolates every test under a
unique repository prefix. It proves immutable idempotence, real conditional ref
CAS, exact deletion on a physically versioned bucket, reopen/history, streamed
range reads, signed snapshot pagination, multipart catalog stability across
concurrent create/abort, cursor tamper/query rejection, multipart completion,
durable workspace resume, same-bucket SlateDB behavior, and field rejection.
The SlateDB coverage includes complete cache deletion on a natively versioned
bucket, native cache delete-marker verification, recreation, canonical head
rebuild, payload rereads, fsck, and proof that canonical physical versions did
not change. The suite also compares raw RustFS with adapter behavior for
checksums, ranges, conditions, and delimiter listing. RustFS
`1.0.0-beta.10` evaluates an
unsatisfiable Range before a failing `If-Match` and returns 416; the adapter
keeps RFC 9110/AWS ordering and returns the precondition's 412 instead.

Real AWS qualification is opt-in and uses the SDK's default credential chain.
Set `PROLLY_S3_AWS=1`, `PROLLY_AWS_REGION`, and at least one of
`PROLLY_AWS_BUCKET_UNVERSIONED` or `PROLLY_AWS_BUCKET_VERSIONED`, then run:

```bash
cargo test --manifest-path s3/Cargo.toml -p prolly-s3-client \
  --all-features --test aws_qualification -- --nocapture
```

The supplied buckets must be isolated general-purpose buckets and the caller
must be allowed to inspect versioning, lifecycle, and Object Lock configuration
in addition to repository-prefix object operations. Optionally set
`PROLLY_AWS_REJECT_IDENTIFIERS` to comma-separated real directory bucket,
access-point, Object Lambda, Outposts, and MRAP identifiers. The test must reject
them before attempting a qualification probe. Qualification data is retained
for audit; remove its unique `prolly-s3-qualification/` prefixes only under the
test account's cleanup policy.

The multi-process RustFS soak runner defaults to 24 hours and repeatedly starts
fresh branch/tag/merge contenders plus multipart-completion recovery workers:

```bash
PROLLY_S3_RUSTFS=1 \
PROLLY_S3_SOAK_SECONDS=86400 \
PROLLY_S3_SOAK_RUN_ID=release-2026-08-09 \
PROLLY_S3_SOAK_EVIDENCE_DIR=/Volumes/Workspace/prolly-build/versioned-s3/soak-evidence/release-2026-08-09 \
bash s3/scripts/run_rustfs_soak.sh
```

The runner builds once, pins the exact test-binary SHA-256, then invokes that
binary directly for every iteration. Each independent-process workflow emits
its exact physical storage footprint and a final successful fsck, then
exact-deletes every physical version in its isolated repository and proves zero
remain. The default cadence starts one iteration per minute, yielding roughly
1,440 multi-process cycles in 24 hours without converting a longevity test into
an unbounded fixture generator. The runner also records
source/toolchain/provider identity, mount, restart count, elapsed time, RustFS
memory, provider-data growth, and build-directory growth. Default bounds are 1
GiB RustFS memory, 16 MiB per workflow, 32 MiB provider growth per iteration, 8
GiB absolute provider growth, and 64 MiB build growth. It refuses to overwrite
an evidence directory, preserves incomplete failures, and runs an independent
verifier before writing evidence checksums. A short one-iteration smoke
validates the harness but does not satisfy the 24-hour release gate.

The restart, contention, and soak scripts default to the pinned client
toolchain (`+1.94.1`). Set `PROLLY_S3_CARGO_TOOLCHAIN` only when deliberately
qualifying another installed Rust toolchain.

The provider-restart drill writes and fscks a repository, restarts only the
configured RustFS Docker container, waits for both Docker health and an
authenticated S3 request to succeed, then reopens, verifies the old payload,
publishes another object, and fscks again. It also reports elapsed time and
data-directory growth:

```bash
PROLLY_S3_RUSTFS=1 bash s3/scripts/run_rustfs_restart_drill.sh
```

It defaults to the `prolly-rustfs` container; override
`PROLLY_RUSTFS_CONTAINER` when necessary.

The active-outage matrix covers ordinary put, two-parent merge, multipart
completion, atomic workspace publication, atomic multi-delete, restore,
administrative reset, and branch tombstone. For each workflow it injects a lost
response after RustFS accepts the ref CAS, restarts the provider before control
returns to the repository engine, and requires four consecutive authenticated
S3 readiness probes. Operation-bearing workflows reconcile exactly one bucket
commit with only bounded publication-coordination replay; reset and branch
deletion reconcile as ref-only operations and prove an identical replay creates
no physical version or commit. The runner checks payloads, delete markers,
merge parents, reflogs, exact operation/ref/tombstone state, wire retries, and
fsck. The scenarios run serially because each restarts the shared provider:

```bash
PROLLY_S3_RUSTFS=1 bash s3/scripts/run_rustfs_active_outage_drill.sh
```

Set `PROLLY_S3_CHAOS_EVIDENCE_DIR` to a new directory to preserve the raw log
and independent verification output; the runner refuses to overwrite an
existing evidence directory.

Both scripts require the container's `/data` mount to match
`PROLLY_RUSTFS_DATA_DIR`, which defaults to `/Volumes/Workspace/prolly-data`.

The IAM rotation drill provisions a generated repository-prefix policy, proves
ordinary adapter read/write/fsck under a restricted runtime identity, denies
cross-prefix writes/listing, physical current/exact-version deletion, and
bucket-versioning mutation, overlaps a new identity before disabling the old
one, then verifies the full history and removes all temporary IAM entities:

```bash
PROLLY_S3_RUSTFS=1 bash s3/scripts/run_rustfs_iam_drill.sh
```

The policy template is
`policies/runtime-prefix-policy.template.json`. RustFS `1.0.0-beta.10` aliases
`ListObjectVersions` to the ordinary list grant and selected-version reads to
the ordinary get grant; the drill records those two provider deviations while
still proving that native-version deletion remains denied. See
`OPERATIONS.md` before treating the RustFS role as equivalent to an AWS IAM
role.

The physical backup/restore drill is deliberately stronger than `clone_to`:
it archives every native object version and delete marker, hashes a canonical
manifest and every body, verifies a quiescent source inventory, rebuilds the
native version stacks in a fresh bucket, and only then independently qualifies
and opens the restored repository:

```bash
PROLLY_S3_RUSTFS=1 bash s3/scripts/run_rustfs_backup_restore_drill.sh
```

The live gate checks repository identity, main/feature/tag history, a retained
logical historical version, native ref-recovery revisions, post-restore write,
and fsck. The three generated buckets are cleaned by exact-version deletion.
External encryption/policy/Object Lock configuration and signing-key inventory
remain operator-owned backup artifacts; see `OPERATIONS.md`.
