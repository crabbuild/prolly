# Prolly S3 client

`prolly-s3-client` keeps files as normal, whole objects in a versioned S3
bucket and adds Git-like history through Prolly commits. It deliberately does
not split files into repository chunks.

## Before you start

Your bucket and deployment must satisfy all of these conditions:

- S3 bucket versioning is `Enabled`.
- This client is the only writer for managed object keys.
- One writer process holds the repository lease; other processes are read-only
  until an explicit takeover.
- Lifecycle rules do not expire current or noncurrent managed versions.
- The IAM identity can read exact versions and conditionally update repository
  metadata under `.prolly/v1/`.

Add the crate from this workspace:

```toml
[dependencies]
prolly-s3-client = { path = "../extensions/s3/client", features = ["foyer-cache"] }
aws-config = "1.8"
aws-sdk-s3 = "1.140"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Create or open a repository

Initialization qualifies the provider, creates format v1 under `.prolly/v1/`,
creates the initial commit, and acquires the writer lease.

```rust
use std::{sync::Arc, time::Duration};

use prolly_s3_client::{
    Client, HmacAttestationSigner, HmacTokenSigner, ProviderIdentity,
};

async fn create_client(
    aws: aws_sdk_s3::Client,
    bucket: &str,
) -> Result<Client, prolly_s3_client::Error> {
    Client::builder()
        .aws_client(aws)
        .bucket(bucket)
        .writer("repository-service")
        .max_parallel_payload_writes(32)
        .max_cached_commits(8_192)
        .max_cached_branches(1_024)
        .max_cached_node_pack_bytes(128 * 1024 * 1024)
        .max_staged_batch_bytes(256 * 1024 * 1024)
        .provider_identity(ProviderIdentity::aws_region("us-west-2"))
        .attestation_signer(Arc::new(HmacAttestationSigner::single(
            "provider-key-2026-01",
            vec![0x41; 32],
        )?))
        .token_signer(Arc::new(HmacTokenSigner::single(
            "cursor-key-2026-01",
            vec![0x42; 32],
        )?))
        .provider_attestation_validity(Duration::from_secs(24 * 60 * 60))
        .initialize()
        .await
}
```

Use `.open().await` instead of `.initialize().await` after the repository
exists. Use `.read_only(true)` for reader processes; they do not acquire the
writer lease.

The HMAC byte vectors above are illustrative. Load independent, rotated key
rings from a secret manager in production.

The cache values above are bounded examples, not universal sizing. Tune them
against your key count and memory budget. Export `performance_snapshot()` for
publication queue/wait telemetry and `s3_operation_metrics()` for SDK request
counts.

## Ingest files in batches (recommended)

Use `ingest_objects` when loading more than one file. It publishes up to 100
whole files per commit by default, reducing commit-envelope and branch-CAS
traffic from two calls per file to two calls per batch.

```rust
use prolly_s3_client::IngestObject;

let files = (0..1_000).map(|index| {
    IngestObject::new(
        format!("imports/file-{index:04}.json"),
        format!(r#"{{"index":{index}}}"#).into_bytes(),
    )
    .content_type("application/json")
    .metadata("source", "initial-import")
});

let report = client.ingest_objects(files).await?;
assert_eq!(report.object_count, 1_000);
assert_eq!(report.commits.len(), 10);
```

The default is bounded by both 100 files and the configured
`max_staged_batch_bytes`. Use `ingest_objects_with_limit` to choose a smaller
commit size. Use multipart upload for any single file larger than the staged
byte limit.

The checked-in RustFS gate measured 10K × 64 KiB files at 1.020 SDK calls/file
and 1.083× uploaded-byte amplification. Treat those as local regression
budgets, not AWS latency or durability claims.

For interactive or independent writes, use the single-file API below.

## Put and read one file

The builders intentionally resemble the AWS Rust SDK:

```rust
use aws_sdk_s3::primitives::ByteStream;

let written = client
    .put_object()
    .bucket(client.bucket())
    .key("reports/summary.txt")
    .body(ByteStream::from_static(b"version one\n"))
    .content_type("text/plain")
    .metadata("source", "quarterly-job")
    .send()
    .await?;

println!("commit: {}", written.snapshot);
println!("logical version: {}", written.output.version_id().unwrap_or_default());

let current = client
    .get_object()
    .bucket(client.bucket())
    .key("reports/summary.txt")
    .send()
    .await?;

let bytes = current.output.body.collect().await?.into_bytes();
assert_eq!(bytes.as_ref(), b"version one\n");
```

`written.snapshot` is the Prolly commit that made the file visible.
`written.output.version_id()` is the logical Prolly object-version ID, not the
provider's raw S3 `VersionId`.

## Read an old snapshot

Every snapshot resolves the exact physical S3 version recorded at that commit:

```rust
use aws_sdk_s3::primitives::ByteStream;

let first = client
    .put_object()
    .bucket(client.bucket())
    .key("config/app.toml")
    .body(ByteStream::from_static(b"mode = 'safe'\n"))
    .send()
    .await?;

client
    .put_object()
    .bucket(client.bucket())
    .key("config/app.toml")
    .body(ByteStream::from_static(b"mode = 'fast'\n"))
    .send()
    .await?;

let old = client
    .at(first.snapshot)
    .await?
    .get_object()
    .bucket(client.bucket())
    .key("config/app.toml")
    .send()
    .await?;

let old_bytes = old.output.body.collect().await?.into_bytes();
assert_eq!(old_bytes.as_ref(), b"mode = 'safe'\n");
```

## Publish a custom atomic change set

Staged changes are invisible until `publish`. The session is intentionally
in-memory; it is not resumable after process loss.

```rust
use aws_sdk_s3::primitives::ByteStream;

let mut commit = client
    .begin_commit()
    .message("publish site assets")
    .start()
    .await?;

commit
    .put_object()
    .bucket(client.bucket())
    .key("site/index.html")
    .body(ByteStream::from_static(b"<h1>Hello</h1>"))
    .content_type("text/html")
    .stage()
    .await?;

commit
    .delete_object()
    .bucket(client.bucket())
    .key("site/old.html")
    .stage()
    .await?;

let receipt = commit.publish().await?;
println!("published commit: {}", receipt.id);
```

For `N` staged keys, publication uses `N + 2` foreground S3 calls. Payload
mutations are bounded and parallel. One commit envelope and one branch CAS make
the entire batch visible. Prefer `ingest_objects` for homogeneous bulk puts;
use a commit session when you need mixed puts and deletes or a custom message.

## List, branch, and diff

```rust
let page = client
    .list_objects_v2()
    .bucket(client.bucket())
    .prefix("reports/")
    .max_keys(100)
    .send()
    .await?;

for object in page.output.contents() {
    println!("{}", object.key().unwrap_or_default());
}

let main_head = client.head_commit().await?;
client.create_branch("review", Some(main_head)).await?;
let review = client.on_branch("review")?;

let (changes, more) = client.diff_page(main_head, review.head_commit().await?, None, 100).await?;
assert!(!more);
for change in changes {
    println!("{:?}", change);
}
```

Continuation tokens require a shared `HmacTokenSigner` so another reader can
verify and resume the same immutable listing snapshot.

## Conditional and idempotent writes

Use `expected_head` to reject publication if the branch moved. Use a stable
`OperationId` when a caller may retry after an ambiguous response:

```rust
use aws_sdk_s3::primitives::ByteStream;
use prolly_s3_client::OperationId;

let expected = client.head_commit().await?;
let operation = OperationId::new();

let result = client
    .put_object()
    .bucket(client.bucket())
    .key("jobs/result.json")
    .body(ByteStream::from_static(br#"{"ok":true}"#))
    .expected_head(expected)
    .operation_id(operation)
    .send()
    .await?;

assert_eq!(result.commit.as_ref().unwrap().operation, operation);
```

The client does not retry logical branch conflicts. Local payload uploads may
run concurrently, while the short metadata-publication phase is serialized; a
stale expected head fails explicitly.

## Add a persistent node cache

Foyer keeps verified immutable Prolly nodes in bounded memory and local disk.
Successful commits write their new nodes through to this cache. Cache hits are
checked against the CID; corruption and cache I/O errors fail open to a
verified S3 read.

```rust
use std::{path::PathBuf, sync::Arc, time::Duration};

use prolly_s3_client::{
    Client, FoyerNodeCache, FoyerNodeCacheConfig, HmacAttestationSigner,
    HmacTokenSigner, ProviderIdentity,
};

let node_cache = FoyerNodeCache::open(FoyerNodeCacheConfig {
    directory: PathBuf::from("/var/cache/prolly-s3/nodes"),
    memory_capacity_bytes: 512 * 1024 * 1024,
    disk_capacity_bytes: 20 * 1024 * 1024 * 1024,
    disk_block_size_bytes: 8 * 1024 * 1024,
    memory_shards: 16,
})
.await?;

let client = Client::builder()
    .aws_client(aws)
    .bucket(bucket)
    .writer("repository-service")
    .provider_identity(ProviderIdentity::aws_region("us-west-2"))
    .attestation_signer(Arc::new(HmacAttestationSigner::single(
        "provider-key-2026-01",
        vec![0x41; 32],
    )?))
    .token_signer(Arc::new(HmacTokenSigner::single(
        "cursor-key-2026-01",
        vec![0x42; 32],
    )?))
    .node_cache(node_cache.clone())
    .max_cached_node_locations(262_144)
    .node_index_maintenance(Duration::from_secs(60), 1_000)
    .open()
    .await?;

// On a cold host, populate the cache before serving traversal-heavy reads.
let snapshot = client.head_commit().await?;
let warmed = client
    .prewarm_node_cache(snapshot, b"", 1_000)
    .await?;
println!("warmed {} objects in {} pages", warmed.object_count, warmed.pages);
```

Use one filesystem owner per cache directory. Drop clients before calling
`node_cache.close().await?` during graceful shutdown. Reopen the same directory
after restart to reuse persisted immutable nodes. A new host should run
`prewarm_node_cache` before taking traversal-heavy traffic.

## Page through large histories and ref sets

Run one maintenance page immediately after bulk import; writable clients also
advance the rebuildable indexes every 60 seconds by default.

```rust
use prolly_s3_client::core::TraversalBudget;

client.advance_scale_indexes(1_000).await?;

let root = client.head_commit().await?;
let mut history_cursor = None;
loop {
    let page = client
        .log_bounded(root, history_cursor.as_ref(), 500, TraversalBudget::default())
        .await?;

    for (commit_id, commit) in page.commits {
        println!("{} {}", commit_id, commit.message.unwrap_or_default());
    }

    history_cursor = page.continuation;
    if history_cursor.is_none() {
        break;
    }
}
```

The derived ref catalog is for enumeration only. Its response includes a scan
epoch and update time; resolve a selected name through the authoritative ref
before mutation.

```rust
let mut after = None;
loop {
    let page = client
        .list_branch_catalog_page(after.as_deref(), 500)
        .await?;

    for branch in &page.branches {
        println!("{} -> {}", branch.name, branch.target);
    }

    after = page.continuation;
    if after.is_none() {
        break;
    }
}
```

Use `diff_bounded` for very large comparisons. Its continuation preserves the
structural traversal frontier, so unchanged CID subtrees stay pruned after a
resume.

## Run partitioned garbage collection

GC v2 requires the writable authoritative client and complete node-index
coverage. Each call has a 1–1,000 item budget and persists its checkpoint.

```rust
use prolly_s3_client::core::GcEpochPhaseV2;

client.advance_node_index(1_000).await?;
let mut epoch = client
    .start_gc_epoch(Duration::from_secs(7 * 24 * 60 * 60))
    .await?;

loop {
    match epoch.phase {
        GcEpochPhaseV2::Ready | GcEpochPhaseV2::Sweeping => {
            epoch = client.sweep_gc_epoch(epoch.id, 500).await?.epoch;
        }
        GcEpochPhaseV2::Completed => break,
        GcEpochPhaseV2::Aborted => {
            eprintln!("GC epoch was aborted; inspect its abort_reason");
            break;
        }
        _ => {
            // This also handles a safe root-discovery restart after a write.
            epoch = client.advance_gc_epoch(epoch.id, 1_000).await?.epoch;
        }
    }
}
```

For a large repository, call `advance_node_index` until its report says
`completed_scan`; one 1,000-object call may be only part of an index epoch.

## Use cases

- Configuration, model, and artifact registries that need exact rollback.
- Data-pipeline outputs that need atomic publication of several files.
- Audit-friendly document stores with named snapshots and branches.
- Versioned static assets where ordinary S3 objects remain directly operable.
- Backup catalogs where Prolly records the logical set and S3 retains bytes.

## Limitations

- No unmanaged or multi-process concurrent writers.
- No repository-level deduplication, chunking, or partial-file updates.
- Commit sessions buffer bodies in memory and fail closed at the configured
  aggregate byte limit; use physical multipart for large individual files.
- Multipart restart requires the caller to persist the upload handle, each
  part's ETag/SHA-256/size, and whole-object checksums.
- Provider-native version IDs cannot be preserved across buckets.
- Raw S3 listing shows physical state, not a branch or historical snapshot.
- Whole-result compatibility methods such as `list_branches`, `merge_bases`,
  merge planning, and repository-wide `fsck` are not billion-scale APIs. Use
  paged catalog/history/diff calls and partition operational work.
- Production AWS scale and throttling qualification is still pending.

The complete RustFS example is
[examples/rustfs_versioned_bucket.rs](examples/rustfs_versioned_bucket.rs).
