# Prolly Prolly S3 client

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
prolly-s3-client = { path = "../extensions/s3/client" }
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

## Put and read a file

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

Every snapshot resolves the exact S3 version recorded at that commit:

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

## Publish several changes atomically

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

For `N` staged keys, publication uses `N + 3` foreground S3 calls.

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

The client does not retry logical branch conflicts. Its exclusive-writer queue
serializes local callers; a stale expected head fails explicitly.

## Use cases

- Configuration, model, and artifact registries that need exact rollback.
- Data-pipeline outputs that need atomic publication of several files.
- Audit-friendly document stores with named snapshots and branches.
- Versioned static assets where ordinary S3 objects remain directly operable.
- Backup catalogs where Prolly records the logical set and S3 retains bytes.

## Limitations

- No unmanaged or multi-process concurrent writers.
- No repository-level deduplication, chunking, or partial-file updates.
- Commit sessions buffer bodies in memory.
- High-level multipart session state is process-local until completion.
- Provider-issued version IDs cannot be preserved across buckets.
- Raw S3 listing shows physical state, not a branch or historical snapshot.
- Production AWS scale and throttling qualification is still pending.

The complete RustFS example is
[examples/rustfs_versioned_bucket.rs](examples/rustfs_versioned_bucket.rs).
