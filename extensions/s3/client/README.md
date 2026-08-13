# Prolly S3 client

The client turns a versioned S3 bucket into a branchable file repository.
Files remain ordinary whole S3 objects under immutable derived keys; Prolly
trees provide snapshots and history.

## Requirements

- an S3 or S3-compatible bucket with versioning enabled;
- conditional writes, exact-version reads, range reads, and strong read-after-write;
- a repository prefix reserved exclusively for this client;
- a stable writer identity and a trusted provider-attestation signer.

Do not write under the repository prefix outside this library.

## Create a client

This complete program shape works with AWS SDK configuration you supply:

```rust
use std::{sync::Arc, time::Duration};
use prolly_s3_client::{
    core::ProviderPerKeyVersionLimit,
    Client, HmacAttestationSigner, ProviderIdentity,
};

async fn create(
    aws: aws_sdk_s3::Client,
    bucket: &str,
) -> Result<Client, prolly_s3_client::Error> {
    Client::builder()
        .aws_client(aws)
        .bucket(bucket)
        .repository_prefix(".prolly")
        .default_branch("main")
        .writer("ingest-worker-01")
        .provider_identity(ProviderIdentity::aws_region("us-west-2"))
        .provider_attestation_validity(Duration::from_secs(24 * 60 * 60))
        .attestation_signer(Arc::new(HmacAttestationSigner::single(
            "provider-key-2026-01",
            vec![0x41; 32],
        )?))
        .provider_per_key_version_limit(
            ProviderPerKeyVersionLimit::Finite(10_000),
        )
        .initialize()
        .await
}
```

Call `initialize` once for a new prefix. Use the same builder inputs with
`open` after restart. The default prefix is `.prolly`.

## Read and write files

```rust
let first = client
    .put_object("documents/readme.txt", b"first revision\n".to_vec())
    .await?;

client
    .put_object("documents/readme.txt", b"second revision\n".to_vec())
    .await?;

let current = client
    .get_object("documents/readme.txt")
    .await?
    .expect("current file");

let historical = client
    .get_object_at(first.id, "documents/readme.txt")
    .await?
    .expect("historical file");

assert_eq!(current.bytes, b"second revision\n");
assert_eq!(historical.bytes, b"first revision\n");
```

A write uploads one whole payload. It does not split a 64 KiB or larger file
into chunks. The content-addressed key deduplicates identical bytes.

For safe retries, generate and persist one operation ID:

```rust
let operation = prolly_s3_client::core::OperationId::new();
let receipt = client
    .put_object_with_operation("documents/readme.txt", bytes, operation)
    .await?;
```

After an ambiguous response, repeat the exact call with the same operation ID.
The client reconciles an already-applied commit before fencing the writer.

## Batch ingestion

Batching is the recommended ingestion path. It uploads payloads while staging
and publishes all tree changes with one branch compare-and-swap.

```rust
let mut commit = client
    .begin_commit()
    .message("import 2026-08-12")
    .checkpoint_every(256)
    .start()
    .await?;

let batch_id = commit.id();
commit.put_object("incoming/0001.json", first_body).await?;
commit.put_object("incoming/0002.json", second_body).await?;
commit.delete_object("incoming/obsolete.json")?;
commit.checkpoint().await?;

let receipt = commit.publish().await?;
println!("commit={} changed={}", receipt.id, receipt.changed_keys);
```

If a durable process stops after saving `batch_id`:

```rust
let mut commit = client.resume_commit(batch_id).await?;
commit.put_object("incoming/0003.json", third_body).await?;
let receipt = commit.publish().await?;
```

For a disposable job, add `.ephemeral()`. That removes checkpoint requests
but cannot recover process-local staged metadata after a crash.

## List files and versions

```rust
let mut after: Option<String> = None;
loop {
    let (_snapshot, page, truncated) =
        client.list_objects("incoming/", after.as_deref(), 1_000).await?;
    after = page
        .last()
        .map(|item| String::from_utf8_lossy(&item.key).into_owned());
    for item in page {
        println!("{}", String::from_utf8_lossy(&item.key));
    }
    if !truncated {
        break;
    }
}

let (_head, versions) =
    client.list_object_versions("documents/readme.txt", 100).await?;
```

## Branch and merge

```rust
use prolly_s3_client::core::{MergePhase, MergePolicy};

let base = client.head().await?;
client.create_branch("feature", Some(base)).await?;

let feature = client.for_branch("feature")?;
feature
    .put_object("documents/feature.txt", b"feature\n".to_vec())
    .await?;

let mut merge = client
    .start_merge("feature", None, MergePolicy::Fail, "merge feature")
    .await?;

while merge.phase != MergePhase::ReadyToPublish {
    merge = client.advance_merge(&merge, 1_000).await?.cursor;
}

let page = client.merge_changes_page(&merge, None, 1_000).await?;
println!("changes={}", page.changes.len());
let receipt = client.publish_merge(&merge).await?;
```

Merge work is immutable and restartable. Persist the canonical `MergeCursor`
returned by each bounded advance. Use `merge_conflicts_page` before publishing
when the policy can produce conflicts.

## Cache immutable nodes

The in-memory cache is enabled through repository limits. For a persistent
Foyer cache, enable the `foyer-cache` feature:

```rust
use std::path::PathBuf;
use prolly_s3_client::{FoyerNodeCache, FoyerNodeCacheConfig};

let cache = FoyerNodeCache::open(FoyerNodeCacheConfig {
    directory: PathBuf::from("./prolly-node-cache"),
    memory_capacity_bytes: 64 * 1024 * 1024,
    disk_capacity_bytes: 4 * 1024 * 1024 * 1024,
    disk_block_size_bytes: 4 * 1024 * 1024,
    memory_shards: 8,
})
.await?;

let client = Client::builder()
    // supply the same required provider and bucket settings
    .node_cache(cache)
    .open()
    .await?;
```

Cache keys include repository identity, tree format, and immutable CID. Cached
bytes are verified before use. Persisted caches improve cold-start traversal
but are never authoritative.

## Performance model

- A single-file write uploads one payload and publishes immutable metadata.
- A batch uploads one payload per changed file, then amortizes tree and
  publication requests across the batch.
- A current or historical read resolves a ref/commit/tree path and one payload.
- Warm immutable-node caches remove most repeated metadata reads.
- Same-branch writers contend on one ref CAS; different branches publish
  independently.

Measure with `s3_operation_metrics` on the provider and workload you will run.
The repository enforces configured request-shape limits, but no universal
latency or cost claim substitutes for AWS qualification.

## Limitations

- The client must be the exclusive authority for its repository prefix.
- One file must fit the repository and provider single-object PUT limits.
- There is no chunked or multipart file representation.
- Concurrent writes to one branch can conflict; batch related changes.
- Immutable unreachable payloads and nodes are not currently reclaimed by a
  production garbage-collection API.
- Cross-repository clone and backup/restore are not production APIs.
- “Millions or billions” requires provider quota, cache, latency, cost,
  throttling, and hot-branch qualification at the intended workload.

See the runnable
[`rustfs_versioned_bucket`](examples/rustfs_versioned_bucket.rs) example and
the repository [qualification guide](../QUALIFICATION.md).
