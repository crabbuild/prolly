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

For a collection already in memory, `ingest_objects` creates durable batches
for you:

```rust
use prolly_s3_client::IngestObject;

let receipts = client
    .ingest_objects(
        vec![IngestObject {
            key: "incoming/0004.json".into(),
            bytes: fourth_body,
            headers: Default::default(),
            user_metadata: Default::default(),
        }],
        1_000,
    )
    .await?;
```

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

## History, diff, and recovery

History and diff cursors keep traversal state bounded:

```rust
use prolly_s3_client::core::TraversalBudget;

let head = client.head().await?;
let log = client
    .log_bounded(head, None, 100, TraversalBudget::default())
    .await?;

let changes = client.diff_bounded(older, head, None, 1_000).await?;
for change in changes.changes {
    println!("{}", String::from_utf8_lossy(&change.key));
}
```

`open_reflog` captures a stable immutable journal snapshot. `reset_branch`
moves a ref directly and records the move. `start_restore` instead creates new
commits with fresh logical versions while reusing immutable payloads:

```rust
let expected = client.head().await?;
let mut restore = client
    .start_restore(older, expected, "restore known-good snapshot")
    .await?;

while !restore.complete {
    restore = client.advance_restore(&restore, 1_000).await?.cursor;
}
```

Persist the returned cursor after each page. Restores larger than the canonical
commit limit are split into multiple atomic commits.

## Integrity, repair, and backup verification

Metadata fsck validates commits, node packs, trees, logical versions, and
payload metadata. Deep mode also downloads and hashes payload bytes:

```rust
let mut fsck = client.start_fsck(true).await?;
while fsck.phase != prolly_s3_client::core::FsckPhase::Complete {
    fsck = client.advance_fsck(&fsck, 1_000).await?.cursor;
}
println!("verified {} commits", fsck.report.commits);
```

Cross-provider repair is logical. It does not copy provider version IDs. It
downloads verified source payloads, rebinds them at the destination, preserves
logical metadata, and removes destination-only keys:

```rust
let source_snapshot = source.head().await?;
let destination_head = destination.head().await?;
let mut repair = destination
    .start_repair_from(
        &source,
        source_snapshot,
        destination_head,
        "repair from primary",
    )
    .await?;

loop {
    let page = destination.advance_repair_from(&source, &repair, 1_000).await?;
    repair = page.cursor;
    if page.complete { break; }
}

let mut verify = source
    .start_backup_verification(
        &destination,
        source_snapshot,
        repair.expected_head,
    )
    .await?;
while !verify.complete {
    verify = source
        .advance_backup_verification(&destination, &verify, 1_000)
        .await?
        .cursor;
}
```

The existing `start_clone_from`, `start_fetch_from`, and `start_push_to`
methods remain snapshot-only aliases for compatibility. Use their `history_`
variants when commit topology matters:

```rust
let source_head = source.head().await?;
let expected_destination_head = destination.head().await?;
let mut transfer = destination
    .start_history_clone_from(
        &source,
        source_head,
        expected_destination_head,
    )
    .await?;

while !transfer.complete {
    transfer = destination
        .advance_history_transfer_from(&source, &transfer, 1_000)
        .await?
        .cursor;
    // Persist `transfer` here so the job can resume after a restart.
}

destination
    .publish_history_transfer(&transfer, "publish imported history")
    .await?;
```

History transfer walks the full source commit DAG parent-first, recreates each
commit with destination-local payload bindings, preserves merge topology, and
records the source-to-destination commit mapping. Commit IDs necessarily
change because repository identity and provider bindings are different.

Retention pins are durable tag-backed roots:

```rust
client.create_retention_pin("quarter-close", head).await?;
```

## Garbage collection

GC is restartable and bounded. Persist the returned cursor after every page.
The grace period must be longer than the maximum time an upload, resumable
commit, merge, repair, or history transfer may remain unpublished:

```rust
use prolly_s3_client::core::GcPhase;

let two_hours_millis = 2 * 60 * 60 * 1_000;
let mut gc = client.start_gc(two_hours_millis).await?;

while gc.phase != GcPhase::Ready {
    gc = client.advance_gc(&gc, 1_000).await?.cursor;
    // Persist `gc` here.
}

while gc.phase != GcPhase::Complete {
    gc = match gc.phase {
        GcPhase::Ready | GcPhase::Sweeping => {
            client.sweep_gc(&gc, 1_000).await?.cursor
        }
        _ => client.advance_gc(&gc, 1_000).await?.cursor,
    };
    // Persist `gc` here.
}
```

The collector sweeps only immutable commit, direct-node, and payload objects.
It never sweeps mutable refs, derived indexes, publication journals, format
markers, or administration data. Branch and tag updates during an epoch write
dirty-root records before their CAS; sweep batches fence publication and catch
up those roots before exact-version deletion. Retention pins are tags, so they
are discovered and journaled by the same protocol.

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

Use `prewarm_node_cache(snapshot)` during startup to traverse both state trees.
Use `node_cache_snapshot()` before and after to observe hits, misses,
insertions, corruptions, coalesced waits, and ranged fetches.

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
- Concurrent GC coordinates all writer handles in the authoritative process.
  Do not run GC while a separately running process can publish to the same
  repository prefix; use one authoritative writer process or quiesce external
  writers first.
- Snapshot clone/fetch/push preserves only the selected logical state. The
  `history_` variants preserve the source commit DAG, but not source commit IDs
  or reflog identity.
- “Millions or billions” requires provider quota, cache, latency, cost,
  throttling, and hot-branch qualification at the intended workload.

See the runnable
[`rustfs_versioned_bucket`](examples/rustfs_versioned_bucket.rs) example and
the repository [qualification guide](../QUALIFICATION.md).
