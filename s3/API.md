# Versioned S3 client API reference

This document describes the implemented v1 Rust API in `prolly-s3-client`.
It is an in-process adapter around `aws_sdk_s3::Client`, not an HTTP proxy and
not a new S3 endpoint. Applications use familiar AWS SDK-shaped builders while
the adapter stores immutable content, Prolly trees, bucket commits, reflogs,
and mutable branch refs in the configured physical bucket.

The machine-readable compatibility contract is
[`compatibility-v1.json`](compatibility-v1.json). Unsupported official AWS
input fields fail with `ErrorCode::UnsupportedParameter`; they are never
silently ignored.

## 1. Version and dependency contract

| Component | v1 contract |
| --- | --- |
| Package | `prolly-s3-client` |
| Client Rust version | 1.94.1 |
| Core Rust version | 1.89.0 |
| AWS SDK | `aws-sdk-s3` 1.140.0 |
| Deployment | In-process Rust adapter |
| Authoritative metadata | Canonical objects and refs in S3 |
| Optional cache | SlateDB advisory index |
| Physical bucket versioning | Optional; useful for native ref recovery |

Until the crates are published, a consumer inside this repository can use:

```toml
[dependencies]
prolly-s3-client = { path = "s3/client", features = ["slatedb-index"] }
aws-config = "1"
aws-credential-types = "1"
aws-sdk-s3 = "1"
aws-types = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Omit `slatedb-index` when SlateDB is not needed.

## 2. Identity model

The API deliberately exposes three different identifiers:

| Identifier | Meaning |
| --- | --- |
| `ObjectVersionId` | One logical version of one key |
| `CommitId` | Immutable snapshot of the entire logical bucket |
| Provider `version_id` | Native physical S3 version used only by recovery and GC |

An object mutation normally creates one `ObjectVersionId` and one `CommitId`.
An atomic commit can create several object versions under one bucket commit.
Physical ETags and native version IDs are storage tokens, not logical history
identifiers.

## 3. Complete client setup

The following executable-style setup targets local RustFS. Production code
must load independent attestation and cursor key rings from a secret manager;
the fixed keys below are demonstration values only.

```rust
use std::{sync::Arc, time::Duration};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_types::region::Region;
use prolly_s3_client::{
    Client, HmacAttestationSigner, HmacTokenSigner, ProviderIdentity,
};

async fn open_client() -> prolly_s3_client::Result<Client> {
    let endpoint = std::env::var("PROLLY_RUSTFS_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let access_key = std::env::var("PROLLY_RUSTFS_ACCESS_KEY")
        .unwrap_or_else(|_| "prollyadmin".to_string());
    let secret_key = std::env::var("PROLLY_RUSTFS_SECRET_KEY")
        .unwrap_or_else(|_| "prolly-local-secret-change-me".to_string());

    let aws_config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "versioned-s3-example",
        ))
        .endpoint_url(&endpoint)
        .force_path_style(true)
        .build();

    Client::builder()
        .aws_client(aws_sdk_s3::Client::from_conf(aws_config))
        .bucket("prolly-versioned-s3-demo")
        .repository_prefix(".prolly/v1")
        .default_branch("main")
        .writer("example-service")
        .logical_retry_limit(3)
        .gc_delete_rate_limit_per_second(100)
        .provider_identity(ProviderIdentity::s3_compatible(endpoint, "us-east-1"))
        .provider_attestation_validity(Duration::from_secs(24 * 60 * 60))
        .attestation_signer(Arc::new(HmacAttestationSigner::single(
            "demo-provider-key",
            vec![0x41; 32],
        )?))
        .token_signer(Arc::new(HmacTokenSigner::single(
            "demo-cursor-key",
            vec![0x42; 32],
        )?))
        .initialize()
        .await
}
```

Use `initialize()` once or idempotently during provisioning. Normal processes
should use the same builder followed by `open()`, which verifies persisted
format and provider attestation without running probe writes.

### 3.1 `ClientBuilder`

| Method | Request | Response or effect |
| --- | --- | --- |
| `aws_client(client)` | Caller-owned `aws_sdk_s3::Client` | Selects credentials, transport, endpoint, and SDK retry configuration |
| `bucket(name)` | Physical and logical bucket name | Binds this client to exactly one bucket |
| `repository_prefix(prefix)` | Reserved physical namespace | Defaults to the core repository default |
| `default_branch(name)` | Branch name | Defaults to `main` |
| `writer(id)` | Stable writer identity | Stored in commits and reflogs |
| `logical_retry_limit(n)` | `0..=16` | Ref-conflict retries; independent of AWS SDK retries |
| `gc_delete_rate_limit_per_second(n)` | `0` or `1..=1000` | Binds delete pacing into new GC runs |
| `token_signer(signer)` | `Arc<dyn TokenSigner>` | Signs restart-safe pagination cursors |
| `cursor_ttl(duration)` | `>0`, at most 24 hours | Cursor validity |
| `cursor_clock_skew(duration)` | At most 15 minutes | Key-retirement safety allowance |
| `advisory_index(index)` | `Arc<dyn AdvisoryIndex>` | Optional correctness-neutral cache |
| `provider_identity(identity)` | AWS region or S3-compatible endpoint identity | Binds provider qualification |
| `attestation_signer(signer)` | `Arc<dyn AttestationSigner>` | Signs and verifies provider attestations |
| `provider_attestation(id)` | Existing `ProviderProfileId` | Requires that exact persisted profile |
| `provider_attestation_validity(duration)` | 1 minute to 30 days | Validity for a new qualification |
| `qualify_provider().await` | Complete provider configuration | `ProviderAttestationV1` after isolated probes |
| `initialize().await` | Complete configuration | Open `Client`, creating format/ref state if absent |
| `open().await` | Complete configuration | Open existing repository without qualification writes |

AWS production uses `ProviderIdentity::aws_region(region)`. Custom endpoints
use `ProviderIdentity::s3_compatible(endpoint, region)`, optionally followed by
`.path_style(false)`.

## 4. Common responses, options, and errors

### 4.1 `Versioned<T>`

S3-shaped operations whose result belongs to a bucket snapshot return:

```rust
pub struct Versioned<T> {
    pub output: T,
    pub snapshot: CommitId,
    pub commit: Option<CommitReceipt>,
}
```

- `output` is the corresponding AWS SDK output type.
- `snapshot` is the immutable bucket commit used or created by the operation.
- `commit` is present for publishing mutations and absent for reads.

`CommitReceipt` contains `id`, `operation`, `branch`, `parents`,
`changed_keys`, `object_versions`, and `idempotent_replay`.

### 4.2 Direct AWS input options

```rust
pub struct ReadOptions {
    pub deadline: Option<std::time::Instant>,
}

pub struct WriteOptions {
    pub operation_id: Option<OperationId>,
    pub expected_head: Option<CommitId>,
    pub logical_retry_limit: Option<u8>,
    pub deadline: Option<std::time::Instant>,
}
```

If a publishing deadline expires after work begins, the client returns
`OutcomeUnknown` with `RetryAdvice::ReconcileOperation` and the stable
operation ID. Call `reconcile_operation` before retrying.

### 4.3 Error response

Every operation returns `prolly_s3_client::Result<T>`. `Error` exposes:

```rust
pub struct Error {
    pub code: ErrorCode,
    pub retry: RetryAdvice,
    pub message: String,
    pub operation_id: Option<String>,
    pub provider_code: Option<Box<str>>,
    pub provider_message: Option<Box<str>>,
    pub provider_request_id: Option<Box<str>>,
}
```

Important retry advice values are `Never`, `Safe`, `After(duration)`,
`ReloadHead`, and `ReconcileOperation`.

## 5. S3-shaped object API

### 5.1 Operation matrix

| Client method | Accepted request fields | Response |
| --- | --- | --- |
| `put_object()` | `bucket`, `key`, `body`, `cache_control`, `content_disposition`, `content_encoding`, `content_language`, `content_type`, `metadata`, `if_match`, `if_none_match`, `content_md5`, `checksum_sha256`; adapter: `operation_id`, `expected_head`, `logical_retry_limit`, `deadline` | `Versioned<PutObjectOutput>`; output includes logical ETag, logical version ID, size, and optional SHA-256 |
| `get_object()` | `bucket`, `key`, `version_id`, `range`, `if_match`, `if_none_match`, `if_modified_since`, `if_unmodified_since`, `checksum_mode`; adapter: `deadline` | `Versioned<GetObjectOutput>` with streaming body, logical version ID, ETag, headers, metadata, and optional checksum |
| `head_object()` | Same as get except `range` | `Versioned<HeadObjectOutput>` without body |
| `delete_object()` | `bucket`, `key`; adapter: `operation_id` | `Versioned<DeleteObjectOutput>` with a new logical delete-marker version |
| `delete_objects()` | `bucket`, AWS `Delete`; adapter: `operation_id` | `Versioned<DeleteObjectsOutput>`; all delete markers publish in one commit |
| `copy_object()` | `bucket`, `key`, `copy_source`; adapter: `operation_id` | `Versioned<CopyObjectOutput>`; same-repository content is reused |
| `list_objects_v2()` | `bucket`, `prefix`, `delimiter`, `max_keys`, `continuation_token`, `start_after`; adapter: `deadline` | `Versioned<ListObjectsV2Output>` pinned to one snapshot |
| `list_object_versions()` | `bucket`, `prefix`, `max_keys`, `key_marker`, `version_id_marker` | `Versioned<ListObjectVersionsOutput>` with logical versions and delete markers |

The requested bucket must equal `client.bucket()`. Cross-bucket logical access
fails closed.

AWS-generator-style optional setters are also available. Each accepts
`Option<T>` and clears the field when passed `None`:

| Builder | Optional setter aliases |
| --- | --- |
| `PutObjectBuilder` | `set_bucket`, `set_key`, `set_body`, `set_content_type`, `set_metadata`, `set_if_match`, `set_if_none_match` |
| `GetObjectBuilder` | `set_bucket`, `set_key`, `set_version_id`, `set_range` |
| `DeleteObjectsBuilder` | `set_delete` |

### 5.2 Put, get, head, and conditional write

```rust
use std::time::{Duration, Instant};
use aws_sdk_s3::primitives::ByteStream;

let written = client
    .put_object()
    .bucket(client.bucket())
    .key("docs/report.txt")
    .body(ByteStream::from_static(b"version one"))
    .content_type("text/plain")
    .metadata("owner", "analytics")
    .if_none_match("*")
    .logical_retry_limit(3)
    .deadline(Instant::now() + Duration::from_secs(30))
    .send()
    .await?;

println!("bucket commit={}", written.snapshot);
println!("logical version={}", written.output.version_id().unwrap());
println!("etag={}", written.output.e_tag().unwrap());

let head = client
    .head_object()
    .bucket(client.bucket())
    .key("docs/report.txt")
    .send()
    .await?;

let read = client
    .get_object()
    .bucket(client.bucket())
    .key("docs/report.txt")
    .range("bytes=0-6")
    .if_match(head.output.e_tag().unwrap())
    .send()
    .await?;

let bytes = read.output.body.collect().await?.into_bytes();
assert_eq!(bytes.as_ref(), b"version");
```

To read a selected logical version, pass the adapter-returned version ID:

```rust
let old = client
    .get_object()
    .bucket(client.bucket())
    .key("docs/report.txt")
    .version_id(written.output.version_id().unwrap())
    .send()
    .await?;
```

### 5.3 Delete one or many keys

```rust
use aws_sdk_s3::types::{Delete, ObjectIdentifier};

let deleted = client
    .delete_object()
    .bucket(client.bucket())
    .key("docs/report.txt")
    .send()
    .await?;

assert!(deleted.output.delete_marker().unwrap_or(false));

let request = Delete::builder()
    .objects(ObjectIdentifier::builder().key("a.txt").build()?)
    .objects(ObjectIdentifier::builder().key("b.txt").build()?)
    .quiet(false)
    .build()?;

let deleted_many = client
    .delete_objects()
    .bucket(client.bucket())
    .delete(request)
    .send()
    .await?;

assert_eq!(deleted_many.commit.as_ref().unwrap().changed_keys, 2);
```

Deleting a selected historical `version_id` is intentionally unsupported;
delete always appends a new logical delete marker.

### 5.4 Copy an object

```rust
let copied = client
    .copy_object()
    .bucket(client.bucket())
    .key("archive/report.txt")
    .copy_source(format!("{}/docs/report.txt", client.bucket()))
    .send()
    .await?;

println!("copy commit={}", copied.snapshot);
```

`copy_source` may include a logical `versionId` query. The source and
destination must belong to the same configured repository.

### 5.5 List current objects

```rust
let mut token = None;
loop {
    let mut request = client
        .list_objects_v2()
        .bucket(client.bucket())
        .prefix("docs/")
        .delimiter("/")
        .max_keys(100);
    if let Some(value) = token.take() {
        request = request.continuation_token(value);
    }
    let page = request.send().await?;
    for object in page.output.contents() {
        println!("{} {}", object.key().unwrap(), object.size().unwrap_or(0));
    }
    token = page.output.next_continuation_token().map(ToOwned::to_owned);
    if token.is_none() {
        break;
    }
}
```

Continuation tokens are HMAC-authenticated and bind the repository, bucket,
branch, prefix, delimiter, immutable commit, resume position, and expiry.

### 5.6 List logical versions

```rust
let page = client
    .list_object_versions()
    .bucket(client.bucket())
    .prefix("docs/")
    .max_keys(100)
    .send()
    .await?;

for version in page.output.versions() {
    println!("version key={} id={}", version.key().unwrap(), version.version_id().unwrap());
}
for marker in page.output.delete_markers() {
    println!("delete marker key={} id={}", marker.key().unwrap(), marker.version_id().unwrap());
}
```

Use `next_key_marker` and `next_version_id_marker` together for the next page.

### 5.7 Execute official AWS SDK inputs

Four operations accept official AWS input objects directly:

```rust
use prolly_s3_client::{ReadOptions, WriteOptions};

let put_input = aws_sdk_s3::operation::put_object::PutObjectInput::builder()
    .bucket(client.bucket())
    .key("official-input.txt")
    .body(ByteStream::from_static(b"official input"))
    .build()?;

let put = client
    .execute_put_object(put_input, WriteOptions::default())
    .await?;

let get_input = aws_sdk_s3::operation::get_object::GetObjectInput::builder()
    .bucket(client.bucket())
    .key("official-input.txt")
    .build()?;
let get = client.execute_get_object(get_input, ReadOptions::default()).await?;

let head_input = aws_sdk_s3::operation::head_object::HeadObjectInput::builder()
    .bucket(client.bucket())
    .key("official-input.txt")
    .build()?;
let head = client.execute_head_object(head_input, ReadOptions::default()).await?;

let list_input = aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Input::builder()
    .bucket(client.bucket())
    .prefix("official-")
    .build()?;
let list = client
    .execute_list_objects_v2(list_input, ReadOptions::default())
    .await?;

let _ = (put, get, head, list);
```

Call `supported_input_fields("put_object")`, `get_object`, `head_object`, or
`list_objects_v2` to inspect the accepted official fields at runtime.

## 6. Multipart API

| Method | Request | Response |
| --- | --- | --- |
| `create_multipart_upload()` | `bucket`, `key`, optional `content_type`, repeated `metadata` | `CreateMultipartUploadOutput` with bucket, key, and durable upload ID |
| `upload_part()` | `bucket`, `key`, `upload_id`, `part_number`, streaming `body` | `UploadPartOutput` with part ETag |
| `upload_part_copy()` | `bucket`, `key`, `upload_id`, `part_number`, `copy_source`, optional `copy_source_range` | `UploadPartCopyOutput` with copied-part ETag and timestamp |
| `list_parts()` | `bucket`, `key`, `upload_id`, optional `part_number_marker`, `max_parts` | `ListPartsOutput` |
| `list_multipart_uploads()` | `bucket`, optional `prefix`, `key_marker`, `upload_id_marker`, `max_uploads` | `ListMultipartUploadsOutput` with signed stable pagination marker |
| `complete_multipart_upload()` | `bucket`, `key`, `upload_id`, AWS `CompletedMultipartUpload`, optional `operation_id` | `Versioned<CompleteMultipartUploadOutput>`; completion publishes one bucket commit |
| `abort_multipart_upload()` | `bucket`, `key`, `upload_id` | Empty `AbortMultipartUploadOutput` |
| `expire_multipart_uploads(limit).await` | Maximum active uploads to expire | Number expired |

End-to-end example:

```rust
use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
};

let created = client
    .create_multipart_upload()
    .bucket(client.bucket())
    .key("large/archive.bin")
    .content_type("application/octet-stream")
    .metadata("source", "example")
    .send()
    .await?;
let upload_id = created.upload_id().unwrap();

let first = client
    .upload_part()
    .bucket(client.bucket())
    .key("large/archive.bin")
    .upload_id(upload_id)
    .part_number(1)
    .body(ByteStream::from(vec![0x11; 5 * 1024 * 1024]))
    .send()
    .await?;

let second = client
    .upload_part()
    .bucket(client.bucket())
    .key("large/archive.bin")
    .upload_id(upload_id)
    .part_number(2)
    .body(ByteStream::from_static(b"final part"))
    .send()
    .await?;

let listed = client
    .list_parts()
    .bucket(client.bucket())
    .key("large/archive.bin")
    .upload_id(upload_id)
    .max_parts(1_000)
    .send()
    .await?;
assert_eq!(listed.parts().len(), 2);

let completed = CompletedMultipartUpload::builder()
    .parts(
        CompletedPart::builder()
            .part_number(1)
            .e_tag(first.e_tag().unwrap())
            .build(),
    )
    .parts(
        CompletedPart::builder()
            .part_number(2)
            .e_tag(second.e_tag().unwrap())
            .build(),
    )
    .build();

let published = client
    .complete_multipart_upload()
    .bucket(client.bucket())
    .key("large/archive.bin")
    .upload_id(upload_id)
    .multipart_upload(completed)
    .send()
    .await?;

println!("multipart commit={}", published.snapshot);
```

To abandon instead of completing:

```rust
client
    .abort_multipart_upload()
    .bucket(client.bucket())
    .key("large/archive.bin")
    .upload_id(upload_id)
    .send()
    .await?;
```

Multipart limits are 10,000 parts, 5 GiB per part, a 5 MiB minimum for each
nonfinal part, and the repository's 5 TiB maximum logical object size.

## 7. Atomic multi-object commit API

`begin_commit` creates a durable workspace. Staged mutations remain invisible
until `publish`; publication requires the original base commit and never
silently rebases.

| Method | Request | Response |
| --- | --- | --- |
| `begin_commit()` | Selected client branch | `CommitBuilder` |
| `CommitBuilder::message(text)` | Commit message | Updated builder |
| `CommitBuilder::expires_after(duration)` | Workspace lifetime | Updated builder |
| `CommitBuilder::start().await` | Completed builder | `CommitSession` |
| `resume_commit(workspace_id).await` | Durable workspace ID | `CommitSession` |
| `session.id()` | None | `WorkspaceId` |
| `session.base_commit()` | None | Original `CommitId` |
| `session.put_object()` | `bucket`, `key`, `body`, optional `content_type`, repeated `metadata` | Staged builder; `.stage().await` returns `()` |
| `session.delete_object()` | `bucket`, `key` | Staged builder; `.stage().await` returns `()` |
| `session.publish().await` | Consumes session | One `CommitReceipt` for all mutations |
| `session.abort().await` | Consumes session | `()`; durable workspace becomes aborted |

```rust
use std::time::Duration;
use aws_sdk_s3::primitives::ByteStream;

let mut transaction = client
    .begin_commit()
    .message("publish website")
    .expires_after(Duration::from_secs(60 * 60))
    .start()
    .await?;

let workspace_id = transaction.id();
let expected_base = transaction.base_commit();

transaction
    .put_object()
    .bucket(client.bucket())
    .key("index.html")
    .body(ByteStream::from_static(b"<h1>Home</h1>"))
    .content_type("text/html")
    .stage()
    .await?;

transaction
    .put_object()
    .bucket(client.bucket())
    .key("app.js")
    .body(ByteStream::from_static(b"console.log('ready')"))
    .content_type("text/javascript")
    .stage()
    .await?;

transaction
    .delete_object()
    .bucket(client.bucket())
    .key("obsolete.js")
    .stage()
    .await?;

let receipt = transaction.publish().await?;
assert_eq!(receipt.parents, vec![expected_base]);
assert_eq!(receipt.changed_keys, 3);

// After a process restart:
let resumed = client.resume_commit(workspace_id).await;
```

The configured format permits at most 10,000 mutations per explicit commit.

## 8. Snapshot and history API

| Method | Request | Response |
| --- | --- | --- |
| `head_commit().await` | Selected branch | Current `CommitId` |
| `at(commit).await` | Exact commit | Read-only `Snapshot` after commit validation |
| `log(limit).await` | First-parent limit | `Vec<(CommitId, BucketCommitV1)>` from current head |
| `log_page(start, after, limit).await` | Immutable start, exclusive commit cursor | One first-parent page |
| `diff(from, to).await` | Two exact commits | Complete `Vec<ObjectDiff>` |
| `diff_page(from, to, after_key, limit).await` | Two commits and exclusive raw-key cursor | `(Vec<ObjectDiff>, truncated)` |

`ObjectDiff` contains the key plus optional `from` and `to` logical version
IDs.

```rust
let head = client.head_commit().await?;
let history = client.log(100).await?;

for (id, commit) in history {
    println!("{} {:?} {}", id, commit.message, commit.author);
}

let snapshot = client.at(head).await?;
assert_eq!(snapshot.commit_id(), head);

let object = snapshot
    .get_object()
    .bucket(client.bucket())
    .key("index.html")
    .send()
    .await?;

let metadata = snapshot
    .head_object()
    .bucket(client.bucket())
    .key("index.html")
    .send()
    .await?;

let listing = snapshot
    .list_objects_v2()
    .bucket(client.bucket())
    .prefix("")
    .send()
    .await?;

let versions = snapshot
    .list_object_versions()
    .bucket(client.bucket())
    .prefix("index")
    .send()
    .await?;

let _ = (object, metadata, listing, versions);
```

`Snapshot` exposes only `commit_id`, `get_object`, `head_object`,
`list_objects_v2`, and `list_object_versions`; it cannot mutate history.

## 9. Branch and tag API

### 9.1 Branches

| Method | Request | Response |
| --- | --- | --- |
| `branch()` | None | Selected branch name |
| `on_branch(name)` | Valid branch name | Cloned `Client` selecting that branch |
| `create_branch(name, from)` | Optional start commit; current head when absent | `BranchHead { name, target, generation }` |
| `list_branches().await` | None | `Vec<BranchHead>` |
| `delete_branch(name, expected).await` | Name and exact current target | `()` |
| `reset_branch(to, expected_head, reason).await` | Target, CAS expectation, audit reason | `RefMoveReceipt` without creating a bucket commit |

```rust
let main = client.head_commit().await?;
let feature = client.create_branch("feature/search", Some(main)).await?;
let feature_client = client.on_branch("feature/search")?;
assert_eq!(feature_client.head_commit().await?, feature.target);

for branch in client.list_branches().await? {
    println!("{} -> {}", branch.name, branch.target);
}

client.delete_branch("feature/search", feature_client.head_commit().await?).await?;
```

### 9.2 Tags

| Method | Request | Response |
| --- | --- | --- |
| `create_tag(name, target).await` | Tag name and commit | `Tag { name, target }` |
| `list_tags().await` | None | `Vec<Tag>` |
| `delete_tag(name, expected).await` | Tag and expected target | `()` |
| `list_tag_reflog(tag).await` | Tag name | `Vec<(ReflogEntryId, ReflogEntryV1)>` |
| `recover_tag(tag, reflog, expected_target, reason).await` | Reflog selection, CAS expectation, reason | Recovered `Tag` |

```rust
let release = client.create_tag("v1.0.0", client.head_commit().await?).await?;
assert_eq!(release.name, "v1.0.0");

let tags = client.list_tags().await?;
client.delete_tag("v1.0.0", release.target).await?;

let reflog = client.list_tag_reflog("v1.0.0").await?;
if let Some((entry, _)) = reflog.first() {
    let _recovered = client
        .recover_tag("v1.0.0", *entry, release.target, "restore release tag")
        .await?;
}
let _ = tags;
```

## 10. Merge, restore, and reflog recovery

| Method | Request | Response |
| --- | --- | --- |
| `merge_bases(left, right).await` | Two commits | All best merge bases |
| `plan_merge(source, selected_base, policy).await` | Source commit, optional base, `MergePolicy` | `MergePlan` with changes and conflicts |
| `merge(source, selected_base, policy, operation, message).await` | Merge identity and optional idempotency/message | Two-parent `CommitReceipt` |
| `restore(source, expected_head, operation, message).await` | Source snapshot and current-head expectation | History-preserving child `CommitReceipt` |
| `list_reflog().await` | Selected branch | `Vec<(ReflogEntryId, ReflogEntryV1)>` |
| `recover_branch(reflog, expected_head, reason).await` | Reflog entry and branch CAS expectation | `RefMoveReceipt` |

`MergePolicy` is `Fail`, `Ours`, or `Theirs`.

```rust
use prolly_s3_client::core::MergePolicy;

let ours = client.head_commit().await?;
let theirs = feature_client.head_commit().await?;
let bases = client.merge_bases(ours, theirs).await?;

let selected_base = bases.first().copied();
let plan = client
    .plan_merge(theirs, selected_base, MergePolicy::Fail)
    .await?;

if plan.conflicts.is_empty() {
    let merged = client
        .merge(
            theirs,
            selected_base,
            MergePolicy::Fail,
            None,
            Some("merge feature/search".to_string()),
        )
        .await?;
    println!("merge commit={}", merged.id);
}

let restored = client
    .restore(theirs, client.head_commit().await?, None, Some("restore snapshot".into()))
    .await?;
println!("restore commit={}", restored.id);
```

## 11. Reliability, recovery, and integrity API

| Method | Request | Response |
| --- | --- | --- |
| `reconcile_operation(id).await` | Stable `OperationId` after timeout/cancellation | `Option<CommitReceipt>` |
| `list_reflog().await` | Selected branch | Validated reflog entries |
| `list_native_branch_ref_versions().await` | Provider with native bucket versioning | `Vec<NativeBranchRefVersion>` |
| `recover_branch_from_native_version(version, expected_head, reason).await` | Native physical ref version and CAS expectation | `RefMoveReceipt` after full target fsck |
| `fsck().await` | Entire repository | `FsckReport` |
| `fsck_commit(commit).await` | One retained commit closure | `FsckReport` |
| `repair_missing_from(source).await` | Another client with matching repository identity | `RepairReport { sync, fsck }` |

```rust
use prolly_s3_client::core::OperationId;

let operation = OperationId::new();
let attempted = client
    .put_object()
    .bucket(client.bucket())
    .key("reliable.txt")
    .body(ByteStream::from_static(b"idempotent input"))
    .operation_id(operation)
    .send()
    .await;

if let Err(error) = &attempted {
    if error.retry == prolly_s3_client::core::RetryAdvice::ReconcileOperation {
        if let Some(receipt) = client.reconcile_operation(operation).await? {
            println!("operation committed as {}", receipt.id);
        }
    }
}

let report = client.fsck().await?;
println!(
    "branches={} commits={} nodes={} versions={} verified_bytes={}",
    report.branches,
    report.commits,
    report.reachable_nodes,
    report.logical_versions,
    report.content_bytes_verified,
);
```

Native branch-ref recovery is administrative. `NativeBranchRefVersion` exposes
the native `version_id`, logical target, generation, operation, writer,
timestamps, and tombstone state. It is never accepted as a logical object
version ID.

## 12. Clone, fetch, repair, and push API

| Method | Request | Response |
| --- | --- | --- |
| `clone_to(target_aws, target_bucket, target_prefix, target_identity, qualification).await` | Empty target and provider qualification settings | `QualifiedClone { copy, provider_profile, target_s3_metrics }` |
| `fetch_from(source).await` | Source client with matching repository identity | `SyncReport`; copies reachable missing closure without moving this ref |
| `fetch_from_resumable(source, run, max_objects).await` | Optional operation ID and bounded batch size | `SyncRunV1` checkpoint |
| `sync_run(run).await` | Existing destination-local run ID | Current `SyncRunV1` |
| `push_to(destination, expected_destination, reason).await` | Destination client, exact expected head, audit reason | `SyncReport` including optional destination `RefMoveReceipt` |

`SyncReport` reports `source_head`, `copied_objects`, `copied_bytes`,
`already_present`, and an optional `ref_move`.

```rust
use prolly_s3_client::ProviderQualificationOptions;

let cloned = client
    .clone_to(
        target_aws_client,
        "backup-bucket",
        ".prolly/v1",
        ProviderIdentity::aws_region("us-west-2"),
        ProviderQualificationOptions::default(),
    )
    .await?;

println!("copied {} immutable objects", cloned.copy.immutable_objects);

let fetched = destination.fetch_from(&client).await?;
println!("fetched {} objects", fetched.copied_objects);

let checkpoint = destination
    .fetch_from_resumable(&client, None, 1_000)
    .await?;
let checkpoint = destination.sync_run(checkpoint.id).await?;

let pushed = client
    .push_to(
        &destination,
        destination.head_commit().await?,
        "publish replicated head",
    )
    .await?;

let repaired = destination.repair_missing_from(&client).await?;
let _ = (checkpoint, pushed, repaired);
```

Clone and sync copy canonical portable history only. Provider attestations,
SlateDB cache data, publication leases, workspaces, uploads, and GC state are
destination-local and are not cloned.

## 13. Retention and garbage collection API

GC is always explicit: create or load a dry-run plan, review it, then sweep.
No worker starts during `open()`.

### 13.1 Retention pins

| Method | Request | Response |
| --- | --- | --- |
| `create_retention_pin(name, target, owner, reason, ttl).await` | Named retained commit and optional expiry | `RetentionPinV1` |
| `list_retention_pins().await` | None | `Vec<RetentionPinV1>` |
| `delete_retention_pin(name, expected).await` | Name and exact pinned target | `()` |

```rust
use std::time::Duration;

let pin = client
    .create_retention_pin(
        "quarterly-release",
        client.head_commit().await?,
        "release-service",
        "retain for audit",
        Some(Duration::from_secs(90 * 24 * 60 * 60)),
    )
    .await?;

let pins = client.list_retention_pins().await?;
client.delete_retention_pin(&pin.name, pin.target).await?;
let _ = pins;
```

### 13.2 GC planning and sweeping

| Method | Request | Response |
| --- | --- | --- |
| `plan_gc(grace, max_candidates).await` | Grace period and hard candidate bound | `GcDryRun` with immutable plan and per-kind counts/bytes |
| `plan_gc_resumable(run, grace, max_candidates).await` | Optional operation ID | Durable `GcMarkRunV1` checkpoint |
| `gc_mark_run(run).await` | Mark operation ID | Current mark checkpoint |
| `load_gc_plan(plan).await` | `GcPlanId` | Hash-validated `GcPlanV1` |
| `sweep_gc(plan).await` | Reviewed plan ID | Complete `GcSweepReport` when possible |
| `sweep_gc_batch(plan, max_candidates).await` | Plan and bounded batch | Checkpointed `GcSweepReport` |
| `gc_run(plan).await` | Plan ID | Current `GcRunV1` generation/state/counters |
| `abort_gc_run(plan, expected_generation, reason).await` | Exact generation and operator reason | Aborted `GcRunV1` |

```rust
let dry_run = client
    .plan_gc(Duration::from_secs(2 * 60 * 60), 100_000)
    .await?;

println!(
    "plan={} candidates={} bytes={}",
    dry_run.plan.id,
    dry_run.plan.body.candidates.len(),
    dry_run.candidate_bytes,
);

// Application/operator approval occurs here.
let mut report = client.sweep_gc_batch(dry_run.plan.id, 1_000).await?;
while !report.complete {
    report = client.sweep_gc_batch(dry_run.plan.id, 1_000).await?;
}
```

An interrupted running sweep blocks publication until it resumes or an
operator proves no delete worker survives and calls `abort_gc_run` with the
current generation and a nonempty reason.

## 14. Provider qualification and capability API

| API | Request | Response |
| --- | --- | --- |
| `ProviderIdentity::aws_region(region)` | AWS region | General-purpose AWS provider identity |
| `ProviderIdentity::s3_compatible(endpoint, region)` | Endpoint and signing region | Path-style S3-compatible identity |
| `ProviderIdentity::path_style(bool)` | Addressing choice | Updated identity |
| `ProviderIdentity::bucket_class()` | None | Declared `BucketClass` |
| `HmacAttestationSigner::new(active, key_ring)` | Active key ID and keys of at least 32 bytes | Signer |
| `HmacAttestationSigner::single(id, key)` | One key | Signer |
| `ClientBuilder::qualify_provider().await` | Configured bucket/endpoint/signer | Signed `ProviderAttestationV1` |
| `client.provider_profile()` | Open client | Current `ProviderProfileId` |
| `client.refresh_capabilities().await` | Open client | Reloaded valid profile without probe writes |

```rust
let profile = client.provider_profile()?;
let refreshed = client.refresh_capabilities().await?;
assert_eq!(profile, refreshed);
```

Qualification proves immutable create-only writes, conditional mutable ref
CAS, physical listing/version capabilities, exact deletion behavior, and
endpoint/bucket binding. An expired or mismatched attestation fails closed.

## 15. Cursor signing API

`TokenSigner` is the extension trait for listing cursors. The built-in
`HmacTokenSigner` supports restart-safe key rotation:

```rust
use prolly_s3_client::{HmacTokenKey, HmacTokenSigner};

let signer = HmacTokenSigner::managed(
    "cursor-2026-09",
    [
        HmacTokenKey::retained("cursor-2026-09", vec![0x19; 32]),
        HmacTokenKey::retired("cursor-2026-08", vec![0x18; 32], retired_at_millis),
        HmacTokenKey::removed("cursor-2026-07", older_retirement_millis),
    ],
)?;
```

| API | Meaning |
| --- | --- |
| `HmacTokenKey::retained(id, secret)` | Active or indefinite verification key |
| `HmacTokenKey::retired(id, secret, time)` | Retains secret until TTL plus skew elapses |
| `HmacTokenKey::removed(id, time)` | Secret-free ledger tombstone after safe removal |
| `HmacTokenSigner::new(active, keys)` | Simple key ring |
| `HmacTokenSigner::managed(active, states)` | Rotation-aware key ring |
| `HmacTokenSigner::single(id, key)` | One nonrotating key |

## 16. Advisory index API

The advisory index may improve performance but never owns canonical state.
Deleting it must not change logical results.

| API | Request | Response |
| --- | --- | --- |
| `MemoryAdvisoryIndex::default()` | None | Process-local advisory index |
| `SlateDbAdvisoryIndex::open_owned(store, repository, writer).await` | SlateDB object store and exclusive writer identity | Owner-bound same-bucket index |
| `AdvisoryIndex::record_commit(repository, receipt).await` | Canonical commit receipt | `()` |
| `AdvisoryIndex::branch_head(repository, branch).await` | Repository and branch | Optional cached `CommitId` |
| `AdvisoryIndex::rebuild_heads(repository, heads).await` | Canonical branch heads | `AdvisoryRebuildReport` |
| `client.rebuild_advisory_index().await` | Configured index | `AdvisoryRebuildReport` |
| `SlateDbAdvisoryIndex::path()` | None | Owned cache path |
| `database()` | None | Diagnostic `&slatedb::Db` handle |
| `flush().await` | None | `()` |
| `close().await` | None | `()` |
| `quarantine_count(repository).await` | Repository ID | Number of corrupt cached records quarantined |

```rust
use std::sync::Arc;
use prolly_s3_client::SlateDbAdvisoryIndex;

let index = Arc::new(
    SlateDbAdvisoryIndex::open_owned(
        slatedb_object_store,
        repository_id,
        "api-service-1",
    )
    .await?,
);

let client = Client::builder()
    // same required builder configuration as section 3
    .advisory_index(index.clone())
    .open()
    .await?;

let rebuild = client.rebuild_advisory_index().await?;
index.flush().await?;
println!("rewritten heads={}", rebuild.written_heads);
```

`SlateDbAdvisoryIndex` is available only with the `slatedb-index` feature.

## 17. Introspection and metrics API

| API | Response |
| --- | --- |
| `client.bucket()` | Configured bucket |
| `client.branch()` | Selected branch |
| `client.repository_id()` | Durable repository identity |
| `client.physical_layout()` | Zero-I/O description of every physical path family and discipline |
| `client.s3_operation_metrics()` | Current object-plane counters |
| `client.reset_s3_operation_metrics()` | Previous counters and starts a new interval |
| `AwsS3ObjectPlane::new(client, bucket)` | Low-level object-plane adapter |
| `plane.client()` / `plane.bucket()` | Underlying SDK client and bucket |
| `plane.metrics()` / `plane.reset_metrics()` | Low-level counters |
| `S3WireAttemptInterceptor::new()` | Cloneable Smithy interceptor |
| `interceptor.metrics()` / `interceptor.reset()` | SDK executions, transmissions, retries, and response classes |

`S3OperationMetrics` contains counts for `get_object`, `head_object`,
`put_object`, `list_objects_v2`, `list_object_versions`, `delete_object`, plus
uploaded and downloaded body bytes. `total_calls()` sums physical SDK calls.

```rust
use prolly_s3_client::S3WireAttemptInterceptor;

let wire = S3WireAttemptInterceptor::new();
let aws_config = aws_sdk_s3::Config::builder()
    // credentials, region, endpoint, and other configuration
    .interceptor(wire.clone())
    .build();
let aws = aws_sdk_s3::Client::from_conf(aws_config);

let before = client.reset_s3_operation_metrics();
wire.reset(); // reset only when no request is in flight

// Run adapter operations.

let logical = client.s3_operation_metrics();
let physical = wire.metrics();
println!(
    "sdk_calls={} transmissions={} retries={}",
    logical.total_calls(),
    physical.transmissions,
    physical.retry_transmissions(),
);
let _ = (aws, before);
```

## 18. Physical layout inspection

`client.physical_layout()` performs no S3 request. It returns the bucket,
repository prefix, and path-family descriptors. Each family declares whether
it is immutable, mutable-CAS, or exact-version managed; whether clone includes
it; and whether GC owns it.

```rust
let layout = client.physical_layout();
println!("bucket={} prefix={}", layout.bucket, layout.repository_prefix);
for family in layout.families {
    println!(
        "{} {:?} clone={} gc={}",
        family.relative_pattern,
        family.discipline,
        family.portable_clone,
        family.gc_managed,
    );
}
```

Do not modify canonical physical objects with raw S3 commands. Raw tools are
appropriate only for inspection and documented recovery procedures.

## 19. Limits and unsupported behavior

| Limit or exclusion | v1 behavior |
| --- | --- |
| Key length | At most 1,024 bytes; keys must be UTF-8 through AWS-shaped APIs |
| Listing page | At most 1,000 results |
| `DeleteObjects` | At most 1,000 keys |
| Explicit atomic commit | At most 10,000 mutations |
| Object size | At most 5 TiB |
| Canonical payload chunk | 8 MiB |
| Logical retry limit | At most 16 |
| Selected-version deletion | Unsupported; append a delete marker instead |
| Cross-bucket copy/access | Unsupported in a bound client |
| Bucket create/delete | Not adapted |
| ACL, policy, lifecycle, Object Lock | Not adapted |
| S3 Select and presigning | Not adapted |
| Directory buckets, Outposts, access points, Object Lambda, MRAP | Outside v1 provider profile |
| Automatic GC | Never started |
| Automatic workspace rebase | Never performed |
| Logical APIs through AWS CLI | Not available; this is not an HTTP proxy |

## 20. Recommended application patterns

### One independent object mutation

Use `put_object`, `delete_object`, or `copy_object` with a stable
`operation_id` when the caller may time out or be canceled.

### Many related mutations

Use `begin_commit` and stage up to 10,000 mutations. This publishes one atomic
bucket snapshot and amortizes commit/ref/reflog overhead.

### Stable reads across concurrent writes

Record `head_commit`, call `at(commit)`, and perform every read or listing from
the returned `Snapshot`.

### Pagination across processes

Configure the same managed `TokenSigner` key ring on every process and retain
retired verification keys for cursor TTL plus clock skew.

### Ambiguous publishing outcome

Reuse the same `OperationId`, call `reconcile_operation`, and retry only after
the result proves the original operation was not committed.

### Maintenance

Create retention pins first, run `fsck`, create and review a bounded GC plan,
then sweep in rate-limited batches. Never delete repository paths directly.

## 21. Executable examples and contracts

- Complete RustFS example:
  [`client/examples/rustfs_versioned_bucket.rs`](client/examples/rustfs_versioned_bucket.rs)
- Minimal and operational guidance: [`README.md`](README.md)
- Exact compatibility manifest: [`compatibility-v1.json`](compatibility-v1.json)
- Operational recovery procedures: [`OPERATIONS.md`](OPERATIONS.md)
- Measured behavior: [`QUALIFICATION.md`](QUALIFICATION.md)
