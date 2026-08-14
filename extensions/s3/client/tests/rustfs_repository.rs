use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::Region,
    primitives::ByteStream,
    types::{BucketVersioningStatus, VersioningConfiguration},
};
use futures_util::{stream, StreamExt};
use md5::{Digest as _, Md5};
use prolly_s3_client::{
    core::{
        decode_canonical, encode_canonical, BatchId, Error, ErrorCode, LogicalObjectVersionKind,
        MergeCursor, MergePhase, MergePolicy, ProviderPerKeyVersionLimit,
    },
    BulkWriteOptions, CheckoutRef, Client, HmacAttestationSigner, ProviderIdentity, PutObjectInput,
};
use sha2::Sha256;

fn rustfs_enabled() -> bool {
    std::env::var("PROLLY_S3_RUSTFS").as_deref() == Ok("1")
}

fn unique_name(label: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_nanos();
    format!("integration/{label}/{nanos}")
}

fn provider_identity() -> ProviderIdentity {
    ProviderIdentity::s3_compatible(
        std::env::var("PROLLY_RUSTFS_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string()),
        "us-east-1",
    )
}

fn attestation_signer() -> Arc<HmacAttestationSigner> {
    Arc::new(HmacAttestationSigner::single("rustfs-authority-test", vec![0x31; 32]).unwrap())
}

async fn rustfs_client() -> (aws_sdk_s3::Client, String) {
    let endpoint = std::env::var("PROLLY_RUSTFS_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let access_key =
        std::env::var("PROLLY_RUSTFS_ACCESS_KEY").unwrap_or_else(|_| "prollyadmin".to_string());
    let secret_key = std::env::var("PROLLY_RUSTFS_SECRET_KEY")
        .unwrap_or_else(|_| "prolly-local-secret-change-me".to_string());
    let bucket = std::env::var("PROLLY_RUSTFS_BUCKET")
        .unwrap_or_else(|_| "prolly-versioned-s3-tests".to_string());
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "rustfs-integration",
        ))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .build();
    let client = aws_sdk_s3::Client::from_conf(config);
    match client.create_bucket().bucket(&bucket).send().await {
        Ok(_) => {}
        Err(error) => {
            let text = format!("{error:?}");
            assert!(
                text.contains("BucketAlreadyOwnedByYou") || text.contains("BucketAlreadyExists"),
                "failed to create RustFS test bucket: {text}"
            );
        }
    }
    client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("enable RustFS bucket versioning");
    (client, bucket)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_client_uses_immutable_payloads_and_fences_takeover() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("repository-client-repository");
    let old_writer = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-old-writer")
        .authority_lease_duration(Duration::from_secs(10))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();
    let first = old_writer
        .put_object("docs/history.txt", b"before takeover".to_vec())
        .await
        .unwrap();
    let stored = old_writer
        .get_object("docs/history.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.bytes, b"before takeover");
    let range = old_writer
        .get_object_range(first.id, "docs/history.txt", 1..=6)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(range.bytes, b"efore ");
    assert!(stored
        .version
        .binding
        .unwrap()
        .path
        .as_str()
        .contains("/payloads/"));
    old_writer
        .put_object("docs/history.txt", b"updated before takeover".to_vec())
        .await
        .unwrap();
    assert_eq!(
        old_writer
            .get_object_at(first.id, "docs/history.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"before takeover"
    );
    let (_, listed, truncated) = old_writer.list_objects("docs/", None, 10).await.unwrap();
    assert!(!truncated);
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key, b"docs/history.txt");
    let (_, versions) = old_writer
        .list_object_versions("docs/history.txt", 10)
        .await
        .unwrap();
    assert_eq!(versions.len(), 2);
    old_writer.delete_object("docs/history.txt").await.unwrap();
    assert!(old_writer
        .get_object("docs/history.txt")
        .await
        .unwrap()
        .is_none());
    let (_, versions) = old_writer
        .list_object_versions("docs/history.txt", 10)
        .await
        .unwrap();
    assert_eq!(versions.len(), 3);
    assert!(matches!(
        versions[0].body.kind,
        LogicalObjectVersionKind::DeleteMarker
    ));

    let replacement = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-new-writer")
        .read_only(true)
        .authority_lease_duration(Duration::from_secs(10))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .open()
        .await
        .unwrap();
    assert_eq!(
        replacement
            .takeover_branch_writer(
                "main",
                "rustfs-repository-old-writer",
                1,
                "old repository test credentials revoked and process isolated",
            )
            .await
            .unwrap(),
        2
    );

    old_writer.reset_s3_operation_metrics();
    let error = old_writer
        .put_object("docs/stale-.txt", b"must not upload".to_vec())
        .await
        .unwrap_err();
    assert_eq!(error.code, prolly_s3_client::ErrorCode::PreconditionFailed);
    let stale_calls = old_writer.reset_s3_operation_metrics();
    assert_eq!(
        stale_calls.put_object, 0,
        "unexpected calls: {stale_calls:?}"
    );

    tokio::time::sleep(Duration::from_secs(11)).await;
    replacement
        .put_object("docs/current-.txt", b"writer-b".to_vec())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_writable_reopen_resumes_the_same_writer() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("repository-writable-reopen");
    let original = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-restartable-writer")
        .authority_lease_duration(Duration::from_secs(10))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();
    original
        .put_object("docs/before-restart.txt", b"before".to_vec())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(11)).await;
    original
        .put_object("docs/after-lease-window.txt", b"renewed".to_vec())
        .await
        .unwrap();
    assert!(original.fenced_branches().unwrap().is_empty());
    original.advance_branch_indexes().await.unwrap();
    drop(original);

    let reopened = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-restartable-writer")
        .authority_lease_duration(Duration::from_secs(10))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .open()
        .await
        .unwrap();
    reopened
        .put_object("docs/after-restart.txt", b"after".to_vec())
        .await
        .unwrap();
    assert_eq!(
        reopened
            .get_object("docs/after-restart.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"after"
    );
    assert!(reopened.fenced_branches().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_ref_lifecycle_uses_catalog_shards_without_ref_scans() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("repository-ref-catalog");
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-ref-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .background_index_maintenance(false)
        .initialize()
        .await
        .unwrap();
    let main = client.head().await.unwrap();
    client.create_branch("feature", Some(main)).await.unwrap();
    let feature = client.checkout("feature").await.unwrap();
    let committed = feature
        .put_object("docs/feature.txt", b"branch-local".to_vec())
        .await
        .unwrap();
    feature.advance_branch_indexes().await.unwrap();
    let tag = client.create_tag("release-1", committed.id).await.unwrap();

    let explicit_branch = client.checkout("refs/heads/feature").await.unwrap();
    assert_eq!(explicit_branch.branch(), Some("feature"));
    let tagged = client
        .checkout(CheckoutRef::Tag("release-1".to_string()))
        .await
        .unwrap();
    assert_eq!(tagged.branch(), None);
    assert_eq!(tagged.head().await.unwrap(), committed.id);
    assert_eq!(
        tagged
            .get_object("docs/feature.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"branch-local"
    );
    assert_eq!(
        tagged
            .put_object("docs/rejected.txt", b"detached".to_vec())
            .await
            .unwrap_err()
            .code,
        prolly_s3_client::ErrorCode::InvalidRevision
    );
    let detached = client.checkout(committed.id).await.unwrap();
    assert_eq!(detached.branch(), None);
    assert_eq!(detached.head().await.unwrap(), committed.id);

    client.reset_s3_operation_metrics();
    let branches = client.list_branch_catalog_page(None, 100).await.unwrap();
    let mut names = branches
        .branches
        .into_iter()
        .map(|branch| branch.name)
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names, vec!["feature", "main"]);
    assert_eq!(
        client.list_tag_catalog_page(None, 100).await.unwrap().tags,
        vec![tag]
    );
    let metrics = client.reset_s3_operation_metrics();
    assert_eq!(metrics.list_objects_v2, 0, "catalog listing scanned refs");
    assert_eq!(
        metrics.list_object_versions, 0,
        "catalog listing scanned ref versions"
    );

    client.delete_tag("release-1", committed.id).await.unwrap();
    client.delete_branch("feature", committed.id).await.unwrap();
    assert!(client
        .list_tag_catalog_page(None, 100)
        .await
        .unwrap()
        .tags
        .is_empty());
    assert_eq!(
        client
            .list_branch_catalog_page(None, 100)
            .await
            .unwrap()
            .branches
            .into_iter()
            .map(|branch| branch.name)
            .collect::<Vec<_>>(),
        vec!["main"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_detached_paged_and_streamed_lists_stay_on_the_selected_commit() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_name("detached-listing"))
        .writer("rustfs-detached-list-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .background_index_maintenance(false)
        .initialize()
        .await
        .unwrap();
    let base = client.head().await.unwrap();
    client.create_branch("feature", Some(base)).await.unwrap();
    let feature = client.checkout("feature").await.unwrap();
    feature
        .put_object("docs/a.txt", b"a".to_vec())
        .await
        .unwrap();
    feature
        .put_object("docs/b.txt", b"b".to_vec())
        .await
        .unwrap();
    let selected = feature
        .put_object("docs/c.txt", b"c".to_vec())
        .await
        .unwrap()
        .id;
    feature.advance_branch_indexes().await.unwrap();

    // Checkout clones retain `feature` as the node-index lookup context while
    // exposing the selected immutable commit as their logical revision.
    let detached = feature.checkout(selected).await.unwrap();
    assert_eq!(detached.branch(), None);
    feature.delete_object("docs/b.txt").await.unwrap();
    feature
        .put_object("docs/later.txt", b"later".to_vec())
        .await
        .unwrap();
    feature.advance_branch_indexes().await.unwrap();

    let branch_page = feature.list_objects_page("docs/", None, 10).await.unwrap();
    assert_ne!(branch_page.snapshot, selected);
    assert_eq!(
        branch_page
            .objects
            .iter()
            .map(|object| object.key.as_slice())
            .collect::<Vec<_>>(),
        vec![
            b"docs/a.txt".as_slice(),
            b"docs/c.txt".as_slice(),
            b"docs/later.txt".as_slice(),
        ]
    );

    let mut continuation = None;
    let mut paged_keys = Vec::new();
    loop {
        let page = detached
            .list_objects_page("docs/", continuation.as_deref(), 1)
            .await
            .unwrap();
        assert_eq!(page.snapshot, selected);
        paged_keys.extend(page.objects.into_iter().map(|object| object.key));
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    let historical_keys = vec![
        b"docs/a.txt".to_vec(),
        b"docs/b.txt".to_vec(),
        b"docs/c.txt".to_vec(),
    ];
    assert_eq!(paged_keys, historical_keys);

    let streamed = detached.stream_objects("docs/", 1);
    futures_util::pin_mut!(streamed);
    let mut streamed_keys = Vec::new();
    while let Some(object) = streamed.next().await {
        streamed_keys.push(object.unwrap().key);
    }
    assert_eq!(streamed_keys, historical_keys);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_commit_session_accounts_for_publication_ticket() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_name("repository-commit-session"))
        .writer("rustfs-repository-batch-writer")
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();
    let mut session = client
        .begin_commit()
        .ephemeral()
        .message("two puts and one delete")
        .start()
        .await
        .unwrap();
    client.reset_s3_operation_metrics();
    session
        .put_object("batch/a.txt", b"first".to_vec())
        .await
        .unwrap();
    session
        .put_stream("batch/b.txt", ByteStream::from_static(b"second"))
        .await
        .unwrap();
    session.delete_object("batch/removed.txt").unwrap();
    let receipt = session.publish().await.unwrap();
    assert_eq!(receipt.changed_keys, 3);
    let calls = client.reset_s3_operation_metrics();
    assert_eq!(
        calls.put_object, 6,
        "two payloads, commit/event/ref publication, and one GC admission ticket require N + 4 PUTs: {calls:?}"
    );
    assert_eq!(
        calls.delete_object, 1,
        "successful publication must exact-delete its admission ticket: {calls:?}"
    );
    assert_eq!(
        calls.head_object, 0,
        "the successful streaming path must not require reconciliation HEADs: {calls:?}"
    );
    assert_eq!(
        client
            .get_object("batch/a.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"first"
    );
    assert_eq!(
        client
            .get_object("batch/b.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"second"
    );
    assert!(client
        .get_object("batch/removed.txt")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_merge_resumes_and_publishes_structural_plan() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("repository-merge");
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-merge-writer")
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();
    client
        .put_object("merge/conflict.txt", b"base".to_vec())
        .await
        .unwrap();
    let base = client.head().await.unwrap();
    client.create_branch("feature", Some(base)).await.unwrap();
    client
        .put_object("merge/conflict.txt", b"ours".to_vec())
        .await
        .unwrap();
    let feature = client.checkout("feature").await.unwrap();
    feature
        .put_object("merge/conflict.txt", b"theirs".to_vec())
        .await
        .unwrap();
    feature
        .put_object("merge/source-only.txt", b"source".to_vec())
        .await
        .unwrap();

    let mut cursor = client
        .start_merge(
            "feature",
            None,
            MergePolicy::Theirs,
            "merge feature through RustFS",
        )
        .await
        .unwrap();
    drop(feature);
    drop(client);
    while cursor.phase != MergePhase::ReadyToPublish {
        let reopened = Client::builder()
            .aws_client(aws.clone())
            .bucket(&bucket)
            .repository_prefix(&repository_prefix)
            .writer("rustfs-repository-merge-writer")
            .background_index_maintenance(false)
            .provider_identity(provider_identity())
            .attestation_signer(attestation_signer())
            .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
            .open()
            .await
            .unwrap();
        let encoded = encode_canonical(&cursor).unwrap();
        let restored: MergeCursor = decode_canonical(&encoded).unwrap();
        cursor = reopened.advance_merge(&restored, 2).await.unwrap().cursor;
        drop(reopened);
    }
    let publisher = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-merge-writer")
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .open()
        .await
        .unwrap();
    let receipt = publisher.publish_merge(&cursor).await.unwrap();
    assert_eq!(receipt.changed_keys, 2);
    assert_eq!(receipt.conflicts, 1);
    assert_eq!(
        publisher
            .get_object("merge/conflict.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"theirs"
    );
    assert_eq!(
        publisher
            .get_object("merge/source-only.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"source"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_durable_session_resumes_without_payload_reupload() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("repository-durable-resume");
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-resumable-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();
    let payload = vec![0x5a; 1024 * 1024];
    let mut session = client
        .begin_commit()
        .message("resume after process loss")
        .start()
        .await
        .unwrap();
    let batch = session.id();
    let operation = session.operation();
    session
        .put_stream("resume/large.bin", ByteStream::from(payload.clone()))
        .await
        .unwrap();
    session.checkpoint().await.unwrap();
    drop(session);
    drop(client);

    let reopened = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-resumable-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .open()
        .await
        .unwrap();
    reopened.reset_s3_operation_metrics();
    let resumed = reopened.resume_commit(batch).await.unwrap();
    assert_eq!(resumed.operation(), operation);
    assert_eq!(resumed.staged_objects(), 1);
    let receipt = resumed.publish().await.unwrap();
    assert_eq!(receipt.operation, operation);
    let calls = reopened.reset_s3_operation_metrics();
    assert!(
        calls.uploaded_body_bytes < payload.len() as u64,
        "resume must publish metadata without uploading the 1 MiB payload again: {calls:?}"
    );
    assert_eq!(
        reopened
            .get_object("resume/large.bin")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        payload
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_cold_open_catches_indexes_before_serving_reads() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("repository-cold-index-catchup");
    let writer = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-index-writer")
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();
    for index in 0..3 {
        writer
            .put_object(
                format!("cold/{index}.txt"),
                format!("value-{index}").into_bytes(),
            )
            .await
            .unwrap();
    }
    drop(writer);

    let reader = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .read_only(true)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .open()
        .await
        .unwrap();
    let health = reader.branch_index_health().await.unwrap();
    assert!(
        health.ready,
        "cold open must await background catch-up: {health:?}"
    );
    assert_eq!(health.lag_generations, 0);
    assert_eq!(
        reader
            .get_object("cold/2.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"value-2"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_over_limit_index_rebuild_resumes_from_canonical_cursor() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("repository-index-rebuild");
    let writer = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-rebuild-writer")
        .journal_index_max_unindexed_events(2)
        .operation_index_limits(2, 2, 8)
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();
    let mut original = None;
    for index in 0..5 {
        let receipt = writer
            .put_object(
                format!("rebuild/{index}.txt"),
                format!("value-{index}").into_bytes(),
            )
            .await
            .unwrap();
        if index == 4 {
            original = Some(receipt);
        }
    }
    drop(writer);

    let reader = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .read_only(true)
        .journal_index_max_unindexed_events(2)
        .operation_index_limits(2, 2, 2)
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .open()
        .await
        .unwrap();
    assert!(!reader.branch_index_health().await.unwrap().ready);
    let mut cursor = reader.start_branch_index_rebuild().await.unwrap();
    loop {
        let bytes = prolly_s3_client::core::encode_canonical(&cursor).unwrap();
        cursor = prolly_s3_client::core::decode_canonical(&bytes).unwrap();
        let step = reader
            .advance_branch_index_rebuild(&cursor, 2)
            .await
            .unwrap();
        cursor = step.cursor;
        if step.complete {
            break;
        }
    }
    assert!(reader.branch_index_health().await.unwrap().ready);
    assert_eq!(
        reader
            .get_object("rebuild/4.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"value-4"
    );

    let mut operation = reader.start_operation_index_rebuild(&cursor).await.unwrap();
    loop {
        let bytes = prolly_s3_client::core::encode_canonical(&operation).unwrap();
        operation = prolly_s3_client::core::decode_canonical(&bytes).unwrap();
        let step = reader
            .advance_operation_index_rebuild(&operation, 2)
            .await
            .unwrap();
        operation = step.cursor;
        if step.complete {
            break;
        }
    }

    loop {
        if reader
            .cleanup_branch_index_rebuild(&cursor, &operation, 1)
            .await
            .unwrap()
            .complete
        {
            break;
        }
    }
    drop(reader);

    let replay_writer = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-repository-rebuild-writer")
        .journal_index_max_unindexed_events(2)
        .operation_index_limits(2, 2, 2)
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .open()
        .await
        .unwrap();
    let original = original.unwrap();
    let replay = replay_writer
        .put_object_with_operation("rebuild/4.txt", b"value-4".to_vec(), original.operation)
        .await
        .unwrap();
    assert!(replay.idempotent_replay);
    assert_eq!(replay.id, original.id);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rustfs_streaming_bulk_write_is_bounded_batched_and_ordered() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, bucket) = rustfs_client().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_name("streaming-bulk-write"))
        .writer("rustfs-streaming-bulk-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();

    let objects = stream::iter((0..257).map(|index| {
        Ok(PutObjectInput {
            key: format!("stream/{index:04}.txt"),
            bytes: format!("value-{index}").into_bytes(),
            headers: Default::default(),
            user_metadata: Default::default(),
        })
    }));
    let receipts = client
        .put_object_stream(
            objects,
            BulkWriteOptions {
                batch_size: 128,
                concurrency: 16,
                checkpoint_every: 32,
            },
        )
        .await
        .unwrap();
    assert_eq!(receipts.len(), 3);
    assert_eq!(
        receipts
            .iter()
            .map(|receipt| receipt.changed_keys)
            .collect::<Vec<_>>(),
        vec![128, 128, 1]
    );
    let (_, first) = client
        .head_object("stream/0000.txt")
        .await
        .unwrap()
        .unwrap();
    let (_, second) = client
        .head_object("stream/0001.txt")
        .await
        .unwrap()
        .unwrap();
    let first_binding = first.version.binding.unwrap();
    let second_binding = second.version.binding.unwrap();
    assert!(first_binding.is_packed());
    assert_eq!(first_binding.path, second_binding.path);
    assert_ne!(first_binding.pack_range, second_binding.pack_range);
    assert_eq!(
        client
            .get_object_range(receipts[0].id, "stream/0000.txt", 0..=u64::MAX)
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"value-0"
    );
    for index in [0, 31, 127, 128, 255, 256] {
        assert_eq!(
            client
                .get_object(format!("stream/{index:04}.txt"))
                .await
                .unwrap()
                .unwrap()
                .bytes,
            format!("value-{index}").as_bytes()
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rustfs_streaming_bulk_failure_preserves_completed_checkpoint() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, bucket) = rustfs_client().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_name("streaming-bulk-resume"))
        .writer("rustfs-streaming-resume-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();

    let objects = stream::iter((0..40).map(|index| {
        if index == 37 {
            Err(Error::new(ErrorCode::Transport, "upstream source failed"))
        } else {
            Ok(PutObjectInput {
                key: format!("resume/{index:04}.txt"),
                bytes: vec![index as u8],
                headers: Default::default(),
                user_metadata: Default::default(),
            })
        }
    }));
    let error = client
        .put_object_stream(
            objects,
            BulkWriteOptions {
                batch_size: 128,
                concurrency: 8,
                checkpoint_every: 16,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Transport);
    assert!(error.message.contains("after 32 staged objects"));
    let batch: BatchId = error.operation_id.unwrap().parse().unwrap();
    let resumed = client.resume_commit(batch).await.unwrap();
    assert_eq!(resumed.staged_objects(), 32);
    let receipt = resumed.publish().await.unwrap();
    assert_eq!(receipt.changed_keys, 32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "10K tiny-file RustFS throughput release gate"]
async fn rustfs_streaming_bulk_exceeds_500_files_per_second() {
    assert!(rustfs_enabled(), "set PROLLY_S3_RUSTFS=1 to run");
    const FILES: usize = 10_000;
    let (aws, bucket) = rustfs_client().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_name("streaming-bulk-throughput"))
        .writer("rustfs-streaming-throughput-writer")
        .authority_lease_duration(Duration::from_secs(600))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();
    let objects = stream::iter((0..FILES).map(|index| {
        Ok(PutObjectInput {
            key: format!("tiny/{index:05}.txt"),
            bytes: format!("{index:016x}").into_bytes(),
            headers: Default::default(),
            user_metadata: Default::default(),
        })
    }));
    client.reset_s3_operation_metrics();
    let started = std::time::Instant::now();
    let receipts = client
        .put_object_stream(objects, BulkWriteOptions::default())
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let throughput = FILES as f64 / elapsed.as_secs_f64();
    let metrics = client.reset_s3_operation_metrics();
    eprintln!(
        "RUSTFS_STREAMING_BULK files={FILES} wall_ms={} files_per_second={throughput:.2} s3_calls={} uploaded_bytes={}",
        elapsed.as_millis(),
        metrics.total_calls(),
        metrics.uploaded_body_bytes,
    );
    assert_eq!(receipts.len(), 1);
    assert!(
        throughput >= 500.0,
        "throughput was {throughput:.2} files/s"
    );
    assert!(
        metrics.put_object < 100,
        "packing should require fewer than 100 physical PUTs: {metrics:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "10K warm-read and cursor-listing RustFS release gate"]
async fn rustfs_warm_reads_and_cursor_listing_meet_scale_slos() {
    assert!(rustfs_enabled(), "set PROLLY_S3_RUSTFS=1 to run");
    const FILES: usize = 10_000;
    const READS: usize = 100;
    let (aws, bucket) = rustfs_client().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_name("read-list-performance"))
        .writer("rustfs-read-list-writer")
        .authority_lease_duration(Duration::from_secs(600))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .background_index_maintenance(false)
        .initialize()
        .await
        .unwrap();
    client
        .put_object_stream(
            stream::iter((0..FILES).map(|index| {
                Ok(PutObjectInput {
                    key: format!("tiny/{index:05}.txt"),
                    bytes: format!("{index:016x}").into_bytes(),
                    headers: Default::default(),
                    user_metadata: Default::default(),
                })
            })),
            BulkWriteOptions::default(),
        )
        .await
        .unwrap();
    client.advance_branch_indexes().await.unwrap();

    let sample = (0..READS)
        .map(|index| index * (FILES / READS))
        .collect::<Vec<_>>();
    for index in &sample {
        client
            .get_object(format!("tiny/{index:05}.txt"))
            .await
            .unwrap()
            .unwrap();
    }
    client.reset_s3_operation_metrics();
    let mut latencies = stream::iter(sample)
        .map(|index| {
            let client = client.clone();
            async move {
                let started = std::time::Instant::now();
                let object = client
                    .get_object(format!("tiny/{index:05}.txt"))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(object.bytes, format!("{index:016x}").as_bytes());
                started.elapsed()
            }
        })
        .buffer_unordered(32)
        .collect::<Vec<_>>()
        .await;
    latencies.sort_unstable();
    let p99 = latencies[(latencies.len() * 99).div_ceil(100) - 1];
    let read_metrics = client.reset_s3_operation_metrics();
    eprintln!(
        "RUSTFS_WARM_READS reads={READS} p99_ms={:.2} downloaded_bytes={} bytes_per_read={:.2} s3_calls={}",
        p99.as_secs_f64() * 1_000.0,
        read_metrics.downloaded_body_bytes,
        read_metrics.downloaded_body_bytes as f64 / READS as f64,
        read_metrics.total_calls(),
    );
    assert!(p99 < Duration::from_millis(100), "warm p99 was {p99:?}");
    assert!(
        read_metrics.downloaded_body_bytes < 4 * 1024 * 1024,
        "warm reads transferred too much metadata: {read_metrics:?}"
    );

    client.reset_s3_operation_metrics();
    let started = std::time::Instant::now();
    let listed = client
        .stream_objects("tiny/", 1_000)
        .collect::<Vec<_>>()
        .await;
    for object in &listed {
        object.as_ref().unwrap();
    }
    let elapsed = started.elapsed();
    let throughput = listed.len() as f64 / elapsed.as_secs_f64();
    let list_metrics = client.reset_s3_operation_metrics();
    eprintln!(
        "RUSTFS_CURSOR_LIST files={} wall_ms={} files_per_second={throughput:.2} downloaded_bytes={} s3_calls={} put_calls={}",
        listed.len(),
        elapsed.as_millis(),
        list_metrics.downloaded_body_bytes,
        list_metrics.total_calls(),
        list_metrics.put_object,
    );
    assert_eq!(listed.len(), FILES);
    assert!(throughput > 10_000.0, "listing achieved {throughput:.2}/s");
    assert_eq!(list_metrics.put_object, 0, "foreground listing wrote data");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "20K branch, structural-diff, and merge performance release gate"]
async fn rustfs_20k_branch_diff_merge_meets_amplification_slos() {
    assert!(rustfs_enabled(), "set PROLLY_S3_RUSTFS=1 to run");
    const FILES: usize = 20_000;
    const CHANGES: usize = 100;
    let (aws, bucket) = rustfs_client().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_name("20k-branch-diff-merge"))
        .writer("rustfs-20k-branch-diff-merge-writer")
        .authority_lease_duration(Duration::from_secs(600))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .background_index_maintenance(false)
        .initialize()
        .await
        .unwrap();
    let make_input = |index: usize, value: &'static str| PutObjectInput {
        key: format!("scale/{index:05}.txt"),
        bytes: format!("{index:016x}-{value}").into_bytes(),
        headers: Default::default(),
        user_metadata: Default::default(),
    };
    let receipts = client
        .put_object_stream(
            stream::iter((0..FILES).map(|index| Ok(make_input(index, "base")))),
            BulkWriteOptions::default(),
        )
        .await
        .unwrap();
    client.advance_branch_indexes().await.unwrap();
    let baseline = receipts.last().unwrap().id;

    let sample = (0..100)
        .map(|index| index * (FILES / 100))
        .collect::<Vec<_>>();
    for index in &sample {
        client
            .get_object(format!("scale/{index:05}.txt"))
            .await
            .unwrap()
            .unwrap();
    }
    client.reset_s3_operation_metrics();
    let read_started = std::time::Instant::now();
    let mut read_latencies = stream::iter(sample)
        .map(|index| {
            let client = client.clone();
            async move {
                let started = std::time::Instant::now();
                client
                    .get_object(format!("scale/{index:05}.txt"))
                    .await
                    .unwrap()
                    .unwrap();
                started.elapsed()
            }
        })
        .buffer_unordered(32)
        .collect::<Vec<_>>()
        .await;
    read_latencies.sort_unstable();
    let read_p99 = read_latencies[(read_latencies.len() * 99).div_ceil(100) - 1];
    let read_metrics = client.reset_s3_operation_metrics();
    eprintln!(
        "RUSTFS_20K_READ wall_ms={:.2} p99_ms={:.2} downloaded_bytes={} bytes_per_read={:.2} s3_calls={}",
        read_started.elapsed().as_secs_f64() * 1_000.0,
        read_p99.as_secs_f64() * 1_000.0,
        read_metrics.downloaded_body_bytes,
        read_metrics.downloaded_body_bytes as f64 / 100.0,
        read_metrics.total_calls(),
    );
    assert!(read_p99 < Duration::from_millis(100));
    assert!(read_metrics.downloaded_body_bytes < 4 * 1024 * 1024);

    client.reset_s3_operation_metrics();
    let list_started = std::time::Instant::now();
    let listed = client
        .stream_objects("scale/", 1_000)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(listed.len(), FILES);
    assert!(listed.iter().all(Result::is_ok));
    let list_elapsed = list_started.elapsed();
    let list_throughput = FILES as f64 / list_elapsed.as_secs_f64();
    let list_metrics = client.reset_s3_operation_metrics();
    eprintln!(
        "RUSTFS_20K_LIST wall_ms={:.2} entries_per_second={list_throughput:.2} downloaded_bytes={} s3_calls={} put_calls={}",
        list_elapsed.as_secs_f64() * 1_000.0,
        list_metrics.downloaded_body_bytes,
        list_metrics.total_calls(),
        list_metrics.put_object,
    );
    assert!(list_throughput > 10_000.0);
    assert_eq!(list_metrics.put_object, 0);

    client.reset_s3_operation_metrics();
    let branch_started = std::time::Instant::now();
    client
        .create_branch("feature", Some(baseline))
        .await
        .unwrap();
    let branch_elapsed = branch_started.elapsed();
    let branch_metrics = client.reset_s3_operation_metrics();
    eprintln!(
        "RUSTFS_20K_BRANCH wall_ms={:.2} downloaded_bytes={} s3_calls={}",
        branch_elapsed.as_secs_f64() * 1_000.0,
        branch_metrics.downloaded_body_bytes,
        branch_metrics.total_calls(),
    );
    assert!(branch_elapsed < Duration::from_millis(500));
    assert!(branch_metrics.downloaded_body_bytes < 512 * 1024);

    let feature = client.checkout("feature").await.unwrap();
    client
        .put_object_stream(
            stream::iter((0..CHANGES).map(|index| Ok(make_input(index, "main")))),
            BulkWriteOptions::default(),
        )
        .await
        .unwrap();
    client.advance_branch_indexes().await.unwrap();
    let main_head = client.head().await.unwrap();
    feature
        .put_object_stream(
            stream::iter((CHANGES..CHANGES * 2).map(|index| Ok(make_input(index, "feature")))),
            BulkWriteOptions::default(),
        )
        .await
        .unwrap();
    feature.advance_branch_indexes().await.unwrap();
    let feature_head = feature.head().await.unwrap();

    for (label, checkout, head) in [
        ("main", &client, main_head),
        ("feature", &feature, feature_head),
    ] {
        checkout.reset_s3_operation_metrics();
        let started = std::time::Instant::now();
        let page = checkout
            .diff_bounded(baseline, head, None, 1_000)
            .await
            .unwrap();
        let elapsed = started.elapsed();
        let metrics = checkout.reset_s3_operation_metrics();
        eprintln!(
            "RUSTFS_20K_DIFF branch={label} wall_ms={:.2} changes={} compared_nodes={} reused_subtrees={} downloaded_bytes={} s3_calls={}",
            elapsed.as_secs_f64() * 1_000.0,
            page.changes.len(), page.compared_nodes, page.reused_subtrees,
            metrics.downloaded_body_bytes, metrics.total_calls(),
        );
        assert_eq!(page.changes.len(), CHANGES);
        assert!(page.continuation.is_none());
        assert!(elapsed < Duration::from_millis(500));
        assert!(metrics.downloaded_body_bytes < 1024 * 1024);
    }

    client.reset_s3_operation_metrics();
    let plan_started = std::time::Instant::now();
    let mut merge = client
        .start_merge("feature", None, MergePolicy::Fail, "20K sparse merge")
        .await
        .unwrap();
    let start_metrics = client.reset_s3_operation_metrics();
    let mut plan_downloaded = start_metrics.downloaded_body_bytes;
    let mut plan_calls = start_metrics.total_calls();
    eprintln!(
        "RUSTFS_20K_MERGE_STEP step=start phase={:?} downloaded_bytes={} s3_calls={}",
        merge.phase,
        start_metrics.downloaded_body_bytes,
        start_metrics.total_calls(),
    );
    let mut processed = 0_usize;
    let mut pages = 0_usize;
    while merge.phase != MergePhase::ReadyToPublish {
        let before = merge.phase;
        let cache_before = client.node_cache_snapshot();
        let page = client.advance_merge(&merge, 256).await.unwrap();
        let cache_after = client.node_cache_snapshot();
        let step_metrics = client.reset_s3_operation_metrics();
        plan_downloaded = plan_downloaded.saturating_add(step_metrics.downloaded_body_bytes);
        plan_calls = plan_calls.saturating_add(step_metrics.total_calls());
        eprintln!(
            "RUSTFS_20K_MERGE_STEP step=advance before={before:?} after={:?} processed={} downloaded_bytes={} node_fetched_bytes={} node_avoided_bytes={} ranged_fetches={} s3_calls={}",
            page.cursor.phase, page.processed,
            step_metrics.downloaded_body_bytes,
            cache_after.fetched_bytes.saturating_sub(cache_before.fetched_bytes),
            cache_after.avoided_bytes.saturating_sub(cache_before.avoided_bytes),
            cache_after.ranged_fetches.saturating_sub(cache_before.ranged_fetches),
            step_metrics.total_calls(),
        );
        processed += page.processed;
        pages += 1;
        merge = page.cursor;
    }
    let plan_elapsed = plan_started.elapsed();
    eprintln!(
        "RUSTFS_20K_MERGE_PLAN wall_ms={:.2} pages={pages} processed={processed} changes={} conflicts={} downloaded_bytes={} s3_calls={}",
        plan_elapsed.as_secs_f64() * 1_000.0,
        merge.planned_changes, merge.conflicts,
        plan_downloaded, plan_calls,
    );
    assert_eq!(merge.planned_changes, CHANGES as u64);
    assert_eq!(merge.conflicts, 0);
    assert!(plan_elapsed < Duration::from_secs(1));
    assert!(plan_downloaded < 2 * 1024 * 1024);

    client.reset_s3_operation_metrics();
    let publish_started = std::time::Instant::now();
    let merged = client.publish_merge(&merge).await.unwrap();
    let publish_elapsed = publish_started.elapsed();
    let publish_metrics = client.reset_s3_operation_metrics();
    eprintln!(
        "RUSTFS_20K_MERGE_PUBLISH wall_ms={:.2} changes={} downloaded_bytes={} s3_calls={}",
        publish_elapsed.as_secs_f64() * 1_000.0,
        merged.changed_keys,
        publish_metrics.downloaded_body_bytes,
        publish_metrics.total_calls(),
    );
    assert_eq!(merged.changed_keys, CHANGES as u64);
    assert!(publish_elapsed < Duration::from_millis(250));
    assert_eq!(
        client
            .get_object(format!("scale/{CHANGES:05}.txt"))
            .await
            .unwrap()
            .unwrap()
            .bytes,
        make_input(CHANGES, "feature").bytes,
    );
}

/// Reproducible release gate for the same-branch publication lane.
///
/// This is intentionally ignored because it creates exactly 10,000 commits
/// and is meant for an isolated RustFS qualification run. Client clones issue
/// work concurrently; the branch-local lane serializes the linear ref history
/// without the former fixed logical-retry ceiling.
#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "expensive 10K-commit RustFS regression gate"]
async fn rustfs_10k_concurrent_commit_regression_gate() {
    assert!(
        rustfs_enabled(),
        "set PROLLY_S3_RUSTFS=1 to run the 10K RustFS gate"
    );

    const COMMITS: usize = 10_000;
    let concurrency = std::env::var("PROLLY_S3_10K_CONCURRENCY")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(32);
    assert!((1..=256).contains(&concurrency));

    let (aws, bucket) = rustfs_client().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_name("10k-concurrent-commits"))
        .writer("rustfs-10k-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();

    client.reset_s3_operation_metrics();
    let started = std::time::Instant::now();
    let completed = Arc::new(AtomicUsize::new(0));
    let results = stream::iter(0..COMMITS)
        .map(|index| {
            let client = client.clone();
            let completed = completed.clone();
            async move {
                let receipt = client
                    .put_object(
                        format!("concurrent/{index:05}.bin"),
                        index.to_be_bytes().to_vec(),
                    )
                    .await?;
                let count = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if count.is_multiple_of(1_000) {
                    eprintln!(
                        "RUSTFS_10K_PROGRESS completed={count} wall_ms={}",
                        started.elapsed().as_millis()
                    );
                }
                Ok::<_, Error>(receipt)
            }
        })
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;
    for result in results {
        result.unwrap();
    }

    let mut after = None;
    let mut files = 0_usize;
    loop {
        let (_, page, truncated) = client
            .list_objects("concurrent/", after.as_deref(), 1_000)
            .await
            .unwrap();
        files += page.len();
        after = page
            .last()
            .map(|item| String::from_utf8(item.key.clone()).unwrap());
        if !truncated {
            break;
        }
    }
    assert_eq!(files, COMMITS);

    let metrics = client.reset_s3_operation_metrics();
    eprintln!(
        "RUSTFS_10K_COMMITS commits={COMMITS} concurrency={concurrency} wall_ms={} s3_calls={} calls_per_commit={:.2}",
        started.elapsed().as_millis(),
        metrics.total_calls(),
        metrics.total_calls() as f64 / COMMITS as f64,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 12)]
#[ignore = "uploads a 65 MiB multipart payload to live RustFS"]
async fn rustfs_streamed_large_object_uses_bounded_multipart_upload() {
    assert!(
        rustfs_enabled(),
        "set PROLLY_S3_RUSTFS=1 to run the multipart RustFS gate"
    );
    const SIZE: usize = 65 * 1_024 * 1_024;
    let (aws, bucket) = rustfs_client().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_name("multipart-stream"))
        .writer("rustfs-multipart-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
        .initialize()
        .await
        .unwrap();

    client.reset_s3_operation_metrics();
    let mut session = client
        .begin_commit()
        .message("multipart stream")
        .start()
        .await
        .unwrap();
    session
        .put_stream("large/payload.bin", ByteStream::from(vec![0x5a; SIZE]))
        .await
        .unwrap();
    let receipt = session.publish().await.unwrap();
    let metrics = client.reset_s3_operation_metrics();
    assert_eq!(metrics.create_multipart_upload, 1);
    assert!(metrics.upload_part >= 2);
    assert_eq!(metrics.complete_multipart_upload, 1);
    assert_eq!(metrics.abort_multipart_upload, 0);
    assert!(metrics.uploaded_body_bytes >= SIZE as u64);
    assert!(metrics.uploaded_body_bytes < SIZE as u64 + 1024 * 1024);

    let tail = client
        .get_object_range(
            receipt.id,
            "large/payload.bin",
            (SIZE as u64 - 32)..=(SIZE as u64 - 1),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tail.bytes, vec![0x5a; 32]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "uploads and resumes a 33 MiB provider-native multipart object"]
async fn rustfs_native_multipart_resumes_after_restart_as_one_object() {
    assert!(
        rustfs_enabled(),
        "set PROLLY_S3_RUSTFS=1 to run the resumable multipart gate"
    );
    const MIB: usize = 1_024 * 1_024;
    let (aws, bucket) = rustfs_client().await;
    let prefix = unique_name("native-multipart-resume");
    let builder = |aws: aws_sdk_s3::Client| {
        Client::builder()
            .aws_client(aws)
            .bucket(&bucket)
            .repository_prefix(&prefix)
            .writer("rustfs-native-multipart-writer")
            .provider_identity(provider_identity())
            .attestation_signer(attestation_signer())
            .provider_per_key_version_limit(ProviderPerKeyVersionLimit::Finite(10_000))
    };
    let client = builder(aws.clone()).initialize().await.unwrap();
    let mut body = vec![0x11; 16 * MIB];
    body.extend(vec![0x22; 16 * MIB]);
    body.extend(vec![0x33; MIB]);
    let session = client
        .begin_commit()
        .message("native multipart resume")
        .start()
        .await
        .unwrap();
    let batch = session.id();
    let mut upload = session
        .begin_multipart_upload(
            "large/resumable.bin",
            body.len() as u64,
            Sha256::digest(&body).into(),
            Md5::digest(&body).into(),
        )
        .await
        .unwrap();
    let first_size = upload.expected_part_size(1).unwrap() as usize;
    let mut tampered = upload.clone();
    tampered.part_size += 1;
    assert_eq!(
        session
            .upload_multipart_part(&mut tampered, 1, Vec::new())
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidRequest
    );
    session
        .upload_multipart_part(&mut upload, 1, body[..first_size].to_vec())
        .await
        .unwrap();
    let persisted = serde_json::to_vec(&upload).unwrap();
    drop(session);
    drop(client);

    let reopened = builder(aws).open().await.unwrap();
    reopened.reset_s3_operation_metrics();
    let mut session = reopened.resume_commit(batch).await.unwrap();
    let mut upload = serde_json::from_slice(&persisted).unwrap();
    session
        .reconcile_multipart_upload(&mut upload)
        .await
        .unwrap();
    assert_eq!(upload.completed_parts.len(), 1);
    let mut offset = first_size;
    for number in 2..=upload.part_count().unwrap() {
        let len = upload.expected_part_size(number).unwrap() as usize;
        session
            .upload_multipart_part(&mut upload, number, body[offset..offset + len].to_vec())
            .await
            .unwrap();
        offset += len;
    }
    session
        .complete_multipart_upload(&mut upload, Default::default(), Default::default())
        .await
        .unwrap();
    session.publish().await.unwrap();
    let metrics = reopened.reset_s3_operation_metrics();
    assert_eq!(metrics.create_multipart_upload, 0);
    assert_eq!(metrics.upload_part, 2);
    assert_eq!(metrics.complete_multipart_upload, 1);
    assert!(metrics.list_parts >= 2);
    assert_eq!(metrics.abort_multipart_upload, 0);
    assert!(metrics.uploaded_body_bytes < body.len() as u64 - 8 * MIB as u64);
    assert_eq!(
        reopened
            .get_object("large/resumable.bin")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        body
    );
}
