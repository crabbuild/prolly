use std::{
    sync::Arc,
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
use prolly_s3_client::{
    core::{
        decode_canonical, encode_canonical, LogicalObjectVersionKind, MergeCursor, MergePhase,
        MergePolicy, ProviderPerKeyVersionLimit,
    },
    CheckoutRef, Client, HmacAttestationSigner, ProviderIdentity,
};

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
async fn rustfs_commit_session_preserves_n_plus_three_puts() {
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
        calls.put_object, 5,
        "two immutable payload PUTs plus commit/event/ref publication must preserve N + 3: {calls:?}"
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
    let results = stream::iter(0..COMMITS)
        .map(|index| {
            let client = client.clone();
            async move {
                client
                    .put_object(
                        format!("concurrent/{index:05}.bin"),
                        index.to_be_bytes().to_vec(),
                    )
                    .await
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
