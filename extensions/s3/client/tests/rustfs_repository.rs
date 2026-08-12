use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::Region,
    primitives::ByteStream,
    types::{BucketVersioningStatus, VersioningConfiguration},
};
use futures_util::StreamExt;
use md5::{Digest as _, Md5};
#[cfg(feature = "foyer-cache")]
use prolly_s3_client::{core::PhysicalBatchMutationV1, FoyerNodeCache, FoyerNodeCacheConfig};
use prolly_s3_client::{
    core::{
        LogicalObjectVersionKindV1, ObjectHeaders, PhysicalMultipartCompletedPart,
        ProviderPerKeyVersionLimitV2, Repository, RepositoryOptions,
    },
    AwsS3ObjectPlane, Client, ClientV2, HmacAttestationSigner, ProviderIdentity,
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
    Arc::new(HmacAttestationSigner::single("rustfs-authority-test-v1", vec![0x31; 32]).unwrap())
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
async fn rustfs_whole_object_write_uses_four_s3_calls_and_preserves_history() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let plane = Arc::new(AwsS3ObjectPlane::new(aws, &bucket));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: unique_name("three-call-repository"),
            writer: "rustfs-physical-writer".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let key = unique_name("three-call-object");

    repository
        .put_bytes(
            "main",
            format!("{key}/warmup.bin").into_bytes(),
            vec![1; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    plane.reset_metrics();
    let first = repository
        .put_bytes(
            "main",
            format!("{key}/measured.bin").into_bytes(),
            vec![2; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let calls = plane.reset_metrics();
    assert_eq!(calls.put_object, 3, "unexpected calls: {calls:?}");
    assert_eq!(calls.get_object, 1, "unexpected calls: {calls:?}");
    assert_eq!(calls.total_calls(), 4, "unexpected calls: {calls:?}");

    plane.reset_metrics();
    assert_eq!(
        repository
            .get_current("main", format!("{key}/measured.bin").as_bytes())
            .await
            .unwrap()
            .bytes,
        vec![2; 64 * 1024]
    );
    let calls = plane.reset_metrics();
    assert_eq!(calls.get_object, 1, "unexpected warm-read calls: {calls:?}");
    assert_eq!(
        calls.total_calls(),
        1,
        "unexpected warm-read calls: {calls:?}"
    );

    repository
        .put_bytes(
            "main",
            format!("{key}/measured.bin").into_bytes(),
            b"new current value".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    plane.reset_metrics();
    assert_eq!(
        repository
            .get_version(
                "main",
                format!("{key}/measured.bin").as_bytes(),
                first.object_versions[0],
            )
            .await
            .unwrap()
            .bytes,
        vec![2; 64 * 1024]
    );
    let calls = plane.reset_metrics();
    assert_eq!(
        calls.get_object, 1,
        "unexpected warm historical-read calls: {calls:?}"
    );
    assert_eq!(
        calls.total_calls(),
        1,
        "unexpected warm historical-read calls: {calls:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_branch_takeover_fences_old_client_before_payload_upload() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("client-branch-takeover-repository");
    let key_prefix = unique_name("client-branch-takeover-object");
    let old_writer = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-old-branch-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .initialize()
        .await
        .unwrap();
    old_writer
        .put_object()
        .bucket(&bucket)
        .key(format!("{key_prefix}/before-takeover.bin"))
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"before"))
        .send()
        .await
        .unwrap();

    let mut new_writer = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-new-branch-writer")
        .read_only(true)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .open()
        .await
        .unwrap();
    assert_eq!(
        new_writer
            .takeover_branch_writer(
                "main",
                "rustfs-old-branch-writer",
                1,
                "old test writer credentials revoked and process isolated",
            )
            .await
            .unwrap(),
        2
    );

    old_writer.reset_s3_operation_metrics();
    let error = old_writer
        .put_object()
        .bucket(&bucket)
        .key(format!("{key_prefix}/must-not-upload.bin"))
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"stale"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(error.code, prolly_s3_client::ErrorCode::PreconditionFailed);
    let stale_calls = old_writer.reset_s3_operation_metrics();
    assert_eq!(
        stale_calls.put_object, 0,
        "stale client uploaded bytes after branch takeover: {stale_calls:?}"
    );
    assert_eq!(
        stale_calls.get_object, 1,
        "stale client must fail at the authority point read: {stale_calls:?}"
    );

    new_writer
        .put_object()
        .bucket(&bucket)
        .key(format!("{key_prefix}/after-takeover.bin"))
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"after"))
        .send()
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_native_v2_client_uses_immutable_payloads_and_fences_takeover() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("native-v2-client-repository");
    let old_writer = ClientV2::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-native-v2-old-writer")
        .authority_lease_duration(Duration::from_secs(10))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
        .initialize()
        .await
        .unwrap();
    let first = old_writer
        .put_object("docs/native-v2.txt", b"before takeover".to_vec())
        .await
        .unwrap();
    let stored = old_writer
        .get_object("docs/native-v2.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.bytes, b"before takeover");
    assert!(stored
        .version
        .binding
        .unwrap()
        .path
        .as_str()
        .contains("/payloads/v2/"));
    old_writer
        .put_object("docs/native-v2.txt", b"updated before takeover".to_vec())
        .await
        .unwrap();
    assert_eq!(
        old_writer
            .get_object_at(first.id, "docs/native-v2.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"before takeover"
    );
    let (_, listed, truncated) = old_writer.list_objects("docs/", None, 10).await.unwrap();
    assert!(!truncated);
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].key, b"docs/native-v2.txt");
    let (_, versions) = old_writer
        .list_object_versions("docs/native-v2.txt", 10)
        .await
        .unwrap();
    assert_eq!(versions.len(), 2);
    old_writer
        .delete_object("docs/native-v2.txt")
        .await
        .unwrap();
    assert!(old_writer
        .get_object("docs/native-v2.txt")
        .await
        .unwrap()
        .is_none());
    let (_, versions) = old_writer
        .list_object_versions("docs/native-v2.txt", 10)
        .await
        .unwrap();
    assert_eq!(versions.len(), 3);
    assert!(matches!(
        versions[0].body.kind,
        LogicalObjectVersionKindV1::DeleteMarker
    ));

    let replacement = ClientV2::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-native-v2-new-writer")
        .read_only(true)
        .authority_lease_duration(Duration::from_secs(10))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
        .open()
        .await
        .unwrap();
    assert_eq!(
        replacement
            .takeover_branch_writer(
                "main",
                "rustfs-native-v2-old-writer",
                1,
                "old native-v2 test credentials revoked and process isolated",
            )
            .await
            .unwrap(),
        2
    );

    old_writer.reset_s3_operation_metrics();
    let error = old_writer
        .put_object("docs/stale-v2.txt", b"must not upload".to_vec())
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
        .put_object("docs/current-v2.txt", b"writer-b".to_vec())
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_native_v2_writable_reopen_resumes_the_same_writer() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("native-v2-writable-reopen");
    let original = ClientV2::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-native-v2-restartable-writer")
        .authority_lease_duration(Duration::from_secs(10))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
        .initialize()
        .await
        .unwrap();
    original
        .put_object("docs/before-restart.txt", b"before".to_vec())
        .await
        .unwrap();
    original.advance_branch_indexes().await.unwrap();
    drop(original);

    let reopened = ClientV2::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-native-v2-restartable-writer")
        .authority_lease_duration(Duration::from_secs(10))
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
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
async fn rustfs_native_v2_ref_lifecycle_uses_catalog_shards_without_ref_scans() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("native-v2-ref-catalog");
    let client = ClientV2::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-native-v2-ref-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
        .background_index_maintenance(false)
        .initialize()
        .await
        .unwrap();
    let main = client.head().await.unwrap();
    client.create_branch("feature", Some(main)).await.unwrap();
    let feature = client.for_branch("feature").unwrap();
    let committed = feature
        .put_object("docs/feature.txt", b"branch-local".to_vec())
        .await
        .unwrap();
    feature.advance_branch_indexes().await.unwrap();
    let tag = client.create_tag("release-1", committed.id).await.unwrap();

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
async fn rustfs_v1_history_migrates_to_native_v2_in_restartable_pages() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let key = format!("{}/history.txt", unique_name("v1-to-v2-object"));
    let source = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(unique_name("v1-to-v2-source"))
        .writer("rustfs-v1-migration-source")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .initialize()
        .await
        .unwrap();
    for body in [b"first".as_slice(), b"second".as_slice()] {
        source
            .put_object()
            .bucket(&bucket)
            .key(&key)
            .body(ByteStream::from(body.to_vec()))
            .send()
            .await
            .unwrap();
    }

    let destination_prefix = unique_name("v1-to-v2-destination");
    let destination = ClientV2::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&destination_prefix)
        .writer("rustfs-v2-migration-destination")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
        .background_index_maintenance(false)
        .initialize()
        .await
        .unwrap();
    let mut cursor = source
        .start_v2_migration(&destination, "imported-main")
        .await
        .unwrap();
    loop {
        cursor = prolly_s3_client::core::decode_canonical(
            &prolly_s3_client::core::encode_canonical(&cursor).unwrap(),
        )
        .unwrap();
        let page = source
            .advance_v2_migration(&destination, &cursor, 100, 1)
            .await
            .unwrap();
        cursor = page.cursor;
        if page.complete {
            break;
        }
    }
    assert_eq!(cursor.migrated_commits, 3);
    assert_eq!(cursor.migrated_payloads, 2);
    let imported = destination.for_branch("imported-main").unwrap();
    assert_eq!(
        imported.get_object(&key).await.unwrap().unwrap().bytes,
        b"second"
    );
    loop {
        if source
            .cleanup_v2_migration(&cursor, 1_000)
            .await
            .unwrap()
            .complete
        {
            break;
        }
    }

    drop(imported);
    drop(destination);
    let cold = ClientV2::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(destination_prefix)
        .writer("rustfs-v2-migration-destination")
        .read_only(true)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
        .background_index_maintenance(false)
        .open()
        .await
        .unwrap()
        .for_branch("imported-main")
        .unwrap();
    assert_eq!(
        cold.get_object(&key).await.unwrap().unwrap().bytes,
        b"second"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_native_v2_commit_session_preserves_n_plus_three_puts() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let client = ClientV2::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_name("native-v2-commit-session"))
        .writer("rustfs-native-v2-batch-writer")
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
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
async fn rustfs_native_v2_durable_session_resumes_without_payload_reupload() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("native-v2-durable-resume");
    let client = ClientV2::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-native-v2-resumable-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
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

    let reopened = ClientV2::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-native-v2-resumable-writer")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
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
async fn rustfs_native_v2_cold_open_catches_indexes_before_serving_reads() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("native-v2-cold-index-catchup");
    let writer = ClientV2::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-native-v2-index-writer")
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
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

    let reader = ClientV2::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .read_only(true)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
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
async fn rustfs_native_v2_over_limit_index_rebuild_resumes_from_canonical_cursor() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_name("native-v2-index-rebuild");
    let writer = ClientV2::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-native-v2-rebuild-writer")
        .journal_index_max_unindexed_events(2)
        .operation_index_limits(2, 2, 8)
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
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

    let reader = ClientV2::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .read_only(true)
        .journal_index_max_unindexed_events(2)
        .operation_index_limits(2, 2, 2)
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
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

    let replay_writer = ClientV2::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("rustfs-native-v2-rebuild-writer")
        .journal_index_max_unindexed_events(2)
        .operation_index_limits(2, 2, 2)
        .background_index_maintenance(false)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .provider_per_key_version_limit(ProviderPerKeyVersionLimitV2::Finite(10_000))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_two_part_multipart_write_uses_eight_s3_calls() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let plane = Arc::new(AwsS3ObjectPlane::new(aws, &bucket));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: unique_name("multipart-repository"),
            writer: "rustfs-physical-multipart-writer".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let key = format!("{}/multipart.bin", unique_name("multipart-object"));
    repository
        .put_bytes(
            "main",
            format!("{key}.warmup").into_bytes(),
            b"warm".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let first_bytes = vec![3; 5 * 1024 * 1024];
    let second_bytes = vec![5; 1024];
    let mut whole = first_bytes.clone();
    whole.extend_from_slice(&second_bytes);
    let checksum_sha256: [u8; 32] = Sha256::digest(&whole).into();
    let checksum_md5: [u8; 16] = Md5::digest(&whole).into();

    plane.reset_metrics();
    let session = repository
        .create_physical_multipart_upload(
            "main",
            key.as_bytes().to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let first = repository
        .upload_physical_multipart_part(&session, 1, first_bytes)
        .await
        .unwrap();
    let second = repository
        .upload_physical_multipart_part(&session, 2, second_bytes)
        .await
        .unwrap();
    let parts = [&first, &second]
        .into_iter()
        .map(|part| PhysicalMultipartCompletedPart {
            part_number: part.part_number,
            etag: part.etag.clone(),
            checksum_sha256: part.checksum_sha256.unwrap(),
            size: part.size,
        })
        .collect();
    repository
        .complete_physical_multipart_upload(
            session.clone(),
            parts,
            checksum_sha256,
            checksum_md5,
            whole.len() as u64,
            Some(session.operation),
        )
        .await
        .unwrap();

    let calls = plane.reset_metrics();
    assert_eq!(calls.create_multipart_upload, 1);
    assert_eq!(calls.upload_part, 2);
    assert_eq!(calls.complete_multipart_upload, 1);
    assert_eq!(calls.put_object, 2);
    assert_eq!(calls.get_object, 2);
    assert_eq!(calls.total_calls(), 8, "unexpected calls: {calls:?}");
    assert_eq!(
        repository
            .get_current("main", key.as_bytes())
            .await
            .unwrap()
            .bytes,
        whole
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rustfs_32_writer_load_preserves_the_four_call_budget() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let plane = Arc::new(AwsS3ObjectPlane::new(aws, &bucket));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: unique_name("load-repository"),
            writer: "rustfs-load-writer".to_string(),
            max_parallel_payload_writes: 32,
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            format!("{}/warmup.bin", unique_name("load-object")).into_bytes(),
            vec![0; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    plane.reset_metrics();
    let started = Instant::now();
    let writes = (0..32).map(|index| {
        let repository = &repository;
        async move {
            let operation_started = Instant::now();
            repository
                .put_bytes(
                    "main",
                    format!("load/{index}.bin").into_bytes(),
                    vec![index as u8; 64 * 1024],
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                    None,
                )
                .await
                .map(|_| operation_started.elapsed())
        }
    });
    let mut latencies = tokio::time::timeout(
        Duration::from_secs(30),
        futures_util::future::join_all(writes),
    )
    .await
    .expect("32 RustFS writes exceeded the local 30 second safety timeout")
    .into_iter()
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let wall = started.elapsed();
    latencies.sort_unstable();
    let percentile = |percent: usize| latencies[(latencies.len() * percent).div_ceil(100) - 1];
    let calls = plane.reset_metrics();
    assert_eq!(calls.put_object, 96, "unexpected calls: {calls:?}");
    assert_eq!(calls.get_object, 32, "unexpected calls: {calls:?}");
    assert_eq!(calls.total_calls(), 128, "unexpected calls: {calls:?}");
    assert_eq!(repository.performance_snapshot().publication_queue_depth, 0);
    eprintln!(
        "rustfs_load writers=32 object_bytes=65536 calls_per_write=4 wall_ms={} p50_ms={} p95_ms={} p99_ms={} writes_per_second={:.2}",
        wall.as_millis(),
        percentile(50).as_millis(),
        percentile(95).as_millis(),
        percentile(99).as_millis(),
        32.0 / wall.as_secs_f64(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "operator-run 10K hot-branch release gate"]
async fn rustfs_10k_concurrent_commits_are_reconciled_and_complete() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_RUSTFS_10K").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_RUSTFS_10K=1");
        return;
    }

    let concurrency = std::env::var("PROLLY_RUSTFS_10K_CONCURRENCY")
        .map(|value| value.parse::<usize>().expect("numeric concurrency"))
        .unwrap_or(32);
    let object_bytes = std::env::var("PROLLY_RUSTFS_10K_OBJECT_BYTES")
        .map(|value| value.parse::<usize>().expect("numeric object size"))
        .unwrap_or(64 * 1024);
    let (aws, bucket) = rustfs_client().await;
    let plane = Arc::new(AwsS3ObjectPlane::new(aws, &bucket));
    let object_prefix = unique_name("ten-thousand-objects");
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: unique_name("ten-thousand-repository"),
            writer: "rustfs-10k-writer".to_string(),
            writer_lease_millis: 60 * 60 * 1_000,
            max_parallel_payload_writes: concurrency,
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();

    let mut previous = 0;
    for target in [1_000, 5_000, 10_000] {
        plane.reset_metrics();
        let started = Instant::now();
        let writes = futures_util::stream::iter(previous..target)
            .map(|index| {
                let repository = &repository;
                let object_prefix = &object_prefix;
                async move {
                    let operation_started = Instant::now();
                    repository
                        .put_bytes(
                            "main",
                            format!("{object_prefix}/{index:05}.bin").into_bytes(),
                            vec![index as u8; object_bytes],
                            ObjectHeaders::default(),
                            BTreeMap::new(),
                            None,
                        )
                        .await
                        .map(|receipt| (operation_started.elapsed(), receipt.idempotent_replay))
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
        let wall = started.elapsed();
        let mut latencies = writes
            .into_iter()
            .map(|result| result.expect("10K commit"))
            .collect::<Vec<_>>();
        let reconciled = latencies.iter().filter(|(_, replay)| *replay).count();
        latencies.sort_unstable_by_key(|(latency, _)| *latency);
        let percentile = |percent: usize| {
            latencies[(latencies.len() * percent).div_ceil(100) - 1]
                .0
                .as_millis()
        };
        let metrics = plane.reset_metrics();
        let tier_writes = target - previous;
        assert!(
            metrics.total_calls() <= (tier_writes as u64 * 401).div_ceil(100),
            "request budget exceeded: {metrics:?}"
        );
        eprintln!(
            "rustfs_10k live_files={target} tier_writes={tier_writes} wall_ms={} writes_per_second={:.2} p50_ms={} p95_ms={} p99_ms={} reconciled={reconciled} s3_calls={} calls_per_write={:.3}",
            wall.as_millis(),
            tier_writes as f64 / wall.as_secs_f64(),
            percentile(50),
            percentile(95),
            percentile(99),
            metrics.total_calls(),
            metrics.total_calls() as f64 / tier_writes as f64,
        );
        previous = target;
    }

    let head = repository.head("main").await.unwrap();
    assert_eq!(repository.commit(head).await.unwrap().generation.0, 10_000);
    let mut after = None;
    let mut listed = 0;
    loop {
        let (objects, truncated) = repository
            .list_objects_at(head, object_prefix.as_bytes(), after.as_deref(), 1_000)
            .await
            .unwrap();
        listed += objects.len();
        after = objects.last().map(|object| object.key.clone());
        if !truncated {
            break;
        }
    }
    assert_eq!(listed, 10_000);
    assert_eq!(repository.performance_snapshot().publication_queue_depth, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[cfg(feature = "foyer-cache")]
#[ignore = "operator-run 10K batched-ingest and persisted-cache release gate"]
async fn rustfs_10k_batched_ingest_has_bounded_bytes_and_persisted_cache() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_RUSTFS_BATCH_10K").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_RUSTFS_BATCH_10K=1");
        return;
    }

    let (aws, bucket) = rustfs_client().await;
    let plane = Arc::new(AwsS3ObjectPlane::new(aws, &bucket));
    let repository_prefix = unique_name("ten-thousand-batched-repository");
    let object_prefix = unique_name("ten-thousand-batched-objects");
    let cache_directory = tempfile::tempdir().unwrap();
    let cache_config = FoyerNodeCacheConfig {
        directory: cache_directory.path().to_path_buf(),
        memory_capacity_bytes: 64 * 1024 * 1024,
        disk_capacity_bytes: 512 * 1024 * 1024,
        disk_block_size_bytes: 8 * 1024 * 1024,
        memory_shards: 16,
    };
    let writer_cache = FoyerNodeCache::open(cache_config.clone()).await.unwrap();
    let options = RepositoryOptions {
        repository_prefix,
        writer: "rustfs-batched-10k-writer".to_string(),
        writer_lease_millis: 60 * 60 * 1_000,
        max_parallel_payload_writes: 32,
        max_cached_node_pack_bytes: 1,
        node_cache: Some(writer_cache.clone()),
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    plane.reset_metrics();
    let started = Instant::now();
    let mut packed_bytes = 0_u64;
    let mut max_pack_bytes = 0_u64;
    for batch_index in 0..100 {
        let batch = repository
            .begin_physical_batch("main", "100-file bulk ingest", 60 * 60 * 1_000)
            .await
            .unwrap();
        let mutations = (0..100)
            .map(|offset| {
                let index = batch_index * 100 + offset;
                PhysicalBatchMutationV1::Put {
                    key: format!("{object_prefix}/{index:05}.bin").into_bytes(),
                    bytes: vec![index as u8; 64 * 1024],
                    headers: ObjectHeaders::default(),
                    user_metadata: BTreeMap::new(),
                }
            })
            .collect();
        let receipt = repository
            .publish_physical_batch(batch, mutations)
            .await
            .unwrap();
        let pack_bytes = repository
            .commit(receipt.id)
            .await
            .unwrap()
            .node_pack
            .expect("batch node pack")
            .object_len;
        packed_bytes += pack_bytes;
        max_pack_bytes = max_pack_bytes.max(pack_bytes);
    }
    let ingest_wall = started.elapsed();
    let ingest_metrics = plane.reset_metrics();
    let logical_payload_bytes = 10_000_u64 * 64 * 1024;
    let upload_ratio = ingest_metrics.uploaded_body_bytes as f64 / logical_payload_bytes as f64;
    assert_eq!(ingest_metrics.total_calls(), 10_300);
    assert!(
        upload_ratio < 1.5,
        "batch upload byte amplification is {upload_ratio:.3}x: {ingest_metrics:?}"
    );
    assert!(
        max_pack_bytes < 2 * 1024 * 1024,
        "one 100-file batch packed {max_pack_bytes} transient bytes"
    );
    let head = repository.head("main").await.unwrap();
    eprintln!(
        "rustfs_batch_10k files=10000 commits=100 wall_ms={} files_per_second={:.2} s3_calls={} calls_per_file={:.3} uploaded_mib={:.2} upload_ratio={:.3} packed_mib={:.2} max_pack_kib={:.2}",
        ingest_wall.as_millis(),
        10_000.0 / ingest_wall.as_secs_f64(),
        ingest_metrics.total_calls(),
        ingest_metrics.total_calls() as f64 / 10_000.0,
        ingest_metrics.uploaded_body_bytes as f64 / (1024.0 * 1024.0),
        upload_ratio,
        packed_bytes as f64 / (1024.0 * 1024.0),
        max_pack_bytes as f64 / 1024.0,
    );

    drop(repository);
    writer_cache.close().await.unwrap();
    drop(writer_cache);
    let reader_cache = FoyerNodeCache::open(cache_config).await.unwrap();
    let reader = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            node_cache: Some(reader_cache.clone()),
            ..options
        },
    )
    .await
    .unwrap();
    plane.reset_metrics();
    let before = reader.performance_snapshot();
    let list_started = Instant::now();
    let mut after = None;
    let mut listed = 0;
    loop {
        let (objects, truncated) = reader
            .list_objects_at(head, object_prefix.as_bytes(), after.as_deref(), 1_000)
            .await
            .unwrap();
        listed += objects.len();
        after = objects.last().map(|object| object.key.clone());
        if !truncated {
            break;
        }
    }
    let list_wall = list_started.elapsed();
    let after_snapshot = reader.performance_snapshot();
    let list_metrics = plane.reset_metrics();
    let ranged_fetches = after_snapshot
        .node_ranged_fetches
        .saturating_sub(before.node_ranged_fetches);
    let cache_hits = after_snapshot
        .node_cache_hits
        .saturating_sub(before.node_cache_hits);
    assert_eq!(listed, 10_000);
    assert_eq!(ranged_fetches, 0, "persisted cache missed immutable nodes");
    assert!(
        list_metrics.total_calls() <= 1,
        "persisted-cache list issued unexpected S3 calls: {list_metrics:?}"
    );
    eprintln!(
        "rustfs_persisted_cache files=10000 list_ms={} s3_calls={} ranged_fetches={ranged_fetches} cache_hits={cache_hits}",
        list_wall.as_millis(),
        list_metrics.total_calls(),
    );
    drop(reader);
    reader_cache.close().await.unwrap();
}
