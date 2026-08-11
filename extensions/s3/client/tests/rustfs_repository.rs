use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::Region,
    types::{BucketVersioningStatus, VersioningConfiguration},
};
use md5::{Digest as _, Md5};
use prolly_s3_client::{
    core::{ObjectHeaders, PhysicalMultipartCompletedPart, Repository, RepositoryOptions},
    AwsS3ObjectPlane,
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
async fn rustfs_whole_object_write_uses_three_s3_calls_and_preserves_history() {
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
    assert_eq!(calls.total_calls(), 3, "unexpected calls: {calls:?}");

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
async fn rustfs_two_part_multipart_write_uses_six_s3_calls() {
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
    assert_eq!(calls.total_calls(), 6, "unexpected calls: {calls:?}");
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
async fn rustfs_32_writer_load_preserves_the_three_call_budget() {
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
    assert_eq!(calls.total_calls(), 96, "unexpected calls: {calls:?}");
    assert_eq!(repository.performance_snapshot().publication_queue_depth, 0);
    eprintln!(
        "rustfs_load writers=32 object_bytes=65536 calls_per_write=3 wall_ms={} p50_ms={} p95_ms={} p99_ms={} writes_per_second={:.2}",
        wall.as_millis(),
        percentile(50).as_millis(),
        percentile(95).as_millis(),
        percentile(99).as_millis(),
        32.0 / wall.as_secs_f64(),
    );
}
