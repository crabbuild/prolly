use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
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
            repository_prefix: unique_name("four-call-repository"),
            writer: "rustfs-prolly-s3-writer".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let key = unique_name("four-call-object");

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
    assert_eq!(calls.put_object, 4, "unexpected calls: {calls:?}");
    assert_eq!(calls.total_calls(), 4, "unexpected calls: {calls:?}");

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
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_two_part_multipart_write_uses_seven_s3_calls() {
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
            writer: "rustfs-prolly-s3-multipart-writer".to_string(),
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
    assert_eq!(calls.put_object, 3);
    assert_eq!(calls.total_calls(), 7, "unexpected calls: {calls:?}");
    assert_eq!(
        repository
            .get_current("main", key.as_bytes())
            .await
            .unwrap()
            .bytes,
        whole
    );
}
