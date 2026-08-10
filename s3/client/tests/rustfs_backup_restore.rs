use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::{
    config::Region,
    primitives::ByteStream,
    types::{BucketVersioningStatus, VersioningConfiguration},
};
use prolly_s3_client::{
    core::{
        decode_canonical, encode_canonical, DeleteOutcome, ListRequest, ObjectPlane,
        PhysicalVersion, RepositoryId,
    },
    AwsS3ObjectPlane, Client, HmacAttestationSigner, ProviderIdentity,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, PartialEq, Eq)]
struct NativeEntry {
    key: String,
    version_id: String,
    delete_marker: bool,
    is_latest: bool,
    last_modified_millis: i64,
    size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PhysicalBackupEntryV1 {
    source_key: String,
    source_version_id: String,
    archive_key: Option<String>,
    delete_marker: bool,
    is_latest: bool,
    last_modified_millis: i64,
    size: u64,
    body_sha256: Option<[u8; 32]>,
    user_metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PhysicalBackupManifestV1 {
    schema_version: u16,
    source_bucket: String,
    repository_prefix: String,
    repository_id: RepositoryId,
    source_bucket_versioning_enabled: bool,
    created_at_millis: u64,
    entries: Vec<PhysicalBackupEntryV1>,
}

fn rustfs_enabled() -> bool {
    std::env::var("PROLLY_S3_RUSTFS").as_deref() == Ok("1")
}

fn provider_identity() -> ProviderIdentity {
    ProviderIdentity::s3_compatible(
        std::env::var("PROLLY_RUSTFS_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string()),
        "us-east-1",
    )
}

fn attestation_signer() -> Arc<HmacAttestationSigner> {
    Arc::new(HmacAttestationSigner::single("integration-attestation-v1", vec![11_u8; 32]).unwrap())
}

fn bucket_names() -> (String, String, String) {
    let run_id = std::env::var("PROLLY_S3_BACKUP_RUN_ID").expect("backup drill run ID");
    assert!(
        !run_id.is_empty()
            && run_id.len() <= 20
            && run_id
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit()),
        "backup run ID must be 1-20 lowercase alphanumeric bytes"
    );
    (
        format!("prolly-backup-source-{run_id}"),
        format!("prolly-backup-archive-{run_id}"),
        format!("prolly-backup-restore-{run_id}"),
    )
}

fn repository_prefix() -> String {
    std::env::var("PROLLY_S3_BACKUP_PREFIX").expect("backup drill repository prefix")
}

async fn rustfs_client() -> aws_sdk_s3::Client {
    let endpoint = std::env::var("PROLLY_RUSTFS_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let access_key =
        std::env::var("PROLLY_RUSTFS_ACCESS_KEY").unwrap_or_else(|_| "prollyadmin".to_string());
    let secret_key = std::env::var("PROLLY_RUSTFS_SECRET_KEY")
        .unwrap_or_else(|_| "prolly-local-secret-change-me".to_string());
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                access_key,
                secret_key,
                None,
                None,
                "rustfs-backup-restore-drill",
            ))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .build(),
    )
}

async fn create_versioned_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
    client.create_bucket().bucket(bucket).send().await.unwrap();
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let versioning = client
        .get_bucket_versioning()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    assert_eq!(versioning.status(), Some(&BucketVersioningStatus::Enabled));
}

fn timestamp_millis(value: Option<&aws_smithy_types::DateTime>) -> i64 {
    value
        .map(|value| {
            value
                .secs()
                .saturating_mul(1_000)
                .saturating_add(i64::from(value.subsec_nanos() / 1_000_000))
        })
        .unwrap_or_default()
}

async fn list_native_entries(
    client: &aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> Vec<NativeEntry> {
    let mut key_marker = None;
    let mut version_marker = None;
    let mut entries = Vec::new();
    loop {
        let output = client
            .list_object_versions()
            .bucket(bucket)
            .prefix(format!("{prefix}/"))
            .set_key_marker(key_marker)
            .set_version_id_marker(version_marker)
            .max_keys(1_000)
            .send()
            .await
            .unwrap();
        entries.extend(output.versions().iter().filter_map(|version| {
            Some(NativeEntry {
                key: version.key()?.to_string(),
                version_id: version.version_id()?.to_string(),
                delete_marker: false,
                is_latest: version.is_latest().unwrap_or(false),
                last_modified_millis: timestamp_millis(version.last_modified()),
                size: version
                    .size()
                    .and_then(|size| u64::try_from(size).ok())
                    .unwrap_or_default(),
            })
        }));
        entries.extend(output.delete_markers().iter().filter_map(|marker| {
            Some(NativeEntry {
                key: marker.key()?.to_string(),
                version_id: marker.version_id()?.to_string(),
                delete_marker: true,
                is_latest: marker.is_latest().unwrap_or(false),
                last_modified_millis: timestamp_millis(marker.last_modified()),
                size: 0,
            })
        }));
        if !output.is_truncated().unwrap_or(false) {
            break;
        }
        key_marker = output.next_key_marker().map(ToString::to_string);
        version_marker = output.next_version_id_marker().map(ToString::to_string);
    }
    entries
}

fn inventory(mut entries: Vec<NativeEntry>) -> Vec<NativeEntry> {
    entries.sort_by(|left, right| {
        left.key
            .cmp(&right.key)
            .then_with(|| left.version_id.cmp(&right.version_id))
            .then_with(|| left.delete_marker.cmp(&right.delete_marker))
            .then_with(|| left.is_latest.cmp(&right.is_latest))
    });
    entries
}

fn inventory_shape(entries: &[NativeEntry], prefix: &str) -> Vec<(String, bool, bool, u64)> {
    let prefix = format!("{prefix}/");
    let mut shape = entries
        .iter()
        .map(|entry| {
            (
                entry
                    .key
                    .strip_prefix(&prefix)
                    .expect("inventory entry below repository prefix")
                    .to_string(),
                entry.delete_marker,
                entry.is_latest,
                entry.size,
            )
        })
        .collect::<Vec<_>>();
    shape.sort();
    shape
}

async fn archive_physical_versions(
    client: &aws_sdk_s3::Client,
    source_bucket: &str,
    archive_bucket: &str,
    archive_prefix: &str,
    repository_prefix: &str,
    repository_id: RepositoryId,
) -> (PhysicalBackupManifestV1, String, usize) {
    let source_before =
        inventory(list_native_entries(client, source_bucket, repository_prefix).await);
    assert!(!source_before.is_empty());
    let mut entries = Vec::with_capacity(source_before.len());
    let mut archived_bytes = 0_usize;
    for (ordinal, version) in source_before.iter().enumerate() {
        if version.delete_marker {
            entries.push(PhysicalBackupEntryV1 {
                source_key: version.key.clone(),
                source_version_id: version.version_id.clone(),
                archive_key: None,
                delete_marker: true,
                is_latest: version.is_latest,
                last_modified_millis: version.last_modified_millis,
                size: 0,
                body_sha256: None,
                user_metadata: BTreeMap::new(),
            });
            continue;
        }
        let source = client
            .get_object()
            .bucket(source_bucket)
            .key(&version.key)
            .version_id(&version.version_id)
            .send()
            .await
            .unwrap();
        let metadata = source
            .metadata()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let body = source.body.collect().await.unwrap().into_bytes().to_vec();
        assert_eq!(u64::try_from(body.len()).unwrap(), version.size);
        let body_sha256: [u8; 32] = Sha256::digest(&body).into();
        let archive_key = format!(
            "{archive_prefix}/objects/{ordinal:020}-{}",
            hex::encode(Sha256::digest(
                [version.key.as_bytes(), version.version_id.as_bytes()].concat()
            ))
        );
        client
            .put_object()
            .bucket(archive_bucket)
            .key(&archive_key)
            .if_none_match("*")
            .set_metadata(Some(metadata.clone().into_iter().collect()))
            .body(ByteStream::from(body))
            .send()
            .await
            .unwrap();
        archived_bytes = archived_bytes.saturating_add(usize::try_from(version.size).unwrap());
        entries.push(PhysicalBackupEntryV1 {
            source_key: version.key.clone(),
            source_version_id: version.version_id.clone(),
            archive_key: Some(archive_key),
            delete_marker: false,
            is_latest: version.is_latest,
            last_modified_millis: version.last_modified_millis,
            size: version.size,
            body_sha256: Some(body_sha256),
            user_metadata: metadata,
        });
    }
    let source_after =
        inventory(list_native_entries(client, source_bucket, repository_prefix).await);
    assert_eq!(
        source_after, source_before,
        "the source changed while the physical backup was being captured"
    );
    let manifest = PhysicalBackupManifestV1 {
        schema_version: 1,
        source_bucket: source_bucket.to_string(),
        repository_prefix: repository_prefix.to_string(),
        repository_id,
        source_bucket_versioning_enabled: true,
        created_at_millis: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap(),
        entries,
    };
    let manifest_bytes = encode_canonical(&manifest).unwrap();
    let manifest_digest = hex::encode(Sha256::digest(&manifest_bytes));
    let manifest_key = format!("{archive_prefix}/manifest-v1.cbor");
    client
        .put_object()
        .bucket(archive_bucket)
        .key(&manifest_key)
        .if_none_match("*")
        .body(ByteStream::from(manifest_bytes))
        .send()
        .await
        .unwrap();
    (manifest, manifest_digest, archived_bytes)
}

async fn load_backup_manifest(
    client: &aws_sdk_s3::Client,
    archive_bucket: &str,
    archive_prefix: &str,
) -> PhysicalBackupManifestV1 {
    let bytes = client
        .get_object()
        .bucket(archive_bucket)
        .key(format!("{archive_prefix}/manifest-v1.cbor"))
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    decode_canonical(&bytes).unwrap()
}

async fn restore_physical_versions(
    client: &aws_sdk_s3::Client,
    archive_bucket: &str,
    restore_bucket: &str,
    manifest: &PhysicalBackupManifestV1,
) {
    assert_eq!(manifest.schema_version, 1);
    assert!(manifest.source_bucket_versioning_enabled);
    let mut entries = manifest.entries.clone();
    entries.sort_by(|left, right| {
        left.source_key
            .cmp(&right.source_key)
            .then_with(|| left.is_latest.cmp(&right.is_latest))
            .then_with(|| left.last_modified_millis.cmp(&right.last_modified_millis))
            .then_with(|| left.source_version_id.cmp(&right.source_version_id))
    });
    for entry in entries {
        if entry.delete_marker {
            client
                .delete_object()
                .bucket(restore_bucket)
                .key(&entry.source_key)
                .send()
                .await
                .unwrap();
            continue;
        }
        let archive_key = entry.archive_key.expect("body-bearing archive entry");
        let body = client
            .get_object()
            .bucket(archive_bucket)
            .key(archive_key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .to_vec();
        assert_eq!(u64::try_from(body.len()).unwrap(), entry.size);
        assert_eq!(
            Some(<[u8; 32]>::from(Sha256::digest(&body))),
            entry.body_sha256
        );
        client
            .put_object()
            .bucket(restore_bucket)
            .key(entry.source_key)
            .set_metadata(Some(entry.user_metadata.into_iter().collect()))
            .body(ByteStream::from(body))
            .send()
            .await
            .unwrap();
    }
}

async fn cleanup_bucket(client: &aws_sdk_s3::Client, bucket: &str) {
    if client.head_bucket().bucket(bucket).send().await.is_err() {
        return;
    }
    let plane = AwsS3ObjectPlane::new(client.clone(), bucket);
    loop {
        let page = plane
            .list(ListRequest {
                prefix: String::new(),
                continuation: None,
                limit: 1_000,
                include_versions: true,
            })
            .await
            .unwrap();
        if page.entries.is_empty() {
            break;
        }
        for entry in page.entries {
            let version_id = entry
                .metadata
                .token
                .version_id
                .expect("versioned drill bucket entry");
            assert!(matches!(
                plane
                    .delete_exact(&entry.path, PhysicalVersion::Versioned { version_id })
                    .await
                    .unwrap(),
                DeleteOutcome::Deleted
            ));
        }
    }
    client.delete_bucket().bucket(bucket).send().await.unwrap();
}

async fn cleanup_all(client: &aws_sdk_s3::Client) {
    let (source, archive, restore) = bucket_names();
    for bucket in [&source, &archive, &restore] {
        cleanup_bucket(client, bucket).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_physical_backup_restore_process_helper() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_BACKUP_DRILL").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and run through the backup/restore drill script");
        return;
    }
    let phase = std::env::var("PROLLY_S3_BACKUP_PHASE").expect("backup drill phase");
    let aws = rustfs_client().await;
    if phase == "cleanup" {
        cleanup_all(&aws).await;
        eprintln!("RUSTFS_BACKUP_RESTORE_CLEANUP buckets_removed=3 result=ok");
        return;
    }
    assert_eq!(phase, "run");
    let (source_bucket, archive_bucket, restore_bucket) = bucket_names();
    let repository_prefix = repository_prefix();
    for bucket in [&source_bucket, &archive_bucket, &restore_bucket] {
        create_versioned_bucket(&aws, bucket).await;
    }

    let source = Client::builder()
        .aws_client(aws.clone())
        .bucket(&source_bucket)
        .repository_prefix(&repository_prefix)
        .writer("backup-source")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .initialize()
        .await
        .unwrap();
    let first = source
        .put_object()
        .bucket(&source_bucket)
        .key("backup/document.txt")
        .body(ByteStream::from_static(b"first retained revision"))
        .send()
        .await
        .unwrap();
    let first_version = first.output.version_id().unwrap().to_string();
    source
        .put_object()
        .bucket(&source_bucket)
        .key("backup/document.txt")
        .body(ByteStream::from_static(b"current retained revision"))
        .send()
        .await
        .unwrap();
    let main_head = source.head_commit().await.unwrap();
    source
        .create_branch("feature", Some(main_head))
        .await
        .unwrap();
    let feature = source.on_branch("feature").unwrap();
    feature
        .put_object()
        .bucket(&source_bucket)
        .key("backup/feature.txt")
        .body(ByteStream::from_static(b"feature branch payload"))
        .send()
        .await
        .unwrap();
    let feature_head = feature.head_commit().await.unwrap();
    source.create_tag("backup-point", main_head).await.unwrap();
    source.fsck().await.unwrap();
    let repository_id = source.repository_id();
    let source_ref_versions = source.list_native_branch_ref_versions().await.unwrap();
    assert!(source_ref_versions.len() >= 3);

    let raw_marker_key = format!("{repository_prefix}/backup-fixture/deleted");
    aws.put_object()
        .bucket(&source_bucket)
        .key(&raw_marker_key)
        .body(ByteStream::from_static(b"deleted raw fixture"))
        .send()
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    aws.delete_object()
        .bucket(&source_bucket)
        .key(&raw_marker_key)
        .send()
        .await
        .unwrap();

    let archive_prefix = "physical-backup/snapshot-0001";
    let (manifest, manifest_digest, archived_bytes) = archive_physical_versions(
        &aws,
        &source_bucket,
        &archive_bucket,
        archive_prefix,
        &repository_prefix,
        repository_id,
    )
    .await;
    assert!(manifest.entries.iter().any(|entry| entry.delete_marker));
    let loaded = load_backup_manifest(&aws, &archive_bucket, archive_prefix).await;
    assert_eq!(loaded, manifest);

    restore_physical_versions(&aws, &archive_bucket, &restore_bucket, &loaded).await;
    let source_inventory = list_native_entries(&aws, &source_bucket, &repository_prefix).await;
    let restored_inventory = list_native_entries(&aws, &restore_bucket, &repository_prefix).await;
    assert_eq!(
        inventory_shape(&restored_inventory, &repository_prefix),
        inventory_shape(&source_inventory, &repository_prefix),
        "restored native-version stack differs in key, kind, latest state, or size"
    );
    let raw_marker = restored_inventory
        .iter()
        .find(|entry| entry.key == raw_marker_key && entry.delete_marker && entry.is_latest)
        .expect("latest raw delete marker was restored");
    assert!(!raw_marker.version_id.is_empty());

    Client::builder()
        .aws_client(aws.clone())
        .bucket(&restore_bucket)
        .repository_prefix(&repository_prefix)
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .qualify_provider()
        .await
        .unwrap();
    let restored = Client::builder()
        .aws_client(aws.clone())
        .bucket(&restore_bucket)
        .repository_prefix(&repository_prefix)
        .writer("backup-restore-verifier")
        .provider_identity(provider_identity())
        .attestation_signer(attestation_signer())
        .open()
        .await
        .unwrap();
    assert_eq!(restored.repository_id(), repository_id);
    assert_eq!(restored.head_commit().await.unwrap(), main_head);
    assert_eq!(
        restored
            .on_branch("feature")
            .unwrap()
            .head_commit()
            .await
            .unwrap(),
        feature_head
    );
    assert_eq!(
        restored
            .list_tags()
            .await
            .unwrap()
            .into_iter()
            .find(|tag| tag.name == "backup-point")
            .unwrap()
            .target,
        main_head
    );
    let historical = restored
        .get_object()
        .bucket(&restore_bucket)
        .key("backup/document.txt")
        .version_id(&first_version)
        .send()
        .await
        .unwrap()
        .output
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(historical.as_ref(), b"first retained revision");
    let current = restored
        .get_object()
        .bucket(&restore_bucket)
        .key("backup/document.txt")
        .send()
        .await
        .unwrap()
        .output
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(current.as_ref(), b"current retained revision");
    let restored_ref_versions = restored.list_native_branch_ref_versions().await.unwrap();
    assert_eq!(restored_ref_versions.len(), source_ref_versions.len());
    restored
        .put_object()
        .bucket(&restore_bucket)
        .key("backup/after-restore.txt")
        .body(ByteStream::from_static(b"writable after physical restore"))
        .send()
        .await
        .unwrap();
    restored.fsck().await.unwrap();
    eprintln!(
        "RUSTFS_BACKUP_RESTORE source_bucket={source_bucket} archive_bucket={archive_bucket} restore_bucket={restore_bucket} prefix={repository_prefix} source_versions={} archived_bodies={} archived_bytes={archived_bytes} delete_markers={} manifest_sha256={manifest_digest} restored_versions={} native_ref_versions={} repository_identity=preserved logical_history=preserved post_restore_write=ok final_fsck=ok",
        manifest.entries.len(),
        manifest
            .entries
            .iter()
            .filter(|entry| !entry.delete_marker)
            .count(),
        manifest
            .entries
            .iter()
            .filter(|entry| entry.delete_marker)
            .count(),
        restored_inventory.len(),
        restored_ref_versions.len(),
    );
}
