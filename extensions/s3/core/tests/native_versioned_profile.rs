use std::{collections::BTreeMap, sync::Arc};

use md5::Md5;
use prolly::{Cid, TreeFormat};
use prolly_s3_core::{
    tree_format_digest, Checksums, CommitGeneration, ContentRef, ErrorCode,
    LogicalObjectVersionKindV2, MemoryObjectPlane, MergePolicy, NativeBatchMutationV1,
    NativeMultipartCompletedPart, NativeObjectBindingV1, NativePut, NodePackEntryV1, NodePackV1,
    ObjectHeaders, ObjectPath, ObjectPlane, ObjectVersionBodyV1, ObjectVersionKindV1,
    ObjectVersionOrder, ObjectVersionV1, OperationId, PhysicalVersion, PhysicalVersioning,
    ProviderCapabilities, Repository, RepositoryId, RepositoryOptions, RepositoryStorageProfile,
};
use sha2::{Digest as _, Sha256};

fn native_options(prefix: &str) -> RepositoryOptions {
    RepositoryOptions {
        repository_prefix: prefix.to_string(),
        storage_profile: RepositoryStorageProfile::NativeVersionedV1,
        writer: "native-writer".to_string(),
        ..RepositoryOptions::default()
    }
}

#[tokio::test]
async fn legacy_native_multipart_protocol_fails_closed() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/fail-closed"),
    )
    .await
    .unwrap();

    let multipart = repository
        .create_multipart_upload(
            "main",
            b"large.bin".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(multipart.code, ErrorCode::MissingCapability);
}

#[tokio::test]
async fn native_clone_replays_history_and_rebinds_physical_versions() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(
        source_plane,
        native_options(".prolly/native-versioned/clone-source"),
    )
    .await
    .unwrap();
    let first = source
        .put_bytes(
            "main",
            b"history.txt".to_vec(),
            b"first".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    source
        .put_bytes(
            "main",
            b"history.txt".to_vec(),
            b"second".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    let report = source
        .clone_to(
            destination_plane.clone(),
            ".prolly/native-versioned/clone-destination",
        )
        .await
        .unwrap();
    assert!(report.immutable_objects >= 7);
    assert_eq!(report.refs, 1);
    let resumed = source
        .clone_to(
            destination_plane.clone(),
            ".prolly/native-versioned/clone-destination",
        )
        .await
        .unwrap();
    assert_eq!(resumed.immutable_objects, 0);
    assert_eq!(resumed.immutable_bytes, 0);
    assert_eq!(resumed.refs, 1);

    let destination = Repository::open(
        destination_plane,
        native_options(".prolly/native-versioned/clone-destination"),
    )
    .await
    .unwrap();
    assert_eq!(destination.repository_id(), source.repository_id());
    assert_eq!(
        destination
            .get_current("main", b"history.txt")
            .await
            .unwrap()
            .bytes,
        b"second"
    );
    assert_eq!(
        destination
            .get_version("main", b"history.txt", first.object_versions[0])
            .await
            .unwrap()
            .bytes,
        b"first"
    );
    let source_version = source
        .head_version("main", b"history.txt", first.object_versions[0])
        .await
        .unwrap()
        .1
        .version;
    let destination_version = destination
        .head_version("main", b"history.txt", first.object_versions[0])
        .await
        .unwrap()
        .1
        .version;
    assert_eq!(source_version.id, destination_version.id);
    assert_ne!(
        source_version.native_binding,
        destination_version.native_binding
    );
}

#[tokio::test]
async fn native_push_replays_only_new_history_and_moves_destination_ref() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(
        source_plane,
        native_options(".prolly/native-versioned/push-source"),
    )
    .await
    .unwrap();
    source
        .put_bytes(
            "main",
            b"sync.txt".to_vec(),
            b"base".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    source
        .clone_to(
            destination_plane.clone(),
            ".prolly/native-versioned/push-destination",
        )
        .await
        .unwrap();
    let destination = Repository::open(
        destination_plane,
        native_options(".prolly/native-versioned/push-destination"),
    )
    .await
    .unwrap();
    let expected_destination = destination.head("main").await.unwrap();
    let next = source
        .put_bytes(
            "main",
            b"sync.txt".to_vec(),
            b"incremental".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let report = source
        .push_to(
            &destination,
            "main",
            "main",
            expected_destination,
            "incremental native push",
        )
        .await
        .unwrap();
    assert_eq!(report.copied_objects, 3);
    assert_eq!(report.copied_bytes, b"incremental".len() as u64);
    assert_eq!(
        destination
            .get_current("main", b"sync.txt")
            .await
            .unwrap()
            .bytes,
        b"incremental"
    );
    assert_eq!(
        destination
            .head_current("main", b"sync.txt")
            .await
            .unwrap()
            .version
            .id,
        next.object_versions[0]
    );
}

#[tokio::test]
async fn native_repair_rebinds_a_missing_destination_payload() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(
        source_plane,
        native_options(".prolly/native-versioned/repair-source"),
    )
    .await
    .unwrap();
    source
        .put_bytes(
            "main",
            b"repair.txt".to_vec(),
            b"recoverable".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    source
        .clone_to(
            destination_plane.clone(),
            ".prolly/native-versioned/repair-destination",
        )
        .await
        .unwrap();
    let destination = Repository::open(
        destination_plane.clone(),
        native_options(".prolly/native-versioned/repair-destination"),
    )
    .await
    .unwrap();
    let damaged = destination
        .head_current("main", b"repair.txt")
        .await
        .unwrap()
        .version;
    let NativeObjectBindingV1::Live { version_id, .. } = damaged.native_binding.clone().unwrap()
    else {
        panic!("expected live native binding")
    };
    destination_plane
        .delete_exact(
            &ObjectPath::new("repair.txt").unwrap(),
            PhysicalVersion::Versioned { version_id },
        )
        .await
        .unwrap();
    assert_eq!(
        destination
            .get_current("main", b"repair.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::MissingClosure
    );

    let repaired = destination
        .repair_missing_from(&source, "main")
        .await
        .unwrap();
    assert!(repaired.sync.ref_move.is_some());
    assert_eq!(
        destination
            .get_current("main", b"repair.txt")
            .await
            .unwrap()
            .bytes,
        b"recoverable"
    );
}

#[tokio::test]
async fn native_fetch_returns_a_destination_local_mapped_head() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(
        source_plane,
        native_options(".prolly/native-versioned/fetch-source"),
    )
    .await
    .unwrap();
    source
        .put_bytes(
            "main",
            b"base.txt".to_vec(),
            b"base".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    source
        .clone_to(
            destination_plane.clone(),
            ".prolly/native-versioned/fetch-destination",
        )
        .await
        .unwrap();
    let destination = Repository::open(
        destination_plane,
        native_options(".prolly/native-versioned/fetch-destination"),
    )
    .await
    .unwrap();
    let source_base = source.head("main").await.unwrap();
    source.create_branch("feature", source_base).await.unwrap();
    source
        .put_bytes(
            "feature",
            b"feature.txt".to_vec(),
            b"feature".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let fetched = destination.fetch_from(&source, "feature").await.unwrap();
    let mapped_head = fetched.source_head.unwrap();
    assert_ne!(mapped_head, source.head("feature").await.unwrap());
    destination.fsck_commit(mapped_head).await.unwrap();
    destination
        .create_branch("imported", mapped_head)
        .await
        .unwrap();
    assert_eq!(
        destination
            .get_current("imported", b"feature.txt")
            .await
            .unwrap()
            .bytes,
        b"feature"
    );
}

#[tokio::test]
async fn native_multipart_uses_n_plus_five_calls_and_replays_without_io() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/multipart-budget"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"warmup.bin".to_vec(),
            b"warm".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let first_bytes = vec![7; 5 * 1024 * 1024];
    let second_bytes = vec![9; 1024];
    let mut whole = first_bytes.clone();
    whole.extend_from_slice(&second_bytes);
    let checksum_sha256: [u8; 32] = Sha256::digest(&whole).into();
    let checksum_md5: [u8; 16] = Md5::digest(&whole).into();

    plane.reset_request_counts();
    let session = repository
        .create_native_multipart_upload(
            "main",
            b"multipart.bin".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let first = repository
        .upload_native_multipart_part(&session, 1, first_bytes)
        .await
        .unwrap();
    let second = repository
        .upload_native_multipart_part(&session, 2, second_bytes)
        .await
        .unwrap();
    let parts = [&first, &second]
        .into_iter()
        .map(|part| NativeMultipartCompletedPart {
            part_number: part.part_number,
            etag: part.etag.clone(),
            checksum_sha256: part.checksum_sha256.unwrap(),
            size: part.size,
        })
        .collect::<Vec<_>>();
    let receipt = repository
        .complete_native_multipart_upload(
            session.clone(),
            parts.clone(),
            checksum_sha256,
            checksum_md5,
            whole.len() as u64,
            Some(session.operation),
        )
        .await
        .unwrap();
    assert_eq!(receipt.changed_keys, 1);
    let requests = plane.request_snapshot();
    assert_eq!(requests.native_multipart_create, 1);
    assert_eq!(requests.native_multipart_upload_part, 2);
    assert_eq!(requests.native_multipart_complete, 1);
    assert_eq!(requests.immutable_put, 2);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 7, "unexpected calls: {requests:?}");
    assert_eq!(
        repository
            .get_current("main", b"multipart.bin")
            .await
            .unwrap()
            .bytes,
        whole
    );

    plane.reset_request_counts();
    let replay = repository
        .complete_native_multipart_upload(
            session.clone(),
            parts,
            checksum_sha256,
            checksum_md5,
            first.size + second.size,
            Some(session.operation),
        )
        .await
        .unwrap();
    assert_eq!(replay.id, receipt.id);
    assert!(replay.idempotent_replay);
    assert_eq!(plane.request_snapshot().total(), 0);
}

#[tokio::test]
async fn two_object_native_batch_is_exactly_five_calls() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/batch-budget"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"warmup.bin".to_vec(),
            b"warm".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let batch = repository
        .begin_native_batch("main", "two objects", 60_000)
        .await
        .unwrap();
    plane.reset_request_counts();
    let receipt = repository
        .publish_native_batch(
            batch,
            vec![
                NativeBatchMutationV1::Put {
                    key: b"batch/a.bin".to_vec(),
                    bytes: b"a".to_vec(),
                    headers: ObjectHeaders::default(),
                    user_metadata: BTreeMap::new(),
                },
                NativeBatchMutationV1::Put {
                    key: b"batch/b.bin".to_vec(),
                    bytes: b"b".to_vec(),
                    headers: ObjectHeaders::default(),
                    user_metadata: BTreeMap::new(),
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(receipt.changed_keys, 2);
    let requests = plane.request_snapshot();
    assert_eq!(requests.native_put, 2);
    assert_eq!(requests.immutable_put, 2);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 5, "unexpected calls: {requests:?}");
}

#[tokio::test]
async fn warm_native_merge_reuses_bindings_in_three_calls() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/merge-budget"),
    )
    .await
    .unwrap();
    let base = repository
        .put_bytes(
            "main",
            b"base.bin".to_vec(),
            b"base".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    repository.create_branch("feature", base.id).await.unwrap();
    let feature = repository
        .put_bytes(
            "feature",
            b"feature.bin".to_vec(),
            b"feature".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    repository
        .put_bytes(
            "main",
            b"main.bin".to_vec(),
            b"main".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    plane.reset_request_counts();
    repository
        .merge(
            "main",
            feature.id,
            Some(base.id),
            MergePolicy::Fail,
            None,
            None,
        )
        .await
        .unwrap();
    let requests = plane.request_snapshot();
    assert_eq!(requests.immutable_put, 2);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 3, "unexpected calls: {requests:?}");
    assert_eq!(
        repository
            .get_current("main", b"feature.bin")
            .await
            .unwrap()
            .bytes,
        b"feature"
    );
}

#[tokio::test]
async fn warm_native_restore_reuses_live_binding_in_three_calls() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/restore-budget"),
    )
    .await
    .unwrap();
    let source = repository
        .put_bytes(
            "main",
            b"restore.bin".to_vec(),
            b"source".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let current = repository
        .put_bytes(
            "main",
            b"restore.bin".to_vec(),
            b"newer".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    plane.reset_request_counts();
    repository
        .restore("main", source.id, current.id, None, None)
        .await
        .unwrap();
    let requests = plane.request_snapshot();
    assert_eq!(requests.immutable_put, 2);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 3, "unexpected calls: {requests:?}");
    assert_eq!(
        repository
            .get_current("main", b"restore.bin")
            .await
            .unwrap()
            .bytes,
        b"source"
    );
}

#[tokio::test]
async fn lost_native_payload_response_is_reconciled_without_duplicate_upload() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/lost-payload"),
    )
    .await
    .unwrap();
    plane.lose_next_native_put_response();
    repository
        .put_bytes(
            "main",
            b"lost.bin".to_vec(),
            b"accepted-before-timeout".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"lost.bin")
            .await
            .unwrap()
            .bytes,
        b"accepted-before-timeout"
    );
    let versions = plane
        .list(prolly_s3_core::ListRequest {
            prefix: "lost.bin".to_string(),
            continuation: None,
            limit: 100,
            include_versions: true,
        })
        .await
        .unwrap();
    assert_eq!(versions.entries.len(), 1);
}

#[tokio::test]
async fn lost_native_copy_response_is_reconciled_without_duplicate_upload() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/lost-copy"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"source.txt".to_vec(),
            b"copy me".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let operation = OperationId::new();
    plane.lose_next_native_put_response();
    let copied = repository
        .copy_object(
            "main",
            b"source.txt",
            None,
            b"destination.txt".to_vec(),
            Some(operation),
        )
        .await
        .unwrap();
    assert_eq!(copied.operation, operation);
    assert_eq!(
        repository
            .get_current("main", b"destination.txt")
            .await
            .unwrap()
            .bytes,
        b"copy me"
    );
    let replay = repository
        .copy_object(
            "main",
            b"source.txt",
            None,
            b"destination.txt".to_vec(),
            Some(operation),
        )
        .await
        .unwrap();
    assert!(replay.idempotent_replay);
}

#[tokio::test]
async fn lost_native_delete_response_is_reconciled_to_the_current_marker() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/lost-delete"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"deleted.txt".to_vec(),
            b"delete me".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let operation = OperationId::new();
    plane.lose_next_native_delete_response();
    let deleted = repository
        .delete_object("main", b"deleted.txt".to_vec(), Some(operation))
        .await
        .unwrap();
    assert_eq!(deleted.operation, operation);
    assert_eq!(
        repository
            .get_current("main", b"deleted.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    let replay = repository
        .delete_object("main", b"deleted.txt".to_vec(), Some(operation))
        .await
        .unwrap();
    assert!(replay.idempotent_replay);
}

#[tokio::test]
async fn native_idempotent_replay_does_not_upload_again() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/idempotency"),
    )
    .await
    .unwrap();
    let operation = OperationId::new();
    let first = repository
        .put_bytes(
            "main",
            b"idempotent.bin".to_vec(),
            b"once".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(operation),
        )
        .await
        .unwrap();
    plane.reset_request_counts();
    let replay = repository
        .put_bytes(
            "main",
            b"idempotent.bin".to_vec(),
            b"once".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(operation),
        )
        .await
        .unwrap();
    assert_eq!(replay.id, first.id);
    assert!(replay.idempotent_replay);
    assert_eq!(plane.request_snapshot().total(), 0);
}

fn native_live_body(bytes: &[u8]) -> ObjectVersionBodyV1 {
    let sha256 = Cid::from_bytes(bytes).0;
    ObjectVersionBodyV1 {
        order: ObjectVersionOrder {
            commit_generation: CommitGeneration(1),
            mutation_ordinal: 0,
        },
        created_at_millis: 7,
        kind: ObjectVersionKindV1::Live {
            content: ContentRef::Empty,
            size: bytes.len() as u64,
            logical_etag: "\"logical\"".to_string(),
            headers: ObjectHeaders::default(),
            checksums: Checksums {
                md5: None,
                sha256: Some(sha256),
                algorithm_values: BTreeMap::new(),
            },
            user_metadata: BTreeMap::new(),
            tags: BTreeMap::new(),
        },
    }
}

#[test]
fn native_object_identity_excludes_provider_binding() {
    let bytes = b"whole object";
    let repository = RepositoryId::from_hash([3; 32]);
    let operation = OperationId::new();
    let body = native_live_body(bytes);
    let checksum_sha256 = Cid::from_bytes(bytes).0;
    let first = ObjectVersionV1::derive_native(
        repository,
        b"asset.bin",
        operation,
        body.clone(),
        NativeObjectBindingV1::Live {
            version_id: "version-a".to_string(),
            provider_etag: "etag-a".to_string(),
            checksum_sha256,
        },
    )
    .unwrap();
    let second = ObjectVersionV1::derive_native(
        repository,
        b"asset.bin",
        operation,
        body,
        NativeObjectBindingV1::Live {
            version_id: "version-b".to_string(),
            provider_etag: "etag-b".to_string(),
            checksum_sha256,
        },
    )
    .unwrap();

    assert_eq!(first.id, second.id);
    assert_ne!(first.native_binding, second.native_binding);
    first.validate_native().unwrap();
    second.validate_native().unwrap();
}

#[test]
fn native_binding_rejects_kind_and_checksum_mismatch() {
    let error = ObjectVersionV1::derive_native(
        RepositoryId::from_hash([4; 32]),
        b"asset.bin",
        OperationId::new(),
        native_live_body(b"expected"),
        NativeObjectBindingV1::DeleteMarker {
            version_id: "delete-version".to_string(),
        },
    )
    .err()
    .unwrap();
    assert_eq!(error.code, ErrorCode::CorruptCommit);

    let logical = LogicalObjectVersionKindV2::DeleteMarker;
    assert!(matches!(logical, LogicalObjectVersionKindV2::DeleteMarker));
}

#[test]
fn node_pack_verifies_cids_ranges_and_corruption() {
    let first_bytes = b"node-a".to_vec();
    let second_bytes = b"node-b".to_vec();
    let first = Cid::from_bytes(&first_bytes);
    let second = Cid::from_bytes(&second_bytes);
    let mut nodes = [(first.clone(), first_bytes), (second, second_bytes)];
    nodes.sort_by_key(|(cid, _)| cid.clone());
    let mut payload = Vec::new();
    let mut entries = Vec::new();
    for (cid, bytes) in nodes {
        let offset = payload.len() as u64;
        payload.extend_from_slice(&bytes);
        let checksum = cid.0;
        entries.push(NodePackEntryV1 {
            cid,
            offset,
            len: bytes.len() as u32,
            sha256: checksum,
        });
    }
    let pack = NodePackV1 {
        format_digest: tree_format_digest(&TreeFormat::default()).unwrap(),
        entries,
        attachments: Vec::new(),
        payload,
    };
    pack.validate().unwrap();
    assert_eq!(pack.node(&first).unwrap().unwrap(), b"node-a");
    assert_eq!(pack.reference().unwrap().node_count, 2);

    let mut corrupt = pack;
    corrupt.payload[0] ^= 1;
    assert_eq!(corrupt.validate().unwrap_err().code, ErrorCode::CorruptNode);
}

#[test]
fn provider_native_profile_requires_enabled_versioning() {
    let mut capabilities = ProviderCapabilities {
        conditional_create: true,
        conditional_update: true,
        strong_get_after_put: true,
        strong_list_after_put: true,
        strong_list_after_delete: true,
        ranged_get: true,
        paged_list: true,
        list_physical_versions: true,
        exact_version_delete: true,
        physical_versioning: PhysicalVersioning::Suspended,
        conflicting_lifecycle_rule: false,
        default_object_lock_retention: false,
        max_object_bytes: 5 * 1024 * 1024,
        max_single_put_bytes: 5 * 1024 * 1024,
    };
    assert_eq!(
        capabilities.validate_native_versioned().unwrap_err().code,
        ErrorCode::ProviderNotQualified
    );
    capabilities.physical_versioning = PhysicalVersioning::Enabled;
    capabilities.validate_native_versioned().unwrap();
}

#[tokio::test]
async fn native_repository_round_trips_exact_physical_versions() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = native_options(".prolly/native-versioned/test");
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    assert_eq!(
        repository.storage_profile(),
        RepositoryStorageProfile::NativeVersionedV1
    );

    let first = repository
        .put_bytes(
            "main",
            b"notes/file.txt".to_vec(),
            b"first".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let first_version = first.object_versions[0];
    repository
        .put_bytes(
            "main",
            b"notes/file.txt".to_vec(),
            b"second".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        repository
            .get_version("main", b"notes/file.txt", first_version)
            .await
            .unwrap()
            .bytes,
        b"first"
    );
    assert_eq!(
        repository
            .get_current("main", b"notes/file.txt")
            .await
            .unwrap()
            .bytes,
        b"second"
    );

    repository
        .copy_object(
            "main",
            b"notes/file.txt",
            Some(first_version),
            b"notes/copied.txt".to_vec(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"notes/copied.txt")
            .await
            .unwrap()
            .bytes,
        b"first"
    );

    let deleted = repository
        .delete_object("main", b"notes/file.txt".to_vec(), None)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"notes/file.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    let marker = repository
        .head_version("main", b"notes/file.txt", deleted.object_versions[0])
        .await
        .unwrap()
        .1;
    assert!(matches!(
        marker.version.native_binding,
        Some(NativeObjectBindingV1::DeleteMarker { .. })
    ));

    let reserved = repository
        .put_bytes(
            "main",
            b".prolly/native-versioned/test/internal".to_vec(),
            vec![1],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(reserved.code, ErrorCode::InvalidKey);

    let mismatch = Repository::open(
        plane,
        RepositoryOptions {
            repository_prefix: options.repository_prefix,
            ..RepositoryOptions::default()
        },
    )
    .await
    .err()
    .unwrap();
    assert_eq!(mismatch.code, ErrorCode::RepositoryFormatConflict);
}

#[tokio::test]
async fn warm_native_put_is_exactly_four_foreground_calls() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/call-budget"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"warmup.bin".to_vec(),
            vec![1; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    plane.reset_request_counts();
    repository
        .put_bytes(
            "main",
            b"measured.bin".to_vec(),
            vec![2; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let requests = plane.request_snapshot();
    assert_eq!(requests.native_put, 1);
    assert_eq!(requests.immutable_put, 2);
    assert_eq!(requests.compare_exchange, 1);
    assert_eq!(requests.total(), 4, "unexpected calls: {requests:?}");
}

#[tokio::test]
async fn native_writer_queue_preserves_four_calls_at_1_8_and_32_callers() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        native_options(".prolly/native-versioned/concurrent-budget"),
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            b"warmup.bin".to_vec(),
            vec![0; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    for writers in [1usize, 8, 32] {
        plane.reset_request_counts();
        let writes = (0..writers).map(|index| {
            repository.put_bytes(
                "main",
                format!("tier-{writers}/object-{index}.bin").into_bytes(),
                vec![index as u8; 64 * 1024],
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
        });
        for result in futures_util::future::join_all(writes).await {
            result.unwrap();
        }
        let requests = plane.request_snapshot();
        assert_eq!(
            requests.total(),
            (writers * 4) as u64,
            "{writers}-writer tier made unexpected calls: {requests:?}"
        );
        assert_eq!(requests.native_put, writers as u64);
        assert_eq!(requests.immutable_put, (writers * 2) as u64);
        assert_eq!(requests.compare_exchange, writers as u64);
    }
}

#[tokio::test]
async fn native_gc_deletes_only_unreachable_exact_versions() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository =
        Repository::initialize(plane.clone(), native_options(".prolly/native-versioned/gc"))
            .await
            .unwrap();
    repository
        .put_bytes(
            "main",
            b"retained.bin".to_vec(),
            b"retained".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let orphan = plane
        .put_native(NativePut {
            path: ObjectPath::new("orphan.bin").unwrap(),
            bytes: b"orphan".to_vec(),
            headers: ObjectHeaders::default(),
            user_metadata: BTreeMap::new(),
            repository: repository.repository_id(),
            operation: OperationId::new(),
            writer_fence_generation: 1,
        })
        .await
        .unwrap();
    let NativeObjectBindingV1::Live { version_id, .. } = orphan.binding else {
        unreachable!()
    };

    let plan = repository.plan_gc(120_000, 10).await.unwrap();
    assert_eq!(plan.plan.body.candidates.len(), 1);
    assert_eq!(plan.plan.body.candidates[0].path.as_str(), "orphan.bin");
    repository.sweep_gc(plan.plan.id).await.unwrap();
    assert!(plane
        .get(prolly_s3_core::GetRequest {
            path: ObjectPath::new("orphan.bin").unwrap(),
            range: None,
            physical_version: Some(PhysicalVersion::Versioned { version_id }),
        })
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repository
            .get_current("main", b"retained.bin")
            .await
            .unwrap()
            .bytes,
        b"retained"
    );
}

#[tokio::test]
async fn checkpointed_reopen_uses_ranged_nodes_without_pack_listing() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = native_options(".prolly/native-versioned/checkpoint");
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    for index in 0..8 {
        repository
            .put_bytes(
                "main",
                format!("objects/{index}.bin").into_bytes(),
                vec![index; 4096],
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let checkpoint = repository
        .create_node_index_checkpoint("main")
        .await
        .unwrap();
    assert!(!checkpoint.entries.is_empty());

    let reopened = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    plane.reset_request_counts();
    assert_eq!(
        reopened
            .get_current("main", b"objects/7.bin")
            .await
            .unwrap()
            .bytes,
        vec![7; 4096]
    );
    let requests = plane.request_snapshot();
    assert_eq!(
        requests.list, 0,
        "point read rebuilt by listing: {requests:?}"
    );
    assert!(requests.get >= 4, "cold read accounting was incomplete");
}

#[tokio::test]
async fn corrupt_checkpoint_falls_back_to_canonical_pack_rebuild() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = native_options(".prolly/native-versioned/checkpoint-corrupt");
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    repository
        .put_bytes(
            "main",
            b"rebuild.bin".to_vec(),
            b"canonical".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    repository
        .create_node_index_checkpoint("main")
        .await
        .unwrap();
    let listed = plane
        .list(prolly_s3_core::ListRequest {
            prefix: format!("{}/node-index/checkpoints/", options.repository_prefix),
            continuation: None,
            limit: 10,
            include_versions: false,
        })
        .await
        .unwrap();
    let checkpoint = listed.entries.into_iter().next().unwrap();
    plane
        .delete_exact(
            &checkpoint.path,
            PhysicalVersion::Versioned {
                version_id: checkpoint.metadata.token.version_id.clone().unwrap(),
            },
        )
        .await
        .unwrap();
    plane
        .put_immutable(prolly_s3_core::ImmutablePut {
            path: checkpoint.path,
            bytes: b"not canonical cbor".to_vec(),
            expected_sha256: Sha256::digest(b"not canonical cbor").into(),
        })
        .await
        .unwrap();

    let reopened = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    plane.reset_request_counts();
    assert_eq!(
        reopened
            .get_current("main", b"rebuild.bin")
            .await
            .unwrap()
            .bytes,
        b"canonical"
    );
    assert!(plane.request_snapshot().list > 0);
}

#[tokio::test]
async fn explicit_takeover_barrier_fences_the_old_writer() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = native_options(".prolly/native-versioned/takeover");
    let old_writer = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    old_writer
        .put_bytes(
            "main",
            b"before.bin".to_vec(),
            b"before".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let mut new_writer = Repository::open(
        plane.clone(),
        RepositoryOptions {
            writer: "replacement-writer".to_string(),
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    assert_eq!(
        new_writer
            .takeover_native_writer(
                "native-writer",
                1,
                "old credentials revoked and process stopped",
            )
            .await
            .unwrap(),
        2
    );
    new_writer
        .put_bytes(
            "main",
            b"after.bin".to_vec(),
            b"after".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        old_writer.renew_writer_lease().await.unwrap_err().code,
        ErrorCode::PreconditionFailed
    );
    plane.reset_request_counts();

    let stale = old_writer
        .put_bytes(
            "main",
            b"stale.bin".to_vec(),
            b"stale".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code, ErrorCode::PreconditionFailed);
    assert_eq!(plane.request_snapshot().native_put, 0);
}
