use std::{collections::BTreeMap, sync::Arc};

use prolly_s3_core::{
    FixedClock, GetRequest, MemoryObjectPlane, ObjectHeaders, ObjectPath, ObjectPlane,
    ProviderPerKeyVersionLimitV2, RepositoryFormatV2, RepositoryV2, RepositoryV2Options,
    SequenceIdSource,
};

#[tokio::test]
async fn native_v2_put_read_replay_and_reopen_use_only_v2_authority() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(10_000));
    let prefix = ".tests/native-v2";
    let options = RepositoryV2Options {
        repository_prefix: prefix.to_string(),
        writer: "writer-a".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0x55, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
        ..RepositoryV2Options::default()
    };
    let repository = RepositoryV2::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    assert_eq!(
        repository.format().format_version,
        RepositoryFormatV2::VERSION
    );
    assert!(plane
        .get(GetRequest {
            path: ObjectPath::new(format!("{prefix}/format/v2.cbor")).unwrap(),
            range: None,
            physical_version: None,
        })
        .await
        .unwrap()
        .is_some());
    assert!(plane
        .get(GetRequest {
            path: ObjectPath::new(format!("{prefix}/format/v1.cbor")).unwrap(),
            range: None,
            physical_version: None,
        })
        .await
        .unwrap()
        .is_none());

    clock.advance(1).unwrap();
    let operation = options.ids.operation();
    let first = repository
        .put_object_with_operation(
            "main",
            b"docs/readme.txt".to_vec(),
            b"native v2".to_vec(),
            ObjectHeaders {
                content_type: Some("text/plain".to_string()),
                ..ObjectHeaders::default()
            },
            BTreeMap::from([("source".to_string(), "test".to_string())]),
            operation,
        )
        .await
        .unwrap();
    assert!(!first.idempotent_replay);
    let replay = repository
        .put_object_with_operation(
            "main",
            b"docs/readme.txt".to_vec(),
            b"native v2".to_vec(),
            ObjectHeaders {
                content_type: Some("text/plain".to_string()),
                ..ObjectHeaders::default()
            },
            BTreeMap::from([("source".to_string(), "test".to_string())]),
            operation,
        )
        .await
        .unwrap();
    assert_eq!(replay.id, first.id);
    assert!(replay.idempotent_replay);

    let object = repository
        .get_object("main", b"docs/readme.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(object.bytes, b"native v2");
    let binding = object.version.binding.unwrap();
    assert!(binding.path.as_str().starts_with(&format!(
        "{prefix}/payloads/v2/{}/sha256/",
        hex::encode(repository.format().repository_id.as_bytes())
    )));
    assert_ne!(binding.path.as_str(), "docs/readme.txt");

    repository.advance_branch_indexes("main").await.unwrap();
    let read_only = RepositoryV2::open(
        plane.clone(),
        RepositoryV2Options {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    plane.reset_request_counts();
    assert_eq!(
        read_only
            .get_object("main", b"docs/readme.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"native v2"
    );
    assert_eq!(plane.request_snapshot().list, 0);
}

#[tokio::test]
async fn native_v2_takeover_fences_old_writer_before_payload_put() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(20_000));
    let prefix = ".tests/native-v2-takeover";
    let old_options = RepositoryV2Options {
        repository_prefix: prefix.to_string(),
        writer: "writer-a".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0x66, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
        ..RepositoryV2Options::default()
    };
    let old = RepositoryV2::initialize(plane.clone(), old_options.clone())
        .await
        .unwrap();
    let replacement = RepositoryV2::open(
        plane.clone(),
        RepositoryV2Options {
            writer: "writer-b".to_string(),
            read_only: true,
            ids: Arc::new(SequenceIdSource::new(0x77, 1)),
            ..old_options
        },
    )
    .await
    .unwrap();
    clock.advance(1).unwrap();
    assert_eq!(
        replacement
            .takeover_branch_writer("main", "writer-a", 1, "writer-a credentials revoked")
            .await
            .unwrap(),
        2
    );

    plane.reset_request_counts();
    let error = old
        .put_object(
            "main",
            b"stale.txt".to_vec(),
            b"must not upload".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, prolly_s3_core::ErrorCode::PreconditionFailed);
    assert_eq!(plane.request_snapshot().immutable_put, 0);

    replacement
        .put_object(
            "main",
            b"current.txt".to_vec(),
            b"writer-b".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
}
