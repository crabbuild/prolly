use std::{collections::BTreeMap, io::Write as _, sync::Arc};

use md5::{Digest as _, Md5};
use prolly_s3_core::{
    FixedClock, GetRequest, ListRequest, LogicalObjectVersionKindV1, MemoryObjectPlane,
    ObjectHeaders, ObjectPath, ObjectPlane, ProviderPerKeyVersionLimitV2, RepositoryFormatV2,
    RepositoryV2, RepositoryV2Options, SequenceIdSource,
};
use sha2::Sha256;

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
async fn native_v2_history_listing_delete_markers_and_payload_dedup_are_stable() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(30_000));
    let prefix = ".tests/native-v2-object-semantics";
    let options = RepositoryV2Options {
        repository_prefix: prefix.to_string(),
        writer: "writer-a".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0x88, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
        ..RepositoryV2Options::default()
    };
    let repository = RepositoryV2::initialize(plane.clone(), options)
        .await
        .unwrap();

    clock.advance(1).unwrap();
    let first = repository
        .put_object(
            "main",
            b"docs/readme.txt".to_vec(),
            b"shared payload".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    clock.advance(1).unwrap();
    repository
        .put_object(
            "main",
            b"docs/guide.txt".to_vec(),
            b"shared payload".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    clock.advance(1).unwrap();
    repository
        .put_object(
            "main",
            b"docs/readme.txt".to_vec(),
            b"current payload".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();

    let historical = repository
        .get_object_at("main", first.id, b"docs/readme.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(historical.bytes, b"shared payload");
    assert_eq!(historical.snapshot, first.id);

    let (snapshot, first_page, truncated) = repository
        .list_objects("main", b"docs/", None, 1)
        .await
        .unwrap();
    assert!(truncated);
    assert_eq!(first_page.len(), 1);
    assert_eq!(first_page[0].key, b"docs/guide.txt");
    let (second_page, truncated) = repository
        .list_objects_at("main", snapshot, b"docs/", Some(&first_page[0].key), 1)
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(second_page.len(), 1);
    assert_eq!(second_page[0].key, b"docs/readme.txt");

    let (_, versions) = repository
        .list_object_versions("main", b"docs/readme.txt", 10)
        .await
        .unwrap();
    assert_eq!(versions.len(), 2);
    assert!(matches!(
        versions[0].body.kind,
        LogicalObjectVersionKindV1::Live { .. }
    ));
    assert!(
        versions[0].body.order.commit_generation.0 > versions[1].body.order.commit_generation.0
    );

    let (version_page, truncated) = repository
        .list_versions_at("main", snapshot, b"docs/", None, 2)
        .await
        .unwrap();
    assert!(truncated);
    assert_eq!(version_page.len(), 2);
    let (remaining_versions, truncated) = repository
        .list_versions_at("main", snapshot, b"docs/", Some(&version_page[1].cursor), 2)
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(remaining_versions.len(), 1);

    clock.advance(1).unwrap();
    let deleted = repository
        .delete_object("main", b"docs/readme.txt".to_vec())
        .await
        .unwrap();
    assert_eq!(deleted.changed_keys, 1);
    assert!(repository
        .get_object("main", b"docs/readme.txt")
        .await
        .unwrap()
        .is_none());
    let (_, versions) = repository
        .list_object_versions("main", b"docs/readme.txt", 10)
        .await
        .unwrap();
    assert_eq!(versions.len(), 3);
    assert!(matches!(
        versions[0].body.kind,
        LogicalObjectVersionKindV1::DeleteMarker
    ));
    assert!(versions[0].binding.is_none());

    let payload_prefix = format!(
        "{prefix}/payloads/v2/{}/sha256/",
        hex::encode(repository.repository_id().as_bytes())
    );
    let payloads = plane
        .list(ListRequest {
            prefix: payload_prefix,
            continuation: None,
            limit: 100,
            include_versions: true,
        })
        .await
        .unwrap();
    assert_eq!(
        payloads.entries.len(),
        2,
        "equal logical payloads must share one immutable physical object"
    );
    assert!(payloads.entries.iter().all(|entry| entry.is_latest));
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

#[tokio::test]
async fn native_v2_writable_reopen_reacquires_authority_and_fences_the_old_handle() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(40_000));
    let prefix = ".tests/native-v2-writable-reopen";
    let options = RepositoryV2Options {
        repository_prefix: prefix.to_string(),
        writer: "writer-a".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0x99, 1)),
        authority_lease_millis: 10_000,
        provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
        ..RepositoryV2Options::default()
    };
    let old = RepositoryV2::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    clock.advance(1).unwrap();
    old.put_object(
        "main",
        b"before-reopen.txt".to_vec(),
        b"old handle".to_vec(),
        ObjectHeaders::default(),
        BTreeMap::new(),
    )
    .await
    .unwrap();
    old.advance_branch_indexes("main").await.unwrap();

    clock.advance(1).unwrap();
    let reopened = RepositoryV2::open(plane.clone(), options).await.unwrap();
    reopened
        .put_object(
            "main",
            b"after-reopen.txt".to_vec(),
            b"new handle".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();

    clock.advance(1).unwrap();
    plane.reset_request_counts();
    let renewal_error = old.renew_shard_authorities().await.unwrap_err();
    assert_eq!(
        renewal_error.code,
        prolly_s3_core::ErrorCode::PreconditionFailed
    );
    assert_eq!(old.fenced_branches().unwrap(), vec!["main"]);
    let error = old
        .put_object(
            "main",
            b"stale-after-reopen.txt".to_vec(),
            b"must not upload".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, prolly_s3_core::ErrorCode::PreconditionFailed);
    assert_eq!(plane.request_snapshot().immutable_put, 0);
}

#[tokio::test]
async fn native_v2_manual_authority_renewal_extends_the_write_window() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(50_000));
    let repository = RepositoryV2::initialize(
        plane,
        RepositoryV2Options {
            repository_prefix: ".tests/native-v2-renewal".to_string(),
            writer: "writer-a".to_string(),
            clock: clock.clone(),
            ids: Arc::new(SequenceIdSource::new(0xaa, 1)),
            authority_lease_millis: 10_000,
            provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
            ..RepositoryV2Options::default()
        },
    )
    .await
    .unwrap();

    clock.advance(4_000).unwrap();
    repository.renew_shard_authorities().await.unwrap();
    clock.advance(7_000).unwrap();
    repository
        .put_object(
            "main",
            b"after-original-expiry.txt".to_vec(),
            b"renewed".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    assert!(repository.fenced_branches().unwrap().is_empty());
}

#[tokio::test]
async fn native_v2_commit_session_batches_payloads_into_one_replayable_publication() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(60_000));
    let repository = RepositoryV2::initialize(
        plane.clone(),
        RepositoryV2Options {
            repository_prefix: ".tests/native-v2-commit-session".to_string(),
            writer: "writer-a".to_string(),
            clock: clock.clone(),
            ids: Arc::new(SequenceIdSource::new(0xbb, 1)),
            provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
            ..RepositoryV2Options::default()
        },
    )
    .await
    .unwrap();
    let session = repository
        .begin_commit_session("main", "atomic import", 60_000)
        .await
        .unwrap();
    let first = repository
        .stage_commit_session_put(
            &session,
            b"batch/a.txt".to_vec(),
            b"first".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let mut spool = tempfile::NamedTempFile::new().unwrap();
    spool.write_all(b"second").unwrap();
    spool.flush().unwrap();
    let second = repository
        .stage_commit_session_file(
            &session,
            b"batch/b.txt".to_vec(),
            spool.path().to_path_buf(),
            6,
            Sha256::digest(b"second").into(),
            Md5::digest(b"second").into(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let mutations = vec![
        second,
        prolly_s3_core::StagedMutationV2::delete(b"batch/removed.txt".to_vec()),
        first,
    ];

    clock.advance(1).unwrap();
    let receipt = repository
        .publish_commit_session(session.clone(), mutations.clone())
        .await
        .unwrap();
    assert_eq!(receipt.changed_keys, 3);
    assert_eq!(receipt.object_versions.len(), 3);
    assert!(!receipt.idempotent_replay);
    assert_eq!(repository.head("main").await.unwrap(), receipt.id);
    assert_eq!(
        repository
            .get_object("main", b"batch/a.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"first"
    );
    assert_eq!(
        repository
            .get_object("main", b"batch/b.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"second"
    );
    assert!(repository
        .get_object("main", b"batch/removed.txt")
        .await
        .unwrap()
        .is_none());

    let replay = repository
        .publish_commit_session(session, mutations)
        .await
        .unwrap();
    assert_eq!(replay.id, receipt.id);
    assert!(replay.idempotent_replay);
}

#[tokio::test]
async fn native_v2_durable_session_resumes_after_process_authority_reacquisition() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(70_000));
    let options = RepositoryV2Options {
        repository_prefix: ".tests/native-v2-durable-session".to_string(),
        writer: "restartable-writer".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0xcc, 1)),
        authority_lease_millis: 10_000,
        provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
        ..RepositoryV2Options::default()
    };
    let original = RepositoryV2::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    let checkpoint = original
        .begin_durable_commit_session("main", "restartable import", 60_000)
        .await
        .unwrap();
    let staged = original
        .stage_commit_session_put(
            &checkpoint.session,
            b"resume/object.txt".to_vec(),
            b"uploaded exactly once".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    original
        .checkpoint_commit_session(&checkpoint.session, vec![staged], 1)
        .await
        .unwrap();
    let payload_puts = plane.request_snapshot().immutable_put;
    drop(original);

    clock.advance(1).unwrap();
    let reopened = RepositoryV2::open(plane.clone(), options).await.unwrap();
    let resumed = reopened
        .resume_commit_session(checkpoint.session.id)
        .await
        .unwrap();
    assert_eq!(resumed.mutations.len(), 1);
    let receipt = reopened
        .publish_commit_session(resumed.session, resumed.mutations)
        .await
        .unwrap();
    assert_eq!(receipt.operation, checkpoint.session.identity.operation);
    assert_eq!(
        reopened
            .get_object("main", b"resume/object.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"uploaded exactly once"
    );
    assert!(
        plane.request_snapshot().immutable_put <= payload_puts + 3,
        "resume may checkpoint authority adoption and publish commit/event, but must not upload a payload"
    );
}

#[tokio::test]
async fn native_v2_expired_session_cleanup_is_bounded_and_exact() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(80_000));
    let repository = RepositoryV2::initialize(
        plane,
        RepositoryV2Options {
            repository_prefix: ".tests/native-v2-session-cleanup".to_string(),
            writer: "cleanup-writer".to_string(),
            clock: clock.clone(),
            ids: Arc::new(SequenceIdSource::new(0xdd, 1)),
            authority_lease_millis: 10_000,
            provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
            ..RepositoryV2Options::default()
        },
    )
    .await
    .unwrap();
    let checkpoint = repository
        .begin_durable_commit_session("main", "expire me", 100)
        .await
        .unwrap();
    clock.advance(101).unwrap();
    let report = repository
        .cleanup_expired_commit_sessions(None, 1)
        .await
        .unwrap();
    assert_eq!(report.scanned, 1);
    assert_eq!(report.deleted, 1);
    let final_page = repository
        .cleanup_expired_commit_sessions(report.continuation, 1)
        .await
        .unwrap();
    assert!(final_page.continuation.is_none());
    let error = repository
        .resume_commit_session(checkpoint.session.id)
        .await
        .unwrap_err();
    assert_eq!(error.code, prolly_s3_core::ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn native_v2_cold_reads_fail_fast_until_background_indexes_catch_up() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(90_000));
    let options = RepositoryV2Options {
        repository_prefix: ".tests/native-v2-background-index".to_string(),
        writer: "index-writer".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0xee, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
        ..RepositoryV2Options::default()
    };
    let writer = RepositoryV2::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    for index in 0..3 {
        clock.advance(1).unwrap();
        writer
            .put_object(
                "main",
                format!("cold/{index}.txt").into_bytes(),
                format!("value-{index}").into_bytes(),
                ObjectHeaders::default(),
                BTreeMap::new(),
            )
            .await
            .unwrap();
    }

    let reader = Arc::new(
        RepositoryV2::open(
            plane.clone(),
            RepositoryV2Options {
                read_only: true,
                ..options
            },
        )
        .await
        .unwrap(),
    );
    plane.reset_request_counts();
    let error = reader.get_object("main", b"cold/2.txt").await.unwrap_err();
    assert_eq!(error.code, prolly_s3_core::ErrorCode::MissingClosure);
    assert!(
        plane.request_snapshot().get <= 3,
        "a foreground read may inspect ref/index heads but must not replay the journal tail"
    );

    let _maintenance = reader
        .start_branch_index_maintenance(std::time::Duration::from_millis(10))
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if reader.branch_index_health("main").await.unwrap().ready {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        reader
            .get_object("main", b"cold/2.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"value-2"
    );
}
