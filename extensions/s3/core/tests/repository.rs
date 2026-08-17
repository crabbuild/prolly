use std::{collections::BTreeMap, io::Write as _, sync::Arc};

use md5::{Digest as _, Md5};
use prolly_s3_core::{
    decode_canonical, encode_canonical, FixedClock, GetRequest, ListRequest,
    LogicalObjectVersionKind, MemoryNodeCache, MemoryObjectPlane, ObjectHeaders, ObjectPath,
    ObjectPlane, ProviderPerKeyVersionLimit, Repository, RepositoryOptions, SequenceIdSource,
};
use sha2::Sha256;

#[tokio::test]
async fn repository_cache_snapshot_includes_ref_catalog_reads_after_reopen() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let cache = Arc::new(MemoryNodeCache::new(64 * 1024 * 1024));
    let options = RepositoryOptions {
        repository_prefix: ".tests/repository-cache-metrics".to_string(),
        writer: "cache-writer".to_string(),
        node_cache: Some(cache.clone()),
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    let root = repository.head("main").await.unwrap();
    repository.create_branch("cached", root).await.unwrap();
    drop(repository);

    let reopened = Repository::open(
        plane,
        RepositoryOptions {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    let before = reopened.node_cache_snapshot();
    let page = reopened.list_branch_catalog_page(None, 100).await.unwrap();
    assert_eq!(page.branches.len(), 2);
    let after = reopened.node_cache_snapshot();
    assert!(
        after.hits > before.hits,
        "ref-catalog cache hits must be included in the repository snapshot"
    );
}

#[tokio::test]
async fn repository_put_read_replay_and_reopen_use_only_authority() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(10_000));
    let prefix = ".tests/repository";
    let options = RepositoryOptions {
        repository_prefix: prefix.to_string(),
        writer: "writer-a".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0x55, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    assert!(plane
        .get(GetRequest {
            path: ObjectPath::new(format!("{prefix}/format/repository.cbor")).unwrap(),
            range: None,
            physical_version: None,
        })
        .await
        .unwrap()
        .is_some());
    clock.advance(1).unwrap();
    let operation = options.ids.operation();
    let first = repository
        .put_object_with_operation(
            "main",
            b"docs/readme.txt".to_vec(),
            b"repository".to_vec(),
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
    let upper = repository
        .prewarm_node_cache_levels("main", first.id, 1)
        .await
        .unwrap();
    assert_eq!(upper.object_nodes, 1);
    assert_eq!(upper.version_nodes, 1);
    assert_eq!(
        repository
            .prewarm_node_cache_levels("main", first.id, 0)
            .await
            .unwrap_err()
            .code,
        prolly_s3_core::ErrorCode::InvalidLimit
    );
    let replay = repository
        .put_object_with_operation(
            "main",
            b"docs/readme.txt".to_vec(),
            b"repository".to_vec(),
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
    assert_eq!(object.bytes, b"repository");
    let binding = object.version.binding.unwrap();
    assert!(binding.path.as_str().starts_with(&format!(
        "{prefix}/payloads/{}/sha256/",
        hex::encode(repository.format().repository_id.as_bytes())
    )));
    assert_ne!(binding.path.as_str(), "docs/readme.txt");

    plane.reset_request_counts();
    assert_eq!(
        repository
            .get_object("main", b"docs/readme.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"repository"
    );
    assert_eq!(
        plane.request_snapshot().get,
        2,
        "a locally indexed exact branch target needs only the ref and whole payload GETs"
    );

    repository.advance_branch_indexes("main").await.unwrap();
    let read_only = Repository::open(
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
        read_only
            .get_object("main", b"docs/readme.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"repository"
    );
    assert_eq!(plane.request_snapshot().list, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_puts_prepare_payloads_before_the_ordered_publication_lane() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Arc::new(
        Repository::initialize(
            plane.clone(),
            RepositoryOptions {
                repository_prefix: ".tests/concurrent-payload-preparation".to_string(),
                writer: "concurrent-payload-writer".to_string(),
                provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
                ..RepositoryOptions::default()
            },
        )
        .await
        .unwrap(),
    );
    plane.set_immutable_put_delay_millis(25);
    plane.reset_immutable_put_concurrency();

    let mut writes = Vec::new();
    for index in 0..8_u8 {
        let repository = repository.clone();
        writes.push(tokio::spawn(async move {
            repository
                .put_object(
                    "main",
                    format!("parallel/{index}.bin").into_bytes(),
                    vec![index; 32],
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                )
                .await
        }));
    }
    for write in writes {
        write.await.unwrap().unwrap();
    }

    assert!(
        plane.max_immutable_puts_in_flight() >= 4,
        "payload preparation remained serialized: max_in_flight={}",
        plane.max_immutable_puts_in_flight()
    );
    let (_, objects, truncated) = repository
        .list_objects("main", b"parallel/", None, 16)
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(objects.len(), 8);
}

#[tokio::test]
async fn repository_history_listing_delete_markers_and_payload_dedup_are_stable() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(30_000));
    let prefix = ".tests/repository-object-semantics";
    let options = RepositoryOptions {
        repository_prefix: prefix.to_string(),
        writer: "writer-a".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0x88, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options)
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
        LogicalObjectVersionKind::Live { .. }
    ));
    assert!(
        versions[0].body.order.commit_generation.0 > versions[1].body.order.commit_generation.0
    );
    let historical_versions = repository
        .list_object_versions_at("main", first.id, b"docs/readme.txt", 10)
        .await
        .unwrap();
    assert_eq!(historical_versions.len(), 1);

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
        LogicalObjectVersionKind::DeleteMarker
    ));
    assert!(versions[0].binding.is_none());

    let payload_prefix = format!(
        "{prefix}/payloads/{}/sha256/",
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
async fn object_list_cursor_is_snapshot_bound_and_resumes_without_replay() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: ".tests/list-cursor".to_string(),
            writer: "list-writer".to_string(),
            ids: Arc::new(SequenceIdSource::new(0x89, 1)),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let session = repository
        .begin_commit_session("main", "seed cursor listing", 60_000)
        .await
        .unwrap();
    let mut mutations = Vec::new();
    for index in 0..250 {
        mutations.push(
            repository
                .stage_commit_session_put(
                    &session,
                    format!("cursor/{index:04}.txt").into_bytes(),
                    vec![index as u8],
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                )
                .await
                .unwrap(),
        );
    }
    repository
        .publish_commit_session(session, mutations)
        .await
        .unwrap();

    let first = repository
        .list_objects_page("main", b"cursor/", None, 100)
        .await
        .unwrap();
    assert_eq!(first.objects.len(), 100);
    let snapshot = first.snapshot;
    repository
        .put_object(
            "main",
            b"cursor/9999.txt".to_vec(),
            b"later".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();

    let historical_first = repository
        .list_objects_page_at("main", snapshot, b"cursor/", None, 100)
        .await
        .unwrap();
    assert_eq!(historical_first.snapshot, snapshot);
    assert_eq!(historical_first.objects, first.objects);

    let current = repository.head("main").await.unwrap();
    let error = repository
        .list_objects_page_at(
            "main",
            current,
            b"cursor/",
            first.continuation.as_deref(),
            100,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        prolly_s3_core::ErrorCode::InvalidContinuationToken
    );

    plane.reset_request_counts();
    let second = repository
        .list_objects_page("main", b"cursor/", first.continuation.as_deref(), 100)
        .await
        .unwrap();
    let third = repository
        .list_objects_page("main", b"cursor/", second.continuation.as_deref(), 100)
        .await
        .unwrap();
    assert_eq!(second.snapshot, snapshot);
    assert_eq!(third.snapshot, snapshot);
    assert_eq!(second.objects.len(), 100);
    assert_eq!(third.objects.len(), 50);
    assert!(third.continuation.is_none());
    assert!(second.objects[0].key > first.objects[99].key);
    assert!(third.objects[0].key > second.objects[99].key);
    assert_eq!(plane.request_snapshot().immutable_put, 0);

    let error = repository
        .list_objects_page("main", b"other/", first.continuation.as_deref(), 100)
        .await
        .unwrap_err();
    assert_eq!(
        error.code,
        prolly_s3_core::ErrorCode::InvalidContinuationToken
    );
}

#[tokio::test]
async fn repository_takeover_fences_old_writer_before_payload_put() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(20_000));
    let prefix = ".tests/repository-takeover";
    let old_options = RepositoryOptions {
        repository_prefix: prefix.to_string(),
        writer: "writer-a".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0x66, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let old = Repository::initialize(plane.clone(), old_options.clone())
        .await
        .unwrap();
    let replacement = Repository::open(
        plane.clone(),
        RepositoryOptions {
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
async fn repository_writable_reopen_reacquires_authority_and_fences_the_old_handle() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(40_000));
    let prefix = ".tests/repository-writable-reopen";
    let options = RepositoryOptions {
        repository_prefix: prefix.to_string(),
        writer: "writer-a".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0x99, 1)),
        authority_lease_millis: 10_000,
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let old = Repository::initialize(plane.clone(), options.clone())
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
    let reopened = Repository::open(plane.clone(), options).await.unwrap();
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
async fn repository_manual_authority_renewal_extends_the_write_window() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(50_000));
    let repository = Repository::initialize(
        plane,
        RepositoryOptions {
            repository_prefix: ".tests/repository-renewal".to_string(),
            writer: "writer-a".to_string(),
            clock: clock.clone(),
            ids: Arc::new(SequenceIdSource::new(0xaa, 1)),
            authority_lease_millis: 10_000,
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
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
async fn durable_commit_session_survives_repeated_authority_renewal_and_restart() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(55_000));
    let options = RepositoryOptions {
        repository_prefix: ".tests/session-multiple-renewals".to_string(),
        writer: "stable-writer".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0xab, 1)),
        authority_lease_millis: 10_000,
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    let checkpoint = repository
        .begin_durable_commit_session("main", "long import", 4 * 60 * 60 * 1_000)
        .await
        .unwrap();
    let original_stamp = checkpoint.session.identity.authority.clone();
    let mut staged = Vec::new();

    for renewal in 0..2_700 {
        clock.advance(4_000).unwrap();
        repository.renew_shard_authorities().await.unwrap();
        if renewal % 900 != 899 {
            continue;
        }
        let index = renewal / 900;
        let mutation = repository
            .stage_commit_session_put(
                &checkpoint.session,
                format!("renewed/{index}.txt").into_bytes(),
                format!("value-{index}").into_bytes(),
                ObjectHeaders::default(),
                BTreeMap::new(),
            )
            .await
            .unwrap();
        staged.push(mutation.clone());
        repository
            .checkpoint_commit_session(
                &checkpoint.session,
                vec![mutation],
                staged.len(),
                u64::try_from(index + 1).unwrap(),
            )
            .await
            .unwrap();
    }
    assert_eq!(checkpoint.session.identity.authority, original_stamp);
    drop(repository);

    clock.advance(1).unwrap();
    let reopened = Repository::open(plane, options).await.unwrap();
    let resumed = reopened
        .resume_commit_session(checkpoint.session.id)
        .await
        .unwrap();
    assert_eq!(resumed.mutations.len(), 3);
    assert_eq!(resumed.session.identity.authority, original_stamp);
    let receipt = reopened
        .publish_commit_session(resumed.session, resumed.mutations)
        .await
        .unwrap();
    assert_eq!(receipt.changed_keys, 3);
}

#[tokio::test]
async fn durable_checkpoint_windows_are_append_only_and_resume_last_write_per_key() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: ".tests/checkpoint-windows".to_string(),
            writer: "checkpoint-window-writer".to_string(),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let checkpoint = repository
        .begin_durable_commit_session("main", "append-only checkpoints", 60_000)
        .await
        .unwrap();
    let first = repository
        .stage_commit_session_put(
            &checkpoint.session,
            b"a".to_vec(),
            b"first".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    repository
        .checkpoint_commit_session(&checkpoint.session, vec![first], 1, 1)
        .await
        .unwrap();
    let second = repository
        .stage_commit_session_put(
            &checkpoint.session,
            b"b".to_vec(),
            b"second".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    repository
        .checkpoint_commit_session(&checkpoint.session, vec![second], 2, 2)
        .await
        .unwrap();
    let replacement = repository
        .stage_commit_session_put(
            &checkpoint.session,
            b"a".to_vec(),
            b"replacement".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    repository
        .checkpoint_commit_session(&checkpoint.session, vec![replacement], 2, 3)
        .await
        .unwrap();

    let page = plane
        .list(ListRequest {
            prefix: ".tests/checkpoint-windows/staging/".to_string(),
            continuation: None,
            limit: 100,
            include_versions: false,
        })
        .await
        .unwrap();
    let mut windows = Vec::new();
    for entry in page.entries {
        let stored = plane
            .get(GetRequest {
                path: entry.path,
                range: None,
                physical_version: None,
            })
            .await
            .unwrap()
            .unwrap();
        windows.push(
            decode_canonical::<prolly_s3_core::CommitSessionCheckpoint>(&stored.bytes).unwrap(),
        );
    }
    windows.sort_by_key(|window| window.sequence);
    assert_eq!(windows.len(), 4);
    assert_eq!(
        windows
            .iter()
            .map(|window| (
                window.sequence,
                window.total_mutations,
                window.mutations.len()
            ))
            .collect::<Vec<_>>(),
        vec![(0, 0, 0), (1, 1, 1), (2, 2, 1), (3, 2, 1)]
    );

    let resumed = repository
        .resume_commit_session(checkpoint.session.id)
        .await
        .unwrap();
    assert_eq!(resumed.sequence, 3);
    assert_eq!(resumed.total_mutations, 2);
    assert_eq!(resumed.mutations.len(), 2);
    repository
        .publish_commit_session(resumed.session, resumed.mutations)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_object("main", b"a")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"replacement"
    );
}

#[tokio::test]
async fn concurrent_session_staging_and_authority_renewal_do_not_self_fence() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(58_000));
    let repository = Arc::new(
        Repository::initialize(
            plane,
            RepositoryOptions {
                repository_prefix: ".tests/session-concurrent-renewal".to_string(),
                writer: "concurrent-writer".to_string(),
                clock: clock.clone(),
                ids: Arc::new(SequenceIdSource::new(0xac, 1)),
                authority_lease_millis: 10_000,
                provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
                ..RepositoryOptions::default()
            },
        )
        .await
        .unwrap(),
    );
    let session = repository
        .begin_commit_session("main", "concurrent renewal import", 120_000)
        .await
        .unwrap();
    let mut mutations = Vec::new();

    for index in 0..64 {
        clock.advance(100).unwrap();
        let renewing = repository.clone();
        let staging = repository.clone();
        let session = session.clone();
        let ((), mutation) = tokio::join!(
            async move { renewing.renew_shard_authorities().await.unwrap() },
            async move {
                staging
                    .stage_commit_session_put(
                        &session,
                        format!("concurrent/{index:03}.txt").into_bytes(),
                        vec![index as u8],
                        ObjectHeaders::default(),
                        BTreeMap::new(),
                    )
                    .await
                    .unwrap()
            }
        );
        mutations.push(mutation);
    }
    assert!(repository.fenced_branches().unwrap().is_empty());
    let receipt = repository
        .publish_commit_session(session, mutations)
        .await
        .unwrap();
    assert_eq!(receipt.changed_keys, 64);
}

#[tokio::test]
async fn real_takeover_fences_an_open_commit_session() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(59_000));
    let prefix = ".tests/session-takeover";
    let old_options = RepositoryOptions {
        repository_prefix: prefix.to_string(),
        writer: "writer-a".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0xad, 1)),
        authority_lease_millis: 10_000,
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let old = Repository::initialize(plane.clone(), old_options.clone())
        .await
        .unwrap();
    let checkpoint = old
        .begin_durable_commit_session("main", "must be fenced", 120_000)
        .await
        .unwrap();
    let staged = old
        .stage_commit_session_put(
            &checkpoint.session,
            b"stale.txt".to_vec(),
            b"stale".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    old.checkpoint_commit_session(&checkpoint.session, vec![staged.clone()], 1, 1)
        .await
        .unwrap();

    let replacement = Repository::open(
        plane,
        RepositoryOptions {
            writer: "writer-b".to_string(),
            read_only: true,
            ids: Arc::new(SequenceIdSource::new(0xae, 1)),
            ..old_options
        },
    )
    .await
    .unwrap();
    clock.advance(1).unwrap();
    replacement
        .takeover_branch_writer("main", "writer-a", 1, "writer-a credentials revoked")
        .await
        .unwrap();

    let error = old
        .publish_commit_session(checkpoint.session.clone(), vec![staged])
        .await
        .unwrap_err();
    assert_eq!(error.code, prolly_s3_core::ErrorCode::PreconditionFailed);
    let resume_error = replacement
        .resume_commit_session(checkpoint.session.id)
        .await
        .unwrap_err();
    assert_eq!(
        resume_error.code,
        prolly_s3_core::ErrorCode::PreconditionFailed
    );
}

#[tokio::test]
async fn repository_commit_session_batches_payloads_into_one_replayable_publication() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(60_000));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: ".tests/repository-commit-session".to_string(),
            writer: "writer-a".to_string(),
            clock: clock.clone(),
            ids: Arc::new(SequenceIdSource::new(0xbb, 1)),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
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
        prolly_s3_core::StagedMutation::delete(b"batch/removed.txt".to_vec()),
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
async fn repository_batch_stages_independent_whole_objects_with_whole_object_deduplication() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: ".tests/repository-whole-payload".to_string(),
            writer: "whole-object-writer".to_string(),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let session = repository
        .begin_commit_session("main", "whole-object import", 60_000)
        .await
        .unwrap();
    let staged = repository
        .stage_commit_session_put_batch(
            &session,
            vec![
                (
                    b"objects/a".to_vec(),
                    b"same".to_vec(),
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                ),
                (
                    b"objects/b".to_vec(),
                    b"same".to_vec(),
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                ),
                (
                    b"objects/c".to_vec(),
                    b"next".to_vec(),
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                ),
            ],
            4,
        )
        .await
        .unwrap();
    let receipt = repository
        .publish_commit_session(session, staged)
        .await
        .unwrap();
    let mut bindings = Vec::new();
    for key in [b"objects/a".as_slice(), b"objects/b", b"objects/c"] {
        bindings.push(
            repository
                .head_object_at("main", receipt.id, key)
                .await
                .unwrap()
                .unwrap()
                .version
                .binding
                .unwrap(),
        );
    }
    assert_eq!(bindings[0].path, bindings[1].path);
    assert_ne!(bindings[1].path, bindings[2].path);
    assert_eq!(bindings[0].checksum_sha256, bindings[1].checksum_sha256);
    assert_ne!(bindings[1].checksum_sha256, bindings[2].checksum_sha256);

    let stored = plane
        .list(ListRequest {
            prefix: ".tests/repository-whole-payload/".to_string(),
            continuation: None,
            limit: 100,
            include_versions: false,
        })
        .await
        .unwrap();
    assert_eq!(
        stored
            .entries
            .iter()
            .filter(|entry| entry.path.as_str().contains("/payloads/"))
            .count(),
        2
    );
    assert!(stored
        .entries
        .iter()
        .all(|entry| !entry.path.as_str().contains("payload-packs")));

    let range = repository
        .get_object_range("main", receipt.id, b"objects/a", 1..=2)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(range.bytes, b"am");
    assert_eq!(range.range, 1..=2);

    let mut fsck = repository.start_fsck("main", true).await.unwrap();
    while fsck.phase != prolly_s3_core::FsckPhase::Complete {
        let page = repository.advance_fsck(&fsck, 100).await.unwrap();
        fsck = decode_canonical(&encode_canonical(&page.cursor).unwrap()).unwrap();
    }
    assert_eq!(fsck.report.physical_payloads_verified, 2);
    assert_eq!(fsck.report.physical_payload_bytes_verified, 8);
    assert_eq!(fsck.report.deep_physical_bytes_read, 8);
}

#[tokio::test]
async fn repository_batch_results_isolates_invalid_objects_after_one_session_validation() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: ".tests/repository-batch-results".to_string(),
            writer: "batch-results-writer".to_string(),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let session = repository
        .begin_commit_session("main", "per-object batch results", 60_000)
        .await
        .unwrap();
    plane.reset_request_counts();
    let results = repository
        .stage_commit_session_put_batch_results(
            &session,
            vec![
                (
                    b"batch-results/a".to_vec(),
                    b"first".to_vec(),
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                ),
                (
                    Vec::new(),
                    b"invalid".to_vec(),
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                ),
                (
                    b"batch-results/b".to_vec(),
                    b"second".to_vec(),
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                ),
            ],
            3,
        )
        .await
        .unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(
        results[1].as_ref().unwrap_err().code,
        prolly_s3_core::ErrorCode::InvalidKey
    );
    assert_eq!(plane.request_snapshot().immutable_put, 2);
    let staged = results.into_iter().filter_map(Result::ok).collect();
    let receipt = repository
        .publish_commit_session(session, staged)
        .await
        .unwrap();
    assert_eq!(receipt.changed_keys, 2);
}

#[tokio::test]
async fn large_commit_delta_is_external_and_survives_toc_only_reopen() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = RepositoryOptions {
        repository_prefix: ".tests/repository-external-delta".to_string(),
        writer: "external-delta-writer".to_string(),
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    let base = repository.head("main").await.unwrap();
    let session = repository
        .begin_commit_session("main", "external delta", 60_000)
        .await
        .unwrap();
    let staged = repository
        .stage_commit_session_put_batch(
            &session,
            (0..129)
                .map(|index| {
                    (
                        format!("external/{index:03}").into_bytes(),
                        vec![index as u8],
                        ObjectHeaders::default(),
                        BTreeMap::new(),
                    )
                })
                .collect(),
            8,
        )
        .await
        .unwrap();
    let receipt = repository
        .publish_commit_session(session, staged)
        .await
        .unwrap();
    assert_eq!(receipt.changed_keys, 129);
    let commit = repository.commit(receipt.id).await.unwrap();
    assert!(commit.delta.changes.is_empty());
    assert!(commit.delta.changes_root.is_some());
    assert_eq!(commit.delta.change_count, 129);
    repository.advance_branch_indexes("main").await.unwrap();
    drop(repository);

    let reopened = Repository::open(
        plane,
        RepositoryOptions {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    let mut continuation = None;
    let mut changes = Vec::new();
    loop {
        let page = reopened
            .diff_page_bounded("main", base, receipt.id, continuation.as_ref(), 32)
            .await
            .unwrap();
        assert_eq!(page.compared_nodes, 0);
        changes.extend(page.changes);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    assert_eq!(changes.len(), 129);
    assert_eq!(changes.first().unwrap().key, b"external/000");
    assert_eq!(changes.last().unwrap().key, b"external/128");
    assert_eq!(
        reopened
            .get_object("main", b"external/128")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        vec![128]
    );
}

#[tokio::test]
async fn repository_durable_session_resumes_after_process_authority_reacquisition() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(70_000));
    let options = RepositoryOptions {
        repository_prefix: ".tests/repository-durable-session".to_string(),
        writer: "restartable-writer".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0xcc, 1)),
        authority_lease_millis: 10_000,
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let original = Repository::initialize(plane.clone(), options.clone())
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
        .checkpoint_commit_session(&checkpoint.session, vec![staged], 1, 1)
        .await
        .unwrap();
    let payload_puts = plane.request_snapshot().immutable_put;
    drop(original);

    clock.advance(1).unwrap();
    let reopened = Repository::open(plane.clone(), options).await.unwrap();
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
async fn repository_expired_session_cleanup_is_bounded_and_exact() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(80_000));
    let repository = Repository::initialize(
        plane,
        RepositoryOptions {
            repository_prefix: ".tests/repository-session-cleanup".to_string(),
            writer: "cleanup-writer".to_string(),
            clock: clock.clone(),
            ids: Arc::new(SequenceIdSource::new(0xdd, 1)),
            authority_lease_millis: 10_000,
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
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
async fn repository_cold_reads_fail_fast_until_background_indexes_catch_up() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(90_000));
    let options = RepositoryOptions {
        repository_prefix: ".tests/repository-background-index".to_string(),
        writer: "index-writer".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0xee, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let writer = Repository::initialize(plane.clone(), options.clone())
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
        Repository::open(
            plane.clone(),
            RepositoryOptions {
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

#[tokio::test]
async fn repository_over_limit_index_lag_rebuilds_in_restartable_pages() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(100_000));
    let options = RepositoryOptions {
        repository_prefix: ".tests/repository-index-rebuild".to_string(),
        writer: "rebuild-writer".to_string(),
        clock: clock.clone(),
        ids: Arc::new(SequenceIdSource::new(0xfa, 1)),
        journal_index_max_unindexed_events: 2,
        operation_index_leaf_entries: 2,
        operation_index_merge_fanout: 2,
        operation_index_max_unindexed_events: 8,
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let writer = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    let mut original = None;
    for index in 0..5 {
        clock.advance(1).unwrap();
        let receipt = writer
            .put_object(
                "main",
                format!("rebuild/{index}.txt").into_bytes(),
                format!("value-{index}").into_bytes(),
                ObjectHeaders::default(),
                BTreeMap::new(),
            )
            .await
            .unwrap();
        if index == 4 {
            original = Some(receipt);
        }
    }
    drop(writer);
    let reader = Repository::open(
        plane.clone(),
        RepositoryOptions {
            read_only: true,
            operation_index_max_unindexed_events: 2,
            ..options.clone()
        },
    )
    .await
    .unwrap();
    let error = reader.advance_branch_indexes("main").await.unwrap_err();
    assert_eq!(error.code, prolly_s3_core::ErrorCode::HistoryLimitExceeded);
    assert!(!reader.branch_index_health("main").await.unwrap().ready);

    let mut cursor = reader.start_branch_index_rebuild("main").await.unwrap();
    let mut steps = 0;
    loop {
        // A workflow may persist this canonical cursor and resume in another
        // process between every bounded step.
        let encoded = prolly_s3_core::encode_canonical(&cursor).unwrap();
        cursor = prolly_s3_core::decode_canonical(&encoded).unwrap();
        let step = reader
            .advance_branch_index_rebuild(&cursor, 2)
            .await
            .unwrap();
        cursor = step.cursor;
        steps += 1;
        if step.complete {
            break;
        }
        assert!(steps < 16);
    }
    assert!(
        steps >= 6,
        "discovery and application are independently paged"
    );
    let health = reader.branch_index_health("main").await.unwrap();
    assert!(
        health.ready,
        "rebuild must publish the selected snapshot roots"
    );
    assert_eq!(
        reader
            .get_object("main", b"rebuild/4.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"value-4"
    );

    let mut operation = reader.start_operation_index_rebuild(&cursor).await.unwrap();
    let early_cleanup = reader
        .cleanup_branch_index_rebuild(&cursor, &operation, 1)
        .await
        .unwrap_err();
    assert_eq!(
        early_cleanup.code,
        prolly_s3_core::ErrorCode::InvalidContinuationToken
    );
    loop {
        let encoded = prolly_s3_core::encode_canonical(&operation).unwrap();
        operation = prolly_s3_core::decode_canonical(&encoded).unwrap();
        let step = reader
            .advance_operation_index_rebuild(&operation, 2)
            .await
            .unwrap();
        operation = step.cursor;
        if step.complete {
            break;
        }
    }

    let mut deleted = 0;
    loop {
        let cleanup = reader
            .cleanup_branch_index_rebuild(&cursor, &operation, 1)
            .await
            .unwrap();
        deleted += cleanup.deleted_objects;
        if cleanup.complete {
            break;
        }
    }
    assert_eq!(deleted, 3);

    drop(reader);
    let replay_writer = Repository::open(
        plane,
        RepositoryOptions {
            operation_index_max_unindexed_events: 2,
            ..options
        },
    )
    .await
    .unwrap();
    let original = original.unwrap();
    let replay = replay_writer
        .put_object_with_operation(
            "main",
            b"rebuild/4.txt".to_vec(),
            b"value-4".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            original.operation,
        )
        .await
        .unwrap();
    assert!(replay.idempotent_replay);
    assert_eq!(replay.id, original.id);
}

#[tokio::test]
async fn repository_ref_lifecycle_uses_event_driven_sharded_catalogs() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(110_000));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: ".tests/repository-ref-catalog".to_string(),
            writer: "writer-a".to_string(),
            clock: clock.clone(),
            ids: Arc::new(SequenceIdSource::new(0xdd, 1)),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let main = repository.head("main").await.unwrap();

    clock.advance(1).unwrap();
    let feature = repository.create_branch("feature", main).await.unwrap();
    assert_eq!(feature.target, main);
    let feature_commit = repository
        .put_object(
            "feature",
            b"feature.txt".to_vec(),
            b"branch-local".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    repository.advance_branch_indexes("feature").await.unwrap();
    assert_eq!(repository.head("main").await.unwrap(), main);

    clock.advance(1).unwrap();
    let tag = repository
        .create_tag("release-1", feature_commit.id)
        .await
        .unwrap();
    assert_eq!(repository.tag("release-1").await.unwrap(), tag);

    plane.reset_request_counts();
    let mut branch_cursor = None;
    let mut branches = Vec::new();
    loop {
        let page = repository
            .list_branch_catalog_page(branch_cursor, 1)
            .await
            .unwrap();
        branches.extend(page.branches.into_iter().map(|branch| branch.name));
        branch_cursor = page.continuation;
        if branch_cursor.is_none() {
            break;
        }
    }
    branches.sort();
    assert_eq!(branches, vec!["feature", "main"]);
    let tags = repository.list_tag_catalog_page(None, 10).await.unwrap();
    assert_eq!(tags.tags, vec![tag.clone()]);
    assert_eq!(
        plane.request_snapshot().list,
        0,
        "steady-state catalog listing must not scan the ref namespace"
    );

    clock.advance(1).unwrap();
    repository
        .delete_tag("release-1", feature_commit.id)
        .await
        .unwrap();
    repository
        .delete_branch("feature", feature_commit.id)
        .await
        .unwrap();
    assert!(repository
        .list_tag_catalog_page(None, 10)
        .await
        .unwrap()
        .tags
        .is_empty());
    assert_eq!(
        repository
            .list_branch_catalog_page(None, 10)
            .await
            .unwrap()
            .branches
            .into_iter()
            .map(|branch| branch.name)
            .collect::<Vec<_>>(),
        vec!["main"]
    );

    plane.reset_request_counts();
    let repair = repository
        .repair_ref_catalog_page(prolly_s3_core::RefKind::Branch, None, 100)
        .await
        .unwrap();
    assert_eq!(repair.scanned, 2);
    assert_eq!(repair.indexed, 2);
    assert!(
        plane.request_snapshot().list > 0,
        "namespace listing is reserved for explicit repair"
    );
}
