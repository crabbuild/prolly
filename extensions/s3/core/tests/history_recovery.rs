use std::{collections::BTreeMap, sync::Arc, time::Duration};

use prolly_s3_core::{
    ErrorCode, FixedClock, FsckPhase, ListRequest, MemoryObjectPlane, ObjectHeaders, ObjectPlane,
    ProviderPerKeyVersionLimit, Repository, RepositoryOptions, SequenceIdSource, TraversalBudget,
};

#[tokio::test]
async fn fsck_checkpoint_resumes_across_processes_and_fences_stale_workers() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = RepositoryOptions {
        repository_prefix: ".tests/fsck-durable-resume".to_string(),
        writer: "fsck-writer".to_string(),
        mutable_control_versions_to_retain: 4,
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    for (key, body) in [
        (b"a".to_vec(), b"alpha".to_vec()),
        (b"b".to_vec(), b"bravo".to_vec()),
    ] {
        repository
            .put_object("main", key, body, ObjectHeaders::default(), BTreeMap::new())
            .await
            .unwrap();
    }

    let initial = repository.start_fsck("main", true).await.unwrap();
    assert_eq!(initial.checkpoint_generation, 1);
    let first = repository.advance_fsck(&initial, 1).await.unwrap().cursor;
    assert_eq!(first.checkpoint_generation, 2);

    let reopened = Repository::open(plane.clone(), options.clone())
        .await
        .unwrap();
    assert_eq!(
        reopened.resume_fsck(initial.job).await.unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        reopened.forget_fsck(initial.job).await.unwrap_err().code,
        ErrorCode::PreconditionFailed
    );
    assert_eq!(
        repository.advance_fsck(&initial, 1).await.unwrap_err().code,
        ErrorCode::RefConflict
    );

    assert_eq!(
        reopened.advance_fsck(&first, 1).await.unwrap_err().code,
        ErrorCode::MissingClosure
    );
    reopened.advance_branch_indexes("main").await.unwrap();
    let mut cursor = reopened.advance_fsck(&first, 1).await.unwrap().cursor;
    drop(repository);
    drop(reopened);
    let resumed_process = Repository::open(plane.clone(), options).await.unwrap();
    assert_eq!(
        resumed_process.resume_fsck(initial.job).await.unwrap(),
        Some(cursor.clone())
    );
    while cursor.phase != FsckPhase::Complete {
        cursor = resumed_process
            .advance_fsck(&cursor, 1)
            .await
            .unwrap()
            .cursor;
    }
    assert_eq!(cursor.report.physical_payloads_verified, 2);
    assert_eq!(cursor.report.deep_physical_bytes_read, 10);
    assert_eq!(
        resumed_process.resume_fsck(initial.job).await.unwrap(),
        Some(cursor)
    );
    let checkpoint_prefix = format!(
        ".tests/fsck-durable-resume/administration/fsck/{}/cursor.cbor",
        initial.job
    );
    let retained = plane
        .list(ListRequest {
            prefix: checkpoint_prefix.clone(),
            continuation: None,
            limit: 1_000,
            include_versions: true,
        })
        .await
        .unwrap();
    assert!(retained.entries.len() <= 4, "{retained:?}");
    resumed_process.forget_fsck(initial.job).await.unwrap();
    assert!(resumed_process
        .resume_fsck(initial.job)
        .await
        .unwrap()
        .is_none());
    assert!(plane
        .list(ListRequest {
            prefix: checkpoint_prefix,
            continuation: None,
            limit: 1_000,
            include_versions: true,
        })
        .await
        .unwrap()
        .entries
        .is_empty());
}

#[tokio::test]
async fn history_diff_reflog_reset_and_recovery_are_bounded_and_audited() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(50_000));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: ".tests/history-recovery".to_string(),
            writer: "history-writer".to_string(),
            clock: clock.clone(),
            ids: Arc::new(SequenceIdSource::new(0x91, 1)),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let root = repository.head("main").await.unwrap();

    clock.advance(1).unwrap();
    let first = repository
        .put_object(
            "main",
            b"a.txt".to_vec(),
            b"one".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    clock.advance(1).unwrap();
    let second = repository
        .put_object(
            "main",
            b"b.txt".to_vec(),
            b"two".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();

    let pin = repository
        .create_retention_pin("before-b", first.id)
        .await
        .unwrap();
    assert_eq!(pin.target, first.id);
    assert_eq!(
        repository
            .list_retention_pins_page(None, 100)
            .await
            .unwrap()
            .pins,
        vec![pin]
    );
    let prewarm = repository
        .prewarm_node_cache("main", second.id)
        .await
        .unwrap();
    assert!(prewarm.object_nodes > 0);
    assert!(prewarm.version_nodes > 0);

    let first_page = repository
        .log_page_bounded(
            "main",
            second.id,
            None,
            1,
            TraversalBudget {
                max_commits: 1,
                max_decoded_bytes: 1024 * 1024,
                max_elapsed: Duration::from_secs(1),
            },
        )
        .await
        .unwrap();
    assert_eq!(first_page.commits[0].0, second.id);
    let second_page = repository
        .log_page_bounded(
            "main",
            second.id,
            first_page.continuation.as_ref(),
            2,
            TraversalBudget::default(),
        )
        .await
        .unwrap();
    assert_eq!(second_page.commits[0].0, first.id);
    assert_eq!(second_page.commits[1].0, root);

    let mut closure = repository.start_commit_closure(&[second.id]).await.unwrap();
    let mut closure_ids = Vec::new();
    loop {
        let page = repository
            .commit_closure_page(&closure, 1, 1)
            .await
            .unwrap();
        closure_ids.extend(page.commits.iter().map(|(id, _)| *id));
        closure = page.cursor;
        if page.complete {
            break;
        }
    }
    assert_eq!(closure_ids, vec![root, first.id, second.id]);

    let diff = repository.diff("main", first.id, second.id).await.unwrap();
    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].key, b"b.txt");
    assert!(diff[0].from.is_none());
    assert!(diff[0].to.is_some());

    let before_reset = repository.open_reflog("main").await.unwrap();
    let newest = repository
        .read_reflog_page(&before_reset, 1)
        .await
        .unwrap()
        .entries
        .remove(0);
    assert_eq!(newest.event.new_target, second.id);

    clock.advance(1).unwrap();
    let reset = repository
        .reset_branch("main", first.id, second.id, "undo b.txt")
        .await
        .unwrap();
    assert_eq!(reset.old_target, second.id);
    assert_eq!(repository.head("main").await.unwrap(), first.id);

    let reflog = repository.open_reflog("main").await.unwrap();
    let reset_event = repository
        .read_reflog_page(&reflog, 1)
        .await
        .unwrap()
        .entries
        .remove(0);
    assert_eq!(reset_event.event.old_target, Some(second.id));
    assert_eq!(reset_event.event.new_target, first.id);

    clock.advance(1).unwrap();
    let recovered = repository
        .recover_branch(
            "main",
            reset_event.event.reflog,
            first.id,
            "recover reset target",
        )
        .await
        .unwrap();
    assert_eq!(recovered.new_target, second.id);
    assert_eq!(repository.head("main").await.unwrap(), second.id);

    let mut fsck = repository.start_fsck("main", true).await.unwrap();
    let report = loop {
        let page = repository.advance_fsck(&fsck, 1).await.unwrap();
        fsck = page.cursor;
        if page.complete {
            break fsck.report;
        }
    };
    assert_eq!(report.commits, 3);
    assert_eq!(report.current_objects, 2);
    assert_eq!(report.logical_versions, 2);
    assert_eq!(report.deep_content_bytes_verified, 12);

    plane.reset_request_counts();
    let (_, metadata) = repository
        .head_object("main", b"b.txt")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata.key, b"b.txt");
    assert_eq!(plane.request_snapshot().head, 0);

    let range = repository
        .get_object_range("main", second.id, b"b.txt", 1..=1)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(range.bytes, b"w");

    clock.advance(1).unwrap();
    let copy = repository
        .copy_object("main", second.id, b"a.txt", b"archive/a.txt".to_vec())
        .await
        .unwrap();
    assert_eq!(copy.changed_keys, 1);
    assert_eq!(
        repository
            .get_object("main", b"archive/a.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"one"
    );
    let delimited = repository
        .list_objects_delimited("main", b"", b"/", None, 100)
        .await
        .unwrap();
    assert_eq!(delimited.common_prefixes, vec![b"archive/".to_vec()]);
    let historical_delimited = repository
        .list_objects_delimited_at("main", second.id, b"", b"/", None, 100)
        .await
        .unwrap();
    assert_eq!(historical_delimited.snapshot, second.id);
    assert_eq!(historical_delimited.objects.len(), 2);
    assert!(historical_delimited.common_prefixes.is_empty());

    clock.advance(1).unwrap();
    let deleted = repository
        .delete_objects("main", vec![b"a.txt".to_vec(), b"b.txt".to_vec()])
        .await
        .unwrap();
    assert_eq!(deleted.changed_keys, 2);

    let mut restore = repository
        .start_restore("main", second.id, deleted.id, "restore the second snapshot")
        .await
        .unwrap();
    let mut restored_receipt = None;
    loop {
        let page = repository.advance_restore(&restore, 1).await.unwrap();
        if page.receipt.is_some() {
            restored_receipt = page.receipt;
        }
        restore = page.cursor;
        if page.complete {
            break;
        }
    }
    let restored = restored_receipt.unwrap();
    assert_eq!(restored.parents, vec![deleted.id]);
    assert_eq!(restored.changed_keys, 3);
    assert_eq!(
        repository
            .get_object("main", b"a.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"one"
    );
    assert_eq!(
        repository
            .get_object("main", b"b.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"two"
    );
    assert!(repository
        .get_object("main", b"archive/a.txt")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn logical_repair_rebinds_across_repositories_and_removes_destination_only_keys() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(
        source_plane.clone(),
        RepositoryOptions {
            repository_prefix: ".tests/repair-source".to_string(),
            writer: "source".to_string(),
            ids: Arc::new(SequenceIdSource::new(0xa1, 1)),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let destination_plane = Arc::new(MemoryObjectPlane::new(false));
    let destination = Repository::initialize(
        destination_plane.clone(),
        RepositoryOptions {
            repository_prefix: ".tests/repair-destination".to_string(),
            writer: "destination".to_string(),
            ids: Arc::new(SequenceIdSource::new(0xb2, 1)),
            provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    source
        .put_object(
            "main",
            b"same.txt".to_vec(),
            b"source".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let source_head = source
        .put_object(
            "main",
            b"new.txt".to_vec(),
            b"new".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap()
        .id;
    destination
        .put_object(
            "main",
            b"same.txt".to_vec(),
            b"destination".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let destination_head = destination
        .put_object(
            "main",
            b"extra.txt".to_vec(),
            b"remove".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap()
        .id;

    let mut repair = destination
        .start_repair_from(
            &source,
            "main",
            source_head,
            "main",
            destination_head,
            "repair from source",
        )
        .await
        .unwrap();
    source_plane.reset_request_counts();
    destination_plane.reset_request_counts();
    loop {
        let page = destination
            .advance_repair_from(&source, &repair, 1)
            .await
            .unwrap();
        repair = page.cursor;
        if page.complete {
            break;
        }
    }
    assert_eq!(repair.report.copied_objects, 2);
    assert_eq!(repair.report.deleted_objects, 1);
    assert_eq!(
        destination_plane.request_snapshot().immutable_transfer,
        repair.report.copied_objects,
        "repair must delegate each complete object transfer to the provider boundary"
    );
    assert_eq!(
        destination
            .get_object("main", b"same.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"source"
    );
    assert_eq!(
        destination
            .get_object("main", b"new.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"new"
    );
    assert!(destination
        .get_object("main", b"extra.txt")
        .await
        .unwrap()
        .is_none());

    let mut verification = source
        .start_backup_verification(
            &destination,
            "main",
            source_head,
            "main",
            repair.expected_head,
        )
        .await
        .unwrap();
    loop {
        let page = source
            .advance_backup_verification(&destination, &verification, 1)
            .await
            .unwrap();
        verification = page.cursor;
        if page.complete {
            break;
        }
    }
    assert_eq!(verification.report.objects_verified, 2);
    assert_eq!(verification.report.content_bytes_verified, 9);
}
