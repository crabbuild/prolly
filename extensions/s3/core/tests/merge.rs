use std::{collections::BTreeMap, sync::Arc};

use prolly_s3_core::{
    decode_canonical, encode_canonical, FixedClock, LogicalObjectVersionKind, MemoryObjectPlane,
    MergeCursor, MergePhase, MergePolicy, ObjectHeaders, ProviderPerKeyVersionLimit, Repository,
    RepositoryOptions, SequenceIdSource,
};

fn options(clock: Arc<FixedClock>) -> RepositoryOptions {
    RepositoryOptions {
        repository_prefix: ".tests/repository-merge".to_string(),
        writer: "merge-writer".to_string(),
        clock,
        ids: Arc::new(SequenceIdSource::new(0x44, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    }
}

async fn put(repository: &Repository<MemoryObjectPlane>, branch: &str, key: &str, value: &str) {
    repository
        .put_object(
            branch,
            key.as_bytes().to_vec(),
            value.as_bytes().to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn repository_merge_is_structural_paged_restartable_and_replayable() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(10_000));
    let options = options(clock.clone());
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();

    clock.advance(1).unwrap();
    put(&repository, "main", "conflict.txt", "base").await;
    let base = repository.head("main").await.unwrap();
    repository.create_branch("feature", base).await.unwrap();

    clock.advance(1).unwrap();
    put(&repository, "main", "conflict.txt", "ours").await;
    put(&repository, "main", "ours-only.txt", "ours-only").await;
    clock.advance(1).unwrap();
    put(&repository, "feature", "conflict.txt", "theirs").await;
    put(&repository, "feature", "theirs-only.txt", "theirs-only").await;
    repository.advance_branch_indexes("main").await.unwrap();
    repository.advance_branch_indexes("feature").await.unwrap();

    let mut cursor = repository
        .start_merge(
            "main",
            "feature",
            None,
            MergePolicy::Theirs,
            "merge feature",
        )
        .await
        .unwrap();
    let mut tampered = cursor.clone();
    tampered.planned_changes = 1;
    assert_eq!(
        repository
            .advance_merge(&tampered, 1)
            .await
            .unwrap_err()
            .code,
        prolly_s3_core::ErrorCode::InvalidContinuationToken
    );
    drop(repository);

    let mut total_processed = 0usize;
    while cursor.phase != MergePhase::ReadyToPublish {
        let reopened = Repository::open(plane.clone(), options.clone())
            .await
            .unwrap();
        let persisted = encode_canonical(&cursor).unwrap();
        let restored: MergeCursor = decode_canonical(&persisted).unwrap();
        let page = reopened.advance_merge(&restored, 2).await.unwrap();
        assert!(page.processed <= 2);
        total_processed += page.processed;
        cursor = page.cursor;
        drop(reopened);
    }
    assert!(total_processed > 0);
    assert_eq!(cursor.conflicts, 1);
    assert_eq!(cursor.planned_changes, 2);
    assert_eq!(cursor.built_changes, 2);

    let reopened = Repository::open(plane.clone(), options.clone())
        .await
        .unwrap();
    let conflict_page = reopened
        .merge_conflicts_page(&cursor, None, 1)
        .await
        .unwrap();
    assert_eq!(conflict_page.conflicts.len(), 1);
    assert_eq!(conflict_page.conflicts[0].key, b"conflict.txt");
    let change_page = reopened.merge_changes_page(&cursor, None, 1).await.unwrap();
    assert_eq!(change_page.changes.len(), 1);
    assert!(change_page.continuation.is_some());
    let second_page = reopened
        .merge_changes_page(&cursor, change_page.continuation.as_ref(), 1)
        .await
        .unwrap();
    assert_eq!(second_page.changes.len(), 1);

    let receipt = reopened.publish_merge(&cursor).await.unwrap();
    assert_eq!(receipt.changed_keys, 2);
    assert_eq!(receipt.conflicts, 1);
    assert_eq!(receipt.parents, [cursor.ours, cursor.theirs]);
    reopened.advance_branch_indexes("main").await.unwrap();
    let replay = reopened.publish_merge(&cursor).await.unwrap();
    assert_eq!(replay.id, receipt.id);
    assert!(replay.idempotent_replay);
    let mut cleanup_cursor = None;
    let mut deleted_plan_nodes = 0usize;
    loop {
        let page = reopened
            .cleanup_merge(&cursor, cleanup_cursor.as_ref(), 1)
            .await
            .unwrap();
        deleted_plan_nodes += page.deleted;
        cleanup_cursor = page.continuation;
        if cleanup_cursor.is_none() {
            break;
        }
    }
    assert!(deleted_plan_nodes > 0);
    drop(reopened);

    let reader = Repository::open(
        plane,
        RepositoryOptions {
            read_only: true,
            ..options
        },
    )
    .await
    .unwrap();
    assert_eq!(
        reader
            .get_object("main", b"conflict.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"theirs"
    );
    assert_eq!(
        reader
            .get_object("main", b"theirs-only.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"theirs-only"
    );
    assert_eq!(
        reader
            .get_object("main", b"ours-only.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"ours-only"
    );
}

#[tokio::test]
async fn repository_fail_policy_persists_conflicts_without_publication() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(30_000));
    let options = RepositoryOptions {
        repository_prefix: ".tests/repository-merge-fail".to_string(),
        ..options(clock.clone())
    };
    let repository = Repository::initialize(plane, options).await.unwrap();
    clock.advance(1).unwrap();
    put(&repository, "main", "same.txt", "base").await;
    let base = repository.head("main").await.unwrap();
    repository.create_branch("feature", base).await.unwrap();
    put(&repository, "main", "same.txt", "ours").await;
    put(&repository, "feature", "same.txt", "theirs").await;
    repository.advance_branch_indexes("main").await.unwrap();
    repository.advance_branch_indexes("feature").await.unwrap();
    let mut cursor = repository
        .start_merge(
            "main",
            "feature",
            None,
            MergePolicy::Fail,
            "detect conflict",
        )
        .await
        .unwrap();
    while !matches!(cursor.phase, MergePhase::Conflicted) {
        cursor = repository.advance_merge(&cursor, 1).await.unwrap().cursor;
    }
    assert_eq!(cursor.conflicts, 1);
    assert_eq!(
        repository.publish_merge(&cursor).await.unwrap_err().code,
        prolly_s3_core::ErrorCode::PreconditionFailed
    );
}

#[tokio::test]
async fn repository_merge_materializes_a_source_delete_and_preserves_history() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(40_000));
    let options = RepositoryOptions {
        repository_prefix: ".tests/repository-merge-delete".to_string(),
        ..options(clock.clone())
    };
    let repository = Repository::initialize(plane, options).await.unwrap();
    put(&repository, "main", "removed.txt", "base").await;
    let base = repository.head("main").await.unwrap();
    repository.create_branch("feature", base).await.unwrap();
    repository
        .delete_object("feature", b"removed.txt".to_vec())
        .await
        .unwrap();

    let mut cursor = repository
        .start_merge(
            "main",
            "feature",
            None,
            MergePolicy::Fail,
            "accept source delete",
        )
        .await
        .unwrap();
    while cursor.phase != MergePhase::ReadyToPublish {
        cursor = repository.advance_merge(&cursor, 2).await.unwrap().cursor;
    }
    let receipt = repository.publish_merge(&cursor).await.unwrap();
    assert_eq!(receipt.changed_keys, 1);
    repository.advance_branch_indexes("main").await.unwrap();
    assert!(repository
        .get_object("main", b"removed.txt")
        .await
        .unwrap()
        .is_none());
    let (_, versions) = repository
        .list_object_versions("main", b"removed.txt", 10)
        .await
        .unwrap();
    assert_eq!(versions.len(), 3);
    assert!(matches!(
        versions[0].body.kind,
        LogicalObjectVersionKind::DeleteMarker
    ));
    assert!(matches!(
        versions[1].body.kind,
        LogicalObjectVersionKind::DeleteMarker
    ));
}

#[tokio::test]
async fn repository_merge_refuses_to_publish_after_the_target_branch_moves() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(45_000));
    let options = RepositoryOptions {
        repository_prefix: ".tests/repository-merge-ref-move".to_string(),
        ..options(clock.clone())
    };
    let repository = Repository::initialize(plane, options).await.unwrap();
    let base = repository.head("main").await.unwrap();
    repository.create_branch("feature", base).await.unwrap();
    put(&repository, "feature", "source.txt", "source").await;

    let mut cursor = repository
        .start_merge(
            "main",
            "feature",
            None,
            MergePolicy::Fail,
            "plan against stable target",
        )
        .await
        .unwrap();
    while cursor.phase != MergePhase::ReadyToPublish {
        cursor = repository.advance_merge(&cursor, 2).await.unwrap().cursor;
    }
    put(&repository, "main", "concurrent.txt", "winner").await;
    let moved = repository.head("main").await.unwrap();

    let error = repository.publish_merge(&cursor).await.unwrap_err();
    assert_eq!(error.code, prolly_s3_core::ErrorCode::RefConflict);
    assert_eq!(repository.head("main").await.unwrap(), moved);
    assert!(repository.fenced_branches().unwrap().is_empty());
    assert!(repository
        .get_object("main", b"source.txt")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore = "10K sparse-merge scale gate"]
async fn repository_sparse_merge_prunes_unchanged_10k_snapshot() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(50_000));
    let options = RepositoryOptions {
        repository_prefix: ".tests/repository-sparse-merge-10k".to_string(),
        ..options(clock.clone())
    };
    let repository = Repository::initialize(plane, options).await.unwrap();
    let session = repository
        .begin_commit_session("main", "10K baseline", 60_000)
        .await
        .unwrap();
    let mut mutations = Vec::with_capacity(10_000);
    for index in 0..10_000usize {
        mutations.push(
            repository
                .stage_commit_session_put(
                    &session,
                    format!("objects/{index:05}.txt").into_bytes(),
                    format!("base-{index:05}").into_bytes(),
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                )
                .await
                .unwrap(),
        );
    }
    let baseline = repository
        .publish_commit_session(session, mutations)
        .await
        .unwrap();
    repository
        .create_branch("feature", baseline.id)
        .await
        .unwrap();
    put(&repository, "main", "objects/00001.txt", "changed-on-main").await;
    put(
        &repository,
        "feature",
        "objects/09998.txt",
        "changed-on-feature",
    )
    .await;
    let mut cursor = repository
        .start_merge(
            "main",
            "feature",
            None,
            MergePolicy::Fail,
            "sparse 10K merge",
        )
        .await
        .unwrap();
    let mut processed = 0usize;
    while cursor.phase != MergePhase::ReadyToPublish {
        let page = repository.advance_merge(&cursor, 8).await.unwrap();
        processed += page.processed;
        cursor = page.cursor;
    }
    assert_eq!(cursor.planned_changes, 1);
    assert_eq!(cursor.conflicts, 0);
    assert!(
        processed < 32,
        "sparse structural merge processed {processed} logical records"
    );
    repository.publish_merge(&cursor).await.unwrap();
    assert_eq!(
        repository
            .get_object("main", b"objects/09998.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"changed-on-feature"
    );
}

#[tokio::test]
#[ignore = "4K commit-graph skip-pointer gate"]
async fn repository_merge_base_skips_deep_first_parent_history() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(70_000));
    let options = RepositoryOptions {
        repository_prefix: ".tests/repository-deep-merge-base".to_string(),
        journal_index_max_unindexed_events: 8_192,
        operation_index_max_unindexed_events: 8_192,
        ..options(clock.clone())
    };
    let repository = Repository::initialize(plane, options).await.unwrap();
    put(&repository, "main", "counter.txt", "0").await;
    let base = repository.head("main").await.unwrap();
    repository.create_branch("stale", base).await.unwrap();
    for generation in 1..=4_096usize {
        put(&repository, "main", "counter.txt", &generation.to_string()).await;
    }
    repository.advance_branch_indexes("main").await.unwrap();
    let cursor = repository
        .start_merge(
            "stale",
            "main",
            None,
            MergePolicy::Theirs,
            "fast-forward-shaped merge",
        )
        .await
        .unwrap();
    assert_eq!(cursor.phase, MergePhase::Planning);
    assert_eq!(cursor.selected_base, Some(base));
    assert_eq!(cursor.visited_commits, 0);
}
