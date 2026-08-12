use std::{collections::BTreeMap, sync::Arc};

use prolly_s3_core::{
    decode_canonical, encode_canonical, FixedClock, LogicalObjectVersionKindV1, MemoryObjectPlane,
    MergeCursorV2, MergePhaseV2, MergePolicy, MergePolicyV2, ObjectHeaders,
    ProviderPerKeyVersionLimitV2, Repository, RepositoryOptions, RepositoryV2, RepositoryV2Options,
    SequenceIdSource,
};

fn options(clock: Arc<FixedClock>) -> RepositoryV2Options {
    RepositoryV2Options {
        repository_prefix: ".tests/native-v2-merge".to_string(),
        writer: "merge-writer".to_string(),
        clock,
        ids: Arc::new(SequenceIdSource::new(0x44, 1)),
        provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
        ..RepositoryV2Options::default()
    }
}

async fn put(repository: &RepositoryV2<MemoryObjectPlane>, branch: &str, key: &str, value: &str) {
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
async fn native_v2_merge_is_structural_paged_restartable_and_replayable() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(10_000));
    let options = options(clock.clone());
    let repository = RepositoryV2::initialize(plane.clone(), options.clone())
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
            MergePolicyV2::Theirs,
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
    while cursor.phase != MergePhaseV2::ReadyToPublish {
        let reopened = RepositoryV2::open(plane.clone(), options.clone())
            .await
            .unwrap();
        let persisted = encode_canonical(&cursor).unwrap();
        let restored: MergeCursorV2 = decode_canonical(&persisted).unwrap();
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

    let reopened = RepositoryV2::open(plane.clone(), options.clone())
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

    let reader = RepositoryV2::open(
        plane,
        RepositoryV2Options {
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
async fn native_v2_fail_policy_persists_conflicts_without_publication() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(30_000));
    let options = RepositoryV2Options {
        repository_prefix: ".tests/native-v2-merge-fail".to_string(),
        ..options(clock.clone())
    };
    let repository = RepositoryV2::initialize(plane, options).await.unwrap();
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
            MergePolicyV2::Fail,
            "detect conflict",
        )
        .await
        .unwrap();
    while !matches!(cursor.phase, MergePhaseV2::Conflicted) {
        cursor = repository.advance_merge(&cursor, 1).await.unwrap().cursor;
    }
    assert_eq!(cursor.conflicts, 1);
    assert_eq!(
        repository.publish_merge(&cursor).await.unwrap_err().code,
        prolly_s3_core::ErrorCode::PreconditionFailed
    );
}

#[tokio::test]
async fn native_v2_merge_materializes_a_source_delete_and_preserves_history() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(40_000));
    let options = RepositoryV2Options {
        repository_prefix: ".tests/native-v2-merge-delete".to_string(),
        ..options(clock.clone())
    };
    let repository = RepositoryV2::initialize(plane, options).await.unwrap();
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
            MergePolicyV2::Fail,
            "accept source delete",
        )
        .await
        .unwrap();
    while cursor.phase != MergePhaseV2::ReadyToPublish {
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
        LogicalObjectVersionKindV1::DeleteMarker
    ));
    assert!(matches!(
        versions[1].body.kind,
        LogicalObjectVersionKindV1::DeleteMarker
    ));
}

#[tokio::test]
async fn native_v2_merge_refuses_to_publish_after_the_target_branch_moves() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(45_000));
    let options = RepositoryV2Options {
        repository_prefix: ".tests/native-v2-merge-ref-move".to_string(),
        ..options(clock.clone())
    };
    let repository = RepositoryV2::initialize(plane, options).await.unwrap();
    let base = repository.head("main").await.unwrap();
    repository.create_branch("feature", base).await.unwrap();
    put(&repository, "feature", "source.txt", "source").await;

    let mut cursor = repository
        .start_merge(
            "main",
            "feature",
            None,
            MergePolicyV2::Fail,
            "plan against stable target",
        )
        .await
        .unwrap();
    while cursor.phase != MergePhaseV2::ReadyToPublish {
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
async fn native_v2_criss_cross_frontier_returns_every_best_base_in_pages() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(
        source_plane,
        RepositoryOptions {
            repository_prefix: ".tests/native-v2-criss-cross-source".to_string(),
            writer: "source-writer".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let base = source
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
    source.create_branch("left", base.id).await.unwrap();
    source.create_branch("right", base.id).await.unwrap();
    let left_one = source
        .put_bytes(
            "left",
            b"left.txt".to_vec(),
            b"left".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let right_one = source
        .put_bytes(
            "right",
            b"right.txt".to_vec(),
            b"right".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let left_two = source
        .merge(
            "left",
            right_one.id,
            None,
            MergePolicy::Fail,
            None,
            Some("left merges right-one".to_string()),
        )
        .await
        .unwrap();
    let right_two = source
        .merge(
            "right",
            left_one.id,
            None,
            MergePolicy::Fail,
            None,
            Some("right merges left-one".to_string()),
        )
        .await
        .unwrap();
    source.create_branch("all", left_two.id).await.unwrap();
    source
        .merge(
            "all",
            right_two.id,
            Some(left_one.id),
            MergePolicy::Fail,
            None,
            Some("retain both criss-cross heads".to_string()),
        )
        .await
        .unwrap();

    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    let destination_options = RepositoryV2Options {
        repository_prefix: ".tests/native-v2-criss-cross-destination".to_string(),
        writer: "destination-writer".to_string(),
        provider_per_key_version_limit: ProviderPerKeyVersionLimitV2::Finite(10_000),
        ..RepositoryV2Options::default()
    };
    let destination = RepositoryV2::initialize(destination_plane, destination_options)
        .await
        .unwrap();
    let mut migration = source
        .start_v1_to_v2_migration(&destination, "all", "imported-all")
        .await
        .unwrap();
    loop {
        let page = source
            .v1_to_v2_migration_page(&destination, &migration, 100, 2)
            .await
            .unwrap();
        migration = page.cursor;
        if page.complete {
            break;
        }
    }
    let mapped_left_two = source
        .v1_to_v2_migration_mapping(&migration, left_two.id)
        .await
        .unwrap()
        .unwrap();
    let mapped_right_two = source
        .v1_to_v2_migration_mapping(&migration, right_two.id)
        .await
        .unwrap()
        .unwrap();
    let mapped_left_one = source
        .v1_to_v2_migration_mapping(&migration, left_one.id)
        .await
        .unwrap()
        .unwrap();
    let mapped_right_one = source
        .v1_to_v2_migration_mapping(&migration, right_one.id)
        .await
        .unwrap()
        .unwrap();
    destination
        .create_branch("left-v2", mapped_left_two)
        .await
        .unwrap();
    destination
        .create_branch("right-v2", mapped_right_two)
        .await
        .unwrap();
    // Keep the imported closure registered: both criss-cross heads reuse
    // ancestor packs that are indexed by the imported branch.
    destination.head("imported-all").await.unwrap();
    let mut cursor = destination
        .start_merge(
            "left-v2",
            "right-v2",
            None,
            MergePolicyV2::Ours,
            "criss-cross merge",
        )
        .await
        .unwrap();
    while cursor.phase != MergePhaseV2::AwaitingBase {
        cursor = destination.advance_merge(&cursor, 1).await.unwrap().cursor;
    }
    assert_eq!(cursor.best_base_count, 2);
    let first = destination
        .merge_bases_page(&cursor, None, 1)
        .await
        .unwrap();
    assert_eq!(first.bases.len(), 1);
    let second = destination
        .merge_bases_page(&cursor, first.continuation.as_ref(), 1)
        .await
        .unwrap();
    assert_eq!(second.bases.len(), 1);
    let discovered = [first.bases[0], second.bases[0]];
    assert!(discovered.contains(&mapped_left_one));
    assert!(discovered.contains(&mapped_right_one));
    let selected = destination
        .select_merge_base(&cursor, mapped_left_one)
        .await
        .unwrap();
    assert_eq!(selected.phase, MergePhaseV2::Planning);
    assert_eq!(selected.selected_base, Some(mapped_left_one));
}

#[tokio::test]
#[ignore = "10K sparse-merge scale gate"]
async fn native_v2_sparse_merge_prunes_unchanged_10k_snapshot() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(50_000));
    let options = RepositoryV2Options {
        repository_prefix: ".tests/native-v2-sparse-merge-10k".to_string(),
        ..options(clock.clone())
    };
    let repository = RepositoryV2::initialize(plane, options).await.unwrap();
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
            MergePolicyV2::Fail,
            "sparse 10K merge",
        )
        .await
        .unwrap();
    let mut processed = 0usize;
    while cursor.phase != MergePhaseV2::ReadyToPublish {
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
async fn native_v2_merge_base_skips_deep_first_parent_history() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let clock = Arc::new(FixedClock::new(70_000));
    let options = RepositoryV2Options {
        repository_prefix: ".tests/native-v2-deep-merge-base".to_string(),
        journal_index_max_unindexed_events: 8_192,
        operation_index_max_unindexed_events: 8_192,
        ..options(clock.clone())
    };
    let repository = RepositoryV2::initialize(plane, options).await.unwrap();
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
            MergePolicyV2::Theirs,
            "fast-forward-shaped merge",
        )
        .await
        .unwrap();
    assert_eq!(cursor.phase, MergePhaseV2::Planning);
    assert_eq!(cursor.selected_base, Some(base));
    assert_eq!(cursor.visited_commits, 0);
}
