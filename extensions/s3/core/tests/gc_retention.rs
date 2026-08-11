use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use prolly_s3_core::{
    CommitGeneration, CommitObjectV1, ErrorCode, FixedClock, GcEpochPhaseV2, GetRequest,
    ImmutablePut, ListRequest, MemoryObjectPlane, ObjectHeaders, ObjectPath, ObjectPlane,
    Repository, RepositoryOptions,
};

#[tokio::test]
async fn partitioned_gc_v2_is_bounded_restartable_and_publication_fenced() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: "gc-v2-partitioned".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    repository.advance_node_index_v2(1_000).await.unwrap();
    let orphan_path = ObjectPath::new(format!(
        "gc-v2-partitioned/commits/sha256/{}/{}/{}",
        "ab",
        "cd",
        "ef".repeat(32)
    ))
    .unwrap();
    let orphan = b"unreachable immutable envelope".to_vec();
    plane
        .put_immutable(ImmutablePut {
            path: orphan_path.clone(),
            expected_sha256: Sha256::digest(&orphan).into(),
            bytes: orphan,
        })
        .await
        .unwrap();
    let epoch = repository
        .start_gc_epoch_v2(2 * 60 * 60 * 1_000)
        .await
        .unwrap();
    let mut current = epoch;
    for _ in 0..100 {
        if matches!(current.phase, GcEpochPhaseV2::Ready) {
            break;
        }
        current = repository
            .advance_gc_epoch_v2(current.id, 2)
            .await
            .unwrap()
            .epoch;
    }
    assert!(matches!(current.phase, GcEpochPhaseV2::Ready));
    assert!(current.candidates >= 1);

    // Any intervening ref publication makes the first sweep call restart root
    // discovery without deleting a candidate.
    let main = repository.head("main").await.unwrap();
    repository.create_tag("gc-fence", main).await.unwrap();
    let restarted = repository.sweep_gc_epoch_v2(current.id, 1).await.unwrap();
    assert!(restarted.restarted_for_new_roots);
    assert_eq!(restarted.processed, 0);
    assert!(matches!(
        restarted.epoch.phase,
        GcEpochPhaseV2::DiscoverRoots
    ));

    current = restarted.epoch;
    for _ in 0..100 {
        if matches!(current.phase, GcEpochPhaseV2::Ready) {
            break;
        }
        current = repository
            .advance_gc_epoch_v2(current.id, 2)
            .await
            .unwrap()
            .epoch;
    }
    assert!(matches!(current.phase, GcEpochPhaseV2::Ready));
    for _ in 0..100 {
        if matches!(current.phase, GcEpochPhaseV2::Completed) {
            break;
        }
        current = repository
            .sweep_gc_epoch_v2(current.id, 1)
            .await
            .unwrap()
            .epoch;
    }
    assert!(matches!(current.phase, GcEpochPhaseV2::Completed));
    assert!(current.deleted_versions >= 1);
    assert!(plane.head(&orphan_path).await.unwrap().is_none());
}

#[tokio::test]
async fn partitioned_gc_retains_an_orphan_envelope_that_supplies_a_shared_live_node() {
    let clock = Arc::new(FixedClock::new(10_000_000));
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = RepositoryOptions {
        repository_prefix: "gc-v2-shared-node".to_string(),
        clock: clock.clone(),
        reflog_retention_millis: 1,
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    let main = repository
        .put_bytes(
            "main",
            b"shared.txt".to_vec(),
            b"live".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let main_encoded = hex::encode(main.as_bytes());
    let main_path = ObjectPath::new(format!(
        "gc-v2-shared-node/commits/sha256/{}/{}/{}",
        &main_encoded[..2],
        &main_encoded[2..4],
        main_encoded
    ))
    .unwrap();
    let source = plane
        .get(GetRequest {
            path: main_path,
            range: None,
            physical_version: None,
        })
        .await
        .unwrap()
        .unwrap();
    let source = CommitObjectV1::decode_object(&source.bytes).unwrap();
    let pack = source.node_pack.clone().unwrap();
    let initial = source.commit.parents[0];
    let mut thin_commit = source.commit.clone();
    thin_commit.parents = vec![initial];
    thin_commit.node_pack = None;
    thin_commit.created_at_millis += 50_000;
    thin_commit.message = Some("reachable state with external node containers".to_string());
    let thin_object = CommitObjectV1::new(thin_commit, None).unwrap();
    let thin = thin_object.commit.id().unwrap();
    let thin_encoded = hex::encode(thin.as_bytes());
    let thin_path = ObjectPath::new(format!(
        "gc-v2-shared-node/commits/sha256/{}/{}/{}",
        &thin_encoded[..2],
        &thin_encoded[2..4],
        thin_encoded
    ))
    .unwrap();
    let thin_bytes = thin_object.encode_object().unwrap();
    plane
        .put_immutable(ImmutablePut {
            path: thin_path,
            expected_sha256: Sha256::digest(&thin_bytes).into(),
            bytes: thin_bytes,
        })
        .await
        .unwrap();
    repository
        .create_branch("live-no-pack", thin)
        .await
        .unwrap();
    repository.delete_branch("main", main).await.unwrap();
    let (orphan, orphan_object) = (0..10_000u64)
        .find_map(|nonce| {
            let mut commit = source.commit.clone();
            commit.parents = vec![initial];
            commit.generation = CommitGeneration(source.commit.generation.0);
            commit.created_at_millis += nonce + 1;
            commit.message = Some(format!("unreachable duplicate node container {nonce}"));
            let object = CommitObjectV1::new(commit, Some(pack.clone())).ok()?;
            let id = object.commit.id().ok()?;
            (id.as_bytes() > main.as_bytes()).then_some((id, object))
        })
        .expect("a later lexicographic commit ID is easy to find");
    let encoded = hex::encode(orphan.as_bytes());
    let orphan_path = ObjectPath::new(format!(
        "gc-v2-shared-node/commits/sha256/{}/{}/{}",
        &encoded[..2],
        &encoded[2..4],
        encoded
    ))
    .unwrap();
    let orphan_bytes = orphan_object.encode_object().unwrap();
    plane
        .put_immutable(ImmutablePut {
            path: orphan_path.clone(),
            expected_sha256: Sha256::digest(&orphan_bytes).into(),
            bytes: orphan_bytes,
        })
        .await
        .unwrap();
    clock.advance(10).unwrap();
    repository.advance_node_index_v2(1_000).await.unwrap();
    let repository = Repository::open(plane.clone(), options).await.unwrap();

    let mut epoch = repository
        .start_gc_epoch_v2(2 * 60 * 60 * 1_000)
        .await
        .unwrap();
    for _ in 0..200 {
        if matches!(epoch.phase, GcEpochPhaseV2::Ready) {
            break;
        }
        epoch = repository
            .advance_gc_epoch_v2(epoch.id, 2)
            .await
            .unwrap()
            .epoch;
    }
    assert!(matches!(epoch.phase, GcEpochPhaseV2::Ready));
    assert!(epoch.marked_nodes > 0);
    for _ in 0..200 {
        if matches!(epoch.phase, GcEpochPhaseV2::Completed) {
            break;
        }
        epoch = repository
            .sweep_gc_epoch_v2(epoch.id, 2)
            .await
            .unwrap()
            .epoch;
    }
    assert!(matches!(epoch.phase, GcEpochPhaseV2::Completed));
    assert!(plane.head(&orphan_path).await.unwrap().is_some());
    assert_eq!(
        repository
            .get_current("live-no-pack", b"shared.txt")
            .await
            .unwrap()
            .bytes,
        b"live"
    );
}
use sha2::{Digest, Sha256};

#[tokio::test]
async fn pins_are_explicit_gc_roots_after_reflog_retention_expires() {
    let clock = Arc::new(FixedClock::new(10_000_000));
    let options = RepositoryOptions {
        repository_prefix: "gc-pins".to_string(),
        clock: clock.clone(),
        reflog_retention_millis: 1,
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(Arc::new(MemoryObjectPlane::new(true)), options)
        .await
        .unwrap();
    let root = repository.head("main").await.unwrap();
    repository.create_branch("scratch", root).await.unwrap();
    let orphan = repository
        .put_bytes(
            "scratch",
            b"valuable".to_vec(),
            b"keep me".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    repository
        .create_retention_pin("legal-hold", orphan, "compliance", "case-42", None)
        .await
        .unwrap();
    repository.delete_branch("scratch", orphan).await.unwrap();
    clock.advance(10).unwrap();

    let pinned = repository
        .plan_gc(2 * 60 * 60 * 1_000, 10_000)
        .await
        .unwrap();
    let orphan_commit_suffix = hex::encode(orphan.as_bytes());
    assert!(!pinned
        .plan
        .body
        .candidates
        .iter()
        .any(|candidate| candidate.path.as_str().ends_with(&orphan_commit_suffix)));
    assert_eq!(repository.list_retention_pins().await.unwrap().len(), 1);

    repository
        .delete_retention_pin("legal-hold", orphan)
        .await
        .unwrap();
    let unpinned = repository
        .plan_gc(2 * 60 * 60 * 1_000, 10_000)
        .await
        .unwrap();
    assert!(unpinned
        .plan
        .body
        .candidates
        .iter()
        .any(|candidate| candidate.path.as_str().ends_with(&orphan_commit_suffix)));
    assert!(repository.list_retention_pins().await.unwrap().is_empty());
}

#[tokio::test]
async fn gc_sweep_checkpoints_each_bounded_batch_and_reports_kinds() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: "gc-resume".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    for ordinal in 0..3 {
        let bytes = format!("orphan-{ordinal}").into_bytes();
        plane
            .put_immutable(ImmutablePut {
                path: ObjectPath::new(format!(
                    "gc-resume/commits/sha256/{ordinal:02}/{ordinal:02}/{}",
                    format!("{ordinal:02}").repeat(32)
                ))
                .unwrap(),
                expected_sha256: Sha256::digest(&bytes).into(),
                bytes,
            })
            .await
            .unwrap();
    }
    let dry_run = repository.plan_gc(2 * 60 * 60 * 1_000, 100).await.unwrap();
    assert_eq!(dry_run.candidates_by_kind.get("commits"), Some(&3));

    let first = repository.sweep_gc_batch(dry_run.plan.id, 1).await.unwrap();
    assert_eq!(first.next_index, 1);
    assert!(!first.complete);
    let second = repository.sweep_gc_batch(dry_run.plan.id, 1).await.unwrap();
    assert_eq!(second.next_index, 2);
    assert!(!second.complete);
    let final_report = repository.sweep_gc_batch(dry_run.plan.id, 1).await.unwrap();
    assert!(final_report.complete);
    assert_eq!(final_report.next_index, 3);
    assert_eq!(final_report.deleted_versions, 3);
    assert_eq!(final_report.deleted_by_kind.get("commits"), Some(&3));
    assert_eq!(
        repository.gc_run(dry_run.plan.id).await.unwrap().next_index,
        3
    );
    assert_eq!(
        repository.sweep_gc_batch(dry_run.plan.id, 1).await.unwrap(),
        final_report
    );
}

#[tokio::test]
async fn gc_delete_rate_is_bound_to_the_run_and_paces_exact_deletes() {
    let invalid = Repository::initialize(
        Arc::new(MemoryObjectPlane::new(true)),
        RepositoryOptions {
            repository_prefix: "gc-invalid-rate".to_string(),
            gc_delete_rate_limit_per_second: 1_001,
            ..RepositoryOptions::default()
        },
    )
    .await
    .err()
    .unwrap();
    assert_eq!(invalid.code, ErrorCode::InvalidLimit);

    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: "gc-rate".to_string(),
            gc_delete_rate_limit_per_second: 20,
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    for ordinal in 0..3 {
        let bytes = format!("paced-orphan-{ordinal}").into_bytes();
        plane
            .put_immutable(ImmutablePut {
                path: ObjectPath::new(format!(
                    "gc-rate/commits/sha256/{ordinal:02}/{ordinal:02}/{}",
                    format!("{ordinal:02}").repeat(32)
                ))
                .unwrap(),
                expected_sha256: Sha256::digest(&bytes).into(),
                bytes,
            })
            .await
            .unwrap();
    }
    let plan = repository.plan_gc(2 * 60 * 60 * 1_000, 100).await.unwrap();
    let started = Instant::now();
    let report = repository.sweep_gc_batch(plan.plan.id, 3).await.unwrap();
    let elapsed = started.elapsed();
    assert!(report.complete);
    assert_eq!(report.deleted_versions, 3);
    assert!(
        elapsed >= Duration::from_millis(90),
        "three deletes at 20/s completed too quickly: {elapsed:?}"
    );
    let run = repository.gc_run(plan.plan.id).await.unwrap();
    assert_eq!(run.delete_rate_limit_per_second, 20);
    assert!(run.last_delete_at_millis > 0);
}

#[tokio::test]
async fn gc_conservatively_preserves_every_physical_ref_version() {
    let clock = Arc::new(FixedClock::new(10_000_000));
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: "gc-physical-refs".to_string(),
            clock,
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    for ordinal in 0..3 {
        repository
            .put_bytes(
                "main",
                format!("key-{ordinal}").into_bytes(),
                format!("value-{ordinal}").into_bytes(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let ref_prefix = "gc-physical-refs/refs/heads/".to_string();
    let before = plane
        .list(ListRequest {
            prefix: ref_prefix.clone(),
            continuation: None,
            limit: 100,
            include_versions: true,
        })
        .await
        .unwrap()
        .entries;
    assert_eq!(before.len(), 4);

    let orphan_bytes = b"collect me, but not physical ref history".to_vec();
    plane
        .put_immutable(ImmutablePut {
            path: ObjectPath::new(format!(
                "gc-physical-refs/commits/sha256/ff/ff/{}",
                "ff".repeat(32)
            ))
            .unwrap(),
            expected_sha256: Sha256::digest(&orphan_bytes).into(),
            bytes: orphan_bytes,
        })
        .await
        .unwrap();
    let plan = repository.plan_gc(2 * 60 * 60 * 1_000, 100).await.unwrap();
    assert!(plan
        .plan
        .body
        .candidates
        .iter()
        .all(|candidate| !candidate.path.as_str().contains("/refs/")));
    assert_eq!(
        repository
            .sweep_gc(plan.plan.id)
            .await
            .unwrap()
            .deleted_versions,
        1
    );
    let after = plane
        .list(ListRequest {
            prefix: ref_prefix,
            continuation: None,
            limit: 100,
            include_versions: true,
        })
        .await
        .unwrap()
        .entries;
    assert_eq!(after.len(), before.len());
    assert_eq!(
        after
            .iter()
            .map(|entry| entry.metadata.token.version_id.as_deref())
            .collect::<Vec<_>>(),
        before
            .iter()
            .map(|entry| entry.metadata.token.version_id.as_deref())
            .collect::<Vec<_>>()
    );
}
