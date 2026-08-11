use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use prolly_s3_core::{
    ErrorCode, FixedClock, ImmutablePut, ListRequest, MemoryObjectPlane, ObjectHeaders, ObjectPath,
    ObjectPlane, Repository, RepositoryOptions,
};
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
