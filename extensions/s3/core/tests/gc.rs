use std::{collections::BTreeMap, sync::Arc, time::Duration};

use prolly_s3_core::{
    GcPhase, ImmutablePut, MemoryObjectPlane, ObjectHeaders, ObjectPath, ObjectPlane, Repository,
    RepositoryOptions,
};
use sha2::{Digest, Sha256};

#[tokio::test]
async fn gc_fences_cross_handle_publications_and_deletes_exact_orphans() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = RepositoryOptions {
        repository_prefix: ".tests/gc".to_string(),
        provider_per_key_version_limit: prolly_s3_core::ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    let session = repository
        .begin_commit_session("main", "shared live pack", 60_000)
        .await
        .unwrap();
    let staged = repository
        .stage_commit_session_put_batch(
            &session,
            vec![
                (
                    b"packed/a".to_vec(),
                    b"alpha".to_vec(),
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                ),
                (
                    b"packed/b".to_vec(),
                    b"bravo".to_vec(),
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                ),
            ],
            2,
        )
        .await
        .unwrap();
    let packed_commit = repository
        .publish_commit_session(session, staged)
        .await
        .unwrap();
    let live_pack = repository
        .head_object_at("main", packed_commit.id, b"packed/a")
        .await
        .unwrap()
        .unwrap()
        .version
        .binding
        .unwrap()
        .path;
    let initial = repository.head("main").await.unwrap();
    repository.create_branch("scratch", initial).await.unwrap();
    let pinned = repository
        .put_object(
            "scratch",
            b"pinned.txt".to_vec(),
            b"retain me".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap()
        .id;
    repository
        .create_retention_pin("legal-hold", pinned)
        .await
        .unwrap();
    repository.delete_branch("scratch", pinned).await.unwrap();

    let orphan_path = ObjectPath::new(format!(
        ".tests/gc/commits/sha256/ff/ff/{}",
        "ff".repeat(32)
    ))
    .unwrap();
    let orphan = b"unreachable immutable object".to_vec();
    plane
        .put_immutable(ImmutablePut {
            path: orphan_path.clone(),
            expected_sha256: Sha256::digest(&orphan).into(),
            bytes: orphan,
        })
        .await
        .unwrap();
    let orphan_pack_path = ObjectPath::new(format!(
        ".tests/gc/payload-packs/sha256/ee/ee/{}",
        "ee".repeat(32)
    ))
    .unwrap();
    let orphan_pack = b"unreachable immutable pack".to_vec();
    plane
        .put_immutable(ImmutablePut {
            path: orphan_pack_path.clone(),
            expected_sha256: Sha256::digest(&orphan_pack).into(),
            bytes: orphan_pack,
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;

    let external_writer = Repository::open(plane.clone(), options.clone())
        .await
        .unwrap();
    let mut gc = repository.start_gc(1).await.unwrap();
    let during_gc = external_writer
        .put_object(
            "main",
            b"during-gc.txt".to_vec(),
            b"must be fenced".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        during_gc.code,
        prolly_s3_core::ErrorCode::PreconditionFailed
    );
    for _ in 0..10_000 {
        if gc.phase == GcPhase::Ready {
            break;
        }
        gc = repository.advance_gc(&gc, 1).await.unwrap().cursor;
    }
    assert_eq!(gc.phase, GcPhase::Ready);
    assert_eq!(gc.report.dirty_roots, 0);
    assert!(gc.report.candidates >= 1);

    let before_sweep = external_writer
        .put_object(
            "main",
            b"before-sweep.txt".to_vec(),
            b"must also be fenced".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        before_sweep.code,
        prolly_s3_core::ErrorCode::PreconditionFailed
    );
    drop(repository);
    let repository = Repository::open(plane.clone(), options).await.unwrap();
    gc = repository.resume_gc().await.unwrap().unwrap();
    let after_restart_writer = Repository::open(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: ".tests/gc".to_string(),
            provider_per_key_version_limit: prolly_s3_core::ProviderPerKeyVersionLimit::Finite(
                10_000,
            ),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let restart_fence = after_restart_writer
        .put_object(
            "main",
            b"restart-race.txt".to_vec(),
            b"must remain fenced".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        restart_fence.code,
        prolly_s3_core::ErrorCode::PreconditionFailed
    );

    for _ in 0..10_000 {
        gc = match gc.phase {
            GcPhase::Ready | GcPhase::Sweeping => repository.sweep_gc(&gc, 1).await.unwrap().cursor,
            GcPhase::Complete => break,
            _ => repository.advance_gc(&gc, 1).await.unwrap().cursor,
        };
    }
    assert_eq!(gc.phase, GcPhase::Complete);
    assert!(gc.report.deleted_versions >= 1);
    assert!(plane.head(&orphan_path).await.unwrap().is_none());
    assert!(plane.head(&orphan_pack_path).await.unwrap().is_none());
    assert!(plane.head(&live_pack).await.unwrap().is_some());
    assert_eq!(
        repository
            .get_object("main", b"packed/b")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"bravo"
    );
    external_writer
        .put_object(
            "main",
            b"after-gc.txt".to_vec(),
            b"publication admission reopened".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    repository.advance_branch_indexes("main").await.unwrap();
    assert_eq!(
        repository
            .get_object("main", b"after-gc.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"publication admission reopened"
    );
    repository.commit(pinned).await.unwrap();
}
