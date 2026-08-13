use std::{collections::BTreeMap, sync::Arc, time::Duration};

use prolly_s3_core::{
    decode_canonical, encode_canonical, GcCursor, GcPhase, ImmutablePut, MemoryObjectPlane,
    ObjectHeaders, ObjectPath, ObjectPlane, Repository, RepositoryOptions,
};
use sha2::{Digest, Sha256};

#[tokio::test]
async fn concurrent_gc_retains_dirty_and_pinned_roots_and_deletes_exact_orphans() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let options = RepositoryOptions {
        repository_prefix: ".tests/gc".to_string(),
        provider_per_key_version_limit: prolly_s3_core::ProviderPerKeyVersionLimit::Finite(10_000),
        ..RepositoryOptions::default()
    };
    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
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
    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut gc = repository.start_gc(1).await.unwrap();
    repository
        .put_object(
            "main",
            b"during-gc.txt".to_vec(),
            b"published while marking".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    for _ in 0..10_000 {
        if gc.phase == GcPhase::Ready {
            break;
        }
        gc = repository.advance_gc(&gc, 1).await.unwrap().cursor;
    }
    assert_eq!(gc.phase, GcPhase::Ready);
    assert!(gc.report.dirty_roots >= 1);
    assert!(gc.report.candidates >= 1);

    repository
        .put_object(
            "main",
            b"before-sweep.txt".to_vec(),
            b"forces dirty-root catch-up".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let restarted = repository.sweep_gc(&gc, 1).await.unwrap();
    assert!(restarted.restarted_for_new_roots);
    assert_eq!(restarted.cursor.phase, GcPhase::CatchUpDirtyRoots);
    gc = restarted.cursor;
    let persisted = encode_canonical(&gc).unwrap();
    drop(repository);
    let repository = Repository::open(plane.clone(), options).await.unwrap();
    gc = decode_canonical::<GcCursor>(&persisted).unwrap();

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
    repository.advance_branch_indexes("main").await.unwrap();
    assert_eq!(
        repository
            .get_object("main", b"during-gc.txt")
            .await
            .unwrap()
            .unwrap()
            .bytes,
        b"published while marking"
    );
    repository.commit(pinned).await.unwrap();
}
