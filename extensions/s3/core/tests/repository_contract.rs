use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use prolly::{Config, Tree};
use prolly_s3_core::{
    decode_canonical, encode_canonical, load_protection_segment, load_publication_lease,
    BucketCommitV1, CanonicalLimits, CompareExchange, CompareExchangeOutcome, DeleteOutcome, Error,
    ErrorCode, GetRequest, ImmutablePut, ImmutablePutOutcome, ListRequest, MemoryObjectPlane,
    MergePolicy, ObjectHeaders, ObjectPath, ObjectPlane, ObjectVersionKindV1, OperationId,
    PhysicalListPage, PhysicalVersion, ProtectionSink, PublicationLease, PublicationLeaseStateV1,
    PublicationLeaseV1, Repository, RepositoryFormatV1, RepositoryOptions, Result, StoredMetadata,
    StoredObject, TreeRootV1,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

#[derive(Clone)]
struct FaultPlane {
    inner: MemoryObjectPlane,
    controls: Arc<FaultControls>,
}

#[tokio::test]
async fn merge_conflicts_and_restore_preserve_coherent_bucket_history() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(plane, options("repo-merge-restore"))
        .await
        .unwrap();
    let root = repository.head("main").await.unwrap();
    repository.create_branch("feature", root).await.unwrap();
    repository
        .put_bytes(
            "main",
            b"ours.txt".to_vec(),
            b"ours".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    repository
        .put_bytes(
            "feature",
            b"theirs.txt".to_vec(),
            b"theirs".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let feature = repository.head("feature").await.unwrap();
    let plan = repository
        .plan_merge("main", feature, None, MergePolicy::Fail)
        .await
        .unwrap();
    assert_eq!(plan.best_bases, [root]);
    assert!(plan.conflicts.is_empty());
    assert_eq!(plan.changes.len(), 1);
    let merged = repository
        .merge(
            "main",
            feature,
            None,
            MergePolicy::Fail,
            None,
            Some("merge feature".to_string()),
        )
        .await
        .unwrap();
    assert_eq!(merged.parents.len(), 2);
    assert_eq!(
        repository
            .get_current("main", b"ours.txt")
            .await
            .unwrap()
            .bytes,
        b"ours"
    );
    assert_eq!(
        repository
            .get_current("main", b"theirs.txt")
            .await
            .unwrap()
            .bytes,
        b"theirs"
    );

    repository
        .create_branch("conflict", merged.id)
        .await
        .unwrap();
    repository
        .put_bytes(
            "main",
            b"same.txt".to_vec(),
            b"ours".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    repository
        .put_bytes(
            "conflict",
            b"same.txt".to_vec(),
            b"theirs".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let conflict = repository.head("conflict").await.unwrap();
    let plan = repository
        .plan_merge("main", conflict, None, MergePolicy::Fail)
        .await
        .unwrap();
    assert_eq!(plan.conflicts.len(), 1);
    assert_eq!(
        repository
            .merge("main", conflict, None, MergePolicy::Fail, None, None)
            .await
            .unwrap_err()
            .code,
        ErrorCode::MergeConflict
    );
    let pre_restore = repository.head("main").await.unwrap();
    let resolved = repository
        .merge("main", conflict, None, MergePolicy::Theirs, None, None)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"same.txt")
            .await
            .unwrap()
            .bytes,
        b"theirs"
    );
    let restored = repository
        .restore(
            "main",
            merged.id,
            resolved.id,
            None,
            Some("restore pre-conflict snapshot".to_string()),
        )
        .await
        .unwrap();
    assert_eq!(restored.parents, [resolved.id]);
    assert_eq!(
        repository
            .get_current("main", b"same.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    assert_ne!(restored.id, pre_restore);
    assert!(
        repository
            .list_object_versions("main", b"same.txt", 100)
            .await
            .unwrap()
            .1
            .len()
            >= 3
    );
    repository.fsck().await.unwrap();
}

#[tokio::test]
async fn administrative_reset_and_tombstone_recovery_are_reflog_guarded() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(plane, options("repo-ref-recovery"))
        .await
        .unwrap();
    let first = repository
        .put_bytes(
            "main",
            b"object.txt".to_vec(),
            b"first".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let second = repository
        .put_bytes(
            "main",
            b"object.txt".to_vec(),
            b"second".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let reset = repository
        .reset_branch("main", first.id, second.id, "operator rollback")
        .await
        .unwrap();
    assert_eq!(reset.new_target, first.id);
    assert_eq!(
        repository
            .get_current("main", b"object.txt")
            .await
            .unwrap()
            .bytes,
        b"first"
    );
    let reset_entry = repository
        .list_reflog("main")
        .await
        .unwrap()
        .into_iter()
        .find(|(_, entry)| entry.message == "operator rollback")
        .unwrap();
    let recovered = repository
        .recover_branch("main", reset_entry.0, first.id, "undo mistaken rollback")
        .await
        .unwrap();
    assert_eq!(recovered.new_target, second.id);
    assert_eq!(
        repository
            .get_current("main", b"object.txt")
            .await
            .unwrap()
            .bytes,
        b"second"
    );

    repository.delete_branch("main", second.id).await.unwrap();
    assert_eq!(
        repository.head("main").await.unwrap_err().code,
        ErrorCode::NoSuchBranch
    );
    let deletion = repository
        .list_reflog("main")
        .await
        .unwrap()
        .into_iter()
        .find(|(_, entry)| entry.message == "delete branch")
        .unwrap();
    repository
        .recover_branch("main", deletion.0, second.id, "recover deleted branch")
        .await
        .unwrap();
    assert_eq!(repository.head("main").await.unwrap(), second.id);

    repository.create_tag("release", second.id).await.unwrap();
    repository.delete_tag("release", second.id).await.unwrap();
    assert!(repository.list_tags().await.unwrap().is_empty());
    let tag_deletion = repository
        .list_tag_reflog("release")
        .await
        .unwrap()
        .into_iter()
        .find(|(_, entry)| entry.message == "delete tag")
        .unwrap();
    repository
        .recover_tag("release", tag_deletion.0, second.id, "recover deleted tag")
        .await
        .unwrap();
    assert_eq!(repository.list_tags().await.unwrap()[0].target, second.id);
}

#[tokio::test]
async fn branch_create_and_delete_reconcile_lost_cas_responses() {
    let plane = Arc::new(FaultPlane::new());
    let repository = Repository::initialize(plane.clone(), options("repo-ref-ambiguity"))
        .await
        .unwrap();
    let head = repository.head("main").await.unwrap();
    plane.arm_ambiguous_ref();
    let branch = repository.create_branch("temporary", head).await.unwrap();
    assert_eq!(branch.target, head);
    plane.arm_ambiguous_ref();
    repository.delete_branch("temporary", head).await.unwrap();
    assert_eq!(
        repository.head("temporary").await.unwrap_err().code,
        ErrorCode::NoSuchBranch
    );

    plane.arm_ambiguous_ref();
    assert_eq!(
        repository.create_tag("release", head).await.unwrap().target,
        head
    );
    plane.arm_ambiguous_ref();
    repository.delete_tag("release", head).await.unwrap();
    let deletion = repository
        .list_tag_reflog("release")
        .await
        .unwrap()
        .into_iter()
        .find(|(_, entry)| entry.message == "delete tag")
        .unwrap();
    plane.arm_ambiguous_ref();
    assert_eq!(
        repository
            .recover_tag("release", deletion.0, head, "lost recovery response")
            .await
            .unwrap()
            .target,
        head
    );
}

#[tokio::test]
async fn criss_cross_history_requires_an_explicit_stable_best_base() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(plane, options("repo-criss-cross"))
        .await
        .unwrap();
    let root = repository.head("main").await.unwrap();
    repository.create_branch("left", root).await.unwrap();
    repository.create_branch("right", root).await.unwrap();
    let left = repository
        .put_bytes(
            "left",
            b"left.txt".to_vec(),
            b"left".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let right = repository
        .put_bytes(
            "right",
            b"right.txt".to_vec(),
            b"right".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let left_merge = repository
        .merge("left", right, None, MergePolicy::Fail, None, None)
        .await
        .unwrap()
        .id;
    let right_merge = repository
        .merge("right", left, None, MergePolicy::Fail, None, None)
        .await
        .unwrap()
        .id;
    let mut expected = vec![left, right];
    expected.sort();
    assert_eq!(
        repository
            .merge_bases(left_merge, right_merge)
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        repository
            .plan_merge("left", right_merge, None, MergePolicy::Fail)
            .await
            .unwrap_err()
            .code,
        ErrorCode::AmbiguousMergeBase
    );
    let plan = repository
        .plan_merge("left", right_merge, Some(expected[0]), MergePolicy::Fail)
        .await
        .unwrap();
    assert_eq!(plan.best_bases, expected);
    assert_eq!(plan.selected_base, Some(expected[0]));
}

#[tokio::test]
async fn unrelated_history_fails_before_merge_planning_or_publication() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(plane.clone(), options("repo-unrelated-history"))
        .await
        .unwrap();
    let root = repository.head("main").await.unwrap();
    let mut unrelated: BucketCommitV1 = repository.commit(root).await.unwrap();
    unrelated.author = "synthetic-unrelated-root".to_string();
    unrelated.metadata.insert(
        "fixture".to_string(),
        b"valid commit with no shared ancestry".to_vec(),
    );
    let unrelated_id = unrelated.id().unwrap();
    assert_ne!(unrelated_id, root);
    let bytes = encode_canonical(&unrelated).unwrap();
    let encoded = hex::encode(unrelated_id.as_bytes());
    plane
        .put_immutable(ImmutablePut {
            path: ObjectPath::new(format!(
                "repo-unrelated-history/commits/sha256/{}/{}/{}",
                &encoded[..2],
                &encoded[2..4],
                encoded
            ))
            .unwrap(),
            expected_sha256: Sha256::digest(&bytes).into(),
            bytes,
        })
        .await
        .unwrap();

    assert_eq!(
        repository
            .merge_bases(root, unrelated_id)
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoMergeBase
    );
    assert_eq!(
        repository
            .plan_merge("main", unrelated_id, None, MergePolicy::Fail)
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoMergeBase
    );
    assert_eq!(repository.head("main").await.unwrap(), root);
}

#[tokio::test]
async fn fenced_gc_retains_active_publications_and_deletes_exact_orphans() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(plane.clone(), options("repo-gc"))
        .await
        .unwrap();
    let orphan =
        ObjectPath::new(format!("repo-gc/chunks/sha256/aa/bb/{}", "aa".repeat(32))).unwrap();
    let protected =
        ObjectPath::new(format!("repo-gc/chunks/sha256/cc/dd/{}", "cc".repeat(32))).unwrap();
    for (path, bytes) in [
        (orphan.clone(), b"orphan".to_vec()),
        (protected.clone(), b"protected".to_vec()),
    ] {
        let expected_sha256: [u8; 32] = Sha256::digest(&bytes).into();
        plane
            .put_immutable(ImmutablePut {
                path,
                bytes,
                expected_sha256,
            })
            .await
            .unwrap();
    }
    let lease = PublicationLease::create_or_resume(
        plane.clone(),
        "repo-gc",
        OperationId::new(),
        "gc-test",
        60 * 60 * 1_000,
    )
    .await
    .unwrap();
    lease.protect(protected.clone()).await.unwrap();

    let dry_run = repository.plan_gc(2 * 60 * 60 * 1_000, 100).await.unwrap();
    assert!(dry_run
        .plan
        .body
        .candidates
        .iter()
        .any(|candidate| candidate.path == orphan));
    assert!(!dry_run
        .plan
        .body
        .candidates
        .iter()
        .any(|candidate| candidate.path == protected));
    let swept = repository.sweep_gc(dry_run.plan.id).await.unwrap();
    assert_eq!(swept.deleted_versions, 1);
    assert!(plane
        .get(GetRequest {
            path: orphan,
            range: None,
            physical_version: None,
        })
        .await
        .unwrap()
        .is_none());
    assert!(plane
        .get(GetRequest {
            path: protected,
            range: None,
            physical_version: None,
        })
        .await
        .unwrap()
        .is_some());

    let later_orphan =
        ObjectPath::new(format!("repo-gc/chunks/sha256/ee/ff/{}", "ee".repeat(32))).unwrap();
    let bytes = b"later orphan".to_vec();
    plane
        .put_immutable(ImmutablePut {
            path: later_orphan,
            expected_sha256: Sha256::digest(&bytes).into(),
            bytes,
        })
        .await
        .unwrap();
    let stale = repository.plan_gc(2 * 60 * 60 * 1_000, 100).await.unwrap();
    repository
        .put_bytes(
            "main",
            b"fence.txt".to_vec(),
            b"move head".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        repository.sweep_gc(stale.plan.id).await.unwrap_err().code,
        ErrorCode::PreconditionFailed
    );
}

#[tokio::test]
async fn clone_to_empty_namespace_preserves_canonical_identity_and_history() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(source_plane.clone(), options("repo-clone-source"))
        .await
        .unwrap();
    let first = source
        .put_bytes(
            "main",
            b"one.txt".to_vec(),
            b"one".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    source.create_branch("feature", first.id).await.unwrap();
    source
        .put_bytes(
            "feature",
            b"two.txt".to_vec(),
            b"two".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    source.create_tag("release", first.id).await.unwrap();

    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    let report = source
        .clone_to(destination_plane.clone(), "repo-clone-destination")
        .await
        .unwrap();
    assert!(report.immutable_objects > 0);
    assert_eq!(report.refs, 3);
    let destination =
        Repository::open(destination_plane.clone(), options("repo-clone-destination"))
            .await
            .unwrap();
    assert_eq!(destination.repository_id(), source.repository_id());
    assert_eq!(
        destination.head("main").await.unwrap(),
        source.head("main").await.unwrap()
    );
    assert_eq!(
        destination.head("feature").await.unwrap(),
        source.head("feature").await.unwrap()
    );
    assert_eq!(
        destination
            .get_current("feature", b"two.txt")
            .await
            .unwrap()
            .bytes,
        b"two"
    );
    destination.fsck().await.unwrap();

    let advanced = source
        .put_bytes(
            "main",
            b"three.txt".to_vec(),
            b"three".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let orphan_path = ObjectPath::new(format!(
        "repo-clone-source/chunks/sha256/de/ad/{}",
        "de".repeat(32)
    ))
    .unwrap();
    let orphan_bytes = b"namespace orphan excluded from sync".to_vec();
    source_plane
        .put_immutable(ImmutablePut {
            path: orphan_path,
            expected_sha256: Sha256::digest(&orphan_bytes).into(),
            bytes: orphan_bytes,
        })
        .await
        .unwrap();
    let fetched = destination.fetch_from(&source, "main").await.unwrap();
    assert_eq!(fetched.source_head, Some(advanced.id));
    assert_eq!(destination.head("main").await.unwrap(), first.id);
    assert!(destination_plane
        .head(
            &ObjectPath::new(format!(
                "repo-clone-destination/chunks/sha256/de/ad/{}",
                "de".repeat(32)
            ))
            .unwrap()
        )
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        destination
            .head_current_in(advanced.id, b"three.txt")
            .await
            .unwrap()
            .version
            .body
            .kind,
        source
            .head_current_in(advanced.id, b"three.txt")
            .await
            .unwrap()
            .version
            .body
            .kind
    );
    let chunk_page = destination_plane
        .list(ListRequest {
            prefix: "repo-clone-destination/chunks/".to_string(),
            continuation: None,
            limit: 1_000,
            include_versions: false,
        })
        .await
        .unwrap();
    let missing = chunk_page.entries.last().unwrap();
    let missing_version = PhysicalVersion::Versioned {
        version_id: missing.metadata.token.version_id.clone().unwrap(),
    };
    assert_eq!(
        destination_plane
            .delete_exact(&missing.path, missing_version)
            .await
            .unwrap(),
        DeleteOutcome::Deleted
    );
    assert!(destination.fsck_commit(advanced.id).await.is_err());
    let repaired = destination
        .repair_missing_from(&source, "main")
        .await
        .unwrap();
    assert_eq!(repaired.sync.source_head, Some(advanced.id));
    assert_eq!(repaired.sync.copied_objects, 1);
    assert!(repaired.fsck.content_bytes_verified > 0);
    let pushed = source
        .push_to(&destination, "main", "main", first.id, "integration push")
        .await
        .unwrap();
    assert_eq!(pushed.source_head, Some(advanced.id));
    assert_eq!(destination.head("main").await.unwrap(), advanced.id);
    assert_eq!(
        destination
            .get_current("main", b"three.txt")
            .await
            .unwrap()
            .bytes,
        b"three"
    );

    let corrupt_plane = Arc::new(FaultPlane::new());
    source
        .clone_to(corrupt_plane.clone(), "repo-corrupt-repair")
        .await
        .unwrap();
    let corrupt_destination =
        Repository::open(corrupt_plane.clone(), options("repo-corrupt-repair"))
            .await
            .unwrap();
    let corrupt_chunk = corrupt_plane
        .list(ListRequest {
            prefix: "repo-corrupt-repair/chunks/".to_string(),
            continuation: None,
            limit: 1_000,
            include_versions: false,
        })
        .await
        .unwrap()
        .entries
        .into_iter()
        .next()
        .unwrap()
        .path;
    corrupt_plane.arm_corrupt_read(corrupt_chunk);
    let before_repair = corrupt_destination
        .fsck_commit(advanced.id)
        .await
        .unwrap_err();
    let repair = corrupt_destination
        .repair_missing_from(&source, "main")
        .await
        .unwrap_err();
    assert_eq!(repair.code, before_repair.code);
    corrupt_plane.clear_corrupt_read();
    corrupt_destination.fsck_commit(advanced.id).await.unwrap();
}

#[tokio::test]
async fn checkpointed_sync_resumes_after_restart_and_stays_pinned_to_its_source_head() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(source_plane.clone(), options("repo-sync-source"))
        .await
        .unwrap();
    source
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

    let destination_plane = Arc::new(MemoryObjectPlane::new(true));
    source
        .clone_to(destination_plane.clone(), "repo-sync-destination")
        .await
        .unwrap();
    let destination = Repository::open(destination_plane.clone(), options("repo-sync-destination"))
        .await
        .unwrap();

    let pinned = source
        .put_bytes(
            "main",
            b"pinned.txt".to_vec(),
            b"pinned closure".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        source
            .sync_closure_batch_to(&destination, "main", None, 0)
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidLimit
    );
    let first_batch = source
        .sync_closure_batch_to(&destination, "main", None, 1)
        .await
        .unwrap();
    assert_eq!(first_batch.source_head, pinned.id);
    assert_eq!(first_batch.generation, 1);
    assert_eq!(first_batch.state, prolly_s3_core::SyncRunStateV1::Running);

    // Reopening both repositories models a fresh worker after process loss.
    drop(source);
    drop(destination);
    let source = Repository::open(source_plane.clone(), options("repo-sync-source"))
        .await
        .unwrap();
    let destination = Repository::open(destination_plane.clone(), options("repo-sync-destination"))
        .await
        .unwrap();

    let later = source
        .put_bytes(
            "main",
            b"later.txt".to_vec(),
            b"must not enter the pinned run".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    assert_ne!(later.id, pinned.id);

    let mut resumed = first_batch;
    for _ in 0..100 {
        resumed = source
            .sync_closure_batch_to(&destination, "main", Some(resumed.id), 3)
            .await
            .unwrap();
        if resumed.state == prolly_s3_core::SyncRunStateV1::Completed {
            break;
        }
    }
    assert_eq!(resumed.state, prolly_s3_core::SyncRunStateV1::Completed);
    assert_eq!(resumed.source_head, pinned.id);
    assert!(resumed.generation > 1);
    destination.fsck_commit(pinned.id).await.unwrap();
    assert_eq!(
        destination
            .head_current_in(pinned.id, b"pinned.txt")
            .await
            .unwrap()
            .version,
        source
            .head_current_in(pinned.id, b"pinned.txt")
            .await
            .unwrap()
            .version
    );
    assert!(destination
        .head_current_in(later.id, b"later.txt")
        .await
        .is_err());
    assert_eq!(destination.sync_run(resumed.id).await.unwrap(), resumed);

    // Completion is idempotent and the operation ID cannot be rebound.
    assert_eq!(
        source
            .sync_closure_batch_to(&destination, "main", Some(resumed.id), 3)
            .await
            .unwrap(),
        resumed
    );
    assert_eq!(
        source
            .sync_closure_batch_to(&destination, "feature", Some(resumed.id), 3)
            .await
            .unwrap_err()
            .code,
        ErrorCode::IdempotencyConflict
    );

    let corrupt = source
        .sync_closure_batch_to(&destination, "main", None, 1)
        .await
        .unwrap();
    let corrupt_path = ObjectPath::new(format!(
        "repo-sync-destination/sync/runs/{}",
        hex::encode(corrupt.id.as_bytes())
    ))
    .unwrap();
    let stored = destination_plane
        .load_mutable(&corrupt_path)
        .await
        .unwrap()
        .unwrap();
    let mut corrupt_checkpoint: prolly_s3_core::SyncRunV1 =
        decode_canonical(&stored.bytes).unwrap();
    corrupt_checkpoint.after_relative_path = Some("chunks/not-in-the-closure".to_string());
    assert!(matches!(
        destination_plane
            .compare_exchange(CompareExchange {
                path: corrupt_path,
                expected: Some(stored.metadata.token),
                bytes: encode_canonical(&corrupt_checkpoint).unwrap(),
            })
            .await
            .unwrap(),
        CompareExchangeOutcome::Applied(_)
    ));
    assert_eq!(
        source
            .sync_closure_batch_to(&destination, "main", Some(corrupt.id), 1)
            .await
            .unwrap_err()
            .code,
        ErrorCode::CorruptCommit
    );
}

#[tokio::test]
async fn checkpointed_gc_mark_recomputes_safely_after_worker_restart() {
    let plane = Arc::new(FaultPlane::new());
    let repository = Repository::initialize(plane.clone(), options("repo-gc-mark-restart"))
        .await
        .unwrap();
    let orphan_path = ObjectPath::new(format!(
        "repo-gc-mark-restart/chunks/sha256/aa/aa/{}",
        "aa".repeat(32)
    ))
    .unwrap();
    let orphan_bytes = b"checkpointed mark orphan".to_vec();
    plane
        .put_immutable(ImmutablePut {
            path: orphan_path.clone(),
            expected_sha256: Sha256::digest(&orphan_bytes).into(),
            bytes: orphan_bytes,
        })
        .await
        .unwrap();

    let run_id = OperationId::new();
    plane.arm_before_write(2);
    let interrupted = repository
        .plan_gc_checkpointed(Some(run_id), 2 * 60 * 60 * 1_000, 100)
        .await
        .unwrap_err();
    assert_eq!(interrupted.code, ErrorCode::Transport);
    assert!(plane.fired());
    let running = repository.gc_mark_run(run_id).await.unwrap();
    assert_eq!(running.state, prolly_s3_core::GcMarkRunStateV1::Running);
    assert!(running.plan.is_none());
    let fixed_planning_time = running.planned_at_millis;

    drop(repository);
    let repository = Repository::open(plane.clone(), options("repo-gc-mark-restart"))
        .await
        .unwrap();
    let completed = repository
        .plan_gc_checkpointed(Some(run_id), 2 * 60 * 60 * 1_000, 100)
        .await
        .unwrap();
    assert_eq!(completed.state, prolly_s3_core::GcMarkRunStateV1::Completed);
    assert_eq!(completed.generation, 1);
    assert_eq!(completed.planned_at_millis, fixed_planning_time);
    let plan = repository
        .load_gc_plan(completed.plan.unwrap())
        .await
        .unwrap();
    assert!(plan
        .body
        .candidates
        .iter()
        .any(|candidate| candidate.path == orphan_path));
    assert_eq!(repository.gc_mark_run(run_id).await.unwrap(), completed);
    assert_eq!(
        repository
            .plan_gc_checkpointed(Some(run_id), 2 * 60 * 60 * 1_000, 100)
            .await
            .unwrap(),
        completed
    );
    assert_eq!(
        repository
            .plan_gc_checkpointed(Some(run_id), 2 * 60 * 60 * 1_000, 101)
            .await
            .unwrap_err()
            .code,
        ErrorCode::IdempotencyConflict
    );
}

#[tokio::test]
async fn fsck_detects_and_repairs_every_immutable_family_and_rejects_missing_ref_targets() {
    let source_plane = Arc::new(MemoryObjectPlane::new(true));
    let source = Repository::initialize(source_plane, options("repo-fsck-matrix-source"))
        .await
        .unwrap();
    let head = source
        .put_bytes(
            "main",
            b"large.txt".to_vec(),
            b"content spanning several canonical chunks".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let families = ["commits", "deltas", "nodes", "content-manifests", "chunks"];

    let missing_plane = Arc::new(MemoryObjectPlane::new(true));
    source
        .clone_to(missing_plane.clone(), "repo-fsck-matrix-missing")
        .await
        .unwrap();
    let missing = Repository::open(missing_plane.clone(), options("repo-fsck-matrix-missing"))
        .await
        .unwrap();
    for family in families {
        let listed = missing_plane
            .list(ListRequest {
                prefix: format!("repo-fsck-matrix-missing/{family}/"),
                continuation: None,
                limit: 1_000,
                include_versions: false,
            })
            .await
            .unwrap()
            .entries;
        assert!(!listed.is_empty(), "fixture has no {family} object");
        let mut detected = false;
        for object in listed {
            let version = PhysicalVersion::Versioned {
                version_id: object.metadata.token.version_id.clone().unwrap(),
            };
            assert_eq!(
                missing_plane
                    .delete_exact(&object.path, version)
                    .await
                    .unwrap(),
                DeleteOutcome::Deleted
            );
            if missing.fsck_commit(head).await.is_err() {
                detected = true;
                break;
            }
        }
        assert!(detected, "no reachable {family} object was detected");
        let repaired = missing.repair_missing_from(&source, "main").await.unwrap();
        assert!(repaired.sync.copied_objects >= 1, "repair {family}");
        missing.fsck_commit(head).await.unwrap();
    }

    let ref_path = ObjectPath::new(format!(
        "repo-fsck-matrix-missing/refs/heads/{}",
        hex::encode("main")
    ))
    .unwrap();
    let stored_ref = missing_plane
        .load_mutable(&ref_path)
        .await
        .unwrap()
        .unwrap();
    let mut invalid_ref: prolly_s3_core::RefValueV1 = decode_canonical(&stored_ref.bytes).unwrap();
    invalid_ref.target = prolly_s3_core::CommitId::from_hash([0xf3; 32]);
    let invalid_metadata = match missing_plane
        .compare_exchange(CompareExchange {
            path: ref_path.clone(),
            expected: Some(stored_ref.metadata.token),
            bytes: encode_canonical(&invalid_ref).unwrap(),
        })
        .await
        .unwrap()
    {
        CompareExchangeOutcome::Applied(metadata) => metadata,
        CompareExchangeOutcome::Conflict(_) => panic!("isolated ref mutation conflicted"),
    };
    assert!(missing.fsck().await.is_err());
    assert!(matches!(
        missing_plane
            .compare_exchange(CompareExchange {
                path: ref_path,
                expected: Some(invalid_metadata.token),
                bytes: stored_ref.bytes,
            })
            .await
            .unwrap(),
        CompareExchangeOutcome::Applied(_)
    ));
    missing.fsck().await.unwrap();

    let corrupt_plane = Arc::new(FaultPlane::new());
    source
        .clone_to(corrupt_plane.clone(), "repo-fsck-matrix-corrupt")
        .await
        .unwrap();
    let corrupt = Repository::open(corrupt_plane.clone(), options("repo-fsck-matrix-corrupt"))
        .await
        .unwrap();
    for family in families {
        let paths = corrupt_plane
            .list(ListRequest {
                prefix: format!("repo-fsck-matrix-corrupt/{family}/"),
                continuation: None,
                limit: 1_000,
                include_versions: false,
            })
            .await
            .unwrap()
            .entries
            .into_iter()
            .map(|entry| entry.path)
            .collect::<Vec<_>>();
        assert!(!paths.is_empty(), "fixture has no {family} object");
        let mut detected = false;
        for path in paths {
            corrupt_plane.arm_corrupt_read(path);
            if corrupt.fsck_commit(head).await.is_err() {
                detected = true;
                corrupt_plane.clear_corrupt_read();
                break;
            }
            corrupt_plane.clear_corrupt_read();
        }
        assert!(detected, "no reachable corrupt {family} was detected");
        corrupt.fsck_commit(head).await.unwrap();
    }
}

struct FaultControls {
    fail_at: AtomicUsize,
    write_count: AtomicUsize,
    fired: AtomicBool,
    ambiguous_ref: AtomicBool,
    pause_after_ref: AtomicBool,
    ref_applied: tokio::sync::Notify,
    corrupt_read_path: RwLock<Option<ObjectPath>>,
}

impl FaultPlane {
    fn new() -> Self {
        Self {
            inner: MemoryObjectPlane::new(false),
            controls: Arc::new(FaultControls {
                fail_at: AtomicUsize::new(usize::MAX),
                write_count: AtomicUsize::new(0),
                fired: AtomicBool::new(false),
                ambiguous_ref: AtomicBool::new(false),
                pause_after_ref: AtomicBool::new(false),
                ref_applied: tokio::sync::Notify::new(),
                corrupt_read_path: RwLock::new(None),
            }),
        }
    }

    fn arm_before_write(&self, write: usize) {
        self.controls.write_count.store(0, Ordering::SeqCst);
        self.controls.fired.store(false, Ordering::SeqCst);
        self.controls.fail_at.store(write, Ordering::SeqCst);
    }

    fn arm_ambiguous_ref(&self) {
        self.controls.ambiguous_ref.store(true, Ordering::SeqCst);
    }

    fn arm_pause_after_ref(&self) {
        self.controls.pause_after_ref.store(true, Ordering::SeqCst);
    }

    async fn wait_until_ref_applied(&self) {
        self.controls.ref_applied.notified().await;
    }

    fn fired(&self) -> bool {
        self.controls.fired.load(Ordering::SeqCst)
    }

    fn arm_corrupt_read(&self, path: ObjectPath) {
        *self.controls.corrupt_read_path.write().unwrap() = Some(path);
    }

    fn clear_corrupt_read(&self) {
        *self.controls.corrupt_read_path.write().unwrap() = None;
    }

    fn fail_now(&self) -> bool {
        let write = self.controls.write_count.fetch_add(1, Ordering::SeqCst) + 1;
        if write == self.controls.fail_at.load(Ordering::SeqCst) {
            self.controls.fired.store(true, Ordering::SeqCst);
            true
        } else {
            false
        }
    }
}

#[async_trait::async_trait]
impl ObjectPlane for FaultPlane {
    async fn get(&self, request: GetRequest) -> Result<Option<StoredObject>> {
        let path = request.path.clone();
        let mut object = self.inner.get(request).await?;
        if self
            .controls
            .corrupt_read_path
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "test lock poisoned"))?
            .as_ref()
            == Some(&path)
        {
            if let Some(object) = &mut object {
                if let Some(first) = object.bytes.first_mut() {
                    *first ^= 0xff;
                }
            }
        }
        Ok(object)
    }

    async fn head(&self, path: &ObjectPath) -> Result<Option<StoredMetadata>> {
        self.inner.head(path).await
    }

    async fn put_immutable(&self, request: ImmutablePut) -> Result<ImmutablePutOutcome> {
        if self.fail_now() {
            return Err(Error::new(
                ErrorCode::Transport,
                "injected failure before immutable write",
            ));
        }
        self.inner.put_immutable(request).await
    }

    async fn load_mutable(&self, path: &ObjectPath) -> Result<Option<StoredObject>> {
        self.inner.load_mutable(path).await
    }

    async fn compare_exchange(&self, request: CompareExchange) -> Result<CompareExchangeOutcome> {
        if self.fail_now() {
            return Err(Error::new(
                ErrorCode::Transport,
                "injected failure before conditional write",
            ));
        }
        let is_branch = request.path.as_str().contains("/refs/heads/")
            || request.path.as_str().contains("/refs/tags/");
        let outcome = self.inner.compare_exchange(request).await?;
        if is_branch
            && matches!(outcome, CompareExchangeOutcome::Applied(_))
            && self.controls.pause_after_ref.load(Ordering::SeqCst)
        {
            self.controls.ref_applied.notify_one();
            std::future::pending::<()>().await;
        }
        if is_branch
            && matches!(outcome, CompareExchangeOutcome::Applied(_))
            && self.controls.ambiguous_ref.swap(false, Ordering::SeqCst)
        {
            self.controls.fired.store(true, Ordering::SeqCst);
            return Err(Error::new(
                ErrorCode::OutcomeUnknown,
                "injected lost response after accepted ref CAS",
            ));
        }
        Ok(outcome)
    }

    async fn list(&self, request: ListRequest) -> Result<PhysicalListPage> {
        self.inner.list(request).await
    }

    async fn delete_exact(
        &self,
        path: &ObjectPath,
        version: PhysicalVersion,
    ) -> Result<DeleteOutcome> {
        self.inner.delete_exact(path, version).await
    }
}

fn options(prefix: &str) -> RepositoryOptions {
    RepositoryOptions {
        repository_prefix: prefix.to_string(),
        writer: "core-test".to_string(),
        limits: CanonicalLimits {
            content_chunk_bytes: 4,
            ..CanonicalLimits::default()
        },
        ..RepositoryOptions::default()
    }
}

#[tokio::test]
async fn legacy_v1_format_without_appended_capability_profile_remains_readable() {
    #[derive(Serialize)]
    struct LegacyRepositoryFormatV1 {
        repository_id: prolly_s3_core::RepositoryId,
        format_version: u16,
        state_tree_format: prolly::TreeFormat,
        content_index_format: prolly::TreeFormat,
        canonical_limits: CanonicalLimits,
        min_reader_version: u32,
        min_writer_version: u32,
        created_at_millis: u64,
    }

    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository_options = options("repo-legacy-v1-format");
    Repository::initialize(plane.clone(), repository_options.clone())
        .await
        .unwrap();
    let path = ObjectPath::new("repo-legacy-v1-format/format/v1.cbor").unwrap();
    let current = plane.load_mutable(&path).await.unwrap().unwrap();
    let format: RepositoryFormatV1 = decode_canonical(&current.bytes).unwrap();
    let current_format_bytes = encode_canonical(&format).unwrap();
    let legacy = LegacyRepositoryFormatV1 {
        repository_id: format.repository_id,
        format_version: format.format_version,
        state_tree_format: format.state_tree_format,
        content_index_format: format.content_index_format,
        canonical_limits: format.canonical_limits,
        min_reader_version: format.min_reader_version,
        min_writer_version: format.min_writer_version,
        created_at_millis: format.created_at_millis,
    };
    assert_eq!(
        current_format_bytes,
        encode_canonical(&legacy).unwrap(),
        "default profile 1 must preserve the original v1 marker bytes"
    );
    plane
        .compare_exchange(CompareExchange {
            path,
            expected: Some(current.metadata.token),
            bytes: encode_canonical(&legacy).unwrap(),
        })
        .await
        .unwrap();
    let before = plane
        .list(ListRequest {
            prefix: "repo-legacy-v1-format/".to_string(),
            continuation: None,
            limit: 1_000,
            include_versions: true,
        })
        .await
        .unwrap();
    let reopened = Repository::open(plane.clone(), repository_options)
        .await
        .unwrap();
    assert_eq!(
        reopened.format().required_capability_profile,
        RepositoryFormatV1::DISTRIBUTED_S3_CAPABILITY_PROFILE
    );
    let after = plane
        .list(ListRequest {
            prefix: "repo-legacy-v1-format/".to_string(),
            continuation: None,
            limit: 1_000,
            include_versions: true,
        })
        .await
        .unwrap();
    assert_eq!(
        after, before,
        "legacy-compatible open changed physical state"
    );
}

#[tokio::test]
async fn future_reader_or_writer_requirement_fails_before_any_open_write() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository_options = options("repo-rolling-version");
    Repository::initialize(plane.clone(), repository_options.clone())
        .await
        .unwrap();
    let format_path = ObjectPath::new("repo-rolling-version/format/v1.cbor").unwrap();

    for (reader, writer, capability_profile) in [(2, 1, 1), (1, 2, 1), (1, 1, 2)] {
        let current = plane.load_mutable(&format_path).await.unwrap().unwrap();
        let mut format: RepositoryFormatV1 = decode_canonical(&current.bytes).unwrap();
        format.min_reader_version = reader;
        format.min_writer_version = writer;
        format.required_capability_profile = capability_profile;
        assert!(matches!(
            plane
                .compare_exchange(CompareExchange {
                    path: format_path.clone(),
                    expected: Some(current.metadata.token),
                    bytes: encode_canonical(&format).unwrap(),
                })
                .await
                .unwrap(),
            CompareExchangeOutcome::Applied(_)
        ));
        let before = plane
            .list(ListRequest {
                prefix: "repo-rolling-version/".to_string(),
                continuation: None,
                limit: 1_000,
                include_versions: true,
            })
            .await
            .unwrap();
        let open_error = match Repository::open(plane.clone(), repository_options.clone()).await {
            Ok(_) => panic!("future protocol requirement was accepted"),
            Err(error) => error,
        };
        assert_eq!(open_error.code, ErrorCode::UnsupportedRepositoryFormat);
        let after = plane
            .list(ListRequest {
                prefix: "repo-rolling-version/".to_string(),
                continuation: None,
                limit: 1_000,
                include_versions: true,
            })
            .await
            .unwrap();
        assert_eq!(after, before, "incompatible open changed physical state");

        let current = plane.load_mutable(&format_path).await.unwrap().unwrap();
        let mut compatible: RepositoryFormatV1 = decode_canonical(&current.bytes).unwrap();
        compatible.min_reader_version = 1;
        compatible.min_writer_version = 1;
        compatible.required_capability_profile =
            RepositoryFormatV1::DISTRIBUTED_S3_CAPABILITY_PROFILE;
        plane
            .compare_exchange(CompareExchange {
                path: format_path.clone(),
                expected: Some(current.metadata.token),
                bytes: encode_canonical(&compatible).unwrap(),
            })
            .await
            .unwrap();
    }
    Repository::open(plane, repository_options).await.unwrap();
}

#[tokio::test]
async fn initialization_is_idempotent_and_runtime_config_is_not_canonical() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let first = Repository::initialize(plane.clone(), options("repo-init"))
        .await
        .unwrap();
    let second = Repository::initialize(plane, options("repo-init"))
        .await
        .unwrap();
    assert_eq!(first.repository_id(), second.repository_id());
    assert_eq!(
        first.head("main").await.unwrap(),
        second.head("main").await.unwrap()
    );

    let mut left_config = Config::default();
    let mut right_config = left_config.clone();
    left_config.runtime.read_parallelism = 1;
    right_config.runtime.read_parallelism = 64;
    right_config.runtime.node_cache_max_bytes = Some(1);
    let left = Tree {
        root: None,
        config: left_config,
    };
    let right = Tree {
        root: None,
        config: right_config,
    };
    assert_eq!(
        TreeRootV1::from_tree(&left).unwrap(),
        TreeRootV1::from_tree(&right).unwrap()
    );
}

#[tokio::test]
async fn put_get_delete_and_version_history_are_bucket_atomic() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(plane.clone(), options("repo-history"))
        .await
        .unwrap();
    let operation = OperationId::new();
    let put = repository
        .put_bytes(
            "main",
            b"folder/object.txt".to_vec(),
            b"abcdefghijklmnop".to_vec(),
            ObjectHeaders {
                content_type: Some("text/plain".to_string()),
                ..ObjectHeaders::default()
            },
            BTreeMap::from([("owner".to_string(), "test".to_string())]),
            Some(operation),
        )
        .await
        .unwrap();
    assert_eq!(put.object_versions.len(), 1);
    let lease = load_publication_lease(plane.as_ref(), "repo-history", operation)
        .await
        .unwrap()
        .expect("ordinary put creates a durable publication lease");
    assert_eq!(lease.proposal, Some(put.id));
    assert!(lease.protection_head.is_some());
    assert!(matches!(
        lease.state,
        PublicationLeaseStateV1::Completed { commit } if commit == put.id
    ));
    let mut segment_id = lease.protection_head;
    let mut protected = 0;
    while let Some(id) = segment_id {
        let segment = load_protection_segment(plane.as_ref(), "repo-history", id)
            .await
            .unwrap()
            .expect("lease protection segment exists");
        assert_eq!(segment.operation, operation);
        assert!(!segment.paths.is_empty());
        assert!(segment.paths.len() <= 1_024);
        protected += segment.paths.len();
        segment_id = segment.previous;
    }
    assert!(
        protected >= 4,
        "content, nodes, delta, commit, and reflog are protected"
    );

    let replay = repository
        .put_bytes(
            "main",
            b"folder/object.txt".to_vec(),
            b"abcdefghijklmnop".to_vec(),
            ObjectHeaders {
                content_type: Some("text/plain".to_string()),
                ..ObjectHeaders::default()
            },
            BTreeMap::from([("owner".to_string(), "test".to_string())]),
            Some(operation),
        )
        .await
        .unwrap();
    assert!(replay.idempotent_replay);

    let current = repository
        .get_current("main", b"folder/object.txt")
        .await
        .unwrap();
    assert_eq!(current.bytes, b"abcdefghijklmnop");
    assert_eq!(current.version.id, put.object_versions[0]);

    repository
        .delete_object("main", b"folder/object.txt".to_vec(), None)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"folder/object.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );

    repository
        .delete_object("main", b"folder/object.txt".to_vec(), None)
        .await
        .unwrap();
    let (_, versions) = repository
        .list_object_versions("main", b"folder/object.txt", 100)
        .await
        .unwrap();
    assert_eq!(versions.len(), 3);
    assert!(matches!(
        versions[0].body.kind,
        ObjectVersionKindV1::DeleteMarker
    ));
    assert!(matches!(
        versions[1].body.kind,
        ObjectVersionKindV1::DeleteMarker
    ));

    let historical = repository
        .get_version("main", b"folder/object.txt", put.object_versions[0])
        .await
        .unwrap();
    assert_eq!(historical.bytes, b"abcdefghijklmnop");
}

#[tokio::test]
async fn failed_or_expired_publications_cannot_move_the_branch() {
    let plane = Arc::new(MemoryObjectPlane::new(false));
    let repository = Repository::initialize(plane.clone(), options("repo-lease-failure"))
        .await
        .unwrap();
    let initial = repository.head("main").await.unwrap();

    let failed_operation = OperationId::new();
    let body_error = repository
        .put_stream(
            "main",
            b"failed-body".to_vec(),
            futures_util::stream::once(async {
                Err::<Vec<u8>, _>("synthetic non-replayable body failure")
            }),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(failed_operation),
        )
        .await
        .unwrap_err();
    assert_eq!(body_error.code, ErrorCode::IncompleteBody);
    assert_eq!(repository.head("main").await.unwrap(), initial);
    let failed_lease =
        load_publication_lease(plane.as_ref(), "repo-lease-failure", failed_operation)
            .await
            .unwrap()
            .unwrap();
    assert!(matches!(
        failed_lease.state,
        PublicationLeaseStateV1::Abandoned
    ));

    let expired_operation = OperationId::new();
    PublicationLease::create_or_resume(
        plane.clone(),
        "repo-lease-failure",
        expired_operation,
        "expired-writer",
        5 * 60 * 1_000,
    )
    .await
    .unwrap();
    let lease_path = ObjectPath::new(format!(
        "repo-lease-failure/publications/{expired_operation}/lease"
    ))
    .unwrap();
    let stored = plane.load_mutable(&lease_path).await.unwrap().unwrap();
    let mut expired: PublicationLeaseV1 = decode_canonical(&stored.bytes).unwrap();
    expired.expires_at_millis = 0;
    assert!(matches!(
        plane
            .compare_exchange(CompareExchange {
                path: lease_path,
                expected: Some(stored.metadata.token),
                bytes: encode_canonical(&expired).unwrap(),
            })
            .await
            .unwrap(),
        prolly_s3_core::CompareExchangeOutcome::Applied(_)
    ));
    let expired_error = repository
        .put_bytes(
            "main",
            b"expired".to_vec(),
            b"must not publish".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(expired_operation),
        )
        .await
        .unwrap_err();
    assert_eq!(expired_error.code, ErrorCode::OperationCanceled);
    assert_eq!(repository.head("main").await.unwrap(), initial);
}

#[tokio::test]
async fn every_injected_prewrite_failure_preserves_a_valid_old_or_new_head() {
    let mut reached_end = false;
    for fail_at in 1..=128 {
        let plane = Arc::new(FaultPlane::new());
        let repository = Repository::initialize(plane.clone(), options("repo-fault-matrix"))
            .await
            .unwrap();
        let before = repository.head("main").await.unwrap();
        plane.arm_before_write(fail_at);
        let result = repository
            .put_bytes(
                "main",
                b"fault.txt".to_vec(),
                b"fault-boundary-body".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                Some(OperationId::new()),
            )
            .await;
        let after = repository.head("main").await.unwrap();
        repository.fsck().await.unwrap();
        if plane.fired() {
            match result {
                Ok(receipt) => {
                    assert_eq!(after, receipt.id);
                    assert_ne!(after, before);
                }
                Err(_) => assert_eq!(after, before),
            }
        } else {
            let receipt = result.expect("fault index beyond the complete write trace");
            assert_eq!(after, receipt.id);
            reached_end = true;
            break;
        }
    }
    assert!(
        reached_end,
        "fault matrix did not exhaust the publication trace"
    );
}

#[tokio::test]
async fn every_merge_prewrite_failure_preserves_a_valid_old_or_new_head() {
    let mut reached_end = false;
    for fail_at in 1..=128 {
        let plane = Arc::new(FaultPlane::new());
        let repository = Repository::initialize(plane.clone(), options("repo-merge-fault-matrix"))
            .await
            .unwrap();
        let root = repository.head("main").await.unwrap();
        repository.create_branch("feature", root).await.unwrap();
        repository
            .put_bytes(
                "main",
                b"main.txt".to_vec(),
                b"main".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
        let before = repository.head("main").await.unwrap();
        let source = repository
            .put_bytes(
                "feature",
                b"feature.txt".to_vec(),
                b"feature".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap()
            .id;
        plane.arm_before_write(fail_at);
        let result = repository
            .merge("main", source, None, MergePolicy::Fail, None, None)
            .await;
        let after = repository.head("main").await.unwrap();
        repository.fsck().await.unwrap();
        if plane.fired() {
            match result {
                Ok(receipt) => {
                    assert_eq!(after, receipt.id);
                    assert_eq!(receipt.parents, [before, source]);
                }
                Err(_) => assert_eq!(after, before),
            }
        } else {
            let receipt = result.expect("fault index beyond the complete merge write trace");
            assert_eq!(after, receipt.id);
            assert_eq!(receipt.parents, [before, source]);
            reached_end = true;
            break;
        }
    }
    assert!(
        reached_end,
        "merge fault matrix did not exhaust its write trace"
    );
}

#[tokio::test]
async fn every_reset_prewrite_failure_preserves_a_valid_old_or_new_ref() {
    let mut reached_end = false;
    for fail_at in 1..=32 {
        let plane = Arc::new(FaultPlane::new());
        let repository = Repository::initialize(plane.clone(), options("repo-reset-fault-matrix"))
            .await
            .unwrap();
        let first = repository
            .put_bytes(
                "main",
                b"object.txt".to_vec(),
                b"first".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap()
            .id;
        let before = repository
            .put_bytes(
                "main",
                b"object.txt".to_vec(),
                b"second".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap()
            .id;
        plane.arm_before_write(fail_at);
        let result = repository
            .reset_branch("main", first, before, "fault-injected reset")
            .await;
        let after = repository.head("main").await.unwrap();
        repository.fsck().await.unwrap();
        if plane.fired() {
            match result {
                Ok(receipt) => {
                    assert_eq!(receipt.old_target, Some(before));
                    assert_eq!(receipt.new_target, first);
                    assert_eq!(after, first);
                }
                Err(_) => assert_eq!(after, before),
            }
        } else {
            let receipt = result.expect("fault index beyond the complete reset write trace");
            assert_eq!(receipt.old_target, Some(before));
            assert_eq!(receipt.new_target, first);
            assert_eq!(after, first);
            reached_end = true;
            break;
        }
    }
    assert!(
        reached_end,
        "reset fault matrix did not exhaust its write trace"
    );
}

#[tokio::test]
async fn merge_and_reset_reconcile_lost_ref_cas_responses() {
    let plane = Arc::new(FaultPlane::new());
    let repository = Repository::initialize(plane.clone(), options("repo-admin-ambiguity"))
        .await
        .unwrap();
    let root = repository.head("main").await.unwrap();
    repository.create_branch("feature", root).await.unwrap();
    let main = repository
        .put_bytes(
            "main",
            b"main.txt".to_vec(),
            b"main".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let feature = repository
        .put_bytes(
            "feature",
            b"feature.txt".to_vec(),
            b"feature".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;

    plane.arm_ambiguous_ref();
    let merged = repository
        .merge("main", feature, None, MergePolicy::Fail, None, None)
        .await
        .unwrap();
    assert_eq!(merged.parents, [main, feature]);
    assert_eq!(repository.head("main").await.unwrap(), merged.id);
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 3);

    plane.arm_ambiguous_ref();
    let reset = repository
        .reset_branch("main", main, merged.id, "lost reset response")
        .await
        .unwrap();
    assert_eq!(reset.old_target, Some(merged.id));
    assert_eq!(reset.new_target, main);
    assert_eq!(repository.head("main").await.unwrap(), main);
    repository.fsck().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_merge_after_ref_acceptance_reconciles_by_operation() {
    let plane = Arc::new(FaultPlane::new());
    let repository = Arc::new(
        Repository::initialize(plane.clone(), options("repo-merge-cancel"))
            .await
            .unwrap(),
    );
    let root = repository.head("main").await.unwrap();
    repository.create_branch("feature", root).await.unwrap();
    let source = repository
        .put_bytes(
            "feature",
            b"feature.txt".to_vec(),
            b"feature".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let operation = OperationId::new();
    plane.arm_pause_after_ref();
    let publishing = {
        let repository = repository.clone();
        tokio::spawn(async move {
            repository
                .merge(
                    "main",
                    source,
                    None,
                    MergePolicy::Fail,
                    Some(operation),
                    None,
                )
                .await
        })
    };
    plane.wait_until_ref_applied().await;
    publishing.abort();
    assert!(publishing.await.unwrap_err().is_cancelled());
    let accepted = repository
        .lookup_operation("main", operation)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(accepted.parents, [root, source]);
    let replay = repository
        .merge(
            "main",
            source,
            None,
            MergePolicy::Fail,
            Some(operation),
            None,
        )
        .await
        .unwrap();
    assert_eq!(replay.id, accepted.id);
    assert!(replay.idempotent_replay);
    assert_eq!(
        repository
            .merge(
                "main",
                source,
                None,
                MergePolicy::Ours,
                Some(operation),
                None,
            )
            .await
            .unwrap_err()
            .code,
        ErrorCode::IdempotencyConflict
    );
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_multi_delete_after_ref_acceptance_reconciles_by_operation() {
    let plane = Arc::new(FaultPlane::new());
    let repository = Arc::new(
        Repository::initialize(plane.clone(), options("repo-multi-delete-cancel"))
            .await
            .unwrap(),
    );
    for key in [b"a".to_vec(), b"b".to_vec()] {
        repository
            .put_bytes(
                "main",
                key,
                b"present".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let operation = OperationId::new();
    let keys = vec![b"a".to_vec(), b"b".to_vec()];
    plane.arm_pause_after_ref();
    let publishing = {
        let repository = repository.clone();
        let keys = keys.clone();
        tokio::spawn(async move {
            repository
                .delete_objects("main", keys, Some(operation))
                .await
        })
    };
    plane.wait_until_ref_applied().await;
    publishing.abort();
    assert!(publishing.await.unwrap_err().is_cancelled());
    let accepted = repository
        .lookup_operation("main", operation)
        .await
        .unwrap()
        .unwrap();
    let replay = repository
        .delete_objects("main", keys, Some(operation))
        .await
        .unwrap();
    assert_eq!(replay.id, accepted.id);
    assert!(replay.idempotent_replay);
    assert_eq!(replay.changed_keys, 2);
    assert_eq!(
        repository.get_current("main", b"a").await.unwrap_err().code,
        ErrorCode::NoSuchKey
    );
    assert_eq!(
        repository.get_current("main", b"b").await.unwrap_err().code,
        ErrorCode::NoSuchKey
    );
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_restore_after_ref_acceptance_reconciles_by_operation() {
    let plane = Arc::new(FaultPlane::new());
    let repository = Arc::new(
        Repository::initialize(plane.clone(), options("repo-restore-cancel"))
            .await
            .unwrap(),
    );
    let source = repository.head("main").await.unwrap();
    let expected = repository
        .put_bytes(
            "main",
            b"remove-on-restore".to_vec(),
            b"present".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let operation = OperationId::new();
    plane.arm_pause_after_ref();
    let publishing = {
        let repository = repository.clone();
        tokio::spawn(async move {
            repository
                .restore("main", source, expected, Some(operation), None)
                .await
        })
    };
    plane.wait_until_ref_applied().await;
    publishing.abort();
    assert!(publishing.await.unwrap_err().is_cancelled());
    let accepted = repository
        .lookup_operation("main", operation)
        .await
        .unwrap()
        .unwrap();
    let replay = repository
        .restore("main", source, expected, Some(operation), None)
        .await
        .unwrap();
    assert_eq!(replay.id, accepted.id);
    assert!(replay.idempotent_replay);
    assert_eq!(
        repository
            .get_current("main", b"remove-on-restore")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 3);
}

#[tokio::test]
async fn accepted_ref_with_lost_response_reconciles_to_one_success() {
    let plane = Arc::new(FaultPlane::new());
    let repository = Repository::initialize(plane.clone(), options("repo-ambiguous-cas"))
        .await
        .unwrap();
    let operation = OperationId::new();
    plane.arm_ambiguous_ref();
    let receipt = repository
        .put_bytes(
            "main",
            b"ambiguous.txt".to_vec(),
            b"published exactly once".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(operation),
        )
        .await
        .unwrap();
    assert!(plane.fired());
    assert_eq!(repository.head("main").await.unwrap(), receipt.id);
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 2);
    assert_eq!(
        repository
            .get_current("main", b"ambiguous.txt")
            .await
            .unwrap()
            .bytes,
        b"published exactly once"
    );
    assert!(matches!(
        load_publication_lease(plane.as_ref(), "repo-ambiguous-cas", operation)
            .await
            .unwrap()
            .unwrap()
            .state,
        PublicationLeaseStateV1::Completed { commit } if commit == receipt.id
    ));
}

#[tokio::test]
async fn cancellation_after_ref_acceptance_is_reconciled_by_operation_handle() {
    let plane = Arc::new(FaultPlane::new());
    let repository = Arc::new(
        Repository::initialize(plane.clone(), options("repo-canceled-cas"))
            .await
            .unwrap(),
    );
    let operation = OperationId::new();
    plane.arm_pause_after_ref();
    let publishing = {
        let repository = repository.clone();
        tokio::spawn(async move {
            repository
                .put_bytes(
                    "main",
                    b"canceled.txt".to_vec(),
                    b"accepted before cancellation".to_vec(),
                    ObjectHeaders::default(),
                    BTreeMap::new(),
                    Some(operation),
                )
                .await
        })
    };
    plane.wait_until_ref_applied().await;
    publishing.abort();
    assert!(publishing.await.unwrap_err().is_cancelled());

    let receipt = repository
        .lookup_operation("main", operation)
        .await
        .unwrap()
        .expect("accepted canceled operation is discoverable");
    assert_eq!(repository.head("main").await.unwrap(), receipt.id);
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 2);
    assert!(matches!(
        load_publication_lease(plane.as_ref(), "repo-canceled-cas", operation)
            .await
            .unwrap()
            .unwrap()
            .state,
        PublicationLeaseStateV1::Completed { commit } if commit == receipt.id
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_disjoint_writers_do_not_lose_updates() {
    let plane = Arc::new(MemoryObjectPlane::new(false));
    Repository::initialize(plane.clone(), options("repo-race"))
        .await
        .unwrap();
    let left = Arc::new(
        Repository::open(plane.clone(), options("repo-race"))
            .await
            .unwrap(),
    );
    let right = Arc::new(
        Repository::open(plane.clone(), options("repo-race"))
            .await
            .unwrap(),
    );
    let left_operation = OperationId::new();
    let right_operation = OperationId::new();

    let left_task = {
        let left = left.clone();
        tokio::spawn(async move {
            left.put_bytes(
                "main",
                b"a".to_vec(),
                b"left".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                Some(left_operation),
            )
            .await
        })
    };
    let right_task = tokio::spawn(async move {
        right
            .put_bytes(
                "main",
                b"b".to_vec(),
                b"right".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                Some(right_operation),
            )
            .await
    });

    left_task.await.unwrap().unwrap();
    right_task.await.unwrap().unwrap();
    assert_eq!(left.get_current("main", b"a").await.unwrap().bytes, b"left");
    assert_eq!(
        left.get_current("main", b"b").await.unwrap().bytes,
        b"right"
    );
    assert_eq!(left.log("main", 10).await.unwrap().len(), 3);
    for operation in [left_operation, right_operation] {
        let lease = load_publication_lease(plane.as_ref(), "repo-race", operation)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            lease.state,
            PublicationLeaseStateV1::Completed { .. }
        ));
        let mut next = lease.protection_head;
        while let Some(id) = next {
            let segment = load_protection_segment(plane.as_ref(), "repo-race", id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(segment.operation, operation);
            next = segment.previous;
        }
    }
}

#[tokio::test]
async fn operation_id_cannot_be_reused_with_different_input() {
    let plane = Arc::new(MemoryObjectPlane::new(false));
    let repository = Repository::initialize(plane, options("repo-idempotency"))
        .await
        .unwrap();
    let operation = OperationId::new();
    repository
        .put_bytes(
            "main",
            b"key".to_vec(),
            b"first".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(operation),
        )
        .await
        .unwrap();
    let error = repository
        .put_bytes(
            "main",
            b"key".to_vec(),
            b"second".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(operation),
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::IdempotencyConflict);
}

#[tokio::test]
async fn branches_are_independent_cas_published_histories() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(plane, options("repo-branches"))
        .await
        .unwrap();
    let main = repository.head("main").await.unwrap();
    repository
        .create_branch("feature/assets", main)
        .await
        .unwrap();
    repository
        .put_bytes(
            "feature/assets",
            b"feature.txt".to_vec(),
            b"branch-only".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("feature/assets", b"feature.txt")
            .await
            .unwrap()
            .bytes,
        b"branch-only"
    );
    assert_eq!(
        repository
            .get_current("main", b"feature.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    let branches = repository.list_branches().await.unwrap();
    assert_eq!(
        branches
            .iter()
            .map(|head| head.name.as_str())
            .collect::<Vec<_>>(),
        ["feature/assets", "main"]
    );
    let feature = repository.head("feature/assets").await.unwrap();
    repository.create_tag("release/v1", feature).await.unwrap();
    assert_eq!(repository.list_tags().await.unwrap()[0].target, feature);
    repository
        .delete_branch("feature/assets", feature)
        .await
        .unwrap();
    assert_eq!(
        repository.head("feature/assets").await.unwrap_err().code,
        ErrorCode::NoSuchBranch
    );
    assert_eq!(repository.list_branches().await.unwrap().len(), 1);
    assert_eq!(repository.list_tags().await.unwrap().len(), 1);
    repository.delete_tag("release/v1", feature).await.unwrap();
    assert!(repository.list_tags().await.unwrap().is_empty());
}

#[tokio::test]
async fn multi_delete_moves_the_bucket_head_once() {
    let plane = Arc::new(MemoryObjectPlane::new(false));
    let repository = Repository::initialize(plane.clone(), options("repo-multi-delete"))
        .await
        .unwrap();
    for key in [b"a".to_vec(), b"b".to_vec()] {
        repository
            .put_bytes(
                "main",
                key,
                b"value".to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let before = repository.head("main").await.unwrap();
    let operation = OperationId::new();
    let receipt = repository
        .delete_objects("main", vec![b"a".to_vec(), b"b".to_vec()], Some(operation))
        .await
        .unwrap();
    assert_eq!(receipt.parents, [before]);
    assert_eq!(receipt.changed_keys, 2);
    assert_eq!(receipt.object_versions.len(), 2);
    assert!(matches!(
        load_publication_lease(plane.as_ref(), "repo-multi-delete", operation)
            .await
            .unwrap()
            .unwrap()
            .state,
        PublicationLeaseStateV1::Completed { commit } if commit == receipt.id
    ));
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 4);
    assert_eq!(
        repository.get_current("main", b"a").await.unwrap_err().code,
        ErrorCode::NoSuchKey
    );
    assert_eq!(
        repository.get_current("main", b"b").await.unwrap_err().code,
        ErrorCode::NoSuchKey
    );
}

#[tokio::test]
async fn multipart_parts_are_invisible_until_idempotent_completion() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(plane.clone(), options("repo-multipart"))
        .await
        .unwrap();
    let initial = repository.head("main").await.unwrap();
    let upload = repository
        .create_multipart_upload(
            "main",
            b"large.bin".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let part = repository
        .upload_part_stream(
            upload,
            1,
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"multipart-body".to_vec())
            }),
        )
        .await
        .unwrap();
    assert_eq!(repository.head("main").await.unwrap(), initial);
    assert_eq!(
        repository
            .get_current("main", b"large.bin")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    let operation = OperationId::new();
    let receipt = repository
        .complete_multipart_upload(upload, vec![(1, part.etag.clone())], Some(operation))
        .await
        .unwrap();
    assert_ne!(receipt.id, initial);
    assert!(matches!(
        load_publication_lease(plane.as_ref(), "repo-multipart", operation)
            .await
            .unwrap()
            .unwrap()
            .state,
        PublicationLeaseStateV1::Completed { commit } if commit == receipt.id
    ));
    assert_eq!(
        repository
            .get_current("main", b"large.bin")
            .await
            .unwrap()
            .bytes,
        b"multipart-body"
    );
    let replay = repository
        .complete_multipart_upload(upload, vec![(1, part.etag)], Some(operation))
        .await
        .unwrap();
    assert_eq!(replay.id, receipt.id);

    let aborted = repository
        .create_multipart_upload(
            "main",
            b"aborted".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    repository.abort_multipart_upload(aborted).await.unwrap();
    assert_eq!(
        repository.list_parts(aborted).await.unwrap_err().code,
        ErrorCode::NoSuchUpload
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_multipart_completion_after_ref_acceptance_reconciles() {
    let plane = Arc::new(FaultPlane::new());
    let repository = Arc::new(
        Repository::initialize(plane.clone(), options("repo-multipart-cancel"))
            .await
            .unwrap(),
    );
    let upload = repository
        .create_multipart_upload(
            "main",
            b"large.bin".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let part = repository
        .upload_part_stream(
            upload,
            1,
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"multipart exactly once".to_vec())
            }),
        )
        .await
        .unwrap();
    let operation = OperationId::new();
    plane.arm_pause_after_ref();
    let completing = {
        let repository = repository.clone();
        let etag = part.etag.clone();
        tokio::spawn(async move {
            repository
                .complete_multipart_upload(upload, vec![(1, etag)], Some(operation))
                .await
        })
    };
    plane.wait_until_ref_applied().await;
    completing.abort();
    assert!(completing.await.unwrap_err().is_cancelled());
    let accepted = repository
        .lookup_operation("main", operation)
        .await
        .unwrap()
        .unwrap();
    let replay = repository
        .complete_multipart_upload(upload, vec![(1, part.etag)], Some(operation))
        .await
        .unwrap();
    assert_eq!(replay.id, accepted.id);
    assert!(matches!(
        repository.multipart_upload(upload).await.unwrap().state,
        prolly_s3_core::MultipartStateV1::Completed { receipt, .. }
            if receipt.id == accepted.id
    ));
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 2);
}

#[tokio::test]
async fn durable_workspace_publishes_mixed_mutations_atomically() {
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(plane.clone(), options("repo-workspace"))
        .await
        .unwrap();
    repository
        .put_bytes(
            "main",
            b"remove.txt".to_vec(),
            b"old".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let workspace = repository
        .begin_workspace("main", "atomic asset update", 60_000)
        .await
        .unwrap();
    let base = workspace.base_commit;
    repository
        .workspace_put_stream(
            workspace.id,
            b"a.txt".to_vec(),
            futures_util::stream::once(async { Ok::<_, std::convert::Infallible>(b"A".to_vec()) }),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    repository
        .workspace_put_stream(
            workspace.id,
            b"b.txt".to_vec(),
            futures_util::stream::once(async { Ok::<_, std::convert::Infallible>(b"B".to_vec()) }),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    repository
        .workspace_delete(workspace.id, b"remove.txt".to_vec())
        .await
        .unwrap();
    assert_eq!(repository.head("main").await.unwrap(), base);
    assert_eq!(
        repository
            .resume_workspace(workspace.id)
            .await
            .unwrap()
            .mutations
            .len(),
        3
    );
    let receipt = repository.publish_workspace(workspace.id).await.unwrap();
    assert!(matches!(
        load_publication_lease(plane.as_ref(), "repo-workspace", workspace.operation)
            .await
            .unwrap()
            .unwrap()
            .state,
        PublicationLeaseStateV1::Completed { commit } if commit == receipt.id
    ));
    assert_eq!(receipt.parents, [base]);
    assert_eq!(receipt.changed_keys, 3);
    assert_eq!(
        repository
            .get_current("main", b"a.txt")
            .await
            .unwrap()
            .bytes,
        b"A"
    );
    assert_eq!(
        repository
            .get_current("main", b"b.txt")
            .await
            .unwrap()
            .bytes,
        b"B"
    );
    assert_eq!(
        repository
            .get_current("main", b"remove.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    assert_eq!(
        repository.publish_workspace(workspace.id).await.unwrap().id,
        receipt.id
    );
    let report = repository.fsck().await.unwrap();
    assert_eq!(report.branches, 1);
    assert_eq!(report.commits, 3);
    assert_eq!(report.logical_versions, 4);
    assert!(report.reachable_nodes > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_workspace_after_ref_acceptance_reconciles_to_one_commit() {
    let plane = Arc::new(FaultPlane::new());
    let repository = Arc::new(
        Repository::initialize(plane.clone(), options("repo-workspace-cancel"))
            .await
            .unwrap(),
    );
    let workspace = repository
        .begin_workspace("main", "cancel after accepted CAS", 60_000)
        .await
        .unwrap();
    repository
        .workspace_put_stream(
            workspace.id,
            b"published.txt".to_vec(),
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"exactly once".to_vec())
            }),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();

    plane.arm_pause_after_ref();
    let publishing = {
        let repository = repository.clone();
        tokio::spawn(async move { repository.publish_workspace(workspace.id).await })
    };
    plane.wait_until_ref_applied().await;
    publishing.abort();
    assert!(publishing.await.unwrap_err().is_cancelled());

    let accepted = repository
        .lookup_operation("main", workspace.operation)
        .await
        .unwrap()
        .expect("workspace operation is reachable after accepted ref CAS");
    assert_eq!(repository.head("main").await.unwrap(), accepted.id);
    let reconciled = repository.publish_workspace(workspace.id).await.unwrap();
    assert_eq!(reconciled.id, accepted.id);
    assert!(reconciled.idempotent_replay);
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 2);
    assert!(matches!(
        repository
            .resume_workspace(workspace.id)
            .await
            .unwrap()
            .state,
        prolly_s3_core::WorkspaceStateV1::Completed { receipt, .. }
            if receipt.id == accepted.id
    ));
}
