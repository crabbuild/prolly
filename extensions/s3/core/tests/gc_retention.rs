use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use prolly_s3_core::{
    CompareExchange, CompareExchangeOutcome, DeleteOutcome, Error, ErrorCode, FixedClock,
    GcRunStateV1, GetRequest, ImmutablePut, ImmutablePutOutcome, ListRequest, MemoryObjectPlane,
    ObjectHeaders, ObjectPath, ObjectPlane, PhysicalListPage, PhysicalVersion, Repository,
    RepositoryOptions, Result, StoredMetadata, StoredObject,
};
use sha2::{Digest, Sha256};

#[derive(Clone)]
struct FailNextDeletePlane {
    inner: MemoryObjectPlane,
    fail_next_delete: Arc<AtomicBool>,
}

impl FailNextDeletePlane {
    fn new() -> Self {
        Self {
            inner: MemoryObjectPlane::new(true),
            fail_next_delete: Arc::new(AtomicBool::new(false)),
        }
    }

    fn arm_delete_failure(&self) {
        self.fail_next_delete.store(true, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl ObjectPlane for FailNextDeletePlane {
    async fn get(&self, request: GetRequest) -> Result<Option<StoredObject>> {
        self.inner.get(request).await
    }

    async fn head(&self, path: &ObjectPath) -> Result<Option<StoredMetadata>> {
        self.inner.head(path).await
    }

    async fn put_immutable(&self, request: ImmutablePut) -> Result<ImmutablePutOutcome> {
        self.inner.put_immutable(request).await
    }

    async fn load_mutable(&self, path: &ObjectPath) -> Result<Option<StoredObject>> {
        self.inner.load_mutable(path).await
    }

    async fn compare_exchange(&self, request: CompareExchange) -> Result<CompareExchangeOutcome> {
        self.inner.compare_exchange(request).await
    }

    async fn list(&self, request: ListRequest) -> Result<PhysicalListPage> {
        self.inner.list(request).await
    }

    async fn delete_exact(
        &self,
        path: &ObjectPath,
        version: PhysicalVersion,
    ) -> Result<DeleteOutcome> {
        if self.fail_next_delete.swap(false, Ordering::SeqCst) {
            return Err(Error::new(
                ErrorCode::Transport,
                "injected GC worker failure before physical deletion",
            ));
        }
        self.inner.delete_exact(path, version).await
    }
}

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
                    "gc-resume/chunks/sha256/{ordinal:02}/{ordinal:02}/{}",
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
    assert_eq!(dry_run.candidates_by_kind.get("chunks"), Some(&3));

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
    assert_eq!(final_report.deleted_by_kind.get("chunks"), Some(&3));
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
async fn interrupted_gc_fails_closed_until_generation_checked_operator_abort() {
    let plane = Arc::new(FailNextDeletePlane::new());
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: "gc-fail-closed".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let bytes = b"orphan".to_vec();
    plane
        .put_immutable(ImmutablePut {
            path: ObjectPath::new(format!(
                "gc-fail-closed/chunks/sha256/00/00/{}",
                "00".repeat(32)
            ))
            .unwrap(),
            expected_sha256: Sha256::digest(&bytes).into(),
            bytes,
        })
        .await
        .unwrap();
    let dry_run = repository.plan_gc(2 * 60 * 60 * 1_000, 100).await.unwrap();
    assert_eq!(dry_run.plan.body.candidates.len(), 1);

    plane.arm_delete_failure();
    let failure = repository
        .sweep_gc_batch(dry_run.plan.id, 1)
        .await
        .unwrap_err();
    assert_eq!(failure.code, ErrorCode::Transport);
    let stranded = repository.gc_run(dry_run.plan.id).await.unwrap();
    assert_eq!(stranded.state, GcRunStateV1::Running);
    assert_eq!(stranded.next_index, 0);

    let publication = repository
        .put_bytes(
            "main",
            b"blocked".to_vec(),
            b"body staged before the publication fence".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(publication.code, ErrorCode::PreconditionFailed);

    let aborted = repository
        .abort_gc_run(
            dry_run.plan.id,
            stranded.generation,
            "confirmed worker crash before delete",
        )
        .await
        .unwrap();
    assert_eq!(aborted.state, GcRunStateV1::Aborted);
    assert_eq!(
        aborted.abort_reason.as_deref(),
        Some("confirmed worker crash before delete")
    );
    assert_eq!(aborted.generation, stranded.generation + 1);

    let stale_abort = repository
        .abort_gc_run(dry_run.plan.id, stranded.generation, "stale retry")
        .await
        .unwrap_err();
    assert_eq!(stale_abort.code, ErrorCode::PreconditionFailed);
    let resume_aborted = repository
        .sweep_gc_batch(dry_run.plan.id, 1)
        .await
        .unwrap_err();
    assert_eq!(resume_aborted.code, ErrorCode::PreconditionFailed);

    repository
        .put_bytes(
            "main",
            b"allowed".to_vec(),
            b"publication resumes only after explicit abort".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
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
                    "gc-rate/chunks/sha256/{ordinal:02}/{ordinal:02}/{}",
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
async fn gc_conservatively_preserves_every_native_ref_version() {
    let clock = Arc::new(FixedClock::new(10_000_000));
    let plane = Arc::new(MemoryObjectPlane::new(true));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: "gc-native-refs".to_string(),
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
    let ref_prefix = "gc-native-refs/refs/heads/".to_string();
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

    let orphan_bytes = b"collect me, but not native ref history".to_vec();
    plane
        .put_immutable(ImmutablePut {
            path: ObjectPath::new(format!(
                "gc-native-refs/chunks/sha256/ff/ff/{}",
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
