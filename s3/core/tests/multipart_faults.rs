use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
};

use prolly_s3_core::{
    decode_canonical, CompareExchange, CompareExchangeOutcome, DeleteOutcome, Error, ErrorCode,
    GetRequest, ImmutablePut, ImmutablePutOutcome, ListRequest, MemoryObjectPlane,
    MultipartStateV1, MultipartUploadV1, ObjectHeaders, ObjectPath, ObjectPlane, PhysicalListPage,
    PhysicalVersion, Repository, RepositoryOptions, Result, StoredMetadata, StoredObject,
};

const NONE: u8 = 0;
const ACTIVE: u8 = 1;
const COMPLETING: u8 = 2;
const COMPLETED: u8 = 3;

#[derive(Clone)]
struct MultipartFaultPlane {
    inner: MemoryObjectPlane,
    control: Arc<Control>,
}

struct Control {
    pause_before: AtomicU8,
    ambiguous_after: AtomicU8,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl MultipartFaultPlane {
    fn new() -> Self {
        Self {
            inner: MemoryObjectPlane::new(true),
            control: Arc::new(Control {
                pause_before: AtomicU8::new(NONE),
                ambiguous_after: AtomicU8::new(NONE),
                reached: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            }),
        }
    }

    fn pause_next(&self, state: u8) {
        self.control.pause_before.store(state, Ordering::SeqCst);
    }

    fn make_next_ambiguous(&self, state: u8) {
        self.control.ambiguous_after.store(state, Ordering::SeqCst);
    }

    async fn wait_until_paused(&self) {
        self.control.reached.notified().await;
    }

    fn release(&self) {
        self.control.release.notify_one();
    }

    fn requested_upload_state(request: &CompareExchange) -> Option<u8> {
        if !request.path.as_str().contains("/multipart/uploads/") {
            return None;
        }
        let upload: MultipartUploadV1 = decode_canonical(&request.bytes).ok()?;
        Some(match upload.state {
            MultipartStateV1::Active => ACTIVE,
            MultipartStateV1::Completing { .. } => COMPLETING,
            MultipartStateV1::Completed { .. } => COMPLETED,
            MultipartStateV1::Aborted => NONE,
        })
    }
}

#[async_trait::async_trait]
impl ObjectPlane for MultipartFaultPlane {
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
        let state = Self::requested_upload_state(&request).unwrap_or(NONE);
        if state != NONE
            && self
                .control
                .pause_before
                .compare_exchange(state, NONE, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.control.reached.notify_one();
            self.control.release.notified().await;
        }
        let outcome = self.inner.compare_exchange(request).await?;
        if matches!(outcome, CompareExchangeOutcome::Applied(_))
            && state != NONE
            && self
                .control
                .ambiguous_after
                .compare_exchange(state, NONE, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(Error::new(
                ErrorCode::Transport,
                "injected lost response after multipart CAS",
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

async fn upload_part(
    repository: &Repository<MultipartFaultPlane>,
    upload: prolly_s3_core::UploadId,
    bytes: &'static [u8],
) -> Result<prolly_s3_core::MultipartPartV1> {
    repository
        .upload_part_stream(
            upload,
            1,
            futures_util::stream::once(
                async move { Ok::<_, std::convert::Infallible>(bytes.to_vec()) },
            ),
        )
        .await
}

#[tokio::test]
async fn upload_and_complete_races_freeze_exactly_one_part_root() {
    let plane = Arc::new(MultipartFaultPlane::new());
    let repository = Arc::new(
        Repository::initialize(
            plane.clone(),
            RepositoryOptions {
                repository_prefix: "multipart-races".to_string(),
                ..RepositoryOptions::default()
            },
        )
        .await
        .unwrap(),
    );

    // Completion wins: a paused replacement cannot mutate Completing/Completed.
    let upload = repository
        .create_multipart_upload(
            "main",
            b"completion-wins".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let original = upload_part(&repository, upload, b"original").await.unwrap();
    plane.pause_next(ACTIVE);
    let replacing_repository = repository.clone();
    let replacement =
        tokio::spawn(
            async move { upload_part(&replacing_repository, upload, b"replacement").await },
        );
    plane.wait_until_paused().await;
    repository
        .complete_multipart_upload(upload, vec![(1, original.etag)], None)
        .await
        .unwrap();
    plane.release();
    assert_eq!(
        replacement.await.unwrap().unwrap_err().code,
        ErrorCode::UploadConflict
    );
    assert_eq!(
        repository
            .get_current("main", b"completion-wins")
            .await
            .unwrap()
            .bytes,
        b"original"
    );

    // Part replacement wins: the paused Completing CAS fails, and a retry can
    // explicitly complete the newly selected ETag.
    let upload = repository
        .create_multipart_upload(
            "main",
            b"part-wins".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    let original = upload_part(&repository, upload, b"old").await.unwrap();
    plane.pause_next(COMPLETING);
    let completing_repository = repository.clone();
    let completion = tokio::spawn(async move {
        completing_repository
            .complete_multipart_upload(upload, vec![(1, original.etag)], None)
            .await
    });
    plane.wait_until_paused().await;
    let replacement = upload_part(&repository, upload, b"new").await.unwrap();
    plane.release();
    assert_eq!(
        completion.await.unwrap().unwrap_err().code,
        ErrorCode::UploadConflict
    );
    repository
        .complete_multipart_upload(upload, vec![(1, replacement.etag)], None)
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"part-wins")
            .await
            .unwrap()
            .bytes,
        b"new"
    );
}

#[tokio::test]
async fn lost_multipart_cas_responses_reconcile_without_duplicate_publication() {
    let plane = Arc::new(MultipartFaultPlane::new());
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: "multipart-ambiguous".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let upload = repository
        .create_multipart_upload(
            "main",
            b"ambiguous".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    plane.make_next_ambiguous(ACTIVE);
    let part = upload_part(&repository, upload, b"reconciled")
        .await
        .unwrap();
    plane.make_next_ambiguous(COMPLETED);
    let receipt = repository
        .complete_multipart_upload(upload, vec![(1, part.etag.clone())], None)
        .await
        .unwrap();
    let replay = repository
        .complete_multipart_upload(upload, vec![(1, part.etag)], Some(receipt.operation))
        .await
        .unwrap();
    assert_eq!(replay.id, receipt.id);
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 2);
}
