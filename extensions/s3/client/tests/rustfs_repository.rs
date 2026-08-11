use std::{
    collections::BTreeMap,
    future::Future,
    process::Command,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketVersioningStatus, ChecksumMode, CompletedMultipartUpload, CompletedPart, Delete,
    ObjectIdentifier, VersioningConfiguration,
};
use aws_smithy_runtime_api::client::{orchestrator::HttpResponse, result::SdkError};
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use base64::{engine::general_purpose::STANDARD, Engine as _};
#[cfg(feature = "slatedb-index")]
use futures_util::{StreamExt, TryStreamExt};
use md5::Md5;
use prolly_s3_client::{
    core::{
        decode_canonical, encode_canonical, CanonicalLimits, CommitId, CompareExchange,
        CompareExchangeOutcome, ContentStore, DeleteOutcome, Error, ErrorCode, FixedClock,
        GetRequest, ImmutablePut, ImmutablePutOutcome, ListRequest, MergePolicy, MultipartStateV1,
        MultipartUploadV1, NativeMultipartCompletedPart, ObjectHeaders, ObjectPath, ObjectPlane,
        ObjectVersionKindV1, OperationId, PhysicalListPage, PhysicalVersion, PhysicalVersioning,
        Repository, RepositoryOptions, RepositoryStorageProfile, RetryAdvice, StoredMetadata,
        StoredObject, MAX_LOGICAL_RETRY_LIMIT,
    },
    AwsS3ObjectPlane, Client, HmacAttestationSigner, HmacTokenSigner, ProviderIdentity,
    S3OperationMetrics, S3WireAttemptInterceptor, S3WireAttemptMetrics, WriteOptions,
};
use sha2::{Digest, Sha256};
use tokio::sync::Barrier;

struct RepeatedBody {
    remaining: u64,
    chunk_bytes: usize,
    value: u8,
}

impl http_body::Body for RepeatedBody {
    type Data = bytes::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if self.remaining == 0 {
            return std::task::Poll::Ready(None);
        }
        let len = usize::try_from(self.remaining.min(self.chunk_bytes as u64)).unwrap();
        self.remaining -= len as u64;
        std::task::Poll::Ready(Some(Ok(http_body::Frame::data(bytes::Bytes::from(vec![
            self.value;
            len
        ])))))
    }

    fn size_hint(&self) -> http_body::SizeHint {
        http_body::SizeHint::with_exact(self.remaining)
    }
}

struct PendingBody;

impl http_body::Body for PendingBody {
    type Data = bytes::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        _context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        std::task::Poll::Pending
    }
}

#[derive(Clone)]
struct RestartAfterAcceptedRefPlane {
    inner: AwsS3ObjectPlane,
    container: Arc<str>,
    armed: Arc<AtomicBool>,
    fired: Arc<AtomicBool>,
    restarts: Arc<AtomicU64>,
    restart_millis: Arc<AtomicU64>,
}

impl RestartAfterAcceptedRefPlane {
    fn new(inner: AwsS3ObjectPlane, container: impl Into<Arc<str>>) -> Self {
        Self {
            inner,
            container: container.into(),
            armed: Arc::new(AtomicBool::new(false)),
            fired: Arc::new(AtomicBool::new(false)),
            restarts: Arc::new(AtomicU64::new(0)),
            restart_millis: Arc::new(AtomicU64::new(0)),
        }
    }

    fn arm(&self) {
        self.fired.store(false, Ordering::SeqCst);
        self.armed.store(true, Ordering::SeqCst);
    }

    fn fired(&self) -> bool {
        self.fired.load(Ordering::SeqCst)
    }

    fn restarts(&self) -> u64 {
        self.restarts.load(Ordering::SeqCst)
    }

    fn restart_millis(&self) -> u64 {
        self.restart_millis.load(Ordering::SeqCst)
    }

    fn reset_s3_metrics(&self) -> S3OperationMetrics {
        self.inner.reset_metrics()
    }
}

fn restart_container_and_wait(container: &str) -> std::result::Result<Duration, String> {
    let started = Instant::now();
    let restart = Command::new("docker")
        .args(["restart", container])
        .output()
        .map_err(|error| format!("failed to execute docker restart: {error}"))?;
    if !restart.status.success() {
        return Err(format!(
            "docker restart failed: stdout={} stderr={}",
            String::from_utf8_lossy(&restart.stdout),
            String::from_utf8_lossy(&restart.stderr)
        ));
    }
    for _ in 0..240 {
        let inspect = Command::new("docker")
            .args([
                "inspect",
                "--format",
                "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
                container,
            ])
            .output()
            .map_err(|error| format!("failed to inspect restarted RustFS: {error}"))?;
        if inspect.status.success() {
            let state = String::from_utf8_lossy(&inspect.stdout);
            if matches!(state.trim(), "healthy" | "running") {
                return Ok(started.elapsed());
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    Err("RustFS did not become healthy within 60 seconds".to_string())
}

#[async_trait::async_trait]
impl ObjectPlane for RestartAfterAcceptedRefPlane {
    async fn get(
        &self,
        request: GetRequest,
    ) -> prolly_s3_client::core::Result<Option<StoredObject>> {
        self.inner.get(request).await
    }

    async fn head(
        &self,
        path: &ObjectPath,
    ) -> prolly_s3_client::core::Result<Option<StoredMetadata>> {
        self.inner.head(path).await
    }

    async fn put_immutable(
        &self,
        request: ImmutablePut,
    ) -> prolly_s3_client::core::Result<ImmutablePutOutcome> {
        self.inner.put_immutable(request).await
    }

    async fn load_mutable(
        &self,
        path: &ObjectPath,
    ) -> prolly_s3_client::core::Result<Option<StoredObject>> {
        self.inner.load_mutable(path).await
    }

    async fn compare_exchange(
        &self,
        request: CompareExchange,
    ) -> prolly_s3_client::core::Result<CompareExchangeOutcome> {
        let is_branch_ref = request.path.as_str().contains("/refs/heads/");
        let outcome = self.inner.compare_exchange(request).await?;
        if is_branch_ref
            && matches!(outcome, CompareExchangeOutcome::Applied(_))
            && self.armed.swap(false, Ordering::SeqCst)
        {
            self.fired.store(true, Ordering::SeqCst);
            let restart_started = Instant::now();
            let container = self.container.to_string();
            tokio::task::spawn_blocking(move || restart_container_and_wait(container.as_str()))
                .await
                .map_err(|error| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        format!("RustFS restart task failed: {error}"),
                    )
                })?
                .map_err(|error| Error::new(ErrorCode::Transport, error))?;
            let mut consecutive_s3_ready = 0_u8;
            for _ in 0..240 {
                if self
                    .inner
                    .list(ListRequest {
                        prefix: String::new(),
                        continuation: None,
                        limit: 1,
                        include_versions: false,
                    })
                    .await
                    .is_ok()
                {
                    consecutive_s3_ready += 1;
                    if consecutive_s3_ready == 4 {
                        break;
                    }
                } else {
                    consecutive_s3_ready = 0;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            if consecutive_s3_ready < 4 {
                return Err(Error::new(
                    ErrorCode::Transport,
                    "RustFS container became healthy but its authenticated S3 API did not remain ready for four consecutive probes within 60 seconds",
                ));
            }
            let elapsed = restart_started.elapsed();
            self.restarts.fetch_add(1, Ordering::SeqCst);
            self.restart_millis.store(
                u64::try_from(elapsed.as_millis()).unwrap(),
                Ordering::SeqCst,
            );
            return Err(Error::new(
                ErrorCode::OutcomeUnknown,
                "injected lost response after accepted branch CAS and RustFS restart",
            ));
        }
        Ok(outcome)
    }

    async fn list(&self, request: ListRequest) -> prolly_s3_client::core::Result<PhysicalListPage> {
        self.inner.list(request).await
    }

    async fn delete_exact(
        &self,
        path: &ObjectPath,
        version: PhysicalVersion,
    ) -> prolly_s3_client::core::Result<DeleteOutcome> {
        self.inner.delete_exact(path, version).await
    }
}

#[cfg(feature = "slatedb-index")]
use prolly_s3_client::{AdvisoryIndex, SlateDbAdvisoryIndex};

#[cfg(feature = "slatedb-index")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct AdvisoryObjectStoreMetrics {
    puts: u64,
    multipart_starts: u64,
    gets: u64,
    heads: u64,
    delete_streams: u64,
    lists: u64,
    delimiter_lists: u64,
    copies: u64,
    uploaded_body_bytes: u64,
    returned_body_bytes: u64,
}

#[cfg(feature = "slatedb-index")]
impl AdvisoryObjectStoreMetrics {
    fn total_calls(self) -> u64 {
        self.puts
            .saturating_add(self.multipart_starts)
            .saturating_add(self.gets)
            .saturating_add(self.heads)
            .saturating_add(self.delete_streams)
            .saturating_add(self.lists)
            .saturating_add(self.delimiter_lists)
            .saturating_add(self.copies)
    }

    fn delta_since(self, earlier: Self) -> Self {
        Self {
            puts: self.puts.saturating_sub(earlier.puts),
            multipart_starts: self
                .multipart_starts
                .saturating_sub(earlier.multipart_starts),
            gets: self.gets.saturating_sub(earlier.gets),
            heads: self.heads.saturating_sub(earlier.heads),
            delete_streams: self.delete_streams.saturating_sub(earlier.delete_streams),
            lists: self.lists.saturating_sub(earlier.lists),
            delimiter_lists: self.delimiter_lists.saturating_sub(earlier.delimiter_lists),
            copies: self.copies.saturating_sub(earlier.copies),
            uploaded_body_bytes: self
                .uploaded_body_bytes
                .saturating_sub(earlier.uploaded_body_bytes),
            returned_body_bytes: self
                .returned_body_bytes
                .saturating_sub(earlier.returned_body_bytes),
        }
    }
}

#[cfg(feature = "slatedb-index")]
#[derive(Debug, Default)]
struct AtomicAdvisoryObjectStoreMetrics {
    puts: AtomicU64,
    multipart_starts: AtomicU64,
    gets: AtomicU64,
    heads: AtomicU64,
    delete_streams: AtomicU64,
    lists: AtomicU64,
    delimiter_lists: AtomicU64,
    copies: AtomicU64,
    uploaded_body_bytes: AtomicU64,
    returned_body_bytes: AtomicU64,
}

#[cfg(feature = "slatedb-index")]
#[derive(Clone, Debug)]
struct CountingAdvisoryObjectStore {
    inner: Arc<dyn slatedb::object_store::ObjectStore>,
    metrics: Arc<AtomicAdvisoryObjectStoreMetrics>,
}

#[cfg(feature = "slatedb-index")]
impl CountingAdvisoryObjectStore {
    fn new(inner: Arc<dyn slatedb::object_store::ObjectStore>) -> Self {
        Self {
            inner,
            metrics: Arc::new(AtomicAdvisoryObjectStoreMetrics::default()),
        }
    }

    fn snapshot(&self) -> AdvisoryObjectStoreMetrics {
        AdvisoryObjectStoreMetrics {
            puts: self.metrics.puts.load(Ordering::Relaxed),
            multipart_starts: self.metrics.multipart_starts.load(Ordering::Relaxed),
            gets: self.metrics.gets.load(Ordering::Relaxed),
            heads: self.metrics.heads.load(Ordering::Relaxed),
            delete_streams: self.metrics.delete_streams.load(Ordering::Relaxed),
            lists: self.metrics.lists.load(Ordering::Relaxed),
            delimiter_lists: self.metrics.delimiter_lists.load(Ordering::Relaxed),
            copies: self.metrics.copies.load(Ordering::Relaxed),
            uploaded_body_bytes: self.metrics.uploaded_body_bytes.load(Ordering::Relaxed),
            returned_body_bytes: self.metrics.returned_body_bytes.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "slatedb-index")]
impl std::fmt::Display for CountingAdvisoryObjectStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "counting-advisory-object-store({})", self.inner)
    }
}

#[cfg(feature = "slatedb-index")]
#[async_trait::async_trait]
impl slatedb::object_store::ObjectStore for CountingAdvisoryObjectStore {
    async fn put_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        payload: slatedb::object_store::PutPayload,
        options: slatedb::object_store::PutOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::PutResult> {
        self.metrics.puts.fetch_add(1, Ordering::Relaxed);
        self.metrics.uploaded_body_bytes.fetch_add(
            u64::try_from(payload.content_length()).unwrap(),
            Ordering::Relaxed,
        );
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        options: slatedb::object_store::PutMultipartOptions,
    ) -> slatedb::object_store::Result<Box<dyn slatedb::object_store::MultipartUpload>> {
        self.metrics
            .multipart_starts
            .fetch_add(1, Ordering::Relaxed);
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &slatedb::object_store::path::Path,
        options: slatedb::object_store::GetOptions,
    ) -> slatedb::object_store::Result<slatedb::object_store::GetResult> {
        let head = options.head;
        if head {
            self.metrics.heads.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.gets.fetch_add(1, Ordering::Relaxed);
        }
        let result = self.inner.get_opts(location, options).await?;
        if !head {
            self.metrics.returned_body_bytes.fetch_add(
                result.range.end.saturating_sub(result.range.start),
                Ordering::Relaxed,
            );
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<
            'static,
            slatedb::object_store::Result<slatedb::object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<
        'static,
        slatedb::object_store::Result<slatedb::object_store::path::Path>,
    > {
        self.metrics.delete_streams.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&slatedb::object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<
        'static,
        slatedb::object_store::Result<slatedb::object_store::ObjectMeta>,
    > {
        self.metrics.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&slatedb::object_store::path::Path>,
    ) -> slatedb::object_store::Result<slatedb::object_store::ListResult> {
        self.metrics.delimiter_lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &slatedb::object_store::path::Path,
        to: &slatedb::object_store::path::Path,
        options: slatedb::object_store::CopyOptions,
    ) -> slatedb::object_store::Result<()> {
        self.metrics.copies.fetch_add(1, Ordering::Relaxed);
        self.inner.copy_opts(from, to, options).await
    }
}

fn rustfs_enabled() -> bool {
    std::env::var("PROLLY_S3_RUSTFS").as_deref() == Ok("1")
}

fn service_error_code<E, R>(error: &SdkError<E, R>) -> Option<&str>
where
    E: ProvideErrorMetadata,
{
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
}

fn assert_service_code_or_status<E>(
    error: &SdkError<E, HttpResponse>,
    expected_code: &str,
    expected_status: u16,
) where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let code = service_error_code(error);
    let status = error
        .raw_response()
        .map(|response| response.status().as_u16());
    assert!(
        code == Some(expected_code) || status == Some(expected_status),
        "native S3 error mismatch: code={code:?}, status={status:?}, error={error:?}"
    );
}

fn assert_permission_denied<E>(error: &SdkError<E, HttpResponse>)
where
    E: ProvideErrorMetadata + std::fmt::Debug,
{
    let code = service_error_code(error);
    let status = error
        .raw_response()
        .map(|response| response.status().as_u16());
    assert!(
        matches!(
            code,
            Some("AccessDenied" | "InvalidAccessKeyId" | "SignatureDoesNotMatch")
        ) || status == Some(403),
        "expected a permission failure: code={code:?}, status={status:?}, error={error:?}"
    );
}

fn completion_input_digest(requested: &[(u32, String)]) -> [u8; 32] {
    let encoded = encode_canonical(&requested).unwrap();
    let domain = b"prolly-s3/operation-input/v1";
    let parts: [&[u8]; 2] = [b"complete-multipart", &encoded];
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u32).to_be_bytes());
    hasher.update(domain);
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_completing_upload_process_helper() {
    if std::env::var("PROLLY_S3_COMPLETING_HELPER").as_deref() != Ok("1") {
        return;
    }
    let (aws, bucket) = rustfs_client().await;
    let prefix = std::env::var("PROLLY_S3_HELPER_PREFIX").unwrap();
    let key = std::env::var("PROLLY_S3_HELPER_KEY").unwrap();
    let upload = std::env::var("PROLLY_S3_HELPER_UPLOAD").unwrap();
    let etag = std::env::var("PROLLY_S3_HELPER_ETAG").unwrap();
    let operation =
        OperationId::from_str(&std::env::var("PROLLY_S3_HELPER_OPERATION").unwrap()).unwrap();
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(prefix)
        .writer("independent-completion-helper")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .open()
        .await
        .unwrap();
    let completed = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .upload_id(upload)
        .operation_id(operation)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(completed.commit.unwrap().operation, operation);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_completing_upload_resumes_in_independent_process() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, bucket) = rustfs_client().await;
    let prefix = unique_prefix("multipart-process-resume");
    let key = "process/resume.bin";
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&prefix)
        .writer("completion-parent")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    let created = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    let upload_text = created.upload_id().unwrap().to_string();
    let uploaded = client
        .upload_part()
        .bucket(&bucket)
        .key(key)
        .upload_id(&upload_text)
        .part_number(1)
        .body(ByteStream::from_static(b"resume from another process"))
        .send()
        .await
        .unwrap();
    let etag = uploaded.e_tag().unwrap().to_string();
    let operation = OperationId::new();
    let internal_key = format!(
        "{prefix}/multipart/uploads/{}",
        upload_text.strip_prefix("pu1_").unwrap()
    );
    let current = aws
        .get_object()
        .bucket(&bucket)
        .key(&internal_key)
        .send()
        .await
        .unwrap();
    let physical_etag = current.e_tag().unwrap().to_string();
    let bytes = current.body.collect().await.unwrap().into_bytes();
    let mut manifest: MultipartUploadV1 = decode_canonical(&bytes).unwrap();
    manifest.state = MultipartStateV1::Completing {
        operation,
        request_digest: completion_input_digest(&[(1, etag.clone())]),
    };
    manifest.generation += 1;
    manifest.updated_at_millis = current_millis();
    aws.put_object()
        .bucket(&bucket)
        .key(&internal_key)
        .if_match(physical_etag)
        .body(ByteStream::from(encode_canonical(&manifest).unwrap()))
        .send()
        .await
        .unwrap();

    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "rustfs_completing_upload_process_helper",
            "--nocapture",
        ])
        .env("PROLLY_S3_COMPLETING_HELPER", "1")
        .env("PROLLY_S3_HELPER_PREFIX", &prefix)
        .env("PROLLY_S3_HELPER_KEY", key)
        .env("PROLLY_S3_HELPER_UPLOAD", &upload_text)
        .env("PROLLY_S3_HELPER_ETAG", &etag)
        .env("PROLLY_S3_HELPER_OPERATION", operation.to_string())
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "independent completion helper failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let body = client
        .get_object()
        .bucket(&bucket)
        .key(key)
        .send()
        .await
        .unwrap()
        .output
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(body.as_ref(), b"resume from another process");
    client.fsck().await.unwrap();
    if let (Ok(run_id), Ok(iteration)) = (
        std::env::var("PROLLY_S3_SOAK_RUN_ID"),
        std::env::var("PROLLY_S3_SOAK_ITERATION"),
    ) {
        let physical_storage_bytes = physical_storage_bytes(aws.clone(), &bucket, &prefix).await;
        eprintln!(
            "SOAK_WORKFLOW run_id={run_id} iteration={iteration} name=multipart-recovery physical_storage_bytes={physical_storage_bytes} final_fsck=ok"
        );
        let deleted_versions =
            delete_all_physical_versions_for_prefix(aws.clone(), &bucket, &prefix).await;
        eprintln!(
            "SOAK_CLEANUP run_id={run_id} iteration={iteration} name=multipart-recovery deleted_versions={deleted_versions} remaining_versions=0"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_ref_contention_process_helper() {
    let Ok(role) = std::env::var("PROLLY_S3_CONTENTION_ROLE") else {
        return;
    };
    let (aws, bucket) = rustfs_client().await;
    let prefix = std::env::var("PROLLY_S3_CONTENTION_PREFIX").unwrap();
    let target =
        CommitId::from_str(&std::env::var("PROLLY_S3_CONTENTION_TARGET").unwrap()).unwrap();
    let name = std::env::var("PROLLY_S3_CONTENTION_NAME").unwrap_or_default();
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(prefix)
        .writer(format!("contention-process-{}", std::process::id()))
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .open()
        .await
        .unwrap();
    let result = match role.as_str() {
        "branch" => client.create_branch(name, Some(target)).await.map(|_| ()),
        "tag" => client.create_tag(name, target).await.map(|_| ()),
        "merge" => client
            .merge(
                target,
                None,
                MergePolicy::Fail,
                None,
                Some("independent process merge".to_string()),
            )
            .await
            .map(|_| ()),
        other => panic!("unknown contention helper role {other}"),
    };
    match result {
        Ok(()) => println!("CONTENTION_RESULT=ok"),
        Err(error) => println!("CONTENTION_RESULT=err:{:?}", error.code),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_branch_tag_and_merge_contend_across_independent_processes() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, bucket) = rustfs_client().await;
    let prefix = unique_prefix("independent-ref-contention");
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&prefix)
        .writer("contention-parent")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    let root = client.head_commit().await.unwrap();
    client.create_branch("source-a", Some(root)).await.unwrap();
    client.create_branch("source-b", Some(root)).await.unwrap();
    let source_a = client.on_branch("source-a").unwrap();
    let source_b = client.on_branch("source-b").unwrap();
    source_a
        .put_object()
        .bucket(&bucket)
        .key("from-a")
        .body(ByteStream::from_static(b"a"))
        .send()
        .await
        .unwrap();
    source_b
        .put_object()
        .bucket(&bucket)
        .key("from-b")
        .body(ByteStream::from_static(b"b"))
        .send()
        .await
        .unwrap();
    let targets = [
        source_a.head_commit().await.unwrap(),
        source_b.head_commit().await.unwrap(),
    ];

    let run_round = |role: &'static str, name: &'static str, round_targets: Vec<CommitId>| {
        let handles = round_targets
            .into_iter()
            .map(|target| {
                let prefix = prefix.clone();
                std::thread::spawn(move || {
                    Command::new(std::env::current_exe().unwrap())
                        .args([
                            "--exact",
                            "rustfs_ref_contention_process_helper",
                            "--nocapture",
                        ])
                        .env("PROLLY_S3_CONTENTION_ROLE", role)
                        .env("PROLLY_S3_CONTENTION_PREFIX", prefix)
                        .env("PROLLY_S3_CONTENTION_NAME", name)
                        .env("PROLLY_S3_CONTENTION_TARGET", target.to_string())
                        .output()
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| {
                let output = handle.join().unwrap();
                assert!(
                    output.status.success(),
                    "contention helper failed:\nstdout:\n{}\nstderr:\n{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                String::from_utf8_lossy(&output.stdout).into_owned()
            })
            .collect::<Vec<_>>()
    };

    let branch_results = run_round(
        "branch",
        "race-branch",
        (0..8).map(|index| targets[index % 2]).collect(),
    );
    assert_eq!(
        branch_results
            .iter()
            .filter(|output| output.contains("CONTENTION_RESULT=ok"))
            .count(),
        1
    );
    assert!(branch_results.iter().all(|output| {
        output.contains("CONTENTION_RESULT=ok")
            || output.contains("CONTENTION_RESULT=err:RefConflict")
    }));
    let branch = client
        .list_branches()
        .await
        .unwrap()
        .into_iter()
        .find(|branch| branch.name == "race-branch")
        .unwrap();
    assert!(targets.contains(&branch.target));

    let tag_results = run_round(
        "tag",
        "race-tag",
        (0..8).map(|index| targets[index % 2]).collect(),
    );
    assert_eq!(
        tag_results
            .iter()
            .filter(|output| output.contains("CONTENTION_RESULT=ok"))
            .count(),
        1
    );
    assert!(tag_results.iter().all(|output| {
        output.contains("CONTENTION_RESULT=ok")
            || output.contains("CONTENTION_RESULT=err:RefConflict")
    }));
    let tag = client
        .list_tags()
        .await
        .unwrap()
        .into_iter()
        .find(|tag| tag.name == "race-tag")
        .unwrap();
    assert!(targets.contains(&tag.target));

    let merge_results = run_round("merge", "", targets.to_vec());
    assert!(merge_results
        .iter()
        .any(|output| output.contains("CONTENTION_RESULT=ok")));
    assert!(merge_results.iter().all(|output| {
        output.contains("CONTENTION_RESULT=ok")
            || output.contains("CONTENTION_RESULT=err:RefConflict")
    }));
    for (key, target) in [("from-a", targets[0]), ("from-b", targets[1])] {
        if client
            .head_object()
            .bucket(&bucket)
            .key(key)
            .send()
            .await
            .is_err()
        {
            client
                .merge(
                    target,
                    None,
                    MergePolicy::Fail,
                    None,
                    Some(format!("reconcile {key}")),
                )
                .await
                .unwrap();
        }
    }
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("from-a")
            .send()
            .await
            .unwrap()
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .as_ref(),
        b"a"
    );
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("from-b")
            .send()
            .await
            .unwrap()
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes()
            .as_ref(),
        b"b"
    );
    client.fsck().await.unwrap();
    if let (Ok(run_id), Ok(iteration)) = (
        std::env::var("PROLLY_S3_SOAK_RUN_ID"),
        std::env::var("PROLLY_S3_SOAK_ITERATION"),
    ) {
        let physical_storage_bytes = physical_storage_bytes(aws.clone(), &bucket, &prefix).await;
        eprintln!(
            "SOAK_WORKFLOW run_id={run_id} iteration={iteration} name=ref-contention physical_storage_bytes={physical_storage_bytes} final_fsck=ok"
        );
        let deleted_versions =
            delete_all_physical_versions_for_prefix(aws.clone(), &bucket, &prefix).await;
        eprintln!(
            "SOAK_CLEANUP run_id={run_id} iteration={iteration} name=ref-contention deleted_versions={deleted_versions} remaining_versions=0"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_native_s3_differential_matrix() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run the RustFS differential matrix");
        return;
    }
    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_prefix("native-differential-repository");
    let native_prefix = format!("{}/", unique_prefix("native-differential-raw"));
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(repository_prefix)
        .writer("native-differential")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .token_signer(Arc::new(
            HmacTokenSigner::single("native-differential-v1", vec![0x31; 32]).unwrap(),
        ))
        .initialize()
        .await
        .unwrap();

    let payload = b"abcdef";
    let checksum = STANDARD.encode(Sha256::digest(payload));
    let native_key = format!("{native_prefix}object.bin");
    let native_put = aws
        .put_object()
        .bucket(&bucket)
        .key(&native_key)
        .if_none_match("*")
        .checksum_sha256(&checksum)
        .body(ByteStream::from_static(payload))
        .send()
        .await
        .unwrap();
    let native_etag = native_put.e_tag().unwrap().to_string();
    let adapted_put = client
        .put_object()
        .bucket(&bucket)
        .key("object.bin")
        .if_none_match("*")
        .checksum_sha256(&checksum)
        .body(ByteStream::from_static(payload))
        .send()
        .await
        .unwrap();
    let adapted_etag = adapted_put.output.e_tag().unwrap().to_string();
    assert_eq!(native_etag, adapted_etag);
    assert_eq!(native_put.checksum_sha256(), Some(checksum.as_str()));
    assert_eq!(
        adapted_put.output.checksum_sha256(),
        Some(checksum.as_str())
    );

    for (range, expected) in [
        ("bytes=1-3", &b"bcd"[..]),
        ("bytes=2-", &b"cdef"[..]),
        ("bytes=-2", &b"ef"[..]),
    ] {
        let native = aws
            .get_object()
            .bucket(&bucket)
            .key(&native_key)
            .if_match(&native_etag)
            .range(range)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        let adapted = client
            .get_object()
            .bucket(&bucket)
            .key("object.bin")
            .if_match(&adapted_etag)
            .range(range)
            .send()
            .await
            .unwrap()
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(native.as_ref(), expected);
        assert_eq!(adapted.as_ref(), expected);
    }

    let native_modified = aws
        .head_object()
        .bucket(&bucket)
        .key(&native_key)
        .send()
        .await
        .unwrap()
        .last_modified
        .unwrap();
    let adapted_modified = *client
        .head_object()
        .bucket(&bucket)
        .key("object.bin")
        .send()
        .await
        .unwrap()
        .output
        .last_modified()
        .unwrap();
    let native_not_modified = aws
        .get_object()
        .bucket(&bucket)
        .key(&native_key)
        .if_modified_since(native_modified)
        .send()
        .await
        .unwrap_err();
    assert_service_code_or_status(&native_not_modified, "NotModified", 304);
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("object.bin")
            .if_modified_since(adapted_modified)
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotModified
    );

    let native_precondition_first = aws
        .get_object()
        .bucket(&bucket)
        .key(&native_key)
        .if_match("\"different\"")
        .range("bytes=999-1000")
        .send()
        .await
        .unwrap_err();
    // RustFS beta.10 evaluates Range first here and returns 416. The adapter
    // intentionally follows RFC 9110/AWS semantics: preconditions precede
    // Range, so the equivalent logical request returns 412.
    assert_service_code_or_status(&native_precondition_first, "InvalidRange", 416);
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("object.bin")
            .if_match("\"different\"")
            .range("bytes=999-1000")
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::PreconditionFailed
    );
    let native_invalid_range = aws
        .get_object()
        .bucket(&bucket)
        .key(&native_key)
        .if_match(&native_etag)
        .range("bytes=999-1000")
        .send()
        .await
        .unwrap_err();
    assert_service_code_or_status(&native_invalid_range, "InvalidRange", 416);
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("object.bin")
            .if_match(&adapted_etag)
            .range("bytes=999-1000")
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidRange
    );

    for relative in [
        "list/a.txt",
        "list/dir/x.txt",
        "list/dir/y.txt",
        "list/z.txt",
    ] {
        aws.put_object()
            .bucket(&bucket)
            .key(format!("{native_prefix}{relative}"))
            .body(ByteStream::from_static(b"listing"))
            .send()
            .await
            .unwrap();
        client
            .put_object()
            .bucket(&bucket)
            .key(relative)
            .body(ByteStream::from_static(b"listing"))
            .send()
            .await
            .unwrap();
    }
    let native_list_prefix = format!("{native_prefix}list/");
    let native_page = aws
        .list_objects_v2()
        .bucket(&bucket)
        .prefix(&native_list_prefix)
        .delimiter("/")
        .max_keys(100)
        .send()
        .await
        .unwrap();
    let adapted_page = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("list/")
        .delimiter("/")
        .max_keys(100)
        .send()
        .await
        .unwrap();
    let native_contents = native_page
        .contents()
        .iter()
        .map(|object| {
            object
                .key()
                .unwrap()
                .strip_prefix(&native_prefix)
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    let adapted_contents = adapted_page
        .output
        .contents()
        .iter()
        .map(|object| object.key().unwrap().to_string())
        .collect::<Vec<_>>();
    let native_prefixes = native_page
        .common_prefixes()
        .iter()
        .map(|prefix| {
            prefix
                .prefix()
                .unwrap()
                .strip_prefix(&native_prefix)
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    let adapted_prefixes = adapted_page
        .output
        .common_prefixes()
        .iter()
        .map(|prefix| prefix.prefix().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(native_contents, adapted_contents);
    assert_eq!(native_prefixes, adapted_prefixes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_ordinary_throughput_probe() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_BENCHMARK").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_BENCHMARK=1 to run the throughput probe");
        return;
    }
    const OPERATIONS: usize = 20;
    const PAYLOAD_BYTES: usize = 64 * 1024;
    let (aws, bucket, wire_metrics) = rustfs_client_with_wire_metrics().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_prefix("ordinary-throughput"))
        .writer("ordinary-throughput-probe")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    let payload = vec![0x6d; PAYLOAD_BYTES];
    client.reset_s3_operation_metrics();
    wire_metrics.reset();

    let write_started = Instant::now();
    for ordinal in 0..OPERATIONS {
        client
            .put_object()
            .bucket(&bucket)
            .key(format!("throughput/{ordinal:04}.bin"))
            .body(ByteStream::from(payload.clone()))
            .send()
            .await
            .unwrap();
    }
    let write_seconds = write_started.elapsed().as_secs_f64();
    let write_metrics = client.reset_s3_operation_metrics();
    let write_wire_metrics = wire_metrics.reset();

    let read_started = Instant::now();
    let mut read_bytes = 0usize;
    for ordinal in 0..OPERATIONS {
        let bytes = client
            .get_object()
            .bucket(&bucket)
            .key(format!("throughput/{ordinal:04}.bin"))
            .send()
            .await
            .unwrap()
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(bytes.as_ref(), payload.as_slice());
        read_bytes += bytes.len();
    }
    let read_seconds = read_started.elapsed().as_secs_f64();
    let read_metrics = client.reset_s3_operation_metrics();
    let read_wire_metrics = wire_metrics.reset();
    assert_eq!(read_bytes, OPERATIONS * PAYLOAD_BYTES);
    assert!(write_metrics.uploaded_body_bytes >= (OPERATIONS * PAYLOAD_BYTES) as u64);
    assert!(read_metrics.downloaded_body_bytes >= (OPERATIONS * PAYLOAD_BYTES) as u64);
    assert_eq!(write_wire_metrics.executions, write_metrics.total_calls());
    assert_eq!(read_wire_metrics.executions, read_metrics.total_calls());
    assert!(write_wire_metrics.transmissions >= write_wire_metrics.executions);
    assert!(read_wire_metrics.transmissions >= read_wire_metrics.executions);
    eprintln!(
        "ORDINARY_PROBE operations={OPERATIONS} payload_bytes={PAYLOAD_BYTES} write_seconds={write_seconds:.3} write_ops_per_second={:.3} read_seconds={read_seconds:.3} read_ops_per_second={:.3} write_sdk_calls={} write_wire_transmissions={} write_wire_retries={} write_get={} write_head={} write_put={} write_list={} write_list_versions={} write_delete={} write_uploaded_bytes={} write_downloaded_bytes={} read_sdk_calls={} read_wire_transmissions={} read_wire_retries={} read_get={} read_head={} read_put={} read_list={} read_list_versions={} read_delete={} read_uploaded_bytes={} read_downloaded_bytes={}",
        OPERATIONS as f64 / write_seconds,
        OPERATIONS as f64 / read_seconds,
        write_metrics.total_calls(),
        write_wire_metrics.transmissions,
        write_wire_metrics.retry_transmissions(),
        write_metrics.get_object,
        write_metrics.head_object,
        write_metrics.put_object,
        write_metrics.list_objects_v2,
        write_metrics.list_object_versions,
        write_metrics.delete_object,
        write_metrics.uploaded_body_bytes,
        write_metrics.downloaded_body_bytes,
        read_metrics.total_calls(),
        read_wire_metrics.transmissions,
        read_wire_metrics.retry_transmissions(),
        read_metrics.get_object,
        read_metrics.head_object,
        read_metrics.put_object,
        read_metrics.list_objects_v2,
        read_metrics.list_object_versions,
        read_metrics.delete_object,
        read_metrics.uploaded_body_bytes,
        read_metrics.downloaded_body_bytes,
    );
}

async fn physical_storage_bytes(aws: aws_sdk_s3::Client, bucket: &str, prefix: &str) -> u64 {
    physical_version_snapshot(aws, bucket, prefix)
        .await
        .into_iter()
        .map(|entry| entry.4)
        .sum()
}

async fn delete_all_physical_versions_for_prefix(
    aws: aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> usize {
    let plane = AwsS3ObjectPlane::new(aws, bucket);
    let list_prefix = format!("{prefix}/");
    let mut continuation = None;
    let mut entries = Vec::new();
    loop {
        let page = plane
            .list(ListRequest {
                prefix: list_prefix.clone(),
                continuation,
                limit: 1_000,
                include_versions: true,
            })
            .await
            .unwrap();
        entries.extend(page.entries);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    let deleted_versions = entries.len();
    for entry in entries {
        let token = entry.metadata.token.clone();
        let version = match token.version_id.clone() {
            Some(version_id) => PhysicalVersion::Versioned { version_id },
            None => PhysicalVersion::Unversioned { token: Some(token) },
        };
        assert_eq!(
            plane.delete_exact(&entry.path, version).await.unwrap(),
            DeleteOutcome::Deleted
        );
    }
    let remaining = plane
        .list(ListRequest {
            prefix: list_prefix,
            continuation: None,
            limit: 1,
            include_versions: true,
        })
        .await
        .unwrap();
    assert!(remaining.entries.is_empty());
    assert!(remaining.continuation.is_none());
    deleted_versions
}

async fn physical_storage_bytes_for_prefixes(
    aws: aws_sdk_s3::Client,
    bucket: &str,
    prefixes: &[&str],
) -> u64 {
    let mut total = 0_u64;
    for prefix in prefixes {
        total = total.saturating_add(physical_storage_bytes(aws.clone(), bucket, prefix).await);
    }
    total
}

fn combine_s3_metrics(left: S3OperationMetrics, right: S3OperationMetrics) -> S3OperationMetrics {
    S3OperationMetrics {
        get_object: left.get_object.saturating_add(right.get_object),
        head_object: left.head_object.saturating_add(right.head_object),
        put_object: left.put_object.saturating_add(right.put_object),
        copy_object: left.copy_object.saturating_add(right.copy_object),
        list_objects_v2: left.list_objects_v2.saturating_add(right.list_objects_v2),
        list_object_versions: left
            .list_object_versions
            .saturating_add(right.list_object_versions),
        delete_object: left.delete_object.saturating_add(right.delete_object),
        create_multipart_upload: left
            .create_multipart_upload
            .saturating_add(right.create_multipart_upload),
        upload_part: left.upload_part.saturating_add(right.upload_part),
        upload_part_copy: left.upload_part_copy.saturating_add(right.upload_part_copy),
        complete_multipart_upload: left
            .complete_multipart_upload
            .saturating_add(right.complete_multipart_upload),
        abort_multipart_upload: left
            .abort_multipart_upload
            .saturating_add(right.abort_multipart_upload),
        list_parts: left.list_parts.saturating_add(right.list_parts),
        list_multipart_uploads: left
            .list_multipart_uploads
            .saturating_add(right.list_multipart_uploads),
        uploaded_body_bytes: left
            .uploaded_body_bytes
            .saturating_add(right.uploaded_body_bytes),
        downloaded_body_bytes: left
            .downloaded_body_bytes
            .saturating_add(right.downloaded_body_bytes),
    }
}

fn report_operation_cost(
    operation: &str,
    logical_bytes: u64,
    elapsed: Duration,
    storage_before: u64,
    storage_after: u64,
    sdk: S3OperationMetrics,
    wire: S3WireAttemptMetrics,
) {
    assert!(wire.executions >= sdk.total_calls());
    assert!(wire.transmissions >= wire.executions);
    let unclassified_wire_executions = wire.executions - sdk.total_calls();
    let stored_delta = i128::from(storage_after) - i128::from(storage_before);
    let upload_amplification = if logical_bytes == 0 {
        0.0
    } else {
        sdk.uploaded_body_bytes as f64 / logical_bytes as f64
    };
    let download_amplification = if logical_bytes == 0 {
        0.0
    } else {
        sdk.downloaded_body_bytes as f64 / logical_bytes as f64
    };
    eprintln!(
        "OPERATION_COST operation={operation} logical_bytes={logical_bytes} elapsed_ms={:.3} storage_before_bytes={storage_before} storage_after_bytes={storage_after} stored_delta_bytes={stored_delta} sdk_calls={} unclassified_wire_executions={unclassified_wire_executions} wire_transmissions={} wire_retries={} get={} head={} put={} list={} list_versions={} delete={} uploaded_bytes={} downloaded_bytes={} upload_amplification={upload_amplification:.6} download_amplification={download_amplification:.6}",
        elapsed.as_secs_f64() * 1_000.0,
        sdk.total_calls(),
        wire.transmissions,
        wire.retry_transmissions(),
        sdk.get_object,
        sdk.head_object,
        sdk.put_object,
        sdk.list_objects_v2,
        sdk.list_object_versions,
        sdk.delete_object,
        sdk.uploaded_body_bytes,
        sdk.downloaded_body_bytes,
    );
}

struct OperationCostContext<'a> {
    client: &'a Client,
    wire: &'a S3WireAttemptInterceptor,
    accounting_aws: &'a aws_sdk_s3::Client,
    bucket: &'a str,
    prefix: &'a str,
}

async fn measure_operation_cost<T, F, Fut>(
    context: &OperationCostContext<'_>,
    operation: &str,
    logical_bytes: u64,
    execute: F,
) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let storage_before = physical_storage_bytes(
        context.accounting_aws.clone(),
        context.bucket,
        context.prefix,
    )
    .await;
    context.client.reset_s3_operation_metrics();
    context.wire.reset();
    let started = Instant::now();
    let output = execute().await;
    let elapsed = started.elapsed();
    let sdk = context.client.reset_s3_operation_metrics();
    let wire = context.wire.reset();
    assert_eq!(wire.executions, sdk.total_calls());
    let storage_after = physical_storage_bytes(
        context.accounting_aws.clone(),
        context.bucket,
        context.prefix,
    )
    .await;
    report_operation_cost(
        operation,
        logical_bytes,
        elapsed,
        storage_before,
        storage_after,
        sdk,
        wire,
    );
    output
}

struct PlaneOperationCostContext<'a> {
    plane: &'a AwsS3ObjectPlane,
    wire: &'a S3WireAttemptInterceptor,
    accounting_aws: &'a aws_sdk_s3::Client,
    bucket: &'a str,
    prefix: &'a str,
}

async fn measure_plane_operation_cost<T, F, Fut>(
    context: &PlaneOperationCostContext<'_>,
    operation: &str,
    execute: F,
) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let storage_before = physical_storage_bytes(
        context.accounting_aws.clone(),
        context.bucket,
        context.prefix,
    )
    .await;
    context.plane.reset_metrics();
    context.wire.reset();
    let started = Instant::now();
    let output = execute().await;
    let elapsed = started.elapsed();
    let sdk = context.plane.reset_metrics();
    let wire = context.wire.reset();
    assert_eq!(wire.executions, sdk.total_calls());
    let storage_after = physical_storage_bytes(
        context.accounting_aws.clone(),
        context.bucket,
        context.prefix,
    )
    .await;
    report_operation_cost(
        operation,
        0,
        elapsed,
        storage_before,
        storage_after,
        sdk,
        wire,
    );
    output
}

struct MultiClientOperationCostContext<'a> {
    clients: &'a [&'a Client],
    wire: &'a S3WireAttemptInterceptor,
    accounting_aws: &'a aws_sdk_s3::Client,
    bucket: &'a str,
    prefixes: &'a [&'a str],
}

async fn measure_multi_client_operation_cost<T, F, Fut>(
    context: &MultiClientOperationCostContext<'_>,
    operation: &str,
    execute: F,
) -> T
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
{
    let storage_before = physical_storage_bytes_for_prefixes(
        context.accounting_aws.clone(),
        context.bucket,
        context.prefixes,
    )
    .await;
    for client in context.clients {
        client.reset_s3_operation_metrics();
    }
    context.wire.reset();
    let started = Instant::now();
    let output = execute().await;
    let elapsed = started.elapsed();
    let sdk = context
        .clients
        .iter()
        .fold(S3OperationMetrics::default(), |total, client| {
            combine_s3_metrics(total, client.reset_s3_operation_metrics())
        });
    let wire = context.wire.reset();
    assert_eq!(wire.executions, sdk.total_calls());
    let storage_after = physical_storage_bytes_for_prefixes(
        context.accounting_aws.clone(),
        context.bucket,
        context.prefixes,
    )
    .await;
    report_operation_cost(
        operation,
        0,
        elapsed,
        storage_before,
        storage_after,
        sdk,
        wire,
    );
    output
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_s3_shaped_operation_cost_matrix() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_COST_MATRIX").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_COST_MATRIX=1 to run the cost matrix");
        return;
    }
    const PAYLOAD_BYTES: usize = 64 * 1024;
    let (aws, bucket, wire) = rustfs_client_with_wire_metrics().await;
    let (accounting_aws, accounting_bucket) = rustfs_client().await;
    assert_eq!(accounting_bucket, bucket);
    let prefix = unique_prefix("operation-cost");
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&prefix)
        .writer("operation-cost-probe")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .token_signer(Arc::new(
            HmacTokenSigner::single("operation-cost-v1", vec![0x27; 32]).unwrap(),
        ))
        .initialize()
        .await
        .unwrap();
    let payload = vec![0x4d; PAYLOAD_BYTES];
    let cost = OperationCostContext {
        client: &client,
        wire: &wire,
        accounting_aws: &accounting_aws,
        bucket: &bucket,
        prefix: &prefix,
    };

    let put = measure_operation_cost(&cost, "put_object", PAYLOAD_BYTES as u64, || async {
        client
            .put_object()
            .bucket(&bucket)
            .key("cost/source.bin")
            .body(ByteStream::from(payload.clone()))
            .send()
            .await
            .unwrap()
    })
    .await;
    let source_version = put.output.version_id().unwrap().to_string();

    measure_operation_cost(&cost, "head_object", 0, || async {
        client
            .head_object()
            .bucket(&bucket)
            .key("cost/source.bin")
            .send()
            .await
            .unwrap();
    })
    .await;
    measure_operation_cost(&cost, "get_object", PAYLOAD_BYTES as u64, || async {
        let bytes = client
            .get_object()
            .bucket(&bucket)
            .key("cost/source.bin")
            .send()
            .await
            .unwrap()
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes();
        assert_eq!(bytes.as_ref(), payload.as_slice());
    })
    .await;
    measure_operation_cost(
        &cost,
        "get_object_version",
        PAYLOAD_BYTES as u64,
        || async {
            client
                .get_object()
                .bucket(&bucket)
                .key("cost/source.bin")
                .version_id(&source_version)
                .send()
                .await
                .unwrap()
                .output
                .body
                .collect()
                .await
                .unwrap();
        },
    )
    .await;
    measure_operation_cost(&cost, "list_objects_v2", 0, || async {
        client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("cost/")
            .send()
            .await
            .unwrap();
    })
    .await;
    measure_operation_cost(&cost, "list_object_versions", 0, || async {
        client
            .list_object_versions()
            .bucket(&bucket)
            .prefix("cost/")
            .send()
            .await
            .unwrap();
    })
    .await;

    measure_operation_cost(&cost, "copy_object", 0, || async {
        client
            .copy_object()
            .bucket(&bucket)
            .key("cost/copied.bin")
            .copy_source(format!("{bucket}/cost/source.bin"))
            .send()
            .await
            .unwrap();
    })
    .await;
    measure_operation_cost(&cost, "delete_object", 0, || async {
        client
            .delete_object()
            .bucket(&bucket)
            .key("cost/copied.bin")
            .send()
            .await
            .unwrap();
    })
    .await;

    for key in ["cost/multi-a.bin", "cost/multi-b.bin"] {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"multi-delete-seed"))
            .send()
            .await
            .unwrap();
    }
    measure_operation_cost(&cost, "delete_objects", 0, || async {
        client
            .delete_objects()
            .bucket(&bucket)
            .delete(
                Delete::builder()
                    .objects(
                        ObjectIdentifier::builder()
                            .key("cost/multi-a.bin")
                            .build()
                            .unwrap(),
                    )
                    .objects(
                        ObjectIdentifier::builder()
                            .key("cost/multi-b.bin")
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .send()
            .await
            .unwrap();
    })
    .await;

    let upload = measure_operation_cost(&cost, "create_multipart_upload", 0, || async {
        client
            .create_multipart_upload()
            .bucket(&bucket)
            .key("cost/multipart.bin")
            .send()
            .await
            .unwrap()
    })
    .await;
    let upload_id = upload.upload_id().unwrap().to_string();
    let part = measure_operation_cost(&cost, "upload_part", PAYLOAD_BYTES as u64, || async {
        client
            .upload_part()
            .bucket(&bucket)
            .key("cost/multipart.bin")
            .upload_id(&upload_id)
            .part_number(1)
            .body(ByteStream::from(payload.clone()))
            .send()
            .await
            .unwrap()
    })
    .await;
    measure_operation_cost(&cost, "list_parts", 0, || async {
        client
            .list_parts()
            .bucket(&bucket)
            .key("cost/multipart.bin")
            .upload_id(&upload_id)
            .send()
            .await
            .unwrap();
    })
    .await;
    measure_operation_cost(&cost, "list_multipart_uploads", 0, || async {
        client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("cost/")
            .send()
            .await
            .unwrap();
    })
    .await;
    measure_operation_cost(&cost, "complete_multipart_upload", 0, || async {
        client
            .complete_multipart_upload()
            .bucket(&bucket)
            .key("cost/multipart.bin")
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .parts(
                        CompletedPart::builder()
                            .part_number(1)
                            .e_tag(part.e_tag().unwrap())
                            .build(),
                    )
                    .build(),
            )
            .send()
            .await
            .unwrap();
    })
    .await;

    let abort_upload = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("cost/abort.bin")
        .send()
        .await
        .unwrap();
    measure_operation_cost(&cost, "abort_multipart_upload", 0, || async {
        client
            .abort_multipart_upload()
            .bucket(&bucket)
            .key("cost/abort.bin")
            .upload_id(abort_upload.upload_id().unwrap())
            .send()
            .await
            .unwrap();
    })
    .await;

    let copy_upload = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("cost/multipart-copy.bin")
        .send()
        .await
        .unwrap();
    measure_operation_cost(&cost, "upload_part_copy", 0, || async {
        client
            .upload_part_copy()
            .bucket(&bucket)
            .key("cost/multipart-copy.bin")
            .upload_id(copy_upload.upload_id().unwrap())
            .part_number(1)
            .copy_source(format!("{bucket}/cost/source.bin"))
            .send()
            .await
            .unwrap();
    })
    .await;

    measure_operation_cost(
        &cost,
        "atomic_commit_session_2_puts",
        (PAYLOAD_BYTES * 2) as u64,
        || async {
            let mut session = client
                .begin_commit()
                .message("cost matrix atomic commit")
                .start()
                .await
                .unwrap();
            for key in ["cost/atomic-a.bin", "cost/atomic-b.bin"] {
                session
                    .put_object()
                    .bucket(&bucket)
                    .key(key)
                    .body(ByteStream::from(payload.clone()))
                    .stage()
                    .await
                    .unwrap();
            }
            session.publish().await.unwrap();
        },
    )
    .await;
    client.fsck().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_s3_shaped_operation_cost_matrix_repository_maintenance() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_COST_MATRIX").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_COST_MATRIX=1 to run the cost matrix");
        return;
    }
    let (aws, _, wire) = rustfs_client_with_wire_metrics().await;
    let (accounting_aws, _) = rustfs_client().await;
    let bucket = std::env::var("PROLLY_S3_COST_VERSIONED_BUCKET")
        .unwrap_or_else(|_| "prolly-versioned-s3-costs".to_string());
    if let Err(error) = aws.create_bucket().bucket(&bucket).send().await {
        let text = format!("{error:?}");
        assert!(
            text.contains("BucketAlreadyOwnedByYou") || text.contains("BucketAlreadyExists"),
            "unexpected create-bucket error: {text}"
        );
    }
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let prefix = unique_prefix("maintenance-cost");
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&prefix)
        .writer("maintenance-cost-probe")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .token_signer(Arc::new(
            HmacTokenSigner::single("maintenance-cost-v1", vec![0x29; 32]).unwrap(),
        ))
        .initialize()
        .await
        .unwrap();
    let root = client.head_commit().await.unwrap();
    let first = client
        .put_object()
        .bucket(&bucket)
        .key("maintenance/base.txt")
        .body(ByteStream::from_static(b"base"))
        .send()
        .await
        .unwrap()
        .snapshot;
    client.create_branch("feature", Some(first)).await.unwrap();
    let feature_client = client.on_branch("feature").unwrap();
    let feature_head = feature_client
        .put_object()
        .bucket(&bucket)
        .key("maintenance/feature.txt")
        .body(ByteStream::from_static(b"feature"))
        .send()
        .await
        .unwrap()
        .snapshot;
    let main_head = client
        .put_object()
        .bucket(&bucket)
        .key("maintenance/main.txt")
        .body(ByteStream::from_static(b"main"))
        .send()
        .await
        .unwrap()
        .snapshot;
    let cost = OperationCostContext {
        client: &client,
        wire: &wire,
        accounting_aws: &accounting_aws,
        bucket: &bucket,
        prefix: &prefix,
    };

    let observed_head = measure_operation_cost(&cost, "admin_head_commit", 0, || async {
        client.head_commit().await.unwrap()
    })
    .await;
    assert_eq!(observed_head, main_head);

    let log = measure_operation_cost(&cost, "admin_log", 0, || async {
        client.log(100).await.unwrap()
    })
    .await;
    assert_eq!(log.len(), 3);

    let branches = measure_operation_cost(&cost, "admin_list_branches", 0, || async {
        client.list_branches().await.unwrap()
    })
    .await;
    assert_eq!(branches.len(), 2);

    let temporary_branch = measure_operation_cost(&cost, "admin_create_branch", 0, || async {
        client
            .create_branch("cost-temporary", Some(main_head))
            .await
            .unwrap()
    })
    .await;
    measure_operation_cost(&cost, "admin_delete_branch", 0, || async {
        client
            .delete_branch("cost-temporary", temporary_branch.target)
            .await
            .unwrap();
    })
    .await;

    measure_operation_cost(&cost, "admin_create_tag", 0, || async {
        client.create_tag("cost-baseline", main_head).await.unwrap()
    })
    .await;
    let tags = measure_operation_cost(&cost, "admin_list_tags", 0, || async {
        client.list_tags().await.unwrap()
    })
    .await;
    assert_eq!(tags.len(), 1);

    measure_operation_cost(&cost, "admin_create_retention_pin", 0, || async {
        client
            .create_retention_pin(
                "cost-baseline",
                main_head,
                "qualification",
                "cost matrix",
                None,
            )
            .await
            .unwrap()
    })
    .await;
    let pins = measure_operation_cost(&cost, "admin_list_retention_pins", 0, || async {
        client.list_retention_pins().await.unwrap()
    })
    .await;
    assert_eq!(pins.len(), 1);

    let diff = measure_operation_cost(&cost, "maintenance_diff", 0, || async {
        client.diff(first, main_head).await.unwrap()
    })
    .await;
    assert_eq!(diff.len(), 1);
    let bases = measure_operation_cost(&cost, "maintenance_merge_bases", 0, || async {
        client.merge_bases(main_head, feature_head).await.unwrap()
    })
    .await;
    assert_eq!(bases, vec![first]);
    let merge_plan = measure_operation_cost(&cost, "maintenance_plan_merge", 0, || async {
        client
            .plan_merge(feature_head, None, MergePolicy::Fail)
            .await
            .unwrap()
    })
    .await;
    assert!(merge_plan.conflicts.is_empty());
    let merge = measure_operation_cost(&cost, "maintenance_merge", 0, || async {
        client
            .merge(
                feature_head,
                None,
                MergePolicy::Fail,
                None,
                Some("cost matrix merge".to_string()),
            )
            .await
            .unwrap()
    })
    .await;
    assert_eq!(merge.parents.len(), 2);

    let restored = measure_operation_cost(&cost, "maintenance_restore", 0, || async {
        client
            .restore(
                root,
                merge.id,
                None,
                Some("cost matrix restore".to_string()),
            )
            .await
            .unwrap()
    })
    .await;
    measure_operation_cost(&cost, "admin_reset_branch", 0, || async {
        client
            .reset_branch(merge.id, restored.id, "cost matrix reset")
            .await
            .unwrap()
    })
    .await;

    let reflog = measure_operation_cost(&cost, "admin_list_reflog", 0, || async {
        client.list_reflog().await.unwrap()
    })
    .await;
    assert!(!reflog.is_empty());
    let native_versions =
        measure_operation_cost(&cost, "admin_list_native_ref_versions", 0, || async {
            client.list_native_branch_ref_versions().await.unwrap()
        })
        .await;
    assert!(!native_versions.is_empty());

    let fsck_commit = measure_operation_cost(&cost, "maintenance_fsck_commit", 0, || async {
        client.fsck_commit(merge.id).await.unwrap()
    })
    .await;
    assert!(fsck_commit.commits >= 1);
    let fsck = measure_operation_cost(&cost, "maintenance_fsck_repository", 0, || async {
        client.fsck().await.unwrap()
    })
    .await;
    assert!(fsck.commits >= 1);
    measure_operation_cost(&cost, "admin_delete_retention_pin", 0, || async {
        client
            .delete_retention_pin("cost-baseline", main_head)
            .await
            .unwrap();
    })
    .await;
    measure_operation_cost(&cost, "admin_delete_tag", 0, || async {
        client.delete_tag("cost-baseline", main_head).await.unwrap();
    })
    .await;

    // Use an injected clock in an isolated repository to age a known orphan
    // past both reflog retention and the minimum publication-lease grace.
    let gc_prefix = format!("{prefix}/destructive-gc");
    let gc_start_millis = u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    let gc_clock = Arc::new(FixedClock::new(gc_start_millis));
    let gc_plane = Arc::new(AwsS3ObjectPlane::new(aws, &bucket));
    let gc_options = RepositoryOptions {
        repository_prefix: gc_prefix.clone(),
        writer: "maintenance-gc-cost".to_string(),
        reflog_retention_millis: 1,
        clock: gc_clock.clone(),
        ..RepositoryOptions::default()
    };
    let orphan_store = ContentStore::new(
        gc_plane.clone(),
        gc_prefix.clone(),
        usize::try_from(gc_options.limits.content_chunk_bytes).unwrap(),
        gc_options.content_index_format.clone(),
    );
    let gc_repository = Repository::initialize(gc_plane.clone(), gc_options)
        .await
        .unwrap();
    orphan_store.write_bytes(vec![0x5d; 1_024]).await.unwrap();
    gc_clock.advance(3 * 60 * 60 * 1_000).unwrap();
    let gc_cost = PlaneOperationCostContext {
        plane: gc_plane.as_ref(),
        wire: &wire,
        accounting_aws: &accounting_aws,
        bucket: &bucket,
        prefix: &gc_prefix,
    };
    let gc = measure_plane_operation_cost(&gc_cost, "maintenance_gc_reclaim_plan", || async {
        gc_repository
            .plan_gc(2 * 60 * 60 * 1_000, 10_000)
            .await
            .unwrap()
    })
    .await;
    assert!(gc.retained_paths > 0);
    assert!(!gc.plan.body.candidates.is_empty());
    let swept = measure_plane_operation_cost(&gc_cost, "maintenance_gc_sweep", || async {
        gc_repository.sweep_gc(gc.plan.id).await.unwrap()
    })
    .await;
    assert!(swept.complete);
    assert!(swept.deleted_versions > 0);
    gc_repository.fsck().await.unwrap();
    client.fsck().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_s3_shaped_operation_cost_matrix_cross_repository() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_COST_MATRIX").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_COST_MATRIX=1 to run the cost matrix");
        return;
    }
    let (aws, bucket, wire) = rustfs_client_with_wire_metrics().await;
    let (accounting_aws, accounting_bucket) = rustfs_client().await;
    assert_eq!(accounting_bucket, bucket);
    let source_prefix = unique_prefix("cross-repository-source-cost");
    let target_prefix = unique_prefix("cross-repository-target-cost");
    let source = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&source_prefix)
        .writer("cross-repository-source")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    for index in 0..2 {
        source
            .put_object()
            .bucket(&bucket)
            .key(format!("cross-repository/base-{index}.bin"))
            .body(ByteStream::from(vec![index as u8; 4 * 1_024]))
            .send()
            .await
            .unwrap();
    }
    let cloned_head = source.head_commit().await.unwrap();
    let prefixes = [source_prefix.as_str(), target_prefix.as_str()];
    let storage_before =
        physical_storage_bytes_for_prefixes(accounting_aws.clone(), &bucket, &prefixes).await;
    source.reset_s3_operation_metrics();
    wire.reset();
    let started = Instant::now();
    let cloned = source
        .clone_to(
            aws.clone(),
            &bucket,
            &target_prefix,
            rustfs_provider_identity(),
            Default::default(),
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let sdk = combine_s3_metrics(
        source.reset_s3_operation_metrics(),
        cloned.target_s3_metrics,
    );
    let clone_wire = wire.reset();
    assert_eq!(
        clone_wire.executions.saturating_sub(sdk.total_calls()),
        3,
        "clone qualification must issue exactly versioning, lifecycle, and object-lock control-plane calls outside the object-plane counters"
    );
    let storage_after =
        physical_storage_bytes_for_prefixes(accounting_aws.clone(), &bucket, &prefixes).await;
    report_operation_cost(
        "maintenance_clone_to_qualified_target",
        cloned.copy.immutable_bytes,
        elapsed,
        storage_before,
        storage_after,
        sdk,
        clone_wire,
    );
    assert!(cloned.copy.immutable_objects > 0);

    let target = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&target_prefix)
        .writer("cross-repository-target")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .provider_attestation(cloned.provider_profile)
        .open()
        .await
        .unwrap();
    assert_eq!(target.head_commit().await.unwrap(), cloned_head);
    let clients = [&source, &target];
    let cost = MultiClientOperationCostContext {
        clients: &clients,
        wire: &wire,
        accounting_aws: &accounting_aws,
        bucket: &bucket,
        prefixes: &prefixes,
    };

    source
        .put_object()
        .bucket(&bucket)
        .key("cross-repository/fetch.bin")
        .body(ByteStream::from(vec![0x61; 4 * 1_024]))
        .send()
        .await
        .unwrap();
    let fetched =
        measure_multi_client_operation_cost(&cost, "maintenance_fetch_missing_closure", || async {
            target.fetch_from(&source).await.unwrap()
        })
        .await;
    assert!(fetched.copied_objects > 0);

    let pushed_head = source
        .put_object()
        .bucket(&bucket)
        .key("cross-repository/resumable.bin")
        .body(ByteStream::from(vec![0x62; 4 * 1_024]))
        .send()
        .await
        .unwrap()
        .snapshot;
    let resumable =
        measure_multi_client_operation_cost(&cost, "maintenance_fetch_resumable", || async {
            target
                .fetch_from_resumable(&source, None, 10_000)
                .await
                .unwrap()
        })
        .await;
    assert_eq!(resumable.state, prolly_s3_core::SyncRunStateV1::Completed);
    assert_eq!(resumable.source_head, pushed_head);

    let pushed = measure_multi_client_operation_cost(&cost, "maintenance_push", || async {
        source
            .push_to(&target, cloned_head, "cost matrix push")
            .await
            .unwrap()
    })
    .await;
    assert_eq!(pushed.source_head, Some(pushed_head));
    assert_eq!(target.head_commit().await.unwrap(), pushed_head);
    source.fsck().await.unwrap();
    target.fsck().await.unwrap();
}

#[cfg(feature = "slatedb-index")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_s3_shaped_operation_cost_matrix_advisory_rebuild() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_COST_MATRIX").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_COST_MATRIX=1 to run the cost matrix");
        return;
    }
    const ADDITIONAL_BRANCHES: usize = 8;

    let (aws, _, wire) = rustfs_client_with_wire_metrics().await;
    let (accounting_aws, _) = rustfs_client().await;
    let bucket = std::env::var("PROLLY_S3_COST_VERSIONED_BUCKET")
        .unwrap_or_else(|_| "prolly-versioned-s3-costs".to_string());
    if let Err(error) = aws.create_bucket().bucket(&bucket).send().await {
        let text = format!("{error:?}");
        assert!(
            text.contains("BucketAlreadyOwnedByYou") || text.contains("BucketAlreadyExists"),
            "unexpected create-bucket error: {text}"
        );
    }
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let raw_store = rustfs_slatedb_object_store(&bucket);
    let advisory_store = CountingAdvisoryObjectStore::new(raw_store);
    let authority_prefix = unique_prefix("advisory-rebuild-cost");
    let base_client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&authority_prefix)
        .writer("advisory-rebuild-bootstrap")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    let repository = base_client.repository_id();
    let index = Arc::new(
        SlateDbAdvisoryIndex::open_owned(
            Arc::new(advisory_store.clone()),
            repository,
            "advisory-rebuild-cost",
        )
        .await
        .unwrap(),
    );
    let cache_prefix = index.path().to_string();
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&authority_prefix)
        .writer("advisory-rebuild-cost")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .advisory_index(index.clone())
        .open()
        .await
        .unwrap();
    drop(base_client);

    let head = client.head_commit().await.unwrap();
    for ordinal in 0..ADDITIONAL_BRANCHES {
        client
            .create_branch(format!("advisory-cost-{ordinal:02}"), Some(head))
            .await
            .unwrap();
    }
    let corrupt_key = format!("prolly-s3/{repository}/branch/main").into_bytes();
    index
        .database()
        .put(corrupt_key, b"corrupt advisory rebuild cost entry".to_vec())
        .await
        .unwrap();
    index.flush().await.unwrap();

    let prefixes = [authority_prefix.as_str(), cache_prefix.as_str()];
    let storage_before =
        physical_storage_bytes_for_prefixes(accounting_aws.clone(), &bucket, &prefixes).await;
    client.reset_s3_operation_metrics();
    wire.reset();
    let advisory_before = advisory_store.snapshot();
    let started = Instant::now();
    let rebuild = client.rebuild_advisory_index().await.unwrap();
    index.flush().await.unwrap();
    let elapsed = started.elapsed();
    let canonical_sdk = client.reset_s3_operation_metrics();
    let canonical_wire = wire.reset();
    let advisory = advisory_store.snapshot().delta_since(advisory_before);
    let storage_after =
        physical_storage_bytes_for_prefixes(accounting_aws, &bucket, &prefixes).await;

    assert_eq!(rebuild.written_heads, ADDITIONAL_BRANCHES + 1);
    assert_eq!(rebuild.removed_entries, 1);
    assert_eq!(rebuild.quarantined_entries, 1);
    assert!(advisory.total_calls() > 0);
    assert_eq!(canonical_wire.executions, canonical_sdk.total_calls());
    report_operation_cost(
        "maintenance_rebuild_slatedb_advisory_index",
        0,
        elapsed,
        storage_before,
        storage_after,
        canonical_sdk,
        canonical_wire,
    );
    eprintln!(
        "ADVISORY_STORE_COST operation=maintenance_rebuild_slatedb_advisory_index api_calls={} put={} multipart_start={} get={} head={} delete_stream={} list={} delimiter_list={} copy={} uploaded_bytes={} returned_bytes={}",
        advisory.total_calls(),
        advisory.puts,
        advisory.multipart_starts,
        advisory.gets,
        advisory.heads,
        advisory.delete_streams,
        advisory.lists,
        advisory.delimiter_lists,
        advisory.copies,
        advisory.uploaded_body_bytes,
        advisory.returned_body_bytes,
    );
    assert_eq!(
        index.quarantine_count(repository).await.unwrap(),
        1,
        "the corrupt advisory entry must be quarantined during the measured rebuild"
    );
    client.fsck().await.unwrap();
    drop(client);
    index.close().await.unwrap();
    let advisory_total = advisory_store.snapshot();
    eprintln!(
        "ADVISORY_STORE_TOTAL api_calls={} put={} multipart_start={} get={} head={} delete_stream={} list={} delimiter_list={} copy={} uploaded_bytes={} returned_bytes={}",
        advisory_total.total_calls(),
        advisory_total.puts,
        advisory_total.multipart_starts,
        advisory_total.gets,
        advisory_total.heads,
        advisory_total.delete_streams,
        advisory_total.lists,
        advisory_total.delimiter_lists,
        advisory_total.copies,
        advisory_total.uploaded_body_bytes,
        advisory_total.returned_body_bytes,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rustfs_contention_latency_probe() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_CONTENTION").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_CONTENTION=1 to run the contention probe");
        return;
    }
    let writers: usize = std::env::var("PROLLY_S3_CONTENTION_WRITERS")
        .unwrap_or_else(|_| "8".to_string())
        .parse()
        .expect("PROLLY_S3_CONTENTION_WRITERS must be an integer");
    assert!((1..=128).contains(&writers));
    let deadline_seconds: u64 = std::env::var("PROLLY_S3_CONTENTION_DEADLINE_SECONDS")
        .unwrap_or_else(|_| "180".to_string())
        .parse()
        .expect("PROLLY_S3_CONTENTION_DEADLINE_SECONDS must be an integer");
    assert!(deadline_seconds > 0);
    let (aws, bucket, wire_metrics) = rustfs_client_with_wire_metrics().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_prefix("contention-latency"))
        .writer(format!("contention-{writers}"))
        .logical_retry_limit(MAX_LOGICAL_RETRY_LIMIT)
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    client.reset_s3_operation_metrics();
    wire_metrics.reset();
    let barrier = Arc::new(Barrier::new(writers));
    let tasks = (0..writers)
        .map(|writer| {
            let client = client.clone();
            let bucket = bucket.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                let started = Instant::now();
                let operation = OperationId::new();
                let mut attempt = 0_u32;
                let result = loop {
                    let result = client
                        .put_object()
                        .bucket(&bucket)
                        .key(format!("contention/{writer:04}.bin"))
                        .body(ByteStream::from(vec![writer as u8; 1_024]))
                        .operation_id(operation)
                        .send()
                        .await;
                    match result {
                        Ok(_) => break Ok(()),
                        Err(error)
                            if matches!(
                                error.code,
                                ErrorCode::OutcomeUnknown
                                    | ErrorCode::RefConflict
                                    | ErrorCode::Throttled
                                    | ErrorCode::Timeout
                                    | ErrorCode::Transport
                            ) =>
                        {
                            match client.reconcile_operation(operation).await {
                                Ok(Some(_)) => break Ok(()),
                                Ok(None) => {}
                                Err(reconcile_error) => break Err(reconcile_error),
                            }
                            if attempt == 15 {
                                break Err(error);
                            }
                            attempt += 1;
                            let jitter = 10 + (writer as u64 % 17) * 3 + u64::from(attempt) * 20;
                            tokio::time::sleep(Duration::from_millis(jitter)).await;
                        }
                        Err(error) => break Err(error),
                    }
                };
                (started.elapsed(), result)
            })
        })
        .collect::<Vec<_>>();
    let completed = tokio::time::timeout(
        Duration::from_secs(deadline_seconds),
        futures_util::future::join_all(tasks),
    )
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{writers}-writer contention tier exceeded its {deadline_seconds}-second qualification deadline"
        )
    });
    let mut latencies = Vec::with_capacity(writers);
    for task in completed {
        let (latency, result) = task.unwrap();
        result.unwrap();
        latencies.push(latency.as_secs_f64() * 1_000.0);
    }
    latencies.sort_by(f64::total_cmp);
    let percentile = |numerator: usize, denominator: usize| {
        let index = (latencies.len() * numerator).div_ceil(denominator) - 1;
        latencies[index.min(latencies.len() - 1)]
    };
    let metrics = client.reset_s3_operation_metrics();
    let wire = wire_metrics.reset();
    assert_eq!(wire.executions, metrics.total_calls());
    client.fsck().await.unwrap();
    eprintln!(
        "CONTENTION_PROBE writers={writers} logical_retry_limit={MAX_LOGICAL_RETRY_LIMIT} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3} max_ms={:.3} sdk_calls={} calls_per_write={:.3} wire_transmissions={} wire_retries={} get={} head={} put={} list={} list_versions={} delete={} uploaded_bytes={} downloaded_bytes={}",
        percentile(50, 100),
        percentile(95, 100),
        percentile(99, 100),
        latencies[latencies.len() - 1],
        metrics.total_calls(),
        metrics.total_calls() as f64 / writers as f64,
        wire.transmissions,
        wire.retry_transmissions(),
        metrics.get_object,
        metrics.head_object,
        metrics.put_object,
        metrics.list_objects_v2,
        metrics.list_object_versions,
        metrics.delete_object,
        metrics.uploaded_body_bytes,
        metrics.downloaded_body_bytes,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rustfs_multipart_streaming_resource_probe() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_RESOURCE_TEST").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_RESOURCE_TEST=1 to run the resource probe");
        return;
    }
    const LOGICAL_BYTES: u64 = 160 * 1024 * 1024;
    const SOURCE_CHUNK_BYTES: usize = 1024 * 1024;
    let (aws, bucket) = rustfs_client().await;
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(unique_prefix("multipart-resource"))
        .writer("multipart-resource-probe")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    let created = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("resource/160-mib.bin")
        .send()
        .await
        .unwrap();
    let upload_id = created.upload_id().unwrap();
    let body = ByteStream::from_body_1_x(RepeatedBody {
        remaining: LOGICAL_BYTES,
        chunk_bytes: SOURCE_CHUNK_BYTES,
        value: 0x5a,
    });
    let part = client
        .upload_part()
        .bucket(&bucket)
        .key("resource/160-mib.bin")
        .upload_id(upload_id)
        .part_number(1)
        .body(body)
        .send()
        .await
        .unwrap();
    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("resource/160-mib.bin")
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let mut body = client
        .get_object()
        .bucket(&bucket)
        .key("resource/160-mib.bin")
        .send()
        .await
        .unwrap()
        .output
        .body;
    let mut observed = 0_u64;
    while let Some(chunk) = body.next().await {
        let chunk = chunk.unwrap();
        assert!(chunk.iter().all(|byte| *byte == 0x5a));
        observed += chunk.len() as u64;
    }
    assert_eq!(observed, LOGICAL_BYTES);
    eprintln!("RESOURCE_PROBE_LOGICAL_BYTES={LOGICAL_BYTES}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_branch_merge_restore_and_gc_fence() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, bucket) = rustfs_client().await;
    let source_prefix = unique_prefix("history-admin");
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&source_prefix)
        .writer("history-admin-integration")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    let root = client.head_commit().await.unwrap();
    client.create_branch("feature", Some(root)).await.unwrap();
    client
        .put_object()
        .bucket(&bucket)
        .key("main.txt")
        .body(ByteStream::from_static(b"main"))
        .send()
        .await
        .unwrap();
    let feature_client = client.on_branch("feature").unwrap();
    feature_client
        .put_object()
        .bucket(&bucket)
        .key("feature.txt")
        .body(ByteStream::from_static(b"feature"))
        .send()
        .await
        .unwrap();
    let feature = feature_client.head_commit().await.unwrap();
    let merge = client
        .merge(
            feature,
            None,
            MergePolicy::Fail,
            None,
            Some("merge feature".to_string()),
        )
        .await
        .unwrap();
    assert_eq!(merge.parents.len(), 2);
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("feature.txt")
            .send()
            .await
            .unwrap()
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes(),
        &b"feature"[..]
    );
    let restored = client
        .restore(root, merge.id, None, Some("restore root".to_string()))
        .await
        .unwrap();
    assert_eq!(restored.parents, [merge.id]);
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("feature.txt")
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    let clone_prefix = unique_prefix("qualified-clone");
    let cloned = client
        .clone_to(
            aws.clone(),
            &bucket,
            &clone_prefix,
            rustfs_provider_identity(),
            Default::default(),
        )
        .await
        .unwrap();
    assert!(cloned.copy.immutable_objects > 0);
    let cloned_client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&clone_prefix)
        .writer("qualified-clone-reader")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .provider_attestation(cloned.provider_profile)
        .open()
        .await
        .unwrap();
    assert_eq!(cloned_client.repository_id(), client.repository_id());
    assert_eq!(cloned_client.head_commit().await.unwrap(), restored.id);
    let sync_commit = client
        .put_object()
        .bucket(&bucket)
        .key("sync.txt")
        .body(ByteStream::from_static(b"sync"))
        .send()
        .await
        .unwrap()
        .snapshot;
    let first_sync_batch = cloned_client
        .fetch_from_resumable(&client, None, 1)
        .await
        .unwrap();
    assert_eq!(
        first_sync_batch.state,
        prolly_s3_core::SyncRunStateV1::Running
    );
    let sync_run = first_sync_batch.id;
    drop(cloned_client);
    let cloned_client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&clone_prefix)
        .writer("qualified-clone-restarted-reader")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .provider_attestation(cloned.provider_profile)
        .open()
        .await
        .unwrap();
    let mut fetched = first_sync_batch;
    for _ in 0..1_000 {
        fetched = cloned_client
            .fetch_from_resumable(&client, Some(sync_run), 3)
            .await
            .unwrap();
        if fetched.state == prolly_s3_core::SyncRunStateV1::Completed {
            break;
        }
    }
    assert_eq!(fetched.state, prolly_s3_core::SyncRunStateV1::Completed);
    assert_eq!(fetched.source_head, sync_commit);
    assert_eq!(cloned_client.head_commit().await.unwrap(), restored.id);
    assert_eq!(
        cloned_client
            .at(sync_commit)
            .await
            .unwrap()
            .get_object()
            .bucket(&bucket)
            .key("sync.txt")
            .send()
            .await
            .unwrap()
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes(),
        &b"sync"[..]
    );
    let pushed = client
        .push_to(&cloned_client, restored.id, "integration push")
        .await
        .unwrap();
    assert_eq!(pushed.source_head, Some(sync_commit));
    assert_eq!(cloned_client.head_commit().await.unwrap(), sync_commit);
    let dry_run = client
        .plan_gc(std::time::Duration::from_secs(2 * 60 * 60), 10_000)
        .await
        .unwrap();
    client
        .put_object()
        .bucket(&bucket)
        .key("after-plan.txt")
        .body(ByteStream::from_static(b"fence"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        client.sweep_gc(dry_run.plan.id).await.unwrap_err().code,
        ErrorCode::PreconditionFailed
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_aws_shaped_client_round_trip() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, bucket) = rustfs_client().await;
    let repository_prefix = unique_prefix("aws-shaped");
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("aws-shaped-integration")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .token_signer(Arc::new(
            HmacTokenSigner::single("integration-v1", vec![7u8; 32]).unwrap(),
        ))
        .initialize()
        .await
        .unwrap();
    let selected_profile = client.provider_profile().unwrap();
    let versions_before_open =
        physical_version_snapshot(aws.clone(), &bucket, &repository_prefix).await;
    let reopened = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("aws-shaped-reopened")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .provider_attestation(selected_profile)
        .open()
        .await
        .unwrap();
    assert_eq!(reopened.repository_id(), client.repository_id());
    assert_eq!(
        physical_version_snapshot(aws.clone(), &bucket, &repository_prefix).await,
        versions_before_open,
        "ordinary open must not create, update, delete, probe, initialize, or repair any physical object"
    );
    let layout = reopened.physical_layout();
    assert_eq!(layout.bucket, bucket);
    assert_eq!(layout.repository_prefix, repository_prefix);
    assert!(layout
        .families
        .iter()
        .any(|family| family.relative_pattern == "providers/<provider-profile-id>.cbor"));
    assert_eq!(client.expire_multipart_uploads(0).await.unwrap(), 0);
    let mismatched = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .provider_identity(ProviderIdentity::s3_compatible(
            "http://different-endpoint.invalid",
            "us-east-1",
        ))
        .attestation_signer(test_attestation_signer())
        .provider_attestation(selected_profile)
        .open()
        .await;
    let mismatch_error = match mismatched {
        Ok(_) => panic!("endpoint-mismatched attestation was accepted"),
        Err(error) => error,
    };
    assert_eq!(mismatch_error.code, ErrorCode::ProviderNotQualified);
    let wrong_signer = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(Arc::new(
            HmacAttestationSigner::single("unknown-key", vec![99_u8; 32]).unwrap(),
        ))
        .provider_attestation(selected_profile)
        .open()
        .await;
    let signer_error = match wrong_signer {
        Ok(_) => panic!("attestation signed by an unknown key was accepted"),
        Err(error) => error,
    };
    assert_eq!(signer_error.code, ErrorCode::ProviderNotQualified);

    let initial_head = client.head_commit().await.unwrap();
    let timed_out_operation = OperationId::new();
    let timed_out = client
        .put_object()
        .bucket(&bucket)
        .key("official/deadline-never-published.txt")
        .body(ByteStream::from_body_1_x(PendingBody))
        .operation_id(timed_out_operation)
        .deadline(Instant::now() + Duration::from_millis(25))
        .send()
        .await
        .unwrap_err();
    assert_eq!(timed_out.code, ErrorCode::OutcomeUnknown);
    assert_eq!(timed_out.retry, RetryAdvice::ReconcileOperation);
    assert_eq!(
        timed_out.operation_id.as_deref(),
        Some(timed_out_operation.to_string()).as_deref()
    );
    assert!(client
        .reconcile_operation(timed_out_operation)
        .await
        .unwrap()
        .is_none());

    let official_checksum = STANDARD.encode(Sha256::digest(b"official input"));
    let official_input = aws_sdk_s3::operation::put_object::PutObjectInput::builder()
        .bucket(&bucket)
        .key("official/input-path.txt")
        .body(ByteStream::from_static(b"official input"))
        .content_type("text/plain")
        .if_none_match("*")
        .checksum_sha256(&official_checksum)
        .build()
        .unwrap();
    let official_operation = OperationId::new();
    let official = client
        .execute_put_object(
            official_input,
            WriteOptions {
                operation_id: Some(official_operation),
                expected_head: Some(initial_head),
                logical_retry_limit: Some(0),
                deadline: Some(Instant::now() + Duration::from_secs(30)),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        official.commit.as_ref().unwrap().operation,
        official_operation
    );
    assert!(official.output.version_id().is_some());
    assert_eq!(
        official.output.checksum_sha256(),
        Some(official_checksum.as_str())
    );
    let unsupported = aws_sdk_s3::operation::put_object::PutObjectInput::builder()
        .bucket(&bucket)
        .key("official/rejected.txt")
        .body(ByteStream::from_static(b"rejected"))
        .storage_class(aws_sdk_s3::types::StorageClass::Glacier)
        .build()
        .unwrap();
    assert_eq!(
        client
            .execute_put_object(unsupported, WriteOptions::default())
            .await
            .unwrap_err()
            .code,
        ErrorCode::UnsupportedParameter
    );

    let first = client
        .put_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .body(ByteStream::from_static(b"first"))
        .content_type("text/plain")
        .metadata("purpose", "contract-test")
        .logical_retry_limit(1)
        .deadline(Instant::now() + Duration::from_secs(30))
        .send()
        .await
        .unwrap();
    let first_version = first.output.version_id().unwrap().to_string();
    assert_eq!(first.snapshot, first.commit.as_ref().unwrap().id);

    let second = client
        .put_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .body(ByteStream::from_static(b"second"))
        .send()
        .await
        .unwrap();
    let second_etag = second.output.e_tag().unwrap().to_string();
    assert_ne!(first.snapshot, second.snapshot);
    assert_ne!(first_version, second.output.version_id().unwrap());

    let first_log_page = client.log_page(second.snapshot, None, 2).await.unwrap();
    assert_eq!(first_log_page.len(), 2);
    let second_log_page = client
        .log_page(second.snapshot, Some(first_log_page.last().unwrap().0), 2)
        .await
        .unwrap();
    assert_eq!(second_log_page.len(), 2);
    assert_eq!(second_log_page.last().unwrap().0, initial_head);

    let (first_diff_page, truncated) = client
        .diff_page(initial_head, second.snapshot, None, 1)
        .await
        .unwrap();
    assert!(truncated);
    let (second_diff_page, truncated) = client
        .diff_page(
            initial_head,
            second.snapshot,
            Some(&first_diff_page[0].key),
            1,
        )
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(second_diff_page.len(), 1);
    assert!(first_diff_page[0].key < second_diff_page[0].key);

    assert_eq!(
        client
            .head_object()
            .bucket(&bucket)
            .key("docs/api.txt")
            .deadline(Instant::now() - Duration::from_millis(1))
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::Timeout
    );

    let conditional_get = client
        .get_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .if_match(&second_etag)
        .checksum_mode(ChecksumMode::Enabled)
        .send()
        .await
        .unwrap();
    assert_eq!(
        conditional_get.output.checksum_sha256(),
        Some(STANDARD.encode(Sha256::digest(b"second")).as_str())
    );
    assert_eq!(
        client
            .head_object()
            .bucket(&bucket)
            .key("docs/api.txt")
            .if_none_match(&second_etag)
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotModified
    );
    let modified = *client
        .head_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .send()
        .await
        .unwrap()
        .output
        .last_modified()
        .unwrap();
    let before_modified = aws_smithy_types::DateTime::from_secs(modified.secs() - 1);
    let after_modified = aws_smithy_types::DateTime::from_secs(modified.secs() + 1);
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("docs/api.txt")
            .if_modified_since(modified)
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::NotModified
    );
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("docs/api.txt")
            .if_unmodified_since(before_modified)
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::PreconditionFailed
    );
    // RFC/S3 precedence: an ETag condition suppresses the corresponding date
    // condition, and preconditions are evaluated before range satisfiability.
    client
        .get_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .if_match(&second_etag)
        .if_unmodified_since(before_modified)
        .send()
        .await
        .unwrap();
    client
        .get_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .if_none_match("\"different\"")
        .if_modified_since(after_modified)
        .send()
        .await
        .unwrap();
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("docs/api.txt")
            .if_match("\"different\"")
            .range("bytes=999-1000")
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::PreconditionFailed
    );
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("docs/api.txt")
            .if_match(&second_etag)
            .range("bytes=999-1000")
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidRange
    );
    assert_eq!(
        client
            .put_object()
            .bucket(&bucket)
            .key("docs/api.txt")
            .if_none_match("*")
            .body(ByteStream::from_static(b"must not publish"))
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::PreconditionFailed
    );
    let conditional_payload = b"checksum-and-create";
    let conditional_sha = STANDARD.encode(Sha256::digest(conditional_payload));
    client
        .put_object()
        .bucket(&bucket)
        .key("docs/conditional-create.txt")
        .if_none_match("*")
        .checksum_sha256(&conditional_sha)
        .body(ByteStream::from_static(conditional_payload))
        .send()
        .await
        .unwrap();

    let immutable = client.at(first.snapshot).await.unwrap();
    let snapshot_body = immutable
        .get_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(snapshot_body.snapshot, first.snapshot);
    assert_eq!(
        snapshot_body
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes(),
        &b"first"[..]
    );

    let current = client
        .get_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .range("bytes=1-3")
        .send()
        .await
        .unwrap();
    assert_eq!(current.output.content_range(), Some("bytes 1-3/6"));
    assert_eq!(
        current.output.body.collect().await.unwrap().into_bytes(),
        &b"eco"[..]
    );

    let historical = client
        .get_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .version_id(&first_version)
        .send()
        .await
        .unwrap();
    assert_eq!(historical.output.content_type(), Some("text/plain"));
    assert_eq!(
        historical
            .output
            .metadata()
            .and_then(|m| m.get("purpose"))
            .map(String::as_str),
        Some("contract-test")
    );
    assert_eq!(
        historical.output.body.collect().await.unwrap().into_bytes(),
        &b"first"[..]
    );

    let listing = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("docs/")
        .send()
        .await
        .unwrap();
    assert_eq!(listing.output.contents().len(), 2);
    assert_eq!(listing.output.contents()[0].key(), Some("docs/api.txt"));
    assert_eq!(
        listing.output.contents()[1].key(),
        Some("docs/conditional-create.txt")
    );

    let copied = client
        .copy_object()
        .bucket(&bucket)
        .key("docs/copied.txt")
        .copy_source(format!("{bucket}/docs/api.txt?versionId={first_version}"))
        .send()
        .await
        .unwrap();
    assert!(copied
        .output
        .copy_object_result()
        .unwrap()
        .e_tag()
        .is_some());
    let copied_body = client
        .get_object()
        .bucket(&bucket)
        .key("docs/copied.txt")
        .send()
        .await
        .unwrap()
        .output
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(copied_body, &b"first"[..]);

    for key in ["docs/b.txt", "docs/c.txt", "docs/d.txt"] {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"page"))
            .send()
            .await
            .unwrap();
    }
    let first_page = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("docs/")
        .max_keys(1)
        .send()
        .await
        .unwrap();
    assert_eq!(first_page.output.is_truncated(), Some(true));
    let pinned = first_page.snapshot;
    let mut keys = vec![first_page.output.contents()[0].key().unwrap().to_string()];
    let mut token = first_page
        .output
        .next_continuation_token()
        .map(str::to_string);

    client
        .put_object()
        .bucket(&bucket)
        .key("docs/after-snapshot.txt")
        .body(ByteStream::from_static(b"not in pinned scan"))
        .send()
        .await
        .unwrap();
    while let Some(cursor) = token {
        let page = client
            .list_objects_v2()
            .bucket(&bucket)
            .prefix("docs/")
            .max_keys(1)
            .continuation_token(cursor)
            .send()
            .await
            .unwrap();
        assert_eq!(page.snapshot, pinned);
        keys.extend(
            page.output
                .contents()
                .iter()
                .filter_map(|value| value.key().map(str::to_string)),
        );
        token = page.output.next_continuation_token().map(str::to_string);
    }
    assert_eq!(
        keys,
        [
            "docs/api.txt",
            "docs/b.txt",
            "docs/c.txt",
            "docs/conditional-create.txt",
            "docs/copied.txt",
            "docs/d.txt"
        ]
    );

    let before_multipart = client.head_commit().await.unwrap();
    let created = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("docs/multipart.bin")
        .content_type("application/octet-stream")
        .send()
        .await
        .unwrap();
    let upload_id = created.upload_id().unwrap().to_string();
    let created_two = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("docs/multipart-2.bin")
        .send()
        .await
        .unwrap();
    let upload_two = created_two.upload_id().unwrap().to_string();
    let created_three = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("docs/multipart-3.bin")
        .send()
        .await
        .unwrap();
    let upload_three = created_three.upload_id().unwrap().to_string();
    let active_uploads = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .prefix("docs/")
        .max_uploads(1)
        .send()
        .await
        .unwrap();
    assert_eq!(active_uploads.uploads().len(), 1);
    assert!(active_uploads.is_truncated().unwrap_or(false));
    let first_key = active_uploads.next_key_marker().unwrap().to_string();
    let first_cursor = active_uploads.next_upload_id_marker().unwrap().to_string();
    assert!(!first_cursor.starts_with("pu1_"));
    let mut snapshot_uploads = active_uploads
        .uploads()
        .iter()
        .filter_map(|upload| upload.upload_id().map(str::to_string))
        .collect::<Vec<_>>();

    // Later pages are pinned to the immutable first-page catalog even when
    // authoritative upload manifests change between requests.
    client
        .abort_multipart_upload()
        .bucket(&bucket)
        .key("docs/multipart-3.bin")
        .upload_id(&upload_three)
        .send()
        .await
        .unwrap();
    let created_late = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("docs/multipart-0.bin")
        .send()
        .await
        .unwrap();
    let upload_late = created_late.upload_id().unwrap().to_string();

    let mut key_marker = first_key.clone();
    let mut upload_marker = first_cursor.clone();
    loop {
        let page = client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("docs/")
            .key_marker(&key_marker)
            .upload_id_marker(&upload_marker)
            .max_uploads(1)
            .send()
            .await
            .unwrap();
        snapshot_uploads.extend(
            page.uploads()
                .iter()
                .filter_map(|upload| upload.upload_id().map(str::to_string)),
        );
        if !page.is_truncated().unwrap_or(false) {
            break;
        }
        key_marker = page.next_key_marker().unwrap().to_string();
        upload_marker = page.next_upload_id_marker().unwrap().to_string();
    }
    snapshot_uploads.sort();
    let mut expected_uploads = vec![upload_id.clone(), upload_two.clone(), upload_three.clone()];
    expected_uploads.sort();
    assert_eq!(snapshot_uploads, expected_uploads);
    assert!(!snapshot_uploads.contains(&upload_late));

    let mut tampered = first_cursor.clone().into_bytes();
    let final_byte = tampered.last_mut().unwrap();
    *final_byte = if *final_byte == b'A' { b'B' } else { b'A' };
    let tampered = String::from_utf8(tampered).unwrap();
    assert_eq!(
        client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("docs/")
            .key_marker(&first_key)
            .upload_id_marker(tampered)
            .max_uploads(1)
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidContinuationToken
    );
    assert_eq!(
        client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("other/")
            .key_marker(&first_key)
            .upload_id_marker(&first_cursor)
            .max_uploads(1)
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidContinuationToken
    );
    assert_eq!(
        client
            .list_multipart_uploads()
            .bucket(&bucket)
            .prefix("docs/")
            .key_marker("docs/not-the-last-key")
            .upload_id_marker(&first_cursor)
            .max_uploads(1)
            .send()
            .await
            .unwrap_err()
            .code,
        ErrorCode::InvalidContinuationToken
    );
    client
        .abort_multipart_upload()
        .bucket(&bucket)
        .key("docs/multipart-2.bin")
        .upload_id(&upload_two)
        .send()
        .await
        .unwrap();
    client
        .abort_multipart_upload()
        .bucket(&bucket)
        .key("docs/multipart-0.bin")
        .upload_id(&upload_late)
        .send()
        .await
        .unwrap();
    let uploaded = client
        .upload_part()
        .bucket(&bucket)
        .key("docs/multipart.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"multipart-via-sdk"))
        .send()
        .await
        .unwrap();
    assert_eq!(client.head_commit().await.unwrap(), before_multipart);
    let completed = client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("docs/multipart.bin")
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(uploaded.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(completed.output.e_tag().unwrap().ends_with("-1\""));
    let multipart_body = client
        .get_object()
        .bucket(&bucket)
        .key("docs/multipart.bin")
        .send()
        .await
        .unwrap()
        .output
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(multipart_body, &b"multipart-via-sdk"[..]);

    let copy_upload = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key("docs/multipart-copy.bin")
        .send()
        .await
        .unwrap();
    let copy_upload_id = copy_upload.upload_id().unwrap();
    let copied_part = client
        .upload_part_copy()
        .bucket(&bucket)
        .key("docs/multipart-copy.bin")
        .upload_id(copy_upload_id)
        .part_number(1)
        .copy_source(format!("{bucket}/docs/api.txt"))
        .copy_source_range("bytes=1-3")
        .send()
        .await
        .unwrap();
    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key("docs/multipart-copy.bin")
        .upload_id(copy_upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(copied_part.copy_part_result().unwrap().e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let copied_body = client
        .get_object()
        .bucket(&bucket)
        .key("docs/multipart-copy.bin")
        .send()
        .await
        .unwrap()
        .output
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(copied_body, &b"eco"[..]);

    let before_session = client.head_commit().await.unwrap();
    let mut session = client
        .begin_commit()
        .message("two objects, one bucket commit")
        .start()
        .await
        .unwrap();
    session
        .put_object()
        .bucket(&bucket)
        .key("atomic/left.txt")
        .body(ByteStream::from_static(b"left"))
        .stage()
        .await
        .unwrap();
    let workspace_id = session.id();
    drop(session);
    let mut session = client.resume_commit(workspace_id).await.unwrap();
    session
        .put_object()
        .bucket(&bucket)
        .key("atomic/right.txt")
        .body(ByteStream::from_static(b"right"))
        .stage()
        .await
        .unwrap();
    assert_eq!(client.head_commit().await.unwrap(), before_session);
    let atomic = session.publish().await.unwrap();
    assert_eq!(atomic.parents, [before_session]);
    assert_eq!(atomic.changed_keys, 2);
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("atomic/left.txt")
            .send()
            .await
            .unwrap()
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes(),
        &b"left"[..]
    );

    let deleted = client
        .delete_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.output.delete_marker(), Some(true));
    assert!(client
        .get_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .send()
        .await
        .is_err());

    let history = client
        .list_object_versions()
        .bucket(&bucket)
        .prefix("docs/api.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(history.output.versions().len(), 2);
    assert_eq!(history.output.delete_markers().len(), 1);
    assert_eq!(history.output.delete_markers()[0].is_latest(), Some(true));

    let version_page = client
        .list_object_versions()
        .bucket(&bucket)
        .prefix("docs/api.txt")
        .max_keys(1)
        .send()
        .await
        .unwrap();
    assert_eq!(version_page.output.is_truncated(), Some(true));
    let next_key = version_page
        .output
        .next_key_marker()
        .expect("truncated version page has a logical key")
        .to_string();
    let next_version = version_page
        .output
        .next_version_id_marker()
        .expect("truncated version page has a signed cursor")
        .to_string();
    assert_eq!(next_key, "docs/api.txt");
    let later_version = client
        .put_object()
        .bucket(&bucket)
        .key("docs/api.txt")
        .body(ByteStream::from_static(
            b"written after the first history page",
        ))
        .send()
        .await
        .unwrap();
    assert_ne!(later_version.snapshot, version_page.snapshot);
    let second_version_page = client
        .list_object_versions()
        .bucket(&bucket)
        .prefix("docs/api.txt")
        .max_keys(1)
        .key_marker(next_key)
        .version_id_marker(next_version)
        .send()
        .await
        .unwrap();
    assert_eq!(second_version_page.snapshot, version_page.snapshot);
    assert_eq!(
        second_version_page.output.versions().len()
            + second_version_page.output.delete_markers().len(),
        1
    );
    let pinned_history = client
        .at(version_page.snapshot)
        .await
        .unwrap()
        .list_object_versions()
        .bucket(&bucket)
        .prefix("docs/api.txt")
        .max_keys(100)
        .send()
        .await
        .unwrap();
    assert_eq!(pinned_history.snapshot, version_page.snapshot);
    assert_eq!(
        pinned_history.output.versions().len() + pinned_history.output.delete_markers().len(),
        3
    );
    let moving_history = client
        .list_object_versions()
        .bucket(&bucket)
        .prefix("docs/api.txt")
        .max_keys(100)
        .send()
        .await
        .unwrap();
    assert_eq!(
        moving_history.output.versions().len() + moving_history.output.delete_markers().len(),
        4
    );

    let key_1023 = "x".repeat(1_023);
    let key_1024 = "y".repeat(1_024);
    let multibyte_1024 = "é".repeat(512);
    for key in [&key_1023, &key_1024, &multibyte_1024] {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"boundary"))
            .send()
            .await
            .unwrap();
    }
    for invalid_key in ["z".repeat(1_025), format!("{multibyte_1024}x")] {
        assert_eq!(
            client
                .put_object()
                .bucket(&bucket)
                .key(invalid_key)
                .body(ByteStream::from_static(b"must fail before publication"))
                .send()
                .await
                .unwrap_err()
                .code,
            ErrorCode::InvalidKey
        );
    }

    for key in ["tree/a/1", "tree/a/2", "tree/b/1", "tree/root"] {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"delimiter fixture"))
            .send()
            .await
            .unwrap();
    }
    let zero_page = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("tree/")
        .max_keys(0)
        .send()
        .await
        .unwrap();
    assert_eq!(zero_page.output.max_keys(), Some(0));
    assert_eq!(zero_page.output.key_count(), Some(0));
    assert!(zero_page.output.contents().is_empty());
    let clamped_page = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("tree/")
        .max_keys(1_001)
        .send()
        .await
        .unwrap();
    assert_eq!(clamped_page.output.max_keys(), Some(1_000));
    assert_eq!(clamped_page.output.key_count(), Some(4));
    let delimiter_page = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("tree/")
        .delimiter("/")
        .max_keys(2)
        .send()
        .await
        .unwrap();
    assert_eq!(delimiter_page.output.key_count(), Some(2));
    assert_eq!(
        delimiter_page
            .output
            .common_prefixes()
            .iter()
            .map(|prefix| prefix.prefix().unwrap())
            .collect::<Vec<_>>(),
        ["tree/a/", "tree/b/"]
    );
    let delimiter_tail = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("tree/")
        .delimiter("/")
        .max_keys(2)
        .continuation_token(
            delimiter_page
                .output
                .next_continuation_token()
                .expect("grouped delimiter page is truncated"),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(delimiter_tail.snapshot, delimiter_page.snapshot);
    assert_eq!(
        delimiter_tail
            .output
            .contents()
            .iter()
            .map(|object| object.key().unwrap())
            .collect::<Vec<_>>(),
        ["tree/root"]
    );
    assert!(delimiter_tail.output.common_prefixes().is_empty());

    for key in ["unicode/α雪one", "unicode/α雪two", "unicode/β"] {
        client
            .put_object()
            .bucket(&bucket)
            .key(key)
            .body(ByteStream::from_static(b"unicode delimiter fixture"))
            .send()
            .await
            .unwrap();
    }
    let unicode_delimiter = client
        .list_objects_v2()
        .bucket(&bucket)
        .prefix("unicode/")
        .delimiter("雪")
        .max_keys(100)
        .send()
        .await
        .unwrap();
    assert_eq!(unicode_delimiter.output.key_count(), Some(2));
    assert_eq!(
        unicode_delimiter.output.common_prefixes()[0].prefix(),
        Some("unicode/α雪")
    );
    assert_eq!(
        unicode_delimiter.output.contents()[0].key(),
        Some("unicode/β")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_reflog_ref_recovery_drill_restores_a_mistaken_reset() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, _) = rustfs_client().await;
    let bucket = format!(
        "prolly-recovery-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    aws.create_bucket().bucket(&bucket).send().await.unwrap();
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let repository_prefix = unique_prefix("ref-recovery-drill");
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer("recovery-drill")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    let first = client
        .put_object()
        .bucket(&bucket)
        .key("recovery/object.txt")
        .body(ByteStream::from_static(b"first"))
        .send()
        .await
        .unwrap()
        .commit
        .unwrap();
    let second = client
        .put_object()
        .bucket(&bucket)
        .key("recovery/object.txt")
        .body(ByteStream::from_static(b"second"))
        .send()
        .await
        .unwrap()
        .commit
        .unwrap();
    let native_versions = client.list_native_branch_ref_versions().await.unwrap();
    let first_native_version = native_versions
        .iter()
        .find(|version| version.target == first.id && !version.tombstone)
        .expect("native ref version targeting first commit")
        .version_id
        .clone();
    let second_native_version = native_versions
        .iter()
        .find(|version| version.target == second.id && !version.tombstone)
        .expect("native ref version targeting second commit")
        .version_id
        .clone();
    let ref_prefix = format!("{repository_prefix}/refs/heads");
    let versions_before_reset = physical_version_snapshot(aws.clone(), &bucket, &ref_prefix).await;

    let reset = client
        .reset_branch(first.id, second.id, "qualification mistaken reset")
        .await
        .unwrap();
    assert_eq!(reset.new_target, first.id);
    let reset_entry = client
        .list_reflog()
        .await
        .unwrap()
        .into_iter()
        .find(|(_, entry)| entry.message == "qualification mistaken reset")
        .expect("reset reflog entry");
    let recovered = client
        .recover_branch(reset_entry.0, first.id, "qualification restore from reflog")
        .await
        .unwrap();
    assert_eq!(recovered.new_target, second.id);
    let native_reset = client
        .recover_branch_from_native_version(
            &first_native_version,
            second.id,
            "qualification restore selected native ref version",
        )
        .await
        .unwrap();
    assert_eq!(native_reset.new_target, first.id);
    let native_restore = client
        .recover_branch_from_native_version(
            &second_native_version,
            first.id,
            "qualification undo native ref recovery",
        )
        .await
        .unwrap();
    assert_eq!(native_restore.new_target, second.id);
    let body = client
        .get_object()
        .bucket(&bucket)
        .key("recovery/object.txt")
        .send()
        .await
        .unwrap()
        .output
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(body.as_ref(), b"second");
    let versions_after_recovery =
        physical_version_snapshot(aws.clone(), &bucket, &ref_prefix).await;
    assert_eq!(
        versions_after_recovery.len(),
        versions_before_reset.len() + 4,
        "each logical or native recovery move must add one native ref version"
    );
    client.fsck().await.unwrap();

    let plane = AwsS3ObjectPlane::new(aws.clone(), &bucket);
    let mut continuation = None;
    loop {
        let page = plane
            .list(ListRequest {
                prefix: String::new(),
                continuation,
                limit: 1_000,
                include_versions: true,
            })
            .await
            .unwrap();
        for entry in page.entries {
            plane
                .delete_exact(
                    &entry.path,
                    PhysicalVersion::Versioned {
                        version_id: entry.metadata.token.version_id.unwrap(),
                    },
                )
                .await
                .unwrap();
        }
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    aws.delete_bucket().bucket(&bucket).send().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_restart_recovery_drill() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let Ok(phase) = std::env::var("PROLLY_S3_RESTART_DRILL_PHASE") else {
        eprintln!(
            "set PROLLY_S3_RESTART_DRILL_PHASE=prepare|ready|verify through the restart script"
        );
        return;
    };
    let prefix = std::env::var("PROLLY_S3_RESTART_PREFIX")
        .expect("restart drill requires PROLLY_S3_RESTART_PREFIX");
    let (aws, bucket) = rustfs_client().await;
    match phase.as_str() {
        "prepare" => {
            let client = Client::builder()
                .aws_client(aws)
                .bucket(&bucket)
                .repository_prefix(&prefix)
                .writer("restart-drill-prepare")
                .provider_identity(rustfs_provider_identity())
                .attestation_signer(test_attestation_signer())
                .initialize()
                .await
                .unwrap();
            client
                .put_object()
                .bucket(&bucket)
                .key("restart/before.txt")
                .body(ByteStream::from_static(b"durable before provider restart"))
                .send()
                .await
                .unwrap();
            client.fsck().await.unwrap();
        }
        "ready" => {
            aws.list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .max_keys(1)
                .send()
                .await
                .unwrap();
        }
        "verify" => {
            let client = Client::builder()
                .aws_client(aws)
                .bucket(&bucket)
                .repository_prefix(&prefix)
                .writer("restart-drill-verify")
                .provider_identity(rustfs_provider_identity())
                .attestation_signer(test_attestation_signer())
                .open()
                .await
                .unwrap();
            let before = client
                .get_object()
                .bucket(&bucket)
                .key("restart/before.txt")
                .send()
                .await
                .unwrap()
                .output
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes();
            assert_eq!(before.as_ref(), b"durable before provider restart");
            client
                .put_object()
                .bucket(&bucket)
                .key("restart/after.txt")
                .body(ByteStream::from_static(b"writable after provider restart"))
                .send()
                .await
                .unwrap();
            client.fsck().await.unwrap();
        }
        other => panic!("unknown restart drill phase {other}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_iam_rotation_process_helper() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_IAM_DRILL").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and run through the IAM drill script");
        return;
    }
    let phase = std::env::var("PROLLY_S3_IAM_PHASE").expect("IAM drill phase");
    let prefix = std::env::var("PROLLY_S3_IAM_PREFIX").expect("IAM drill prefix");
    let (aws, bucket) = if matches!(phase.as_str(), "prepare" | "verify") {
        rustfs_client().await
    } else {
        let access_key =
            std::env::var("PROLLY_S3_IAM_ACCESS_KEY").expect("restricted IAM access key");
        let secret_key =
            std::env::var("PROLLY_S3_IAM_SECRET_KEY").expect("restricted IAM secret key");
        rustfs_client_with_credentials(access_key, secret_key).await
    };

    match phase.as_str() {
        "prepare" => {
            aws.put_bucket_versioning()
                .bucket(&bucket)
                .versioning_configuration(
                    VersioningConfiguration::builder()
                        .status(BucketVersioningStatus::Enabled)
                        .build(),
                )
                .send()
                .await
                .unwrap();
            let client = Client::builder()
                .aws_client(aws)
                .bucket(&bucket)
                .repository_prefix(&prefix)
                .writer("iam-provisioner")
                .provider_identity(rustfs_provider_identity())
                .attestation_signer(test_attestation_signer())
                .initialize()
                .await
                .unwrap();
            client
                .put_object()
                .bucket(&bucket)
                .key("iam/before-rotation.txt")
                .body(ByteStream::from_static(b"created by the provisioner"))
                .send()
                .await
                .unwrap();
            client.fsck().await.unwrap();
        }
        "old-active" => {
            let client = Client::builder()
                .aws_client(aws.clone())
                .bucket(&bucket)
                .repository_prefix(&prefix)
                .writer("iam-runtime-old")
                .provider_identity(rustfs_provider_identity())
                .attestation_signer(test_attestation_signer())
                .open()
                .await
                .unwrap();
            assert_eq!(
                client
                    .get_object()
                    .bucket(&bucket)
                    .key("iam/before-rotation.txt")
                    .send()
                    .await
                    .unwrap()
                    .output
                    .body
                    .collect()
                    .await
                    .unwrap()
                    .into_bytes()
                    .as_ref(),
                b"created by the provisioner"
            );
            client
                .put_object()
                .bucket(&bucket)
                .key("iam/old-credential.txt")
                .body(ByteStream::from_static(
                    b"created by the old runtime credential",
                ))
                .send()
                .await
                .unwrap();
            client.fsck().await.unwrap();

            let outside_put = aws
                .put_object()
                .bucket(&bucket)
                .key(format!("{prefix}-outside/forbidden"))
                .body(ByteStream::from_static(b"must be denied"))
                .send()
                .await
                .unwrap_err();
            assert_permission_denied(&outside_put);
            let outside_list = aws
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(format!("{prefix}-outside/"))
                .send()
                .await
                .unwrap_err();
            assert_permission_denied(&outside_list);
            let path_delete = aws
                .delete_object()
                .bucket(&bucket)
                .key(format!("{prefix}/format/v1.cbor"))
                .send()
                .await
                .unwrap_err();
            assert_permission_denied(&path_delete);
            let exact_version_delete = aws
                .delete_object()
                .bucket(&bucket)
                .key(format!("{prefix}/format/v1.cbor"))
                .version_id("forbidden-version")
                .send()
                .await
                .unwrap_err();
            assert_permission_denied(&exact_version_delete);
            let native_versions = aws
                .list_object_versions()
                .bucket(&bucket)
                .prefix(format!("{prefix}/"))
                .send()
                .await
                .unwrap();
            let format_path = format!("{prefix}/format/v1.cbor");
            let format_version = native_versions
                .versions()
                .iter()
                .find(|version| version.key() == Some(format_path.as_str()))
                .expect("RustFS ListBucket permission exposes native versions")
                .version_id()
                .expect("native version ID")
                .to_string();
            let native_version_read = aws
                .get_object()
                .bucket(&bucket)
                .key(&format_path)
                .version_id(format_version)
                .send()
                .await
                .unwrap()
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes();
            let current_read = aws
                .get_object()
                .bucket(&bucket)
                .key(&format_path)
                .send()
                .await
                .unwrap()
                .body
                .collect()
                .await
                .unwrap()
                .into_bytes();
            assert_eq!(native_version_read, current_read);
            let versioning_mutation = aws
                .put_bucket_versioning()
                .bucket(&bucket)
                .versioning_configuration(
                    VersioningConfiguration::builder()
                        .status(BucketVersioningStatus::Suspended)
                        .build(),
                )
                .send()
                .await
                .unwrap_err();
            assert_permission_denied(&versioning_mutation);
        }
        "new-active" => {
            let client = Client::builder()
                .aws_client(aws)
                .bucket(&bucket)
                .repository_prefix(&prefix)
                .writer("iam-runtime-new")
                .provider_identity(rustfs_provider_identity())
                .attestation_signer(test_attestation_signer())
                .open()
                .await
                .unwrap();
            for (key, expected) in [
                (
                    "iam/before-rotation.txt",
                    b"created by the provisioner".as_slice(),
                ),
                (
                    "iam/old-credential.txt",
                    b"created by the old runtime credential".as_slice(),
                ),
            ] {
                assert_eq!(
                    client
                        .get_object()
                        .bucket(&bucket)
                        .key(key)
                        .send()
                        .await
                        .unwrap()
                        .output
                        .body
                        .collect()
                        .await
                        .unwrap()
                        .into_bytes()
                        .as_ref(),
                    expected
                );
            }
            client
                .put_object()
                .bucket(&bucket)
                .key("iam/new-credential.txt")
                .body(ByteStream::from_static(
                    b"created by the new runtime credential",
                ))
                .send()
                .await
                .unwrap();
            client.fsck().await.unwrap();
        }
        "old-revoked" => {
            let error = match Client::builder()
                .aws_client(aws)
                .bucket(&bucket)
                .repository_prefix(&prefix)
                .writer("iam-runtime-revoked")
                .provider_identity(rustfs_provider_identity())
                .attestation_signer(test_attestation_signer())
                .open()
                .await
            {
                Ok(_) => panic!("revoked credential unexpectedly opened the repository"),
                Err(error) => error,
            };
            assert_eq!(error.code, ErrorCode::PermissionDenied);
            assert_eq!(error.retry, prolly_s3_client::core::RetryAdvice::Never);
            assert_eq!(error.provider_code.as_deref(), Some("InvalidRequest"));
            assert_eq!(
                error.provider_message.as_deref(),
                Some("ErrAccessKeyDisabled")
            );
        }
        "verify" => {
            let client = Client::builder()
                .aws_client(aws.clone())
                .bucket(&bucket)
                .repository_prefix(&prefix)
                .writer("iam-verifier")
                .provider_identity(rustfs_provider_identity())
                .attestation_signer(test_attestation_signer())
                .open()
                .await
                .unwrap();
            for (key, expected) in [
                (
                    "iam/before-rotation.txt",
                    b"created by the provisioner".as_slice(),
                ),
                (
                    "iam/old-credential.txt",
                    b"created by the old runtime credential".as_slice(),
                ),
                (
                    "iam/new-credential.txt",
                    b"created by the new runtime credential".as_slice(),
                ),
            ] {
                assert_eq!(
                    client
                        .get_object()
                        .bucket(&bucket)
                        .key(key)
                        .send()
                        .await
                        .unwrap()
                        .output
                        .body
                        .collect()
                        .await
                        .unwrap()
                        .into_bytes()
                        .as_ref(),
                    expected
                );
            }
            assert_eq!(client.log(10).await.unwrap().len(), 4);
            client.fsck().await.unwrap();
            let versioning = aws
                .get_bucket_versioning()
                .bucket(&bucket)
                .send()
                .await
                .unwrap();
            assert_eq!(versioning.status(), Some(&BucketVersioningStatus::Enabled));
        }
        other => panic!("unknown IAM drill phase {other}"),
    }
    eprintln!(
        "RUSTFS_IAM_PHASE phase={phase} prefix={prefix} result=ok rustfs_native_version_action_aliases={}",
        u8::from(phase == "old-active") * 2 + u8::from(phase == "old-revoked")
    );
}

struct ActiveOutageScenario {
    plane: Arc<RestartAfterAcceptedRefPlane>,
    repository: Repository<RestartAfterAcceptedRefPlane>,
    accounting_aws: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
    wire: S3WireAttemptInterceptor,
}

async fn active_outage_scenario(name: &str, writer: &str) -> ActiveOutageScenario {
    let container =
        std::env::var("PROLLY_RUSTFS_CONTAINER").unwrap_or_else(|_| "prolly-rustfs".to_string());
    let prefix_root =
        std::env::var("PROLLY_S3_CHAOS_PREFIX").unwrap_or_else(|_| unique_prefix("active-outage"));
    let prefix = format!("{prefix_root}/{name}");
    let (aws, bucket, wire) = rustfs_client_with_wire_metrics().await;
    let (accounting_aws, accounting_bucket) = rustfs_client().await;
    assert_eq!(accounting_bucket, bucket);
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let plane = Arc::new(RestartAfterAcceptedRefPlane::new(
        AwsS3ObjectPlane::new(aws, &bucket),
        Arc::<str>::from(container),
    ));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: prefix.clone(),
            writer: writer.to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    ActiveOutageScenario {
        plane,
        repository,
        accounting_aws,
        bucket,
        prefix,
        wire,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_active_outage_reconciles_accepted_branch_publication() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_CHAOS").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_CHAOS=1 through the chaos script");
        return;
    }
    let container =
        std::env::var("PROLLY_RUSTFS_CONTAINER").unwrap_or_else(|_| "prolly-rustfs".to_string());
    let prefix_root =
        std::env::var("PROLLY_S3_CHAOS_PREFIX").unwrap_or_else(|_| unique_prefix("active-outage"));
    let prefix = format!("{prefix_root}/ordinary");
    let (aws, bucket, wire) = rustfs_client_with_wire_metrics().await;
    let (accounting_aws, accounting_bucket) = rustfs_client().await;
    assert_eq!(accounting_bucket, bucket);
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let plane = Arc::new(RestartAfterAcceptedRefPlane::new(
        AwsS3ObjectPlane::new(aws, &bucket),
        Arc::<str>::from(container),
    ));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: prefix.clone(),
            writer: "active-outage-chaos".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let storage_before = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    plane.reset_s3_metrics();
    wire.reset();
    plane.arm();
    let operation = OperationId::new();
    let started = Instant::now();
    let receipt = repository
        .put_bytes(
            "main",
            b"chaos/accepted-before-restart.txt".to_vec(),
            b"one logical version after an accepted CAS loses its response".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(operation),
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let first_sdk = plane.reset_s3_metrics();
    let first_wire = wire.reset();
    let storage_after = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after = physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;

    assert!(plane.fired());
    assert_eq!(plane.restarts(), 1);
    assert_eq!(receipt.operation, operation);
    assert!(
        receipt.idempotent_replay,
        "the accepted CAS must be recovered through durable operation reconciliation"
    );
    assert_eq!(first_wire.executions, first_sdk.total_calls());
    assert!(first_wire.transmissions >= first_wire.executions);

    plane.reset_s3_metrics();
    wire.reset();
    let replay = repository
        .put_bytes(
            "main",
            b"chaos/accepted-before-restart.txt".to_vec(),
            b"one logical version after an accepted CAS loses its response".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            Some(operation),
        )
        .await
        .unwrap();
    let replay_sdk = plane.reset_s3_metrics();
    let replay_wire = wire.reset();
    let storage_after_replay =
        physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after_replay =
        physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert_eq!(replay.id, receipt.id);
    assert_eq!(replay.operation, operation);
    assert!(replay.idempotent_replay);
    assert_eq!(replay_wire.executions, replay_sdk.total_calls());
    assert!(replay_wire.transmissions >= replay_wire.executions);
    let replay_versions = versions_after_replay
        .iter()
        .filter(|entry| !versions_after.contains(entry))
        .collect::<Vec<_>>();
    assert!(
        replay_versions
            .iter()
            .all(|entry| entry.0.contains("/publications/")),
        "an idempotent replay may terminalize coordination state but must not add commits, refs, reflogs, trees, or content: {replay_versions:?}"
    );
    let replay_storage_delta = storage_after_replay.saturating_sub(storage_after);
    assert!(replay_storage_delta <= 4 * 1_024);

    let body = repository
        .get_current("main", b"chaos/accepted-before-restart.txt")
        .await
        .unwrap();
    assert_eq!(
        body.bytes,
        b"one logical version after an accepted CAS loses its response"
    );
    let log = repository.log("main", 10).await.unwrap();
    assert_eq!(
        log.len(),
        2,
        "initialization plus one recovered publication must be the complete history"
    );
    repository.fsck().await.unwrap();
    eprintln!(
        "ACTIVE_OUTAGE_CHAOS scenario=ordinary prefix={prefix} accepted_lost_responses=1 reconciled_operations=1 provider_restarts={} restart_ms={} operation_elapsed_ms={:.3} storage_before_bytes={storage_before} storage_after_bytes={storage_after} stored_delta_bytes={} first_sdk_calls={} first_wire_transmissions={} first_wire_retries={} replay_sdk_calls={} replay_wire_transmissions={} replay_wire_retries={} replay_coordination_versions={} replay_coordination_bytes={replay_storage_delta} logical_versions=1 final_fsck=ok",
        plane.restarts(),
        plane.restart_millis(),
        elapsed.as_secs_f64() * 1_000.0,
        i128::from(storage_after) - i128::from(storage_before),
        first_sdk.total_calls(),
        first_wire.transmissions,
        first_wire.retry_transmissions(),
        replay_sdk.total_calls(),
        replay_wire.transmissions,
        replay_wire.retry_transmissions(),
        replay_versions.len(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_active_outage_reconciles_atomic_multi_delete() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_CHAOS").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_CHAOS=1 through the chaos script");
        return;
    }
    let ActiveOutageScenario {
        plane,
        repository,
        accounting_aws,
        bucket,
        prefix,
        wire,
    } = active_outage_scenario("multi-delete", "active-outage-multi-delete").await;
    let keys = vec![
        b"chaos/delete-a.txt".to_vec(),
        b"chaos/delete-b.txt".to_vec(),
    ];
    for (key, body) in keys
        .iter()
        .zip([b"delete A".as_slice(), b"delete B".as_slice()])
    {
        repository
            .put_bytes(
                "main",
                key.clone(),
                body.to_vec(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let storage_before = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    plane.reset_s3_metrics();
    wire.reset();
    plane.arm();
    let operation = OperationId::new();
    let started = Instant::now();
    let receipt = repository
        .delete_objects("main", keys.clone(), Some(operation))
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let first_sdk = plane.reset_s3_metrics();
    let first_wire = wire.reset();
    let storage_after = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after = physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert!(plane.fired());
    assert_eq!(plane.restarts(), 1);
    assert_eq!(receipt.operation, operation);
    assert_eq!(receipt.changed_keys, 2);
    assert_eq!(receipt.object_versions.len(), 2);
    assert!(receipt.idempotent_replay);
    assert_eq!(first_wire.executions, first_sdk.total_calls());
    assert!(first_wire.transmissions >= first_wire.executions);

    plane.reset_s3_metrics();
    wire.reset();
    let replay = repository
        .delete_objects("main", keys.clone(), Some(operation))
        .await
        .unwrap();
    let replay_sdk = plane.reset_s3_metrics();
    let replay_wire = wire.reset();
    let storage_after_replay =
        physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after_replay =
        physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert_eq!(replay.id, receipt.id);
    assert_eq!(replay.object_versions, receipt.object_versions);
    assert!(replay.idempotent_replay);
    assert_eq!(replay_wire.executions, replay_sdk.total_calls());
    assert!(replay_wire.transmissions >= replay_wire.executions);
    let replay_versions = versions_after_replay
        .iter()
        .filter(|entry| !versions_after.contains(entry))
        .collect::<Vec<_>>();
    assert!(
        replay_versions
            .iter()
            .all(|entry| entry.0.contains("/publications/")),
        "multi-delete replay may terminalize publication coordination only: {replay_versions:?}"
    );
    let replay_storage_delta = storage_after_replay.saturating_sub(storage_after);
    assert!(replay_storage_delta <= 4 * 1_024);
    for key in &keys {
        assert_eq!(
            repository.get_current("main", key).await.unwrap_err().code,
            ErrorCode::NoSuchKey
        );
        let logical_versions = repository
            .list_object_versions("main", key, 10)
            .await
            .unwrap()
            .1;
        assert_eq!(logical_versions.len(), 2);
        assert!(matches!(
            logical_versions[0].body.kind,
            ObjectVersionKindV1::DeleteMarker
        ));
    }
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 4);
    repository.fsck().await.unwrap();
    eprintln!(
        "ACTIVE_OUTAGE_CHAOS scenario=multi-delete prefix={prefix} accepted_lost_responses=1 reconciled_operations=1 provider_restarts={} restart_ms={} operation_elapsed_ms={:.3} storage_before_bytes={storage_before} storage_after_bytes={storage_after} stored_delta_bytes={} first_sdk_calls={} first_wire_transmissions={} first_wire_retries={} replay_sdk_calls={} replay_wire_transmissions={} replay_wire_retries={} replay_coordination_versions={} replay_coordination_bytes={replay_storage_delta} logical_versions=2 delete_markers=2 final_fsck=ok",
        plane.restarts(),
        plane.restart_millis(),
        elapsed.as_secs_f64() * 1_000.0,
        i128::from(storage_after) - i128::from(storage_before),
        first_sdk.total_calls(),
        first_wire.transmissions,
        first_wire.retry_transmissions(),
        replay_sdk.total_calls(),
        replay_wire.transmissions,
        replay_wire.retry_transmissions(),
        replay_versions.len(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_active_outage_reconciles_restore_publication() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_CHAOS").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_CHAOS=1 through the chaos script");
        return;
    }
    let ActiveOutageScenario {
        plane,
        repository,
        accounting_aws,
        bucket,
        prefix,
        wire,
    } = active_outage_scenario("restore", "active-outage-restore").await;
    let source = repository.head("main").await.unwrap();
    let expected = repository
        .put_bytes(
            "main",
            b"chaos/remove-on-restore.txt".to_vec(),
            b"restore removes this value".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap()
        .id;
    let operation = OperationId::new();
    let storage_before = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    plane.reset_s3_metrics();
    wire.reset();
    plane.arm();
    let started = Instant::now();
    let receipt = repository
        .restore(
            "main",
            source,
            expected,
            Some(operation),
            Some("active outage restore".to_string()),
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let first_sdk = plane.reset_s3_metrics();
    let first_wire = wire.reset();
    let storage_after = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after = physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert!(plane.fired());
    assert_eq!(plane.restarts(), 1);
    assert_eq!(receipt.operation, operation);
    assert_eq!(receipt.changed_keys, 1);
    assert!(receipt.idempotent_replay);
    assert_eq!(first_wire.executions, first_sdk.total_calls());
    assert!(first_wire.transmissions >= first_wire.executions);

    plane.reset_s3_metrics();
    wire.reset();
    let replay = repository
        .restore(
            "main",
            source,
            expected,
            Some(operation),
            Some("active outage restore".to_string()),
        )
        .await
        .unwrap();
    let replay_sdk = plane.reset_s3_metrics();
    let replay_wire = wire.reset();
    let storage_after_replay =
        physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after_replay =
        physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert_eq!(replay.id, receipt.id);
    assert_eq!(replay.object_versions, receipt.object_versions);
    assert!(replay.idempotent_replay);
    assert_eq!(replay_wire.executions, replay_sdk.total_calls());
    assert!(replay_wire.transmissions >= replay_wire.executions);
    let replay_versions = versions_after_replay
        .iter()
        .filter(|entry| !versions_after.contains(entry))
        .collect::<Vec<_>>();
    assert!(
        replay_versions
            .iter()
            .all(|entry| entry.0.contains("/publications/")),
        "restore replay may terminalize publication coordination only: {replay_versions:?}"
    );
    let replay_storage_delta = storage_after_replay.saturating_sub(storage_after);
    assert!(replay_storage_delta <= 4 * 1_024);
    assert_eq!(
        repository
            .get_current("main", b"chaos/remove-on-restore.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    let logical_versions = repository
        .list_object_versions("main", b"chaos/remove-on-restore.txt", 10)
        .await
        .unwrap()
        .1;
    assert_eq!(logical_versions.len(), 2);
    assert!(matches!(
        logical_versions[0].body.kind,
        ObjectVersionKindV1::DeleteMarker
    ));
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 3);
    repository.fsck().await.unwrap();
    eprintln!(
        "ACTIVE_OUTAGE_CHAOS scenario=restore prefix={prefix} accepted_lost_responses=1 reconciled_operations=1 provider_restarts={} restart_ms={} operation_elapsed_ms={:.3} storage_before_bytes={storage_before} storage_after_bytes={storage_after} stored_delta_bytes={} first_sdk_calls={} first_wire_transmissions={} first_wire_retries={} replay_sdk_calls={} replay_wire_transmissions={} replay_wire_retries={} replay_coordination_versions={} replay_coordination_bytes={replay_storage_delta} logical_versions=1 delete_markers=1 final_fsck=ok",
        plane.restarts(),
        plane.restart_millis(),
        elapsed.as_secs_f64() * 1_000.0,
        i128::from(storage_after) - i128::from(storage_before),
        first_sdk.total_calls(),
        first_wire.transmissions,
        first_wire.retry_transmissions(),
        replay_sdk.total_calls(),
        replay_wire.transmissions,
        replay_wire.retry_transmissions(),
        replay_versions.len(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_active_outage_reconciles_administrative_reset() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_CHAOS").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_CHAOS=1 through the chaos script");
        return;
    }
    let ActiveOutageScenario {
        plane,
        repository,
        accounting_aws,
        bucket,
        prefix,
        wire,
    } = active_outage_scenario("reset", "active-outage-reset").await;
    let first = repository
        .put_bytes(
            "main",
            b"chaos/reset.txt".to_vec(),
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
            b"chaos/reset.txt".to_vec(),
            b"second".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let storage_before = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    plane.reset_s3_metrics();
    wire.reset();
    plane.arm();
    let started = Instant::now();
    let receipt = repository
        .reset_branch("main", first.id, second.id, "active outage reset")
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let first_sdk = plane.reset_s3_metrics();
    let first_wire = wire.reset();
    let storage_after = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after = physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert!(plane.fired());
    assert_eq!(plane.restarts(), 1);
    assert_eq!(receipt.old_target, Some(second.id));
    assert_eq!(receipt.new_target, first.id);
    assert_eq!(repository.head("main").await.unwrap(), first.id);
    assert_eq!(first_wire.executions, first_sdk.total_calls());
    assert!(first_wire.transmissions >= first_wire.executions);

    plane.reset_s3_metrics();
    wire.reset();
    let duplicate = repository
        .reset_branch("main", first.id, second.id, "duplicate reset")
        .await
        .unwrap_err();
    let replay_sdk = plane.reset_s3_metrics();
    let replay_wire = wire.reset();
    assert_eq!(duplicate.code, ErrorCode::PreconditionFailed);
    assert_eq!(replay_wire.executions, replay_sdk.total_calls());
    let storage_after_replay =
        physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after_replay =
        physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert_eq!(versions_after_replay, versions_after);
    assert_eq!(storage_after_replay, storage_after);
    assert_eq!(
        repository
            .get_current("main", b"chaos/reset.txt")
            .await
            .unwrap()
            .bytes,
        b"first"
    );
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 2);
    assert_eq!(repository.list_reflog("main").await.unwrap().len(), 4);
    repository.fsck().await.unwrap();
    eprintln!(
        "ACTIVE_OUTAGE_CHAOS scenario=reset prefix={prefix} accepted_lost_responses=1 reconciled_ref_moves=1 provider_restarts={} restart_ms={} operation_elapsed_ms={:.3} storage_before_bytes={storage_before} storage_after_bytes={storage_after} stored_delta_bytes={} first_sdk_calls={} first_wire_transmissions={} first_wire_retries={} duplicate_sdk_calls={} duplicate_wire_transmissions={} duplicate_wire_retries={} duplicate_versions=0 bucket_commits_created=0 reflog_entries=4 final_fsck=ok",
        plane.restarts(),
        plane.restart_millis(),
        elapsed.as_secs_f64() * 1_000.0,
        i128::from(storage_after) - i128::from(storage_before),
        first_sdk.total_calls(),
        first_wire.transmissions,
        first_wire.retry_transmissions(),
        replay_sdk.total_calls(),
        replay_wire.transmissions,
        replay_wire.retry_transmissions(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_active_outage_reconciles_branch_tombstone() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_CHAOS").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_CHAOS=1 through the chaos script");
        return;
    }
    let ActiveOutageScenario {
        plane,
        repository,
        accounting_aws,
        bucket,
        prefix,
        wire,
    } = active_outage_scenario("branch-delete", "active-outage-branch-delete").await;
    let root = repository.head("main").await.unwrap();
    repository.create_branch("doomed", root).await.unwrap();
    let storage_before = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    plane.reset_s3_metrics();
    wire.reset();
    plane.arm();
    let started = Instant::now();
    repository.delete_branch("doomed", root).await.unwrap();
    let elapsed = started.elapsed();
    let first_sdk = plane.reset_s3_metrics();
    let first_wire = wire.reset();
    let storage_after = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after = physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert!(plane.fired());
    assert_eq!(plane.restarts(), 1);
    assert_eq!(first_wire.executions, first_sdk.total_calls());
    assert!(first_wire.transmissions >= first_wire.executions);
    assert_eq!(
        repository.head("doomed").await.unwrap_err().code,
        ErrorCode::NoSuchBranch
    );
    assert!(!repository
        .list_branches()
        .await
        .unwrap()
        .iter()
        .any(|branch| branch.name == "doomed"));

    plane.reset_s3_metrics();
    wire.reset();
    let duplicate = repository.delete_branch("doomed", root).await.unwrap_err();
    let replay_sdk = plane.reset_s3_metrics();
    let replay_wire = wire.reset();
    assert_eq!(duplicate.code, ErrorCode::NoSuchBranch);
    assert_eq!(replay_wire.executions, replay_sdk.total_calls());
    let storage_after_replay =
        physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after_replay =
        physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert_eq!(versions_after_replay, versions_after);
    assert_eq!(storage_after_replay, storage_after);
    assert_eq!(repository.list_reflog("doomed").await.unwrap().len(), 2);
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 1);
    repository.fsck().await.unwrap();
    eprintln!(
        "ACTIVE_OUTAGE_CHAOS scenario=branch-delete prefix={prefix} accepted_lost_responses=1 reconciled_tombstones=1 provider_restarts={} restart_ms={} operation_elapsed_ms={:.3} storage_before_bytes={storage_before} storage_after_bytes={storage_after} stored_delta_bytes={} first_sdk_calls={} first_wire_transmissions={} first_wire_retries={} duplicate_sdk_calls={} duplicate_wire_transmissions={} duplicate_wire_retries={} duplicate_versions=0 bucket_commits_created=0 reflog_entries=2 final_fsck=ok",
        plane.restarts(),
        plane.restart_millis(),
        elapsed.as_secs_f64() * 1_000.0,
        i128::from(storage_after) - i128::from(storage_before),
        first_sdk.total_calls(),
        first_wire.transmissions,
        first_wire.retry_transmissions(),
        replay_sdk.total_calls(),
        replay_wire.transmissions,
        replay_wire.retry_transmissions(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_active_outage_reconciles_workspace_publication() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_CHAOS").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_CHAOS=1 through the chaos script");
        return;
    }
    let container =
        std::env::var("PROLLY_RUSTFS_CONTAINER").unwrap_or_else(|_| "prolly-rustfs".to_string());
    let prefix_root =
        std::env::var("PROLLY_S3_CHAOS_PREFIX").unwrap_or_else(|_| unique_prefix("active-outage"));
    let prefix = format!("{prefix_root}/workspace");
    let (aws, bucket, wire) = rustfs_client_with_wire_metrics().await;
    let (accounting_aws, accounting_bucket) = rustfs_client().await;
    assert_eq!(accounting_bucket, bucket);
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let plane = Arc::new(RestartAfterAcceptedRefPlane::new(
        AwsS3ObjectPlane::new(aws, &bucket),
        Arc::<str>::from(container),
    ));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: prefix.clone(),
            writer: "active-outage-workspace".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let workspace = repository
        .begin_workspace("main", "active outage workspace", 60_000)
        .await
        .unwrap();
    repository
        .workspace_put_stream(
            workspace.id,
            b"chaos/workspace-a.txt".to_vec(),
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"workspace A".to_vec())
            }),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();
    repository
        .workspace_put_stream(
            workspace.id,
            b"chaos/workspace-b.txt".to_vec(),
            futures_util::stream::once(async {
                Ok::<_, std::convert::Infallible>(b"workspace B".to_vec())
            }),
            ObjectHeaders::default(),
            BTreeMap::new(),
        )
        .await
        .unwrap();

    let storage_before = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    plane.reset_s3_metrics();
    wire.reset();
    plane.arm();
    let started = Instant::now();
    let receipt = repository.publish_workspace(workspace.id).await.unwrap();
    let elapsed = started.elapsed();
    let first_sdk = plane.reset_s3_metrics();
    let first_wire = wire.reset();
    let storage_after = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after = physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert!(plane.fired());
    assert_eq!(plane.restarts(), 1);
    assert_eq!(receipt.operation, workspace.operation);
    assert_eq!(receipt.changed_keys, 2);
    assert!(receipt.idempotent_replay);
    assert_eq!(first_wire.executions, first_sdk.total_calls());
    assert!(first_wire.transmissions >= first_wire.executions);

    plane.reset_s3_metrics();
    wire.reset();
    let replay = repository.publish_workspace(workspace.id).await.unwrap();
    let replay_sdk = plane.reset_s3_metrics();
    let replay_wire = wire.reset();
    let storage_after_replay =
        physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after_replay =
        physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert_eq!(replay.id, receipt.id);
    assert_eq!(replay.operation, workspace.operation);
    assert!(replay.idempotent_replay);
    assert_eq!(replay_wire.executions, replay_sdk.total_calls());
    assert!(replay_wire.transmissions >= replay_wire.executions);
    let replay_versions = versions_after_replay
        .iter()
        .filter(|entry| !versions_after.contains(entry))
        .collect::<Vec<_>>();
    assert!(
        replay_versions.iter().all(|entry| {
            entry.0.contains("/publications/") || entry.0.contains("/workspaces/")
        }),
        "workspace replay may touch only terminal workspace/publication coordination: {replay_versions:?}"
    );
    let replay_storage_delta = storage_after_replay.saturating_sub(storage_after);
    assert!(replay_storage_delta <= 4 * 1_024);
    assert_eq!(
        repository
            .get_current("main", b"chaos/workspace-a.txt")
            .await
            .unwrap()
            .bytes,
        b"workspace A"
    );
    assert_eq!(
        repository
            .get_current("main", b"chaos/workspace-b.txt")
            .await
            .unwrap()
            .bytes,
        b"workspace B"
    );
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 2);
    repository.fsck().await.unwrap();
    eprintln!(
        "ACTIVE_OUTAGE_CHAOS scenario=workspace prefix={prefix} accepted_lost_responses=1 reconciled_operations=1 provider_restarts={} restart_ms={} operation_elapsed_ms={:.3} storage_before_bytes={storage_before} storage_after_bytes={storage_after} stored_delta_bytes={} first_sdk_calls={} first_wire_transmissions={} first_wire_retries={} replay_sdk_calls={} replay_wire_transmissions={} replay_wire_retries={} replay_coordination_versions={} replay_coordination_bytes={replay_storage_delta} logical_versions=2 final_fsck=ok",
        plane.restarts(),
        plane.restart_millis(),
        elapsed.as_secs_f64() * 1_000.0,
        i128::from(storage_after) - i128::from(storage_before),
        first_sdk.total_calls(),
        first_wire.transmissions,
        first_wire.retry_transmissions(),
        replay_sdk.total_calls(),
        replay_wire.transmissions,
        replay_wire.retry_transmissions(),
        replay_versions.len(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_active_outage_reconciles_multipart_completion() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_CHAOS").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_CHAOS=1 through the chaos script");
        return;
    }
    let container =
        std::env::var("PROLLY_RUSTFS_CONTAINER").unwrap_or_else(|_| "prolly-rustfs".to_string());
    let prefix_root =
        std::env::var("PROLLY_S3_CHAOS_PREFIX").unwrap_or_else(|_| unique_prefix("active-outage"));
    let prefix = format!("{prefix_root}/multipart");
    let (aws, bucket, wire) = rustfs_client_with_wire_metrics().await;
    let (accounting_aws, accounting_bucket) = rustfs_client().await;
    assert_eq!(accounting_bucket, bucket);
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let plane = Arc::new(RestartAfterAcceptedRefPlane::new(
        AwsS3ObjectPlane::new(aws, &bucket),
        Arc::<str>::from(container),
    ));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: prefix.clone(),
            writer: "active-outage-multipart".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let upload = repository
        .create_multipart_upload(
            "main",
            b"chaos/multipart.bin".to_vec(),
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
                Ok::<_, std::convert::Infallible>(
                    b"multipart completion survives accepted-CAS restart".to_vec(),
                )
            }),
        )
        .await
        .unwrap();
    let operation = OperationId::new();
    let requested_parts = vec![(1, part.etag.clone())];
    let storage_before = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    plane.reset_s3_metrics();
    wire.reset();
    plane.arm();
    let started = Instant::now();
    let receipt = repository
        .complete_multipart_upload(upload, requested_parts.clone(), Some(operation))
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let first_sdk = plane.reset_s3_metrics();
    let first_wire = wire.reset();
    let storage_after = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after = physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert!(plane.fired());
    assert_eq!(plane.restarts(), 1);
    assert_eq!(receipt.operation, operation);
    assert!(receipt.idempotent_replay);
    assert_eq!(first_wire.executions, first_sdk.total_calls());
    assert!(first_wire.transmissions >= first_wire.executions);

    plane.reset_s3_metrics();
    wire.reset();
    let replay = repository
        .complete_multipart_upload(upload, requested_parts, Some(operation))
        .await
        .unwrap();
    let replay_sdk = plane.reset_s3_metrics();
    let replay_wire = wire.reset();
    let storage_after_replay =
        physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after_replay =
        physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert_eq!(replay.id, receipt.id);
    assert_eq!(replay.operation, operation);
    assert!(replay.idempotent_replay);
    assert_eq!(replay_wire.executions, replay_sdk.total_calls());
    assert!(replay_wire.transmissions >= replay_wire.executions);
    let replay_versions = versions_after_replay
        .iter()
        .filter(|entry| !versions_after.contains(entry))
        .collect::<Vec<_>>();
    assert!(
        replay_versions.iter().all(|entry| {
            entry.0.contains("/publications/") || entry.0.contains("/multipart/uploads/")
        }),
        "multipart replay may touch only terminal upload/publication coordination: {replay_versions:?}"
    );
    let replay_storage_delta = storage_after_replay.saturating_sub(storage_after);
    assert!(replay_storage_delta <= 4 * 1_024);
    assert_eq!(
        repository
            .get_current("main", b"chaos/multipart.bin")
            .await
            .unwrap()
            .bytes,
        b"multipart completion survives accepted-CAS restart"
    );
    assert_eq!(repository.log("main", 10).await.unwrap().len(), 2);
    repository.fsck().await.unwrap();
    eprintln!(
        "ACTIVE_OUTAGE_CHAOS scenario=multipart prefix={prefix} accepted_lost_responses=1 reconciled_operations=1 provider_restarts={} restart_ms={} operation_elapsed_ms={:.3} storage_before_bytes={storage_before} storage_after_bytes={storage_after} stored_delta_bytes={} first_sdk_calls={} first_wire_transmissions={} first_wire_retries={} replay_sdk_calls={} replay_wire_transmissions={} replay_wire_retries={} replay_coordination_versions={} replay_coordination_bytes={replay_storage_delta} logical_versions=1 final_fsck=ok",
        plane.restarts(),
        plane.restart_millis(),
        elapsed.as_secs_f64() * 1_000.0,
        i128::from(storage_after) - i128::from(storage_before),
        first_sdk.total_calls(),
        first_wire.transmissions,
        first_wire.retry_transmissions(),
        replay_sdk.total_calls(),
        replay_wire.transmissions,
        replay_wire.retry_transmissions(),
        replay_versions.len(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_active_outage_reconciles_merge_publication() {
    if !rustfs_enabled() || std::env::var("PROLLY_S3_CHAOS").as_deref() != Ok("1") {
        eprintln!("set PROLLY_S3_RUSTFS=1 and PROLLY_S3_CHAOS=1 through the chaos script");
        return;
    }
    let container =
        std::env::var("PROLLY_RUSTFS_CONTAINER").unwrap_or_else(|_| "prolly-rustfs".to_string());
    let prefix_root =
        std::env::var("PROLLY_S3_CHAOS_PREFIX").unwrap_or_else(|_| unique_prefix("active-outage"));
    let prefix = format!("{prefix_root}/merge");
    let (aws, bucket, wire) = rustfs_client_with_wire_metrics().await;
    let (accounting_aws, accounting_bucket) = rustfs_client().await;
    assert_eq!(accounting_bucket, bucket);
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let plane = Arc::new(RestartAfterAcceptedRefPlane::new(
        AwsS3ObjectPlane::new(aws, &bucket),
        Arc::<str>::from(container),
    ));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: prefix.clone(),
            writer: "active-outage-merge".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    let root = repository.head("main").await.unwrap();
    repository.create_branch("feature", root).await.unwrap();
    let main_commit = repository
        .put_bytes(
            "main",
            b"chaos/main-only.txt".to_vec(),
            b"main survives merge restart".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    repository
        .put_bytes(
            "feature",
            b"chaos/feature-only.txt".to_vec(),
            b"feature survives merge restart".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let feature_head = repository.head("feature").await.unwrap();
    let operation = OperationId::new();
    let storage_before = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    plane.reset_s3_metrics();
    wire.reset();
    plane.arm();
    let started = Instant::now();
    let receipt = repository
        .merge(
            "main",
            feature_head,
            None,
            MergePolicy::Fail,
            Some(operation),
            Some("active outage merge".to_string()),
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let first_sdk = plane.reset_s3_metrics();
    let first_wire = wire.reset();
    let storage_after = physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after = physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;

    assert!(plane.fired());
    assert_eq!(plane.restarts(), 1);
    assert_eq!(receipt.operation, operation);
    assert_eq!(receipt.parents, vec![main_commit.id, feature_head]);
    assert_eq!(receipt.changed_keys, 1);
    assert!(receipt.idempotent_replay);
    assert_eq!(first_wire.executions, first_sdk.total_calls());
    assert!(first_wire.transmissions >= first_wire.executions);

    plane.reset_s3_metrics();
    wire.reset();
    let replay = repository
        .merge(
            "main",
            feature_head,
            None,
            MergePolicy::Fail,
            Some(operation),
            Some("active outage merge".to_string()),
        )
        .await
        .unwrap();
    let replay_sdk = plane.reset_s3_metrics();
    let replay_wire = wire.reset();
    let storage_after_replay =
        physical_storage_bytes(accounting_aws.clone(), &bucket, &prefix).await;
    let versions_after_replay =
        physical_version_snapshot(accounting_aws.clone(), &bucket, &prefix).await;
    assert_eq!(replay.id, receipt.id);
    assert_eq!(replay.parents, receipt.parents);
    assert_eq!(replay.operation, operation);
    assert!(replay.idempotent_replay);
    assert_eq!(replay_wire.executions, replay_sdk.total_calls());
    assert!(replay_wire.transmissions >= replay_wire.executions);
    let replay_versions = versions_after_replay
        .iter()
        .filter(|entry| !versions_after.contains(entry))
        .collect::<Vec<_>>();
    assert!(
        replay_versions
            .iter()
            .all(|entry| entry.0.contains("/publications/")),
        "merge replay may terminalize publication coordination but must not add commits, refs, reflogs, trees, deltas, or content: {replay_versions:?}"
    );
    let replay_storage_delta = storage_after_replay.saturating_sub(storage_after);
    assert!(replay_storage_delta <= 4 * 1_024);
    assert_eq!(
        repository
            .get_current("main", b"chaos/main-only.txt")
            .await
            .unwrap()
            .bytes,
        b"main survives merge restart"
    );
    assert_eq!(
        repository
            .get_current("main", b"chaos/feature-only.txt")
            .await
            .unwrap()
            .bytes,
        b"feature survives merge restart"
    );
    let log = repository.log("main", 10).await.unwrap();
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].0, receipt.id);
    assert_eq!(log[0].1.parents, vec![main_commit.id, feature_head]);
    repository.fsck().await.unwrap();
    eprintln!(
        "ACTIVE_OUTAGE_CHAOS scenario=merge prefix={prefix} accepted_lost_responses=1 reconciled_operations=1 provider_restarts={} restart_ms={} operation_elapsed_ms={:.3} storage_before_bytes={storage_before} storage_after_bytes={storage_after} stored_delta_bytes={} first_sdk_calls={} first_wire_transmissions={} first_wire_retries={} replay_sdk_calls={} replay_wire_transmissions={} replay_wire_retries={} replay_coordination_versions={} replay_coordination_bytes={replay_storage_delta} logical_versions=1 merge_parents=2 final_fsck=ok",
        plane.restarts(),
        plane.restart_millis(),
        elapsed.as_secs_f64() * 1_000.0,
        i128::from(storage_after) - i128::from(storage_before),
        first_sdk.total_calls(),
        first_wire.transmissions,
        first_wire.retry_transmissions(),
        replay_sdk.total_calls(),
        replay_wire.transmissions,
        replay_wire.retry_transmissions(),
        replay_versions.len(),
    );
}

#[cfg(feature = "slatedb-index")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_same_bucket_slatedb_is_advisory_only() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, bucket) = rustfs_client().await;

    let object_store = rustfs_slatedb_object_store(&bucket);
    let authority_prefix = unique_prefix("slatedb-authority");
    let base_client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&authority_prefix)
        .writer("slatedb-bootstrap")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    let repository = base_client.repository_id();
    let index = Arc::new(
        SlateDbAdvisoryIndex::open_owned(object_store.clone(), repository, "writer-a")
            .await
            .unwrap(),
    );
    assert_eq!(
        index.path(),
        format!(
            ".prolly-cache/{repository}/{}",
            hex::encode("writer-a".as_bytes())
        )
    );
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&authority_prefix)
        .writer("slatedb-integration")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .advisory_index(index.clone())
        .open()
        .await
        .unwrap();
    drop(base_client);
    let receipt = client
        .put_object()
        .bucket(&bucket)
        .key("cached/head.txt")
        .body(ByteStream::from_static(b"canonical data remains in S3"))
        .send()
        .await
        .unwrap()
        .commit
        .unwrap();
    index.flush().await.unwrap();
    assert_eq!(
        index
            .branch_head(client.repository_id(), "main")
            .await
            .unwrap(),
        Some(receipt.id)
    );

    let advisory_key = format!("prolly-s3/{}/branch/main", client.repository_id()).into_bytes();
    index
        .database()
        .put(advisory_key, b"not a canonical advisory receipt".to_vec())
        .await
        .unwrap();
    index.flush().await.unwrap();
    assert_eq!(
        index
            .branch_head(client.repository_id(), "main")
            .await
            .unwrap_err()
            .code,
        ErrorCode::CorruptCommit
    );
    assert_eq!(index.quarantine_count(repository).await.unwrap(), 1);
    assert_eq!(index.branch_head(repository, "main").await.unwrap(), None);
    let rebuilt_report = client.rebuild_advisory_index().await.unwrap();
    assert_eq!(rebuilt_report.written_heads, 1);
    assert_eq!(
        index.branch_head(repository, "main").await.unwrap(),
        Some(receipt.id)
    );

    // Full rebuild also discovers and quarantines corruption that has not yet
    // been touched by an advisory read.
    let advisory_key = format!("prolly-s3/{repository}/branch/main").into_bytes();
    index
        .database()
        .put(advisory_key, b"second corrupt advisory head".to_vec())
        .await
        .unwrap();
    index.flush().await.unwrap();
    let rebuilt_report = client.rebuild_advisory_index().await.unwrap();
    assert_eq!(rebuilt_report.removed_entries, 1);
    assert_eq!(rebuilt_report.quarantined_entries, 1);
    assert_eq!(rebuilt_report.written_heads, 1);
    assert_eq!(index.quarantine_count(repository).await.unwrap(), 2);
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key("cached/head.txt")
            .send()
            .await
            .unwrap()
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes(),
        &b"canonical data remains in S3"[..]
    );
    let rebuilt = client
        .put_object()
        .bucket(&bucket)
        .key("cached/rebuild.txt")
        .body(ByteStream::from_static(
            b"canonical publication repairs advisory head",
        ))
        .send()
        .await
        .unwrap()
        .commit
        .unwrap();
    index.flush().await.unwrap();
    assert_eq!(
        index
            .branch_head(client.repository_id(), "main")
            .await
            .unwrap(),
        Some(rebuilt.id)
    );

    drop(client);
    index.close().await.unwrap();
    drop(index);
    let reopened = SlateDbAdvisoryIndex::open_owned(object_store.clone(), repository, "writer-a")
        .await
        .unwrap();
    assert_eq!(
        reopened.branch_head(repository, "main").await.unwrap(),
        Some(rebuilt.id)
    );
    reopened.close().await.unwrap();
    assert_eq!(
        SlateDbAdvisoryIndex::open_owned(object_store, repository, "invalid/writer")
            .await
            .err()
            .unwrap()
            .code,
        ErrorCode::InvalidRequest
    );
}

#[cfg(feature = "slatedb-index")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_complete_slatedb_cache_loss_rebuilds_from_canonical_s3() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, bucket) = rustfs_client().await;
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let object_store = rustfs_slatedb_object_store(&bucket);
    let authority_prefix = unique_prefix("slatedb-cache-loss-authority");
    let base_client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&authority_prefix)
        .writer("slatedb-cache-loss-bootstrap")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    let repository = base_client.repository_id();
    let index = Arc::new(
        SlateDbAdvisoryIndex::open_owned(object_store.clone(), repository, "cache-loss-writer")
            .await
            .unwrap(),
    );
    let cache_prefix = index.path().to_string();
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&authority_prefix)
        .writer("slatedb-cache-loss")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .advisory_index(index.clone())
        .open()
        .await
        .unwrap();
    drop(base_client);

    let main_head = client
        .put_object()
        .bucket(&bucket)
        .key("cache-loss/main.txt")
        .body(ByteStream::from_static(
            b"canonical main survives cache loss",
        ))
        .send()
        .await
        .unwrap()
        .snapshot;
    client
        .create_branch("cache-loss-feature", Some(main_head))
        .await
        .unwrap();
    let feature_client = client.on_branch("cache-loss-feature").unwrap();
    let feature_head = feature_client
        .put_object()
        .bucket(&bucket)
        .key("cache-loss/feature.txt")
        .body(ByteStream::from_static(
            b"canonical feature survives cache loss",
        ))
        .send()
        .await
        .unwrap()
        .snapshot;
    index.flush().await.unwrap();
    assert_eq!(
        index.branch_head(repository, "main").await.unwrap(),
        Some(main_head)
    );
    assert_eq!(
        index
            .branch_head(repository, "cache-loss-feature")
            .await
            .unwrap(),
        Some(feature_head)
    );
    let authority_before = physical_version_snapshot(aws.clone(), &bucket, &authority_prefix).await;
    assert!(!authority_before.is_empty());

    drop(feature_client);
    drop(client);
    index.close().await.unwrap();
    drop(index);

    let cache_path = slatedb::object_store::path::Path::from(cache_prefix.as_str());
    let cached_objects = object_store
        .list(Some(&cache_path))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(
        !cached_objects.is_empty(),
        "the drill must remove a materialized SlateDB cache"
    );
    let cached_object_count = cached_objects.len();
    let locations = futures_util::stream::iter(
        cached_objects
            .into_iter()
            .map(|metadata| Ok::<_, slatedb::object_store::Error>(metadata.location)),
    )
    .boxed();
    let deleted = object_store
        .delete_stream(locations)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(deleted.len(), cached_object_count);
    assert!(
        object_store
            .list(Some(&cache_path))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty(),
        "all current SlateDB cache objects must be absent before rebuild"
    );
    let cache_versions_after_loss =
        physical_version_snapshot(aws.clone(), &bucket, &cache_prefix).await;
    assert!(
        cache_versions_after_loss.iter().any(|entry| entry.3),
        "native version history must show cache-loss delete markers"
    );
    assert_eq!(
        physical_version_snapshot(aws.clone(), &bucket, &authority_prefix).await,
        authority_before,
        "deleting the advisory namespace must not mutate canonical authority"
    );

    let rebuilt_index = Arc::new(
        SlateDbAdvisoryIndex::open_owned(object_store.clone(), repository, "cache-loss-writer")
            .await
            .unwrap(),
    );
    assert_eq!(rebuilt_index.path(), cache_prefix);
    assert_eq!(
        rebuilt_index.branch_head(repository, "main").await.unwrap(),
        None,
        "a newly recreated cache must not invent canonical heads"
    );
    let rebuilt_client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&authority_prefix)
        .writer("slatedb-cache-loss-rebuild")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .advisory_index(rebuilt_index.clone())
        .open()
        .await
        .unwrap();
    let report = rebuilt_client.rebuild_advisory_index().await.unwrap();
    assert_eq!(report.written_heads, 2);
    assert_eq!(report.removed_entries, 0);
    assert_eq!(report.quarantined_entries, 0);
    assert!(!report.resumed_from_checkpoint);
    assert_eq!(
        rebuilt_index.branch_head(repository, "main").await.unwrap(),
        Some(main_head)
    );
    assert_eq!(
        rebuilt_index
            .branch_head(repository, "cache-loss-feature")
            .await
            .unwrap(),
        Some(feature_head)
    );
    let main_body = rebuilt_client
        .get_object()
        .bucket(&bucket)
        .key("cache-loss/main.txt")
        .send()
        .await
        .unwrap()
        .output
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(main_body.as_ref(), b"canonical main survives cache loss");
    let feature_body = rebuilt_client
        .on_branch("cache-loss-feature")
        .unwrap()
        .get_object()
        .bucket(&bucket)
        .key("cache-loss/feature.txt")
        .send()
        .await
        .unwrap()
        .output
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(
        feature_body.as_ref(),
        b"canonical feature survives cache loss"
    );
    rebuilt_client.fsck().await.unwrap();
    assert_eq!(
        physical_version_snapshot(aws, &bucket, &authority_prefix).await,
        authority_before,
        "cache recreation and rebuild must leave canonical versions unchanged"
    );
    drop(rebuilt_client);
    rebuilt_index.close().await.unwrap();
}

fn unique_prefix(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("integration/{name}/{nanos}")
}

fn current_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}

#[cfg(feature = "slatedb-index")]
fn rustfs_slatedb_object_store(bucket: &str) -> Arc<dyn slatedb::object_store::ObjectStore> {
    let endpoint = std::env::var("PROLLY_RUSTFS_SLATE_ENDPOINT").unwrap_or_else(|_| {
        std::env::var("PROLLY_RUSTFS_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string())
    });
    let access_key =
        std::env::var("PROLLY_RUSTFS_ACCESS_KEY").unwrap_or_else(|_| "prollyadmin".to_string());
    let secret_key = std::env::var("PROLLY_RUSTFS_SECRET_KEY")
        .unwrap_or_else(|_| "prolly-local-secret-change-me".to_string());
    Arc::new(
        slatedb::object_store::aws::AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_access_key_id(access_key)
            .with_secret_access_key(secret_key)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .build()
            .unwrap(),
    )
}

fn rustfs_provider_identity() -> ProviderIdentity {
    ProviderIdentity::s3_compatible(
        std::env::var("PROLLY_RUSTFS_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string()),
        "us-east-1",
    )
}

fn test_attestation_signer() -> Arc<HmacAttestationSigner> {
    Arc::new(HmacAttestationSigner::single("integration-attestation-v1", vec![11_u8; 32]).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_native_versioned_whole_object_write_uses_four_calls() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket, wire) = rustfs_client_with_wire_metrics().await;
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let prefix = unique_prefix("native-versioned-four-calls");
    let object_root = unique_prefix("native-versioned-objects");
    let plane = Arc::new(AwsS3ObjectPlane::new(aws, &bucket));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: prefix,
            storage_profile: RepositoryStorageProfile::NativeVersionedV1,
            writer: "rustfs-native-writer".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();

    repository
        .put_bytes(
            "main",
            format!("{object_root}/warmup.bin").into_bytes(),
            vec![1; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    plane.reset_metrics();
    wire.reset();
    let measured_key = format!("{object_root}/measured.bin");
    let first = repository
        .put_bytes(
            "main",
            measured_key.as_bytes().to_vec(),
            vec![2; 64 * 1024],
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let sdk = plane.reset_metrics();
    let wire = wire.reset();
    assert_eq!(sdk.total_calls(), 4, "unexpected SDK calls: {sdk:?}");
    assert_eq!(sdk.put_object, 4, "every publication step is one put");
    assert_eq!(wire.executions, 4, "unexpected SDK executions: {wire:?}");
    assert_eq!(
        wire.transmissions, 4,
        "RustFS should not retry the measured write: {wire:?}"
    );

    let first_version = first.object_versions[0];
    repository
        .put_bytes(
            "main",
            measured_key.as_bytes().to_vec(),
            b"new current value".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_version("main", measured_key.as_bytes(), first_version)
            .await
            .unwrap()
            .bytes,
        vec![2; 64 * 1024]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_native_versioned_multipart_uses_n_plus_five_calls() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }

    let (aws, bucket, wire) = rustfs_client_with_wire_metrics().await;
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let prefix = unique_prefix("native-versioned-multipart");
    let key = format!(
        "{}/multipart.bin",
        unique_prefix("native-versioned-objects")
    );
    let plane = Arc::new(AwsS3ObjectPlane::new(aws, &bucket));
    let repository = Repository::initialize(
        plane.clone(),
        RepositoryOptions {
            repository_prefix: prefix,
            storage_profile: RepositoryStorageProfile::NativeVersionedV1,
            writer: "rustfs-native-multipart-writer".to_string(),
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    repository
        .put_bytes(
            "main",
            format!("{key}.warmup").into_bytes(),
            b"warm".to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

    let first_bytes = vec![3; 5 * 1024 * 1024];
    let second_bytes = vec![5; 1024];
    let mut whole = first_bytes.clone();
    whole.extend_from_slice(&second_bytes);
    let checksum_sha256: [u8; 32] = Sha256::digest(&whole).into();
    let checksum_md5: [u8; 16] = Md5::digest(&whole).into();

    plane.reset_metrics();
    wire.reset();
    let session = repository
        .create_native_multipart_upload(
            "main",
            key.as_bytes().to_vec(),
            ObjectHeaders::default(),
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    let first = repository
        .upload_native_multipart_part(&session, 1, first_bytes)
        .await
        .unwrap();
    let second = repository
        .upload_native_multipart_part(&session, 2, second_bytes)
        .await
        .unwrap();
    let parts = [&first, &second]
        .into_iter()
        .map(|part| NativeMultipartCompletedPart {
            part_number: part.part_number,
            etag: part.etag.clone(),
            checksum_sha256: part.checksum_sha256.unwrap(),
            size: part.size,
        })
        .collect();
    repository
        .complete_native_multipart_upload(
            session.clone(),
            parts,
            checksum_sha256,
            checksum_md5,
            whole.len() as u64,
            Some(session.operation),
        )
        .await
        .unwrap();
    let sdk = plane.reset_metrics();
    let wire = wire.reset();
    assert_eq!(sdk.create_multipart_upload, 1);
    assert_eq!(sdk.upload_part, 2);
    assert_eq!(sdk.complete_multipart_upload, 1);
    assert_eq!(sdk.put_object, 3);
    assert_eq!(sdk.total_calls(), 7, "unexpected SDK calls: {sdk:?}");
    assert_eq!(wire.executions, 7, "unexpected SDK executions: {wire:?}");
    assert_eq!(wire.transmissions, 7, "unexpected wire calls: {wire:?}");
    assert_eq!(
        repository
            .get_current("main", key.as_bytes())
            .await
            .unwrap()
            .bytes,
        whole
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_native_multipart_client_lists_completes_and_aborts() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, bucket) = rustfs_client().await;
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let repository_prefix = unique_prefix("native-multipart-client");
    let writer = "native-multipart-client-writer";
    let client = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer(writer)
        .native_versioned()
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .initialize()
        .await
        .unwrap();
    let key_root = unique_prefix("native-multipart-client-objects");
    let key = format!("{key_root}/complete.bin");
    let create = client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap();
    let first_bytes = vec![11; 5 * 1024 * 1024];
    let second_bytes = vec![13; 1024];
    let first = client
        .upload_part()
        .bucket(&bucket)
        .key(&key)
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(first_bytes.clone()))
        .send()
        .await
        .unwrap();
    let second = client
        .upload_part()
        .bucket(&bucket)
        .key(&key)
        .upload_id(upload_id)
        .part_number(2)
        .body(ByteStream::from(second_bytes.clone()))
        .send()
        .await
        .unwrap();
    let listed = client
        .list_parts()
        .bucket(&bucket)
        .key(&key)
        .upload_id(upload_id)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.parts().len(), 2);

    drop(client);
    let client = Client::builder()
        .aws_client(aws)
        .bucket(&bucket)
        .repository_prefix(&repository_prefix)
        .writer(writer)
        .native_versioned()
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .open()
        .await
        .unwrap();

    let mut whole = first_bytes;
    whole.extend_from_slice(&second_bytes);
    client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(&key)
        .upload_id(upload_id)
        .checksum_sha256(STANDARD.encode(Sha256::digest(&whole)))
        .checksum_md5(STANDARD.encode(Md5::digest(&whole)))
        .expected_size(whole.len() as u64)
        .part_size(1, 5 * 1024 * 1024)
        .part_size(2, 1024)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(first.e_tag().unwrap())
                        .checksum_sha256(first.checksum_sha256().unwrap())
                        .build(),
                )
                .parts(
                    CompletedPart::builder()
                        .part_number(2)
                        .e_tag(second.e_tag().unwrap())
                        .checksum_sha256(second.checksum_sha256().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        client
            .get_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .unwrap()
            .output
            .body
            .collect()
            .await
            .unwrap()
            .into_bytes(),
        whole.as_slice()
    );

    let abort_key = format!("{key_root}/abort.bin");
    client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(&abort_key)
        .send()
        .await
        .unwrap();
    let uploads = client
        .list_multipart_uploads()
        .bucket(&bucket)
        .prefix(&key_root)
        .send()
        .await
        .unwrap();
    let discovered = uploads
        .uploads()
        .iter()
        .find(|upload| upload.key() == Some(abort_key.as_str()))
        .unwrap_or_else(|| panic!("abort upload missing from listing: {uploads:?}"));
    client
        .abort_multipart_upload()
        .bucket(&bucket)
        .key(&abort_key)
        .upload_id(discovered.upload_id().unwrap())
        .send()
        .await
        .unwrap();
}

async fn rustfs_client() -> (aws_sdk_s3::Client, String) {
    rustfs_client_with_optional_wire_metrics(None).await
}

async fn rustfs_client_with_wire_metrics() -> (aws_sdk_s3::Client, String, S3WireAttemptInterceptor)
{
    let metrics = S3WireAttemptInterceptor::new();
    let (client, bucket) = rustfs_client_with_optional_wire_metrics(Some(metrics.clone())).await;
    (client, bucket, metrics)
}

async fn rustfs_client_with_optional_wire_metrics(
    wire_metrics: Option<S3WireAttemptInterceptor>,
) -> (aws_sdk_s3::Client, String) {
    let endpoint = std::env::var("PROLLY_RUSTFS_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let access_key =
        std::env::var("PROLLY_RUSTFS_ACCESS_KEY").unwrap_or_else(|_| "prollyadmin".to_string());
    let secret_key = std::env::var("PROLLY_RUSTFS_SECRET_KEY")
        .unwrap_or_else(|_| "prolly-local-secret-change-me".to_string());
    let bucket = std::env::var("PROLLY_RUSTFS_BUCKET")
        .unwrap_or_else(|_| "prolly-versioned-s3-tests".to_string());
    let mut config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "rustfs-integration",
        ))
        .endpoint_url(endpoint)
        .force_path_style(true);
    if let Some(wire_metrics) = wire_metrics {
        config = config.interceptor(wire_metrics);
    }
    let config = config.build();
    let client = aws_sdk_s3::Client::from_conf(config);
    match client.create_bucket().bucket(&bucket).send().await {
        Ok(_) => {}
        Err(error) => {
            let text = format!("{error:?}");
            assert!(
                text.contains("BucketAlreadyOwnedByYou") || text.contains("BucketAlreadyExists"),
                "failed to create RustFS test bucket: {text}"
            );
        }
    }
    (client, bucket)
}

async fn rustfs_client_with_credentials(
    access_key: String,
    secret_key: String,
) -> (aws_sdk_s3::Client, String) {
    let endpoint = std::env::var("PROLLY_RUSTFS_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
    let bucket = std::env::var("PROLLY_RUSTFS_BUCKET")
        .unwrap_or_else(|_| "prolly-versioned-s3-tests".to_string());
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "rustfs-iam-drill",
        ))
        .endpoint_url(endpoint)
        .force_path_style(true)
        .build();
    (aws_sdk_s3::Client::from_conf(config), bucket)
}

async fn physical_version_snapshot(
    aws: aws_sdk_s3::Client,
    bucket: &str,
    prefix: &str,
) -> Vec<(String, String, String, bool, u64)> {
    let plane = AwsS3ObjectPlane::new(aws, bucket);
    let mut continuation = None;
    let mut snapshot = Vec::new();
    loop {
        let page = plane
            .list(ListRequest {
                prefix: format!("{prefix}/"),
                continuation,
                limit: 1_000,
                include_versions: true,
            })
            .await
            .unwrap();
        snapshot.extend(page.entries.into_iter().map(|entry| {
            (
                entry.path.as_str().to_string(),
                entry.metadata.token.version_id.unwrap_or_default(),
                entry.metadata.token.etag,
                entry.metadata.delete_marker,
                entry.metadata.len,
            )
        }));
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    snapshot.sort();
    snapshot
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_conditional_object_plane_conformance() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (client, bucket) = rustfs_client().await;
    let plane = Arc::new(AwsS3ObjectPlane::new(client, bucket));
    let prefix = unique_prefix("object-plane");
    let immutable_path = ObjectPath::new(format!("{prefix}/immutable")).unwrap();
    let immutable = ImmutablePut {
        path: immutable_path,
        bytes: b"immutable".to_vec(),
        expected_sha256: Sha256::digest(b"immutable").into(),
    };
    assert!(matches!(
        plane.put_immutable(immutable.clone()).await.unwrap(),
        ImmutablePutOutcome::Created(_)
    ));
    assert!(matches!(
        plane.put_immutable(immutable).await.unwrap(),
        ImmutablePutOutcome::AlreadyPresent(_)
    ));

    let ref_path = ObjectPath::new(format!("{prefix}/ref")).unwrap();
    plane.reset_metrics();
    let created = match plane
        .compare_exchange(CompareExchange {
            path: ref_path.clone(),
            expected: None,
            bytes: b"zero".to_vec(),
        })
        .await
        .unwrap()
    {
        CompareExchangeOutcome::Applied(metadata) => metadata,
        other => panic!("unexpected create result: {other:?}"),
    };
    let create_metrics = plane.reset_metrics();
    assert_eq!(create_metrics.put_object, 1);
    assert_eq!(create_metrics.get_object, 0);

    let tasks = (0..32)
        .map(|writer| {
            let plane = plane.clone();
            let path = ref_path.clone();
            let expected = created.token.clone();
            tokio::spawn(async move {
                plane
                    .compare_exchange(CompareExchange {
                        path,
                        expected: Some(expected),
                        bytes: format!("writer-{writer}").into_bytes(),
                    })
                    .await
            })
        })
        .collect::<Vec<_>>();
    let mut applied = 0;
    let mut conflicts = 0;
    for task in tasks {
        match task.await.unwrap().unwrap() {
            CompareExchangeOutcome::Applied(_) => applied += 1,
            CompareExchangeOutcome::Conflict(_) => conflicts += 1,
        }
    }
    assert_eq!(applied, 1);
    assert_eq!(conflicts, 31);
    let update_metrics = plane.reset_metrics();
    assert_eq!(update_metrics.put_object, 32);
    assert_eq!(update_metrics.get_object, 31);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_exact_delete_preserves_other_native_versions() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (aws, _) = rustfs_client().await;
    let bucket = format!(
        "prolly-native-{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    aws.create_bucket().bucket(&bucket).send().await.unwrap();
    aws.put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let qualification = Client::builder()
        .aws_client(aws.clone())
        .bucket(&bucket)
        .repository_prefix("qualification/v1")
        .provider_identity(rustfs_provider_identity())
        .attestation_signer(test_attestation_signer())
        .qualify_provider()
        .await
        .unwrap();
    assert_eq!(
        qualification.body.capabilities.physical_versioning,
        PhysicalVersioning::Enabled
    );

    let plane = AwsS3ObjectPlane::new(aws.clone(), &bucket);
    let path = ObjectPath::new("native/ref").unwrap();
    let first = match plane
        .compare_exchange(CompareExchange {
            path: path.clone(),
            expected: None,
            bytes: b"first".to_vec(),
        })
        .await
        .unwrap()
    {
        CompareExchangeOutcome::Applied(metadata) => metadata,
        CompareExchangeOutcome::Conflict(_) => panic!("fresh versioned key conflicted"),
    };
    let second = match plane
        .compare_exchange(CompareExchange {
            path: path.clone(),
            expected: Some(first.token.clone()),
            bytes: b"second".to_vec(),
        })
        .await
        .unwrap()
    {
        CompareExchangeOutcome::Applied(metadata) => metadata,
        CompareExchangeOutcome::Conflict(_) => panic!("expected versioned update to win"),
    };
    let first_version = first.token.version_id.expect("native version ID");
    assert_ne!(
        Some(first_version.as_str()),
        second.token.version_id.as_deref()
    );
    assert_eq!(
        plane
            .delete_exact(
                &path,
                PhysicalVersion::Versioned {
                    version_id: first_version.clone(),
                },
            )
            .await
            .unwrap(),
        DeleteOutcome::Deleted
    );
    assert_eq!(
        plane.load_mutable(&path).await.unwrap().unwrap().bytes,
        b"second"
    );
    let remaining = plane
        .list(ListRequest {
            prefix: "native/".to_string(),
            continuation: None,
            limit: 100,
            include_versions: true,
        })
        .await
        .unwrap();
    assert!(remaining.entries.iter().all(|entry| {
        entry.metadata.token.version_id.as_deref() != Some(first_version.as_str())
    }));

    let repository_prefix = "gc-native-ref-retention";
    let future_clock = Arc::new(FixedClock::new(
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
            + 3 * 60 * 60 * 1_000,
    ));
    let repository_plane = Arc::new(AwsS3ObjectPlane::new(aws.clone(), &bucket));
    let repository = Repository::initialize(
        repository_plane.clone(),
        RepositoryOptions {
            repository_prefix: repository_prefix.to_string(),
            writer: "native-ref-retention-test".to_string(),
            clock: future_clock,
            gc_delete_rate_limit_per_second: 20,
            ..RepositoryOptions::default()
        },
    )
    .await
    .unwrap();
    for ordinal in 0..3 {
        repository
            .put_bytes(
                "main",
                format!("object-{ordinal}").into_bytes(),
                format!("value-{ordinal}").into_bytes(),
                ObjectHeaders::default(),
                BTreeMap::new(),
                None,
            )
            .await
            .unwrap();
    }
    let ref_prefix = format!("{repository_prefix}/refs/heads/");
    let refs_before = repository_plane
        .list(ListRequest {
            prefix: ref_prefix.clone(),
            continuation: None,
            limit: 100,
            include_versions: true,
        })
        .await
        .unwrap()
        .entries;
    assert_eq!(refs_before.len(), 4);
    for ordinal in 0..3 {
        let bytes = format!("rustfs-orphan-{ordinal}").into_bytes();
        repository_plane
            .put_immutable(ImmutablePut {
                path: ObjectPath::new(format!(
                    "{repository_prefix}/chunks/sha256/{ordinal:02}/{ordinal:02}/{}",
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
    assert_eq!(plan.plan.body.candidates.len(), 3);
    assert!(plan
        .plan
        .body
        .candidates
        .iter()
        .all(|candidate| !candidate.path.as_str().contains("/refs/")));
    let started = Instant::now();
    let swept = repository.sweep_gc_batch(plan.plan.id, 3).await.unwrap();
    assert_eq!(swept.deleted_versions, 3);
    assert!(started.elapsed() >= Duration::from_millis(90));
    let refs_after = repository_plane
        .list(ListRequest {
            prefix: ref_prefix,
            continuation: None,
            limit: 100,
            include_versions: true,
        })
        .await
        .unwrap()
        .entries;
    assert_eq!(
        refs_after
            .iter()
            .map(|entry| entry.metadata.token.version_id.as_deref())
            .collect::<Vec<_>>(),
        refs_before
            .iter()
            .map(|entry| entry.metadata.token.version_id.as_deref())
            .collect::<Vec<_>>()
    );

    for entry in remaining.entries {
        if let Some(version_id) = entry.metadata.token.version_id {
            plane
                .delete_exact(&entry.path, PhysicalVersion::Versioned { version_id })
                .await
                .unwrap();
        }
    }
    let mut all_versions = Vec::new();
    let mut continuation = None;
    loop {
        let page = plane
            .list(ListRequest {
                prefix: String::new(),
                continuation,
                limit: 1_000,
                include_versions: true,
            })
            .await
            .unwrap();
        all_versions.extend(page.entries);
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    for entry in all_versions {
        if let Some(version_id) = entry.metadata.token.version_id {
            plane
                .delete_exact(&entry.path, PhysicalVersion::Versioned { version_id })
                .await
                .unwrap();
        }
    }
    aws.delete_bucket().bucket(&bucket).send().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rustfs_bucket_repository_round_trip_and_history() {
    if !rustfs_enabled() {
        eprintln!("set PROLLY_S3_RUSTFS=1 to run RustFS integration tests");
        return;
    }
    let (client, bucket) = rustfs_client().await;
    let plane = Arc::new(AwsS3ObjectPlane::new(client, bucket));
    let options = RepositoryOptions {
        repository_prefix: unique_prefix("repository"),
        writer: "rustfs-integration".to_string(),
        limits: CanonicalLimits {
            content_chunk_bytes: 4,
            ..CanonicalLimits::default()
        },
        ..RepositoryOptions::default()
    };

    let repository = Repository::initialize(plane.clone(), options.clone())
        .await
        .unwrap();
    let put = repository
        .put_bytes(
            "main",
            b"docs/readme.txt".to_vec(),
            b"versioned over rustfs".to_vec(),
            ObjectHeaders {
                content_type: Some("text/plain".to_string()),
                ..ObjectHeaders::default()
            },
            BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        repository
            .get_current("main", b"docs/readme.txt")
            .await
            .unwrap()
            .bytes,
        b"versioned over rustfs"
    );

    let reopened = Repository::open(plane, options).await.unwrap();
    assert_eq!(reopened.head("main").await.unwrap(), put.id);
    reopened
        .delete_object("main", b"docs/readme.txt".to_vec(), None)
        .await
        .unwrap();
    assert_eq!(
        reopened
            .get_current("main", b"docs/readme.txt")
            .await
            .unwrap_err()
            .code,
        ErrorCode::NoSuchKey
    );
    assert_eq!(
        reopened
            .get_version("main", b"docs/readme.txt", put.object_versions[0])
            .await
            .unwrap()
            .bytes,
        b"versioned over rustfs"
    );
    assert_eq!(reopened.log("main", 10).await.unwrap().len(), 3);
}
