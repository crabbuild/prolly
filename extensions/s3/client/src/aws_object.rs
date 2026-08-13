use std::{
    collections::BTreeMap,
    io::SeekFrom,
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier},
    Client,
};
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use aws_types::request_id::RequestId;
use futures_util::{stream, StreamExt, TryStreamExt};
use prolly_s3_core::{
    CompareExchange, CompareExchangeOutcome, DeleteOutcome, Error, ErrorCode, GetRequest,
    ImmutableFilePut, ImmutablePut, ImmutablePutOutcome, ListRequest, ObjectPath, ObjectPlane,
    PhysicalListEntry, PhysicalListPage, PhysicalVersion, Result, RetryAdvice, StorageToken,
    StoredMetadata, StoredObject,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

const MULTIPART_THRESHOLD_BYTES: u64 = 64 * 1_024 * 1_024;
const MIN_MULTIPART_PART_BYTES: u64 = 16 * 1_024 * 1_024;
const MAX_MULTIPART_PART_BYTES: u64 = 5 * 1_024 * 1_024 * 1_024;
const MAX_MULTIPART_PARTS: u64 = 10_000;
const MULTIPART_UPLOAD_CONCURRENCY: usize = 8;

/// Object-plane calls issued to the AWS SDK and body bytes handed to or
/// collected from it. SDK-internal HTTP retries are intentionally not counted;
/// use a Smithy interceptor or provider telemetry for wire-attempt accounting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct S3OperationMetrics {
    pub get_object: u64,
    pub head_object: u64,
    pub put_object: u64,
    pub create_multipart_upload: u64,
    pub upload_part: u64,
    pub complete_multipart_upload: u64,
    pub abort_multipart_upload: u64,
    pub list_objects_v2: u64,
    pub list_object_versions: u64,
    pub delete_object: u64,
    pub delete_objects: u64,
    pub uploaded_body_bytes: u64,
    pub downloaded_body_bytes: u64,
}

impl S3OperationMetrics {
    pub fn total_calls(self) -> u64 {
        self.get_object
            + self.head_object
            + self.put_object
            + self.create_multipart_upload
            + self.upload_part
            + self.complete_multipart_upload
            + self.abort_multipart_upload
            + self.list_objects_v2
            + self.list_object_versions
            + self.delete_object
            + self.delete_objects
    }
}

#[derive(Default)]
struct AtomicS3OperationMetrics {
    get_object: AtomicU64,
    head_object: AtomicU64,
    put_object: AtomicU64,
    create_multipart_upload: AtomicU64,
    upload_part: AtomicU64,
    complete_multipart_upload: AtomicU64,
    abort_multipart_upload: AtomicU64,
    list_objects_v2: AtomicU64,
    list_object_versions: AtomicU64,
    delete_object: AtomicU64,
    delete_objects: AtomicU64,
    uploaded_body_bytes: AtomicU64,
    downloaded_body_bytes: AtomicU64,
}

struct MultipartAbortGuard {
    client: Client,
    bucket: String,
    key: String,
    upload_id: Option<String>,
    metrics: Arc<AtomicS3OperationMetrics>,
}

impl MultipartAbortGuard {
    fn disarm(&mut self) {
        self.upload_id = None;
    }
}

impl Drop for MultipartAbortGuard {
    fn drop(&mut self) {
        let Some(upload_id) = self.upload_id.take() else {
            return;
        };
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let metrics = self.metrics.clone();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                metrics
                    .abort_multipart_upload
                    .fetch_add(1, Ordering::Relaxed);
                let _ = client
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(upload_id)
                    .send()
                    .await;
            });
        }
    }
}

impl AtomicS3OperationMetrics {
    fn snapshot(&self) -> S3OperationMetrics {
        S3OperationMetrics {
            get_object: self.get_object.load(Ordering::Relaxed),
            head_object: self.head_object.load(Ordering::Relaxed),
            put_object: self.put_object.load(Ordering::Relaxed),
            create_multipart_upload: self.create_multipart_upload.load(Ordering::Relaxed),
            upload_part: self.upload_part.load(Ordering::Relaxed),
            complete_multipart_upload: self.complete_multipart_upload.load(Ordering::Relaxed),
            abort_multipart_upload: self.abort_multipart_upload.load(Ordering::Relaxed),
            list_objects_v2: self.list_objects_v2.load(Ordering::Relaxed),
            list_object_versions: self.list_object_versions.load(Ordering::Relaxed),
            delete_object: self.delete_object.load(Ordering::Relaxed),
            delete_objects: self.delete_objects.load(Ordering::Relaxed),
            uploaded_body_bytes: self.uploaded_body_bytes.load(Ordering::Relaxed),
            downloaded_body_bytes: self.downloaded_body_bytes.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) -> S3OperationMetrics {
        S3OperationMetrics {
            get_object: self.get_object.swap(0, Ordering::Relaxed),
            head_object: self.head_object.swap(0, Ordering::Relaxed),
            put_object: self.put_object.swap(0, Ordering::Relaxed),
            create_multipart_upload: self.create_multipart_upload.swap(0, Ordering::Relaxed),
            upload_part: self.upload_part.swap(0, Ordering::Relaxed),
            complete_multipart_upload: self.complete_multipart_upload.swap(0, Ordering::Relaxed),
            abort_multipart_upload: self.abort_multipart_upload.swap(0, Ordering::Relaxed),
            list_objects_v2: self.list_objects_v2.swap(0, Ordering::Relaxed),
            list_object_versions: self.list_object_versions.swap(0, Ordering::Relaxed),
            delete_object: self.delete_object.swap(0, Ordering::Relaxed),
            delete_objects: self.delete_objects.swap(0, Ordering::Relaxed),
            uploaded_body_bytes: self.uploaded_body_bytes.swap(0, Ordering::Relaxed),
            downloaded_body_bytes: self.downloaded_body_bytes.swap(0, Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct AwsS3ObjectPlane {
    client: Client,
    bucket: String,
    metrics: Arc<AtomicS3OperationMetrics>,
}

impl AwsS3ObjectPlane {
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            metrics: Arc::default(),
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn metrics(&self) -> S3OperationMetrics {
        self.metrics.snapshot()
    }

    /// Resets all object-plane counters and returns their previous values.
    pub fn reset_metrics(&self) -> S3OperationMetrics {
        self.metrics.reset()
    }

    async fn get_current(&self, path: &ObjectPath) -> Result<Option<StoredObject>> {
        self.get(GetRequest {
            path: path.clone(),
            range: None,
            physical_version: None,
        })
        .await
    }

    async fn put_multipart_file(&self, request: &ImmutableFilePut) -> Result<ImmutablePutOutcome> {
        if let Some(existing) = self.head(&request.path).await? {
            if existing.len == request.size && existing.sha256 == request.expected_sha256 {
                return Ok(ImmutablePutOutcome::AlreadyPresent(existing));
            }
            return Err(Error::new(
                ErrorCode::CorruptContent,
                format!("different bytes exist at {}", request.path),
            ));
        }

        self.metrics
            .create_multipart_upload
            .fetch_add(1, Ordering::Relaxed);
        let created = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(request.path.as_str())
            .metadata("prolly-sha256", hex::encode(request.expected_sha256))
            .send()
            .await
            .map_err(|error| map_sdk_error("CreateMultipartUpload immutable spool", error))?;
        let upload_id = created.upload_id().ok_or_else(|| {
            Error::new(
                ErrorCode::OutcomeUnknown,
                "multipart create succeeded without an upload ID",
            )
        })?;
        let mut abort = MultipartAbortGuard {
            client: self.client.clone(),
            bucket: self.bucket.clone(),
            key: request.path.as_str().to_string(),
            upload_id: Some(upload_id.to_string()),
            metrics: self.metrics.clone(),
        };

        let part_size = multipart_part_size(request.size)?;
        let part_count = request.size.div_ceil(part_size);
        let parts = stream::iter(0..part_count)
            .map(|index| {
                let client = self.client.clone();
                let bucket = self.bucket.clone();
                let key = request.path.as_str().to_string();
                let upload_id = upload_id.to_string();
                let body_path = request.body_path.clone();
                let metrics = self.metrics.clone();
                async move {
                    let offset = index.checked_mul(part_size).ok_or_else(|| {
                        Error::new(ErrorCode::EntityTooLarge, "multipart offset overflow")
                    })?;
                    let len = part_size.min(request.size - offset);
                    let mut file = tokio::fs::File::open(body_path)
                        .await
                        .map_err(|error| transport_error("multipart spool open", error))?;
                    file.seek(SeekFrom::Start(offset))
                        .await
                        .map_err(|error| transport_error("multipart spool seek", error))?;
                    let mut bytes = vec![
                        0;
                        usize::try_from(len).map_err(|_| {
                            Error::new(ErrorCode::EntityTooLarge, "multipart part exceeds usize")
                        })?
                    ];
                    file.read_exact(&mut bytes)
                        .await
                        .map_err(|error| transport_error("multipart spool read", error))?;
                    let part_number = i32::try_from(index + 1).map_err(|_| {
                        Error::new(
                            ErrorCode::EntityTooLarge,
                            "multipart part count exceeds i32",
                        )
                    })?;
                    metrics.upload_part.fetch_add(1, Ordering::Relaxed);
                    metrics
                        .uploaded_body_bytes
                        .fetch_add(len, Ordering::Relaxed);
                    let uploaded = client
                        .upload_part()
                        .bucket(bucket)
                        .key(key)
                        .upload_id(upload_id)
                        .part_number(part_number)
                        .body(ByteStream::from(bytes))
                        .send()
                        .await
                        .map_err(|error| map_sdk_error("UploadPart immutable spool", error))?;
                    Ok::<CompletedPart, Error>(
                        CompletedPart::builder()
                            .part_number(part_number)
                            .set_e_tag(uploaded.e_tag().map(ToString::to_string))
                            .build(),
                    )
                }
            })
            .buffer_unordered(MULTIPART_UPLOAD_CONCURRENCY)
            .try_collect::<Vec<_>>()
            .await?;
        let mut parts = parts;
        parts.sort_by_key(|part| part.part_number());
        let completed = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        self.metrics
            .complete_multipart_upload
            .fetch_add(1, Ordering::Relaxed);
        let output = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(request.path.as_str())
            .upload_id(upload_id)
            .multipart_upload(completed)
            .send()
            .await
            .map_err(|error| map_sdk_error("CompleteMultipartUpload immutable spool", error))?;
        abort.disarm();
        Ok(ImmutablePutOutcome::Created(StoredMetadata {
            token: StorageToken {
                etag: output.e_tag().unwrap_or_default().to_string(),
                version_id: output.version_id().map(ToString::to_string),
            },
            len: request.size,
            sha256: request.expected_sha256,
            last_modified_millis: 0,
            delete_marker: false,
            user_metadata: BTreeMap::from([(
                "prolly-sha256".to_string(),
                hex::encode(request.expected_sha256),
            )]),
        }))
    }
}

#[async_trait::async_trait]
impl ObjectPlane for AwsS3ObjectPlane {
    async fn get(&self, request: GetRequest) -> Result<Option<StoredObject>> {
        self.metrics.get_object.fetch_add(1, Ordering::Relaxed);
        let mut operation = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(request.path.as_str());
        if let Some(range) = request.range.as_ref() {
            operation = operation.range(format_range(range));
        }
        if let Some(PhysicalVersion::Versioned { version_id }) = request.physical_version {
            operation = operation.version_id(version_id);
        }
        let output = match operation.send().await {
            Ok(output) => output,
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => return Err(map_sdk_error("GetObject", error)),
        };
        let etag = output.e_tag().unwrap_or_default().to_string();
        let version_id = output.version_id().map(ToString::to_string);
        let last_modified_millis = output
            .last_modified()
            .and_then(|value| u64::try_from(value.secs()).ok())
            .and_then(|seconds| seconds.checked_mul(1_000))
            .unwrap_or_default();
        let content_length = output.content_length();
        let delete_marker = output.delete_marker().unwrap_or(false);
        let user_metadata: BTreeMap<String, String> = output
            .metadata()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let collected = output
            .body
            .collect()
            .await
            .map_err(|error| transport_error("GetObject body", error))?;
        let bytes = collected.into_bytes().to_vec();
        self.metrics
            .downloaded_body_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        // A ranged GET contains only the requested bytes, so `digest` is not
        // the checksum of the immutable object. Immutable writes persist the
        // whole-object digest as user metadata; retain that logical metadata
        // on both full and ranged responses. Full reads still hash and verify
        // the returned body in `PayloadStore::get`.
        let sha256 = user_metadata
            .get("prolly-sha256")
            .and_then(|encoded| hex::decode(encoded).ok())
            .and_then(|bytes| bytes.try_into().ok())
            .unwrap_or(digest);
        Ok(Some(StoredObject {
            metadata: StoredMetadata {
                token: StorageToken { etag, version_id },
                len: content_length
                    .and_then(|value| u64::try_from(value).ok())
                    .unwrap_or(bytes.len() as u64),
                sha256,
                last_modified_millis,
                delete_marker,
                user_metadata,
            },
            bytes,
        }))
    }

    async fn head(&self, path: &ObjectPath) -> Result<Option<StoredMetadata>> {
        self.metrics.head_object.fetch_add(1, Ordering::Relaxed);
        let output = match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path.as_str())
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => return Err(map_sdk_error("HeadObject", error)),
        };
        let sha256 = output
            .metadata()
            .and_then(|metadata| metadata.get("prolly-sha256"))
            .and_then(|encoded| hex::decode(encoded).ok())
            .and_then(|bytes| bytes.try_into().ok())
            .unwrap_or([0u8; 32]);
        Ok(Some(StoredMetadata {
            token: StorageToken {
                etag: output.e_tag().unwrap_or_default().to_string(),
                version_id: output.version_id().map(ToString::to_string),
            },
            len: output
                .content_length()
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or_default(),
            sha256,
            last_modified_millis: output
                .last_modified()
                .and_then(|value| u64::try_from(value.secs()).ok())
                .and_then(|seconds| seconds.checked_mul(1_000))
                .unwrap_or_default(),
            delete_marker: output.delete_marker().unwrap_or(false),
            user_metadata: output
                .metadata()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        }))
    }

    async fn put_immutable(&self, request: ImmutablePut) -> Result<ImmutablePutOutcome> {
        self.metrics.put_object.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .uploaded_body_bytes
            .fetch_add(request.bytes.len() as u64, Ordering::Relaxed);
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(request.path.as_str())
            .if_none_match("*")
            .metadata("prolly-sha256", hex::encode(request.expected_sha256))
            .body(ByteStream::from(request.bytes.clone()))
            .send()
            .await;
        match result {
            Ok(output) => Ok(ImmutablePutOutcome::Created(StoredMetadata {
                token: StorageToken {
                    etag: output.e_tag().unwrap_or_default().to_string(),
                    version_id: output.version_id().map(ToString::to_string),
                },
                len: request.bytes.len() as u64,
                sha256: request.expected_sha256,
                last_modified_millis: 0,
                delete_marker: false,
                user_metadata: BTreeMap::from([(
                    "prolly-sha256".to_string(),
                    hex::encode(request.expected_sha256),
                )]),
            })),
            Err(error) if is_precondition_failed(&error) => {
                let existing = self.get_current(&request.path).await?.ok_or_else(|| {
                    Error::new(
                        ErrorCode::OutcomeUnknown,
                        "immutable create conflicted but the winner is not readable",
                    )
                })?;
                if existing.bytes != request.bytes
                    || existing.metadata.sha256 != request.expected_sha256
                {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        format!("different bytes exist at {}", request.path),
                    ));
                }
                Ok(ImmutablePutOutcome::AlreadyPresent(existing.metadata))
            }
            Err(error) => Err(map_sdk_error("PutObject immutable", error)),
        }
    }

    async fn put_immutable_file(&self, request: ImmutableFilePut) -> Result<ImmutablePutOutcome> {
        let file_size = std::fs::metadata(&request.body_path)
            .map_err(|error| transport_error("immutable spool metadata", error))?
            .len();
        if file_size != request.size {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "immutable spool size changed before upload",
            ));
        }
        if request.size >= MULTIPART_THRESHOLD_BYTES {
            return self.put_multipart_file(&request).await;
        }
        self.metrics.put_object.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .uploaded_body_bytes
            .fetch_add(request.size, Ordering::Relaxed);
        let body = ByteStream::from_path(&request.body_path)
            .await
            .map_err(|error| transport_error("immutable spool open", error))?;
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(request.path.as_str())
            .if_none_match("*")
            .metadata("prolly-sha256", hex::encode(request.expected_sha256))
            .body(body)
            .send()
            .await;
        match result {
            Ok(output) => Ok(ImmutablePutOutcome::Created(StoredMetadata {
                token: StorageToken {
                    etag: output.e_tag().unwrap_or_default().to_string(),
                    version_id: output.version_id().map(ToString::to_string),
                },
                len: request.size,
                sha256: request.expected_sha256,
                last_modified_millis: 0,
                delete_marker: false,
                user_metadata: BTreeMap::from([(
                    "prolly-sha256".to_string(),
                    hex::encode(request.expected_sha256),
                )]),
            })),
            Err(error) => {
                let precondition_failed = is_precondition_failed(&error);
                let original = map_sdk_error("PutObject immutable spool", error);
                match self.head(&request.path).await {
                    Ok(Some(existing))
                        if existing.len == request.size
                            && existing.sha256 == request.expected_sha256 =>
                    {
                        Ok(ImmutablePutOutcome::AlreadyPresent(existing))
                    }
                    Ok(Some(_)) => Err(Error::new(
                        ErrorCode::CorruptContent,
                        format!("different bytes exist at {}", request.path),
                    )),
                    Ok(None) if precondition_failed => Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        "immutable create conflicted but the winner is not readable",
                    )),
                    Ok(None) | Err(_) => Err(original),
                }
            }
        }
    }

    async fn load_mutable(&self, path: &ObjectPath) -> Result<Option<StoredObject>> {
        self.get_current(path).await
    }

    async fn compare_exchange(&self, request: CompareExchange) -> Result<CompareExchangeOutcome> {
        self.metrics.put_object.fetch_add(1, Ordering::Relaxed);
        let len = request.bytes.len() as u64;
        let sha256: [u8; 32] = Sha256::digest(&request.bytes).into();
        self.metrics
            .uploaded_body_bytes
            .fetch_add(len, Ordering::Relaxed);
        let mut operation = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(request.path.as_str())
            .metadata("prolly-sha256", hex::encode(Sha256::digest(&request.bytes)))
            .body(ByteStream::from(request.bytes));
        operation = match request.expected.as_ref() {
            Some(expected) => operation.if_match(expected.etag.clone()),
            None => operation.if_none_match("*"),
        };
        match operation.send().await {
            Ok(output) => {
                let etag = output.e_tag().ok_or_else(|| {
                    Error::new(
                        ErrorCode::OutcomeUnknown,
                        "CAS was accepted but the provider omitted its ETag",
                    )
                })?;
                Ok(CompareExchangeOutcome::Applied(StoredMetadata {
                    token: StorageToken {
                        etag: etag.to_string(),
                        version_id: output.version_id().map(ToString::to_string),
                    },
                    len,
                    sha256,
                    last_modified_millis: 0,
                    delete_marker: false,
                    user_metadata: BTreeMap::new(),
                }))
            }
            Err(error) if is_precondition_failed(&error) => Ok(CompareExchangeOutcome::Conflict(
                self.get_current(&request.path).await?,
            )),
            Err(error) => Err(map_sdk_error("PutObject compare-exchange", error)),
        }
    }

    async fn list(&self, request: ListRequest) -> Result<PhysicalListPage> {
        if request.include_versions {
            return self.list_versions(request).await;
        }
        self.metrics.list_objects_v2.fetch_add(1, Ordering::Relaxed);
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(request.prefix)
            .set_continuation_token(request.continuation)
            .max_keys(i32::try_from(request.limit.min(1_000)).unwrap_or(1_000))
            .send()
            .await
            .map_err(|error| map_sdk_error("ListObjectsV2", error))?;
        let entries = output
            .contents()
            .iter()
            .filter_map(|object| {
                let path = ObjectPath::new(object.key()?).ok()?;
                Some(PhysicalListEntry {
                    path,
                    metadata: StoredMetadata {
                        token: StorageToken {
                            etag: object.e_tag().unwrap_or_default().to_string(),
                            version_id: None,
                        },
                        len: object
                            .size()
                            .and_then(|value| u64::try_from(value).ok())
                            .unwrap_or_default(),
                        sha256: [0; 32],
                        last_modified_millis: object
                            .last_modified()
                            .and_then(|value| u64::try_from(value.secs()).ok())
                            .and_then(|seconds| seconds.checked_mul(1_000))
                            .unwrap_or_default(),
                        delete_marker: false,
                        user_metadata: BTreeMap::new(),
                    },
                    is_latest: true,
                })
            })
            .collect();
        Ok(PhysicalListPage {
            entries,
            continuation: output.next_continuation_token().map(ToString::to_string),
        })
    }

    async fn delete_exact(
        &self,
        path: &ObjectPath,
        version: PhysicalVersion,
    ) -> Result<DeleteOutcome> {
        self.metrics.delete_object.fetch_add(1, Ordering::Relaxed);
        let mut operation = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(path.as_str());
        operation = match version {
            PhysicalVersion::Versioned { version_id } => operation.version_id(version_id),
            PhysicalVersion::Unversioned { token: Some(token) } => operation.if_match(token.etag),
            PhysicalVersion::Unversioned { token: None } => operation,
        };
        match operation.send().await {
            Ok(_) => Ok(DeleteOutcome::Deleted),
            Err(error) if is_not_found(&error) => Ok(DeleteOutcome::NotFound),
            Err(error) if is_precondition_failed(&error) => Ok(DeleteOutcome::TokenMismatch),
            Err(error) => Err(map_sdk_error("DeleteObject", error)),
        }
    }

    async fn delete_exact_batch(
        &self,
        objects: Vec<(ObjectPath, PhysicalVersion)>,
    ) -> Result<Vec<DeleteOutcome>> {
        if objects.len() > 1_000 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "exact delete batch cannot exceed 1,000 versions",
            ));
        }
        if objects.is_empty() {
            return Ok(Vec::new());
        }
        if objects
            .iter()
            .any(|(_, version)| !matches!(version, PhysicalVersion::Versioned { .. }))
        {
            let mut outcomes = Vec::with_capacity(objects.len());
            for (path, version) in objects {
                outcomes.push(self.delete_exact(&path, version).await?);
            }
            return Ok(outcomes);
        }

        let identifiers = objects
            .iter()
            .map(|(path, version)| {
                let PhysicalVersion::Versioned { version_id } = version else {
                    unreachable!("unversioned objects were handled above");
                };
                ObjectIdentifier::builder()
                    .key(path.as_str())
                    .version_id(version_id)
                    .build()
                    .map_err(|error| {
                        Error::new(
                            ErrorCode::InvalidRequest,
                            format!("invalid exact delete identifier: {error}"),
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let delete = Delete::builder()
            .set_objects(Some(identifiers))
            .quiet(true)
            .build()
            .map_err(|error| {
                Error::new(
                    ErrorCode::InvalidRequest,
                    format!("invalid exact delete batch: {error}"),
                )
            })?;
        self.metrics.delete_objects.fetch_add(1, Ordering::Relaxed);
        let output = self
            .client
            .delete_objects()
            .bucket(&self.bucket)
            .delete(delete)
            .send()
            .await
            .map_err(|error| map_sdk_error("DeleteObjects exact versions", error))?;
        if let Some(error) = output.errors().first() {
            return Err(Error::new(
                ErrorCode::Transport,
                format!(
                    "DeleteObjects exact versions failed for key {:?}, version {:?}: {}: {}",
                    error.key(),
                    error.version_id(),
                    error.code().unwrap_or("Unknown"),
                    error
                        .message()
                        .unwrap_or("provider omitted an error message")
                ),
            )
            .provider_metadata(
                error.code().map(ToString::to_string),
                error.message().map(ToString::to_string),
            )
            .retry(RetryAdvice::Safe));
        }
        Ok(vec![DeleteOutcome::Deleted; objects.len()])
    }
}

impl AwsS3ObjectPlane {
    async fn list_versions(&self, request: ListRequest) -> Result<PhysicalListPage> {
        self.metrics
            .list_object_versions
            .fetch_add(1, Ordering::Relaxed);
        let (key_marker, version_marker) = request
            .continuation
            .as_deref()
            .and_then(decode_version_cursor)
            .unwrap_or((None, None));
        let output = self
            .client
            .list_object_versions()
            .bucket(&self.bucket)
            .prefix(request.prefix)
            .set_key_marker(key_marker)
            .set_version_id_marker(version_marker)
            .max_keys(i32::try_from(request.limit.min(1_000)).unwrap_or(1_000))
            .send()
            .await
            .map_err(|error| map_sdk_error("ListObjectVersions", error))?;
        let mut entries = Vec::new();
        for version in output.versions() {
            let Some(key) = version.key() else { continue };
            let Some(version_id) = version.version_id() else {
                continue;
            };
            let Ok(path) = ObjectPath::new(key) else {
                continue;
            };
            entries.push(PhysicalListEntry {
                path,
                metadata: StoredMetadata {
                    token: StorageToken {
                        etag: version.e_tag().unwrap_or_default().to_string(),
                        version_id: Some(version_id.to_string()),
                    },
                    len: version
                        .size()
                        .and_then(|value| u64::try_from(value).ok())
                        .unwrap_or_default(),
                    sha256: [0; 32],
                    last_modified_millis: version
                        .last_modified()
                        .and_then(|value| u64::try_from(value.secs()).ok())
                        .and_then(|seconds| seconds.checked_mul(1_000))
                        .unwrap_or_default(),
                    delete_marker: false,
                    user_metadata: BTreeMap::new(),
                },
                is_latest: version.is_latest().unwrap_or(false),
            });
        }
        for marker in output.delete_markers() {
            let Some(key) = marker.key() else { continue };
            let Some(version_id) = marker.version_id() else {
                continue;
            };
            let Ok(path) = ObjectPath::new(key) else {
                continue;
            };
            entries.push(PhysicalListEntry {
                path,
                metadata: StoredMetadata {
                    token: StorageToken {
                        etag: String::new(),
                        version_id: Some(version_id.to_string()),
                    },
                    len: 0,
                    sha256: [0; 32],
                    last_modified_millis: marker
                        .last_modified()
                        .and_then(|value| u64::try_from(value.secs()).ok())
                        .and_then(|seconds| seconds.checked_mul(1_000))
                        .unwrap_or_default(),
                    delete_marker: true,
                    user_metadata: BTreeMap::new(),
                },
                is_latest: marker.is_latest().unwrap_or(false),
            });
        }
        let continuation = if output.is_truncated().unwrap_or(false) {
            Some(encode_version_cursor(
                output.next_key_marker(),
                output.next_version_id_marker(),
            ))
        } else {
            None
        };
        Ok(PhysicalListPage {
            entries,
            continuation,
        })
    }
}

fn format_range(range: &RangeInclusive<u64>) -> String {
    format!("bytes={}-{}", range.start(), range.end())
}

fn multipart_part_size(size: u64) -> Result<u64> {
    if size == 0 {
        return Err(Error::new(
            ErrorCode::InvalidRequest,
            "multipart upload requires a nonempty body",
        ));
    }
    let minimum_for_limit = size.div_ceil(MAX_MULTIPART_PARTS);
    let mebibyte = 1_024 * 1_024;
    let part_size = MIN_MULTIPART_PART_BYTES.max(minimum_for_limit.div_ceil(mebibyte) * mebibyte);
    if part_size > MAX_MULTIPART_PART_BYTES || size.div_ceil(part_size) > MAX_MULTIPART_PARTS {
        return Err(Error::new(
            ErrorCode::EntityTooLarge,
            "multipart upload exceeds S3 part limits",
        ));
    }
    Ok(part_size)
}

fn encode_version_cursor(key: Option<&str>, version: Option<&str>) -> String {
    let key = key.unwrap_or_default();
    let version = version.unwrap_or_default();
    format!("{}:{key}{version}", key.len())
}

fn decode_version_cursor(value: &str) -> Option<(Option<String>, Option<String>)> {
    let (length, rest) = value.split_once(':')?;
    let length: usize = length.parse().ok()?;
    if rest.len() < length || !rest.is_char_boundary(length) {
        return None;
    }
    let (key, version) = rest.split_at(length);
    Some((
        (!key.is_empty()).then(|| key.to_string()),
        (!version.is_empty()).then(|| version.to_string()),
    ))
}

fn is_not_found<E, R>(error: &aws_smithy_runtime_api::client::result::SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .is_some_and(|code| matches!(code, "NoSuchKey" | "NotFound" | "NoSuchVersion"))
}

fn is_precondition_failed<E, R>(
    error: &aws_smithy_runtime_api::client::result::SdkError<E, R>,
) -> bool
where
    E: ProvideErrorMetadata,
{
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .is_some_and(|code| matches!(code, "PreconditionFailed" | "ConditionalRequestConflict"))
}

fn map_sdk_error<E, R>(
    operation: &str,
    error: aws_smithy_runtime_api::client::result::SdkError<E, R>,
) -> Error
where
    E: ProvideErrorMetadata + RequestId + std::fmt::Debug,
    R: std::fmt::Debug,
{
    let provider_code = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .map(ToString::to_string);
    let provider_message = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::message)
        .map(ToString::to_string);
    let provider_request_id = error
        .as_service_error()
        .and_then(RequestId::request_id)
        .map(ToString::to_string);
    let error_code = match (
        provider_code.as_deref().unwrap_or_default(),
        provider_message.as_deref(),
    ) {
        ("AccessDenied" | "InvalidAccessKeyId" | "SignatureDoesNotMatch", _)
        | ("InvalidRequest", Some("ErrAccessKeyDisabled")) => ErrorCode::PermissionDenied,
        ("SlowDown" | "ServiceUnavailable", _) => ErrorCode::Throttled,
        ("RequestTimeout", _) => ErrorCode::Timeout,
        _ => ErrorCode::Transport,
    };
    Error::new(error_code, format!("{operation} failed: {error:?}"))
        .provider_metadata(provider_code, provider_message)
        .provider_request_id(provider_request_id)
        .retry(if matches!(error_code, ErrorCode::PermissionDenied) {
            RetryAdvice::Never
        } else {
            RetryAdvice::Safe
        })
}

fn transport_error(operation: &str, error: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Transport, format!("{operation} failed: {error}"))
        .retry(RetryAdvice::Safe)
}

#[cfg(test)]
mod tests {
    use aws_smithy_runtime_api::client::result::SdkError;
    use aws_smithy_types::error::metadata::{ErrorMetadata, ProvideErrorMetadata};
    use aws_types::request_id::RequestId;

    use super::{map_sdk_error, multipart_part_size, MAX_MULTIPART_PARTS};
    use prolly_s3_core::{ErrorCode, RetryAdvice};

    #[derive(Debug)]
    struct ServiceFailure {
        metadata: ErrorMetadata,
    }

    impl ProvideErrorMetadata for ServiceFailure {
        fn meta(&self) -> &ErrorMetadata {
            &self.metadata
        }
    }

    impl RequestId for ServiceFailure {
        fn request_id(&self) -> Option<&str> {
            Some("request-123")
        }
    }

    #[test]
    fn service_errors_preserve_provider_code_message_and_request_id() {
        let sdk_error = SdkError::service_error(
            ServiceFailure {
                metadata: ErrorMetadata::builder()
                    .code("SlowDown")
                    .message("reduce request rate")
                    .build(),
            },
            (),
        );
        let mapped = map_sdk_error("fixture", sdk_error);
        assert_eq!(mapped.code, ErrorCode::Throttled);
        assert_eq!(mapped.retry, RetryAdvice::Safe);
        assert_eq!(mapped.provider_code.as_deref(), Some("SlowDown"));
        assert_eq!(
            mapped.provider_message.as_deref(),
            Some("reduce request rate")
        );
        assert_eq!(mapped.provider_request_id.as_deref(), Some("request-123"));
    }

    #[test]
    fn rustfs_disabled_access_key_is_terminal_permission_denial() {
        let sdk_error = SdkError::service_error(
            ServiceFailure {
                metadata: ErrorMetadata::builder()
                    .code("InvalidRequest")
                    .message("ErrAccessKeyDisabled")
                    .build(),
            },
            (),
        );
        let mapped = map_sdk_error("fixture", sdk_error);
        assert_eq!(mapped.code, ErrorCode::PermissionDenied);
        assert_eq!(mapped.retry, RetryAdvice::Never);
        assert_eq!(mapped.provider_code.as_deref(), Some("InvalidRequest"));
        assert_eq!(
            mapped.provider_message.as_deref(),
            Some("ErrAccessKeyDisabled")
        );
    }

    #[test]
    fn multipart_layout_is_bounded_through_the_repository_limit() {
        let small = multipart_part_size(64 * 1_024 * 1_024).unwrap();
        assert_eq!(small, 16 * 1_024 * 1_024);
        let maximum = 5 * 1_024 * 1_024 * 1_024 * 1_024_u64;
        let large = multipart_part_size(maximum).unwrap();
        assert!(maximum.div_ceil(large) <= MAX_MULTIPART_PARTS);
        assert!(large <= 5 * 1_024 * 1_024 * 1_024);
    }
}
