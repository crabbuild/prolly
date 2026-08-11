use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use md5::{Digest as _, Md5};
use serde::{Deserialize, Serialize};

use crate::{
    codec::sha256, Checksums, Error, ErrorCode, NativeObjectBindingV1, ObjectHeaders, OperationId,
    RepositoryId, Result,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ObjectPath(String);

impl ObjectPath {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        if value.is_empty()
            || value.starts_with('/')
            || value.ends_with('/')
            || value.len() > 1_024
            || value.split('/').any(|part| part.is_empty() || part == "..")
        {
            return Err(Error::new(
                ErrorCode::InvalidKey,
                format!("invalid physical object path: {value:?}"),
            ));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for ObjectPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageToken {
    pub etag: String,
    pub version_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhysicalVersion {
    Unversioned { token: Option<StorageToken> },
    Versioned { version_id: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredMetadata {
    pub token: StorageToken,
    pub len: u64,
    pub sha256: [u8; 32],
    pub last_modified_millis: u64,
    pub delete_marker: bool,
    pub user_metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObject {
    pub bytes: Vec<u8>,
    pub metadata: StoredMetadata,
}

#[derive(Clone, Debug)]
pub struct GetRequest {
    pub path: ObjectPath,
    pub range: Option<RangeInclusive<u64>>,
    pub physical_version: Option<PhysicalVersion>,
}

#[derive(Clone, Debug)]
pub struct ImmutablePut {
    pub path: ObjectPath,
    pub bytes: Vec<u8>,
    pub expected_sha256: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct NativePut {
    pub path: ObjectPath,
    pub bytes: Vec<u8>,
    pub headers: ObjectHeaders,
    pub user_metadata: BTreeMap<String, String>,
    pub repository: RepositoryId,
    pub operation: OperationId,
    pub writer_fence_generation: u64,
}

#[derive(Clone, Debug)]
pub struct NativeFilePut {
    pub path: ObjectPath,
    pub body_path: PathBuf,
    pub size: u64,
    pub checksum_sha256: [u8; 32],
    pub checksum_md5: [u8; 16],
    pub headers: ObjectHeaders,
    pub user_metadata: BTreeMap<String, String>,
    pub repository: RepositoryId,
    pub operation: OperationId,
    pub writer_fence_generation: u64,
}

#[derive(Clone, Debug)]
pub struct NativeFileGet {
    pub path: ObjectPath,
    pub version_id: String,
    pub body_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeFileGetResult {
    pub size: u64,
    pub checksum_sha256: [u8; 32],
    pub checksum_md5: [u8; 16],
}

#[derive(Clone, Debug)]
pub struct NativeCopy {
    pub source: ObjectPath,
    pub source_version_id: String,
    pub destination: ObjectPath,
    pub headers: ObjectHeaders,
    pub user_metadata: BTreeMap<String, String>,
    pub repository: RepositoryId,
    pub operation: OperationId,
    pub writer_fence_generation: u64,
    pub checksum_sha256: [u8; 32],
    pub size: u64,
    pub logical_etag: String,
    pub checksums: Checksums,
}

#[derive(Clone, Debug)]
pub struct NativeDelete {
    pub path: ObjectPath,
    pub repository: RepositoryId,
    pub operation: OperationId,
    pub writer_fence_generation: u64,
}

#[derive(Clone, Debug)]
pub struct NativeMultipartCreate {
    pub path: ObjectPath,
    pub headers: ObjectHeaders,
    pub user_metadata: BTreeMap<String, String>,
    pub repository: RepositoryId,
    pub operation: OperationId,
    pub writer_fence_generation: u64,
}

#[derive(Clone, Debug)]
pub struct NativeMultipartUploadPart {
    pub path: ObjectPath,
    pub upload_id: String,
    pub part_number: u32,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct NativeMultipartFilePart {
    pub path: ObjectPath,
    pub upload_id: String,
    pub part_number: u32,
    pub body_path: PathBuf,
    pub size: u64,
    pub checksum_sha256: [u8; 32],
}

#[derive(Clone, Debug)]
pub struct NativeMultipartUploadPartCopy {
    pub source: ObjectPath,
    pub source_version_id: String,
    pub destination: ObjectPath,
    pub upload_id: String,
    pub part_number: u32,
    pub range: Option<RangeInclusive<u64>>,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeMultipartCompletedPart {
    pub part_number: u32,
    pub etag: String,
    pub checksum_sha256: [u8; 32],
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeMultipartPartResult {
    pub part_number: u32,
    pub etag: String,
    pub checksum_sha256: Option<[u8; 32]>,
    pub size: u64,
}

#[derive(Clone, Debug)]
pub struct NativeMultipartComplete {
    pub path: ObjectPath,
    pub upload_id: String,
    pub parts: Vec<NativeMultipartCompletedPart>,
    pub checksum_sha256: [u8; 32],
    pub checksum_md5: [u8; 16],
    pub size: u64,
}

#[derive(Clone, Debug)]
pub struct NativeMultipartAbort {
    pub path: ObjectPath,
    pub upload_id: String,
}

#[derive(Clone, Debug)]
pub struct NativeMultipartListParts {
    pub path: ObjectPath,
    pub upload_id: String,
    pub after_part_number: u32,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeMultipartListPartsPage {
    pub parts: Vec<NativeMultipartPartResult>,
    pub next_part_number: Option<u32>,
}

#[derive(Clone, Debug)]
pub struct NativeMultipartListUploads {
    pub prefix: String,
    pub key_marker: Option<String>,
    pub upload_id_marker: Option<String>,
    pub limit: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeMultipartUploadEntry {
    pub path: ObjectPath,
    pub upload_id: String,
    pub initiated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeMultipartListUploadsPage {
    pub uploads: Vec<NativeMultipartUploadEntry>,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeObjectWriteResult {
    pub binding: NativeObjectBindingV1,
    pub size: u64,
    pub logical_etag: String,
    pub checksums: Checksums,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ImmutablePutOutcome {
    Created(StoredMetadata),
    AlreadyPresent(StoredMetadata),
}

#[derive(Clone, Debug)]
pub struct CompareExchange {
    pub path: ObjectPath,
    pub expected: Option<StorageToken>,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompareExchangeOutcome {
    Applied(StoredMetadata),
    Conflict(Option<StoredObject>),
}

#[derive(Clone, Debug)]
pub struct ListRequest {
    pub prefix: String,
    pub continuation: Option<String>,
    pub limit: usize,
    pub include_versions: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalListEntry {
    pub path: ObjectPath,
    pub metadata: StoredMetadata,
    pub is_latest: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalListPage {
    pub entries: Vec<PhysicalListEntry>,
    pub continuation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    NotFound,
    TokenMismatch,
}

#[allow(async_fn_in_trait)]
#[async_trait::async_trait]
pub trait ObjectPlane: Send + Sync + 'static {
    async fn get(&self, request: GetRequest) -> Result<Option<StoredObject>>;
    async fn head(&self, path: &ObjectPath) -> Result<Option<StoredMetadata>>;
    async fn put_immutable(&self, request: ImmutablePut) -> Result<ImmutablePutOutcome>;
    async fn load_mutable(&self, path: &ObjectPath) -> Result<Option<StoredObject>>;
    async fn compare_exchange(&self, request: CompareExchange) -> Result<CompareExchangeOutcome>;
    async fn list(&self, request: ListRequest) -> Result<PhysicalListPage>;
    async fn delete_exact(
        &self,
        path: &ObjectPath,
        version: PhysicalVersion,
    ) -> Result<DeleteOutcome>;

    async fn put_native(&self, _request: NativePut) -> Result<NativeObjectWriteResult> {
        Err(Error::new(
            ErrorCode::MissingCapability,
            "object plane does not support native object writes",
        ))
    }

    async fn put_native_file(&self, request: NativeFilePut) -> Result<NativeObjectWriteResult> {
        let bytes = std::fs::read(&request.body_path).map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("native spool could not be read: {error}"),
            )
        })?;
        if bytes.len() as u64 != request.size
            || sha256(&bytes) != request.checksum_sha256
            || <[u8; 16]>::from(Md5::digest(&bytes)) != request.checksum_md5
        {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "native spool identity changed before upload",
            ));
        }
        self.put_native(NativePut {
            path: request.path,
            bytes,
            headers: request.headers,
            user_metadata: request.user_metadata,
            repository: request.repository,
            operation: request.operation,
            writer_fence_generation: request.writer_fence_generation,
        })
        .await
    }

    async fn get_native_file(&self, request: NativeFileGet) -> Result<NativeFileGetResult> {
        let object = self
            .get(GetRequest {
                path: request.path,
                range: None,
                physical_version: Some(PhysicalVersion::Versioned {
                    version_id: request.version_id,
                }),
            })
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingClosure,
                    "native source object version is missing",
                )
            })?;
        std::fs::write(&request.body_path, &object.bytes).map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("native transfer spool could not be written: {error}"),
            )
        })?;
        Ok(NativeFileGetResult {
            size: object.bytes.len() as u64,
            checksum_sha256: sha256(&object.bytes),
            checksum_md5: Md5::digest(&object.bytes).into(),
        })
    }

    async fn copy_native(&self, _request: NativeCopy) -> Result<NativeObjectWriteResult> {
        Err(Error::new(
            ErrorCode::MissingCapability,
            "object plane does not support native object copies",
        ))
    }

    async fn delete_native(&self, _request: NativeDelete) -> Result<NativeObjectBindingV1> {
        Err(Error::new(
            ErrorCode::MissingCapability,
            "object plane does not support native delete markers",
        ))
    }

    async fn create_native_multipart(&self, _request: NativeMultipartCreate) -> Result<String> {
        Err(Error::new(
            ErrorCode::MissingCapability,
            "object plane does not support native multipart creation",
        ))
    }

    async fn upload_native_multipart_part(
        &self,
        _request: NativeMultipartUploadPart,
    ) -> Result<NativeMultipartPartResult> {
        Err(Error::new(
            ErrorCode::MissingCapability,
            "object plane does not support native multipart parts",
        ))
    }

    async fn upload_native_multipart_file_part(
        &self,
        request: NativeMultipartFilePart,
    ) -> Result<NativeMultipartPartResult> {
        let bytes = std::fs::read(&request.body_path).map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("native multipart spool could not be read: {error}"),
            )
        })?;
        if bytes.len() as u64 != request.size || sha256(&bytes) != request.checksum_sha256 {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "native multipart spool identity changed before upload",
            ));
        }
        self.upload_native_multipart_part(NativeMultipartUploadPart {
            path: request.path,
            upload_id: request.upload_id,
            part_number: request.part_number,
            bytes,
        })
        .await
    }

    async fn upload_native_multipart_part_copy(
        &self,
        _request: NativeMultipartUploadPartCopy,
    ) -> Result<NativeMultipartPartResult> {
        Err(Error::new(
            ErrorCode::MissingCapability,
            "object plane does not support native multipart part copy",
        ))
    }

    async fn complete_native_multipart(
        &self,
        _request: NativeMultipartComplete,
    ) -> Result<NativeObjectWriteResult> {
        Err(Error::new(
            ErrorCode::MissingCapability,
            "object plane does not support native multipart completion",
        ))
    }

    async fn abort_native_multipart(&self, _request: NativeMultipartAbort) -> Result<()> {
        Err(Error::new(
            ErrorCode::MissingCapability,
            "object plane does not support native multipart abort",
        ))
    }

    async fn list_native_multipart_parts(
        &self,
        _request: NativeMultipartListParts,
    ) -> Result<NativeMultipartListPartsPage> {
        Err(Error::new(
            ErrorCode::MissingCapability,
            "object plane does not support native multipart part listing",
        ))
    }

    async fn list_native_multipart_uploads(
        &self,
        _request: NativeMultipartListUploads,
    ) -> Result<NativeMultipartListUploadsPage> {
        Err(Error::new(
            ErrorCode::MissingCapability,
            "object plane does not support native multipart upload listing",
        ))
    }
}

#[derive(Clone)]
pub struct MemoryObjectPlane {
    inner: Arc<RwLock<MemoryState>>,
    versioned: bool,
    requests: Arc<MemoryRequestCounters>,
    lose_next_native_put_response: Arc<AtomicBool>,
    lose_next_native_delete_response: Arc<AtomicBool>,
    native_put_delay_millis: Arc<AtomicU64>,
    native_put_in_flight: Arc<AtomicU64>,
    native_put_max_in_flight: Arc<AtomicU64>,
}

struct InFlightGuard(Arc<AtomicU64>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct MemoryRequestCounters {
    get: AtomicU64,
    head: AtomicU64,
    immutable_put: AtomicU64,
    compare_exchange: AtomicU64,
    list: AtomicU64,
    delete_exact: AtomicU64,
    native_put: AtomicU64,
    native_copy: AtomicU64,
    native_delete: AtomicU64,
    native_multipart_create: AtomicU64,
    native_multipart_upload_part: AtomicU64,
    native_multipart_upload_part_copy: AtomicU64,
    native_multipart_complete: AtomicU64,
    native_multipart_abort: AtomicU64,
    native_multipart_list_parts: AtomicU64,
    native_multipart_list_uploads: AtomicU64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemoryRequestSnapshot {
    pub get: u64,
    pub head: u64,
    pub immutable_put: u64,
    pub compare_exchange: u64,
    pub list: u64,
    pub delete_exact: u64,
    pub native_put: u64,
    pub native_copy: u64,
    pub native_delete: u64,
    pub native_multipart_create: u64,
    pub native_multipart_upload_part: u64,
    pub native_multipart_upload_part_copy: u64,
    pub native_multipart_complete: u64,
    pub native_multipart_abort: u64,
    pub native_multipart_list_parts: u64,
    pub native_multipart_list_uploads: u64,
}

impl MemoryRequestSnapshot {
    pub fn total(&self) -> u64 {
        self.get
            + self.head
            + self.immutable_put
            + self.compare_exchange
            + self.list
            + self.delete_exact
            + self.native_put
            + self.native_copy
            + self.native_delete
            + self.native_multipart_create
            + self.native_multipart_upload_part
            + self.native_multipart_upload_part_copy
            + self.native_multipart_complete
            + self.native_multipart_abort
            + self.native_multipart_list_parts
            + self.native_multipart_list_uploads
    }
}

#[derive(Default)]
struct MemoryState {
    objects: BTreeMap<ObjectPath, Vec<MemoryVersion>>,
    multipart: BTreeMap<String, MemoryMultipartUpload>,
    sequence: u64,
}

#[derive(Clone)]
struct MemoryMultipartUpload {
    request: NativeMultipartCreate,
    parts: BTreeMap<u32, Vec<u8>>,
    initiated_at_millis: u64,
}

#[derive(Clone)]
struct MemoryVersion {
    bytes: Option<Vec<u8>>,
    metadata: StoredMetadata,
}

impl MemoryObjectPlane {
    pub fn new(versioned: bool) -> Self {
        Self {
            inner: Arc::new(RwLock::new(MemoryState::default())),
            versioned,
            requests: Arc::new(MemoryRequestCounters::default()),
            lose_next_native_put_response: Arc::new(AtomicBool::new(false)),
            lose_next_native_delete_response: Arc::new(AtomicBool::new(false)),
            native_put_delay_millis: Arc::new(AtomicU64::new(0)),
            native_put_in_flight: Arc::new(AtomicU64::new(0)),
            native_put_max_in_flight: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Test hook for proving bounded payload parallelism without relying on
    /// wall-clock timing assertions.
    pub fn set_native_put_delay_millis(&self, delay_millis: u64) {
        self.native_put_delay_millis
            .store(delay_millis, Ordering::Relaxed);
    }

    pub fn max_native_puts_in_flight(&self) -> u64 {
        self.native_put_max_in_flight.load(Ordering::Relaxed)
    }

    pub fn reset_native_put_concurrency(&self) {
        self.native_put_max_in_flight.store(0, Ordering::Relaxed);
    }

    pub fn lose_next_native_put_response(&self) {
        self.lose_next_native_put_response
            .store(true, Ordering::Relaxed);
    }

    pub fn lose_next_native_delete_response(&self) {
        self.lose_next_native_delete_response
            .store(true, Ordering::Relaxed);
    }

    pub fn request_snapshot(&self) -> MemoryRequestSnapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        MemoryRequestSnapshot {
            get: load(&self.requests.get),
            head: load(&self.requests.head),
            immutable_put: load(&self.requests.immutable_put),
            compare_exchange: load(&self.requests.compare_exchange),
            list: load(&self.requests.list),
            delete_exact: load(&self.requests.delete_exact),
            native_put: load(&self.requests.native_put),
            native_copy: load(&self.requests.native_copy),
            native_delete: load(&self.requests.native_delete),
            native_multipart_create: load(&self.requests.native_multipart_create),
            native_multipart_upload_part: load(&self.requests.native_multipart_upload_part),
            native_multipart_upload_part_copy: load(
                &self.requests.native_multipart_upload_part_copy,
            ),
            native_multipart_complete: load(&self.requests.native_multipart_complete),
            native_multipart_abort: load(&self.requests.native_multipart_abort),
            native_multipart_list_parts: load(&self.requests.native_multipart_list_parts),
            native_multipart_list_uploads: load(&self.requests.native_multipart_list_uploads),
        }
    }

    pub fn reset_request_counts(&self) {
        for counter in [
            &self.requests.get,
            &self.requests.head,
            &self.requests.immutable_put,
            &self.requests.compare_exchange,
            &self.requests.list,
            &self.requests.delete_exact,
            &self.requests.native_put,
            &self.requests.native_copy,
            &self.requests.native_delete,
            &self.requests.native_multipart_create,
            &self.requests.native_multipart_upload_part,
            &self.requests.native_multipart_upload_part_copy,
            &self.requests.native_multipart_complete,
            &self.requests.native_multipart_abort,
            &self.requests.native_multipart_list_parts,
            &self.requests.native_multipart_list_uploads,
        ] {
            counter.store(0, Ordering::Relaxed);
        }
    }

    fn next_metadata(state: &mut MemoryState, bytes: &[u8], versioned: bool) -> StoredMetadata {
        state.sequence = state.sequence.saturating_add(1);
        let digest = sha256(bytes);
        StoredMetadata {
            token: StorageToken {
                etag: format!("\"{}\"", hex::encode(digest)),
                version_id: versioned.then(|| format!("memory-{}", state.sequence)),
            },
            len: bytes.len() as u64,
            sha256: digest,
            last_modified_millis: state.sequence,
            delete_marker: false,
            user_metadata: BTreeMap::new(),
        }
    }

    fn current(versions: &[MemoryVersion]) -> Option<&MemoryVersion> {
        versions
            .iter()
            .rev()
            .find(|version| version.bytes.is_some())
    }

    fn current_raw(versions: &[MemoryVersion]) -> Option<&MemoryVersion> {
        versions.last()
    }
}

impl Default for MemoryObjectPlane {
    fn default() -> Self {
        Self::new(false)
    }
}

#[async_trait::async_trait]
impl ObjectPlane for MemoryObjectPlane {
    async fn get(&self, request: GetRequest) -> Result<Option<StoredObject>> {
        self.requests.get.fetch_add(1, Ordering::Relaxed);
        let state = self
            .inner
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let Some(versions) = state.objects.get(&request.path) else {
            return Ok(None);
        };
        let selected = match request.physical_version {
            Some(PhysicalVersion::Versioned { ref version_id }) => {
                versions.iter().find(|version| {
                    version.metadata.token.version_id.as_deref() == Some(version_id.as_str())
                })
            }
            _ => Self::current_raw(versions),
        };
        let Some(selected) = selected else {
            return Ok(None);
        };
        let Some(bytes) = selected.bytes.as_ref() else {
            return Ok(None);
        };
        let bytes = if let Some(range) = request.range {
            if bytes.is_empty()
                || *range.start() > *range.end()
                || *range.start() >= bytes.len() as u64
            {
                return Err(Error::new(ErrorCode::InvalidRange, "unsatisfiable range"));
            }
            let end = (*range.end()).min(bytes.len() as u64 - 1);
            bytes[*range.start() as usize..=end as usize].to_vec()
        } else {
            bytes.clone()
        };
        Ok(Some(StoredObject {
            bytes,
            metadata: selected.metadata.clone(),
        }))
    }

    async fn head(&self, path: &ObjectPath) -> Result<Option<StoredMetadata>> {
        self.requests.head.fetch_add(1, Ordering::Relaxed);
        let state = self
            .inner
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        Ok(state
            .objects
            .get(path)
            .and_then(|versions| Self::current_raw(versions))
            .filter(|version| version.bytes.is_some())
            .map(|version| version.metadata.clone()))
    }

    async fn put_immutable(&self, request: ImmutablePut) -> Result<ImmutablePutOutcome> {
        self.requests.immutable_put.fetch_add(1, Ordering::Relaxed);
        if sha256(&request.bytes) != request.expected_sha256 {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "immutable put checksum mismatch",
            ));
        }
        let mut state = self
            .inner
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        if let Some(current) = state
            .objects
            .get(&request.path)
            .and_then(|versions| Self::current(versions))
        {
            if current.bytes.as_deref() == Some(request.bytes.as_slice()) {
                return Ok(ImmutablePutOutcome::AlreadyPresent(
                    current.metadata.clone(),
                ));
            }
            return Err(Error::new(
                ErrorCode::CorruptContent,
                format!("different bytes already exist at {}", request.path),
            ));
        }
        let metadata = Self::next_metadata(&mut state, &request.bytes, self.versioned);
        state
            .objects
            .entry(request.path)
            .or_default()
            .push(MemoryVersion {
                bytes: Some(request.bytes),
                metadata: metadata.clone(),
            });
        Ok(ImmutablePutOutcome::Created(metadata))
    }

    async fn load_mutable(&self, path: &ObjectPath) -> Result<Option<StoredObject>> {
        self.get(GetRequest {
            path: path.clone(),
            range: None,
            physical_version: None,
        })
        .await
    }

    async fn compare_exchange(&self, request: CompareExchange) -> Result<CompareExchangeOutcome> {
        self.requests
            .compare_exchange
            .fetch_add(1, Ordering::Relaxed);
        let mut state = self
            .inner
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let current = state
            .objects
            .get(&request.path)
            .and_then(|versions| Self::current_raw(versions))
            .and_then(|version| {
                version.bytes.as_ref().map(|bytes| StoredObject {
                    bytes: bytes.clone(),
                    metadata: version.metadata.clone(),
                })
            });
        let matches = match (&request.expected, &current) {
            (None, None) => true,
            (Some(expected), Some(current)) => expected == &current.metadata.token,
            _ => false,
        };
        if !matches {
            return Ok(CompareExchangeOutcome::Conflict(current));
        }
        let metadata = Self::next_metadata(&mut state, &request.bytes, self.versioned);
        let versions = state.objects.entry(request.path).or_default();
        if !self.versioned {
            versions.clear();
        }
        versions.push(MemoryVersion {
            bytes: Some(request.bytes),
            metadata: metadata.clone(),
        });
        Ok(CompareExchangeOutcome::Applied(metadata))
    }

    async fn list(&self, request: ListRequest) -> Result<PhysicalListPage> {
        self.requests.list.fetch_add(1, Ordering::Relaxed);
        let state = self
            .inner
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let after = request.continuation.as_deref();
        let mut entries = Vec::new();
        let limit = request.limit.max(1);
        for (path, versions) in &state.objects {
            if !path.as_str().starts_with(&request.prefix)
                || after.is_some_and(|after| path.as_str() <= after)
            {
                continue;
            }
            let candidates: Vec<&MemoryVersion> = if request.include_versions {
                versions.iter().collect()
            } else {
                Self::current_raw(versions).into_iter().collect()
            };
            for version in candidates {
                if version.bytes.is_some()
                    || (request.include_versions && version.metadata.delete_marker)
                {
                    entries.push(PhysicalListEntry {
                        path: path.clone(),
                        metadata: version.metadata.clone(),
                        is_latest: versions
                            .last()
                            .is_some_and(|latest| latest.metadata.token == version.metadata.token),
                    });
                }
                if entries.len() == limit {
                    let continuation = Some(path.as_str().to_string());
                    return Ok(PhysicalListPage {
                        entries,
                        continuation,
                    });
                }
            }
        }
        Ok(PhysicalListPage {
            entries,
            continuation: None,
        })
    }

    async fn delete_exact(
        &self,
        path: &ObjectPath,
        version: PhysicalVersion,
    ) -> Result<DeleteOutcome> {
        self.requests.delete_exact.fetch_add(1, Ordering::Relaxed);
        let mut state = self
            .inner
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let Some(versions) = state.objects.get_mut(path) else {
            return Ok(DeleteOutcome::NotFound);
        };
        match version {
            PhysicalVersion::Versioned { version_id } => {
                let Some(index) = versions.iter().position(|entry| {
                    entry.metadata.token.version_id.as_deref() == Some(version_id.as_str())
                }) else {
                    return Ok(DeleteOutcome::NotFound);
                };
                versions.remove(index);
            }
            PhysicalVersion::Unversioned { token } => {
                let Some(current) = Self::current_raw(versions) else {
                    return Ok(DeleteOutcome::NotFound);
                };
                if token
                    .as_ref()
                    .is_some_and(|token| token != &current.metadata.token)
                {
                    return Ok(DeleteOutcome::TokenMismatch);
                }
                versions.clear();
            }
        }
        if versions.is_empty() {
            state.objects.remove(path);
        }
        Ok(DeleteOutcome::Deleted)
    }

    async fn put_native(&self, request: NativePut) -> Result<NativeObjectWriteResult> {
        self.requests.native_put.fetch_add(1, Ordering::Relaxed);
        if !self.versioned {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "native writes require a versioned memory object plane",
            ));
        }
        let in_flight = self.native_put_in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.native_put_max_in_flight
            .fetch_max(in_flight, Ordering::Relaxed);
        let _in_flight = InFlightGuard(self.native_put_in_flight.clone());
        let delay = self.native_put_delay_millis.load(Ordering::Relaxed);
        if delay != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
        let size = request.bytes.len() as u64;
        let sha256 = sha256(&request.bytes);
        let md5: [u8; 16] = Md5::digest(&request.bytes).into();
        let logical_etag = format!("\"{}\"", hex::encode(md5));
        let mut state = self
            .inner
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let mut metadata = Self::next_metadata(&mut state, &request.bytes, true);
        metadata.user_metadata = request.user_metadata.clone();
        metadata.user_metadata.insert(
            "prolly-repository-id".to_string(),
            request.repository.to_string(),
        );
        metadata.user_metadata.insert(
            "prolly-operation-id".to_string(),
            request.operation.to_string(),
        );
        metadata.user_metadata.insert(
            "prolly-writer-fence".to_string(),
            request.writer_fence_generation.to_string(),
        );
        metadata
            .user_metadata
            .insert("prolly-sha256".to_string(), hex::encode(sha256));
        let version_id = metadata.token.version_id.clone().ok_or_else(|| {
            Error::new(
                ErrorCode::ProviderNotQualified,
                "versioned memory object plane omitted VersionId",
            )
        })?;
        let provider_etag = metadata.token.etag.clone();
        state
            .objects
            .entry(request.path)
            .or_default()
            .push(MemoryVersion {
                bytes: Some(request.bytes),
                metadata,
            });
        let result = NativeObjectWriteResult {
            binding: NativeObjectBindingV1::Live {
                version_id,
                provider_etag,
                checksum_sha256: sha256,
            },
            size,
            logical_etag,
            checksums: Checksums {
                md5: Some(md5),
                sha256: Some(sha256),
                algorithm_values: BTreeMap::new(),
            },
        };
        if self
            .lose_next_native_put_response
            .swap(false, Ordering::Relaxed)
        {
            return Err(Error::new(
                ErrorCode::Transport,
                "injected lost native PutObject response",
            ));
        }
        Ok(result)
    }

    async fn copy_native(&self, request: NativeCopy) -> Result<NativeObjectWriteResult> {
        self.requests.native_copy.fetch_add(1, Ordering::Relaxed);
        if !self.versioned {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "native copies require a versioned memory object plane",
            ));
        }
        let bytes = {
            let state = self
                .inner
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
            state
                .objects
                .get(&request.source)
                .and_then(|versions| {
                    versions.iter().find(|version| {
                        version.metadata.token.version_id.as_deref()
                            == Some(request.source_version_id.as_str())
                    })
                })
                .and_then(|version| version.bytes.clone())
                .ok_or_else(|| Error::new(ErrorCode::NoSuchVersion, "native copy source missing"))?
        };
        let result = self
            .put_native(NativePut {
                path: request.destination,
                bytes,
                headers: request.headers,
                user_metadata: request.user_metadata,
                repository: request.repository,
                operation: request.operation,
                writer_fence_generation: request.writer_fence_generation,
            })
            .await;
        self.requests.native_put.fetch_sub(1, Ordering::Relaxed);
        result
    }

    async fn delete_native(&self, request: NativeDelete) -> Result<NativeObjectBindingV1> {
        self.requests.native_delete.fetch_add(1, Ordering::Relaxed);
        if !self.versioned {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "native delete markers require a versioned memory object plane",
            ));
        }
        let mut state = self
            .inner
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let mut metadata = Self::next_metadata(&mut state, &[], true);
        metadata.delete_marker = true;
        metadata.len = 0;
        let version_id = metadata.token.version_id.clone().ok_or_else(|| {
            Error::new(
                ErrorCode::ProviderNotQualified,
                "versioned memory delete omitted VersionId",
            )
        })?;
        state
            .objects
            .entry(request.path)
            .or_default()
            .push(MemoryVersion {
                bytes: None,
                metadata,
            });
        if self
            .lose_next_native_delete_response
            .swap(false, Ordering::Relaxed)
        {
            return Err(Error::new(
                ErrorCode::Transport,
                "injected lost native DeleteObject response",
            ));
        }
        Ok(NativeObjectBindingV1::DeleteMarker { version_id })
    }

    async fn create_native_multipart(&self, request: NativeMultipartCreate) -> Result<String> {
        self.requests
            .native_multipart_create
            .fetch_add(1, Ordering::Relaxed);
        if !self.versioned {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "native multipart requires a versioned memory object plane",
            ));
        }
        let mut state = self
            .inner
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        state.sequence = state.sequence.saturating_add(1);
        let upload_id = format!("memory-multipart-{}", state.sequence);
        let initiated_at_millis = state.sequence;
        state.multipart.insert(
            upload_id.clone(),
            MemoryMultipartUpload {
                request,
                parts: BTreeMap::new(),
                initiated_at_millis,
            },
        );
        Ok(upload_id)
    }

    async fn upload_native_multipart_part(
        &self,
        request: NativeMultipartUploadPart,
    ) -> Result<NativeMultipartPartResult> {
        self.requests
            .native_multipart_upload_part
            .fetch_add(1, Ordering::Relaxed);
        if !(1..=10_000).contains(&request.part_number) {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "part number must be between 1 and 10000",
            ));
        }
        let checksum_sha256 = sha256(&request.bytes);
        let md5: [u8; 16] = Md5::digest(&request.bytes).into();
        let size = request.bytes.len() as u64;
        let mut state = self
            .inner
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let upload = state
            .multipart
            .get_mut(&request.upload_id)
            .filter(|upload| upload.request.path == request.path)
            .ok_or_else(|| {
                Error::new(ErrorCode::NoSuchUpload, "native multipart upload missing")
            })?;
        upload.parts.insert(request.part_number, request.bytes);
        Ok(NativeMultipartPartResult {
            part_number: request.part_number,
            etag: format!("\"{}\"", hex::encode(md5)),
            checksum_sha256: Some(checksum_sha256),
            size,
        })
    }

    async fn upload_native_multipart_part_copy(
        &self,
        request: NativeMultipartUploadPartCopy,
    ) -> Result<NativeMultipartPartResult> {
        self.requests
            .native_multipart_upload_part_copy
            .fetch_add(1, Ordering::Relaxed);
        let bytes = {
            let state = self
                .inner
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
            let bytes = state
                .objects
                .get(&request.source)
                .and_then(|versions| {
                    versions.iter().find(|version| {
                        version.metadata.token.version_id.as_deref()
                            == Some(request.source_version_id.as_str())
                    })
                })
                .and_then(|version| version.bytes.clone())
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::NoSuchVersion,
                        "native multipart copy source missing",
                    )
                })?;
            match request.range {
                Some(range)
                    if !bytes.is_empty()
                        && range.start() <= range.end()
                        && *range.start() < bytes.len() as u64 =>
                {
                    let end = (*range.end()).min(bytes.len() as u64 - 1);
                    bytes[*range.start() as usize..=end as usize].to_vec()
                }
                Some(_) => {
                    return Err(Error::new(
                        ErrorCode::InvalidRange,
                        "native multipart copy range is unsatisfiable",
                    ))
                }
                None => bytes,
            }
        };
        let checksum_sha256 = sha256(&bytes);
        let md5: [u8; 16] = Md5::digest(&bytes).into();
        let size = bytes.len() as u64;
        let mut state = self
            .inner
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let upload = state
            .multipart
            .get_mut(&request.upload_id)
            .filter(|upload| upload.request.path == request.destination)
            .ok_or_else(|| {
                Error::new(ErrorCode::NoSuchUpload, "native multipart upload missing")
            })?;
        upload.parts.insert(request.part_number, bytes);
        Ok(NativeMultipartPartResult {
            part_number: request.part_number,
            etag: format!("\"{}\"", hex::encode(md5)),
            checksum_sha256: Some(checksum_sha256),
            size,
        })
    }

    async fn complete_native_multipart(
        &self,
        request: NativeMultipartComplete,
    ) -> Result<NativeObjectWriteResult> {
        self.requests
            .native_multipart_complete
            .fetch_add(1, Ordering::Relaxed);
        let upload = {
            let state = self
                .inner
                .read()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
            state
                .multipart
                .get(&request.upload_id)
                .filter(|upload| upload.request.path == request.path)
                .cloned()
                .ok_or_else(|| {
                    Error::new(ErrorCode::NoSuchUpload, "native multipart upload missing")
                })?
        };
        let mut bytes = Vec::new();
        for completed in &request.parts {
            let part = upload.parts.get(&completed.part_number).ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidRequest,
                    "completed multipart part is missing",
                )
            })?;
            if sha256(part) != completed.checksum_sha256 || part.len() as u64 != completed.size {
                return Err(Error::new(
                    ErrorCode::ChecksumMismatch,
                    "completed multipart part does not match its receipt",
                ));
            }
            bytes.extend_from_slice(part);
        }
        if bytes.len() as u64 != request.size || sha256(&bytes) != request.checksum_sha256 {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "completed multipart object does not match its declared checksum or size",
            ));
        }
        if <[u8; 16]>::from(Md5::digest(&bytes)) != request.checksum_md5 {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "completed multipart object does not match its declared MD5 checksum",
            ));
        }
        let result = self
            .put_native(NativePut {
                path: request.path,
                bytes,
                headers: upload.request.headers,
                user_metadata: upload.request.user_metadata,
                repository: upload.request.repository,
                operation: upload.request.operation,
                writer_fence_generation: upload.request.writer_fence_generation,
            })
            .await;
        self.requests.native_put.fetch_sub(1, Ordering::Relaxed);
        if result.is_ok() {
            let mut state = self
                .inner
                .write()
                .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
            state.multipart.remove(&request.upload_id);
        }
        result
    }

    async fn abort_native_multipart(&self, request: NativeMultipartAbort) -> Result<()> {
        self.requests
            .native_multipart_abort
            .fetch_add(1, Ordering::Relaxed);
        let mut state = self
            .inner
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        match state.multipart.get(&request.upload_id) {
            Some(upload) if upload.request.path == request.path => {
                state.multipart.remove(&request.upload_id);
                Ok(())
            }
            _ => Err(Error::new(
                ErrorCode::NoSuchUpload,
                "native multipart upload missing",
            )),
        }
    }

    async fn list_native_multipart_parts(
        &self,
        request: NativeMultipartListParts,
    ) -> Result<NativeMultipartListPartsPage> {
        self.requests
            .native_multipart_list_parts
            .fetch_add(1, Ordering::Relaxed);
        let state = self
            .inner
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let upload = state
            .multipart
            .get(&request.upload_id)
            .filter(|upload| upload.request.path == request.path)
            .ok_or_else(|| {
                Error::new(ErrorCode::NoSuchUpload, "native multipart upload missing")
            })?;
        let limit = request.limit.min(1_000);
        let mut parts = upload
            .parts
            .iter()
            .filter(|(part_number, _)| **part_number > request.after_part_number)
            .take(limit.saturating_add(1))
            .map(|(part_number, bytes)| {
                let md5: [u8; 16] = Md5::digest(bytes).into();
                NativeMultipartPartResult {
                    part_number: *part_number,
                    etag: format!("\"{}\"", hex::encode(md5)),
                    checksum_sha256: Some(sha256(bytes)),
                    size: bytes.len() as u64,
                }
            })
            .collect::<Vec<_>>();
        let next_part_number = (parts.len() > limit)
            .then(|| {
                parts
                    .get(limit.saturating_sub(1))
                    .map(|part| part.part_number)
            })
            .flatten();
        parts.truncate(limit);
        Ok(NativeMultipartListPartsPage {
            parts,
            next_part_number,
        })
    }

    async fn list_native_multipart_uploads(
        &self,
        request: NativeMultipartListUploads,
    ) -> Result<NativeMultipartListUploadsPage> {
        self.requests
            .native_multipart_list_uploads
            .fetch_add(1, Ordering::Relaxed);
        let state = self
            .inner
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let mut uploads = state
            .multipart
            .iter()
            .filter(|(_, upload)| upload.request.path.as_str().starts_with(&request.prefix))
            .map(|(upload_id, upload)| NativeMultipartUploadEntry {
                path: upload.request.path.clone(),
                upload_id: upload_id.clone(),
                initiated_at_millis: upload.initiated_at_millis,
            })
            .collect::<Vec<_>>();
        uploads.sort_by(|left, right| {
            (left.path.as_str(), left.upload_id.as_str())
                .cmp(&(right.path.as_str(), right.upload_id.as_str()))
        });
        uploads.retain(|upload| match request.key_marker.as_deref() {
            None => true,
            Some(marker) if upload.path.as_str() > marker => true,
            Some(marker) if upload.path.as_str() == marker => request
                .upload_id_marker
                .as_deref()
                .is_some_and(|upload_marker| upload.upload_id.as_str() > upload_marker),
            Some(_) => false,
        });
        let limit = request.limit.min(1_000);
        let has_more = uploads.len() > limit;
        uploads.truncate(limit);
        let (next_key_marker, next_upload_id_marker) = if has_more {
            uploads
                .last()
                .map(|upload| {
                    (
                        Some(upload.path.as_str().to_string()),
                        Some(upload.upload_id.clone()),
                    )
                })
                .unwrap_or_default()
        } else {
            (None, None)
        };
        Ok(NativeMultipartListUploadsPage {
            uploads,
            next_key_marker,
            next_upload_id_marker,
        })
    }
}
