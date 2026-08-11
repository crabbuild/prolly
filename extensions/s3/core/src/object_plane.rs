use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
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
}

#[derive(Clone)]
pub struct MemoryObjectPlane {
    inner: Arc<RwLock<MemoryState>>,
    versioned: bool,
    requests: Arc<MemoryRequestCounters>,
    lose_next_native_put_response: Arc<AtomicBool>,
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
    }
}

#[derive(Default)]
struct MemoryState {
    objects: BTreeMap<ObjectPath, Vec<MemoryVersion>>,
    sequence: u64,
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
        }
    }

    pub fn lose_next_native_put_response(&self) {
        self.lose_next_native_put_response
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
        Ok(NativeObjectBindingV1::DeleteMarker { version_id })
    }
}
