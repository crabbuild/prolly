use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use serde::{Deserialize, Serialize};

use crate::{codec::sha256, Error, ErrorCode, Result};

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
pub struct ImmutableFilePut {
    pub path: ObjectPath,
    pub body_path: PathBuf,
    pub size: u64,
    pub expected_sha256: [u8; 32],
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

    /// Upload an immutable object from a file without requiring the caller to
    /// retain its complete body in memory. Providers with streaming upload
    /// support should override this default implementation.
    async fn put_immutable_file(&self, request: ImmutableFilePut) -> Result<ImmutablePutOutcome> {
        let bytes = std::fs::read(&request.body_path).map_err(|error| {
            Error::new(
                ErrorCode::Transport,
                format!("immutable spool could not be read: {error}"),
            )
        })?;
        if bytes.len() as u64 != request.size || sha256(&bytes) != request.expected_sha256 {
            return Err(Error::new(
                ErrorCode::ChecksumMismatch,
                "immutable spool identity changed before upload",
            ));
        }
        self.put_immutable(ImmutablePut {
            path: request.path,
            bytes,
            expected_sha256: request.expected_sha256,
        })
        .await
    }
    async fn load_mutable(&self, path: &ObjectPath) -> Result<Option<StoredObject>>;
    async fn compare_exchange(&self, request: CompareExchange) -> Result<CompareExchangeOutcome>;
    async fn list(&self, request: ListRequest) -> Result<PhysicalListPage>;
    async fn delete_exact(
        &self,
        path: &ObjectPath,
        version: PhysicalVersion,
    ) -> Result<DeleteOutcome>;

    /// Delete at most 1,000 exact physical versions. Providers with a bulk
    /// delete API should override this; the default preserves correctness for
    /// simpler object planes.
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
        let mut outcomes = Vec::with_capacity(objects.len());
        for (path, version) in objects {
            outcomes.push(self.delete_exact(&path, version).await?);
        }
        Ok(outcomes)
    }
}

#[derive(Clone)]
pub struct MemoryObjectPlane {
    inner: Arc<RwLock<MemoryState>>,
    versioned: bool,
    requests: Arc<MemoryRequestCounters>,
    conflict_after_next_compare_exchange: Arc<AtomicBool>,
    immutable_put_delay_millis: Arc<AtomicU64>,
    immutable_put_in_flight: Arc<AtomicU64>,
    immutable_put_max_in_flight: Arc<AtomicU64>,
    compare_exchange_delay_millis: Arc<AtomicU64>,
    compare_exchange_in_flight: Arc<AtomicU64>,
    compare_exchange_max_in_flight: Arc<AtomicU64>,
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
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemoryRequestSnapshot {
    pub get: u64,
    pub head: u64,
    pub immutable_put: u64,
    pub compare_exchange: u64,
    pub list: u64,
    pub delete_exact: u64,
}

impl MemoryRequestSnapshot {
    pub fn total(&self) -> u64 {
        self.get
            + self.head
            + self.immutable_put
            + self.compare_exchange
            + self.list
            + self.delete_exact
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
            conflict_after_next_compare_exchange: Arc::new(AtomicBool::new(false)),
            immutable_put_delay_millis: Arc::new(AtomicU64::new(0)),
            immutable_put_in_flight: Arc::new(AtomicU64::new(0)),
            immutable_put_max_in_flight: Arc::new(AtomicU64::new(0)),
            compare_exchange_delay_millis: Arc::new(AtomicU64::new(0)),
            compare_exchange_in_flight: Arc::new(AtomicU64::new(0)),
            compare_exchange_max_in_flight: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Test hook for proving immutable payload preparation overlaps before an
    /// ordered publication lane.
    pub fn set_immutable_put_delay_millis(&self, delay_millis: u64) {
        self.immutable_put_delay_millis
            .store(delay_millis, Ordering::Relaxed);
    }

    pub fn max_immutable_puts_in_flight(&self) -> u64 {
        self.immutable_put_max_in_flight.load(Ordering::Relaxed)
    }

    pub fn reset_immutable_put_concurrency(&self) {
        self.immutable_put_max_in_flight.store(0, Ordering::Relaxed);
    }

    /// Test hook for proving independent mutable-object publications overlap.
    pub fn set_compare_exchange_delay_millis(&self, delay_millis: u64) {
        self.compare_exchange_delay_millis
            .store(delay_millis, Ordering::Relaxed);
    }

    pub fn max_compare_exchanges_in_flight(&self) -> u64 {
        self.compare_exchange_max_in_flight.load(Ordering::Relaxed)
    }

    pub fn reset_compare_exchange_concurrency(&self) {
        self.compare_exchange_max_in_flight
            .store(0, Ordering::Relaxed);
    }

    /// Test hook that applies the next successful CAS, then reports a
    /// conflict containing the applied value. This models a provider or SDK
    /// retry whose first wire attempt committed but whose response was lost.
    pub fn conflict_after_next_compare_exchange(&self) {
        self.conflict_after_next_compare_exchange
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
        let in_flight = self.immutable_put_in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.immutable_put_max_in_flight
            .fetch_max(in_flight, Ordering::Relaxed);
        let _in_flight = InFlightGuard(self.immutable_put_in_flight.clone());
        let delay = self.immutable_put_delay_millis.load(Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
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
        let in_flight = self
            .compare_exchange_in_flight
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        self.compare_exchange_max_in_flight
            .fetch_max(in_flight, Ordering::Relaxed);
        let _in_flight = InFlightGuard(self.compare_exchange_in_flight.clone());
        let delay = self.compare_exchange_delay_millis.load(Ordering::Relaxed);
        if delay > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        }
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
            bytes: Some(request.bytes.clone()),
            metadata: metadata.clone(),
        });
        if self
            .conflict_after_next_compare_exchange
            .swap(false, Ordering::Relaxed)
        {
            return Ok(CompareExchangeOutcome::Conflict(Some(StoredObject {
                bytes: request.bytes,
                metadata,
            })));
        }
        Ok(CompareExchangeOutcome::Applied(metadata))
    }

    async fn list(&self, request: ListRequest) -> Result<PhysicalListPage> {
        self.requests.list.fetch_add(1, Ordering::Relaxed);
        let state = self
            .inner
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "memory lock poisoned"))?;
        let version_cursor = request
            .include_versions
            .then(|| {
                request
                    .continuation
                    .as_deref()
                    .and_then(decode_memory_version_cursor)
            })
            .flatten();
        let after = (!request.include_versions)
            .then_some(request.continuation.as_deref())
            .flatten();
        let mut entries = Vec::new();
        let limit = request.limit.max(1);
        for (path, versions) in &state.objects {
            if !path.as_str().starts_with(&request.prefix)
                || after.is_some_and(|after| path.as_str() <= after)
                || version_cursor
                    .as_ref()
                    .is_some_and(|(cursor_path, _)| path.as_str() < cursor_path.as_str())
            {
                continue;
            }
            let start = version_cursor
                .as_ref()
                .filter(|(cursor_path, _)| cursor_path == path.as_str())
                .map_or(0, |(_, index)| *index);
            let candidates: Vec<(usize, &MemoryVersion)> = if request.include_versions {
                versions.iter().enumerate().skip(start).collect()
            } else {
                Self::current_raw(versions)
                    .into_iter()
                    .map(|version| (versions.len().saturating_sub(1), version))
                    .collect()
            };
            for (index, version) in candidates {
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
                    let continuation = if request.include_versions {
                        Some(encode_memory_version_cursor(path.as_str(), index + 1))
                    } else {
                        Some(path.as_str().to_string())
                    };
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
}

fn encode_memory_version_cursor(path: &str, next_index: usize) -> String {
    format!("memory-:{}:{next_index}", hex::encode(path.as_bytes()))
}

fn decode_memory_version_cursor(value: &str) -> Option<(String, usize)> {
    let suffix = value.strip_prefix("memory-:")?;
    let (path, index) = suffix.rsplit_once(':')?;
    Some((
        String::from_utf8(hex::decode(path).ok()?).ok()?,
        index.parse().ok()?,
    ))
}
