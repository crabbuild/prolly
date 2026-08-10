use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    sync::{Arc, RwLock},
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
}

#[derive(Clone)]
pub struct MemoryObjectPlane {
    inner: Arc<RwLock<MemoryState>>,
    versioned: bool,
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
                if version.bytes.is_some() {
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
