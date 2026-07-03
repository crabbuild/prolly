//! Large value offloading helpers.
//!
//! The core tree stores byte values inline. This module layers an optional
//! content-addressed blob store on top so applications can keep large payloads
//! out of leaf nodes while preserving normal map semantics.

use std::collections::HashMap;
use std::fs::{self, DirEntry, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::cid::Cid;
use super::error::Error;

const VALUE_REF_MAGIC: &[u8; 4] = b"PLVB";
const VALUE_REF_VERSION: u8 = 1;
const VALUE_REF_INLINE: u8 = 0;
const VALUE_REF_BLOB: u8 = 1;
const VALUE_REF_HEADER_LEN: usize = 6;
const U64_LEN: usize = 8;

/// Default maximum value size stored directly in leaf nodes by large-value
/// helpers.
pub const DEFAULT_INLINE_VALUE_THRESHOLD: usize = 64 * 1024;

/// Content-addressed reference to an offloaded blob.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlobRef {
    /// Content ID of the blob bytes.
    pub cid: Cid,
    /// Blob length in bytes.
    pub len: u64,
}

impl BlobRef {
    /// Create a blob reference from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            cid: Cid::from_bytes(bytes),
            len: bytes.len() as u64,
        }
    }

    /// Validate that bytes match this content-addressed reference.
    pub fn validate_bytes(&self, bytes: &[u8]) -> Result<(), Error> {
        validate_blob_reference(self, bytes)
    }
}

/// Value stored in a leaf by the large-value helper layer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueRef {
    /// Inline value bytes.
    Inline(Vec<u8>),
    /// Content-addressed blob reference.
    Blob(BlobRef),
}

impl ValueRef {
    /// Encode this reference as a deterministic leaf value envelope.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Inline(value) => {
                let mut out = Vec::with_capacity(VALUE_REF_HEADER_LEN + U64_LEN + value.len());
                write_header(&mut out, VALUE_REF_INLINE);
                out.extend_from_slice(&(value.len() as u64).to_be_bytes());
                out.extend_from_slice(value);
                out
            }
            Self::Blob(reference) => {
                let mut out = Vec::with_capacity(VALUE_REF_HEADER_LEN + 32 + U64_LEN);
                write_header(&mut out, VALUE_REF_BLOB);
                out.extend_from_slice(reference.cid.as_bytes());
                out.extend_from_slice(&reference.len.to_be_bytes());
                out
            }
        }
    }

    /// Decode a value reference envelope.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if !bytes.starts_with(VALUE_REF_MAGIC) {
            return Err(Error::Deserialize(
                "value reference missing PLVB magic".to_string(),
            ));
        }
        decode_value_ref(bytes)
    }

    /// Decode a stored value, treating non-envelope bytes as inline values.
    pub fn from_stored_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.starts_with(VALUE_REF_MAGIC) {
            decode_value_ref(bytes)
        } else {
            Ok(Self::Inline(bytes.to_vec()))
        }
    }

    /// Whether raw inline bytes must be escaped to avoid being interpreted as a
    /// value reference envelope.
    pub fn inline_requires_escape(value: &[u8]) -> bool {
        value.starts_with(VALUE_REF_MAGIC)
    }
}

/// Large-value offload policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LargeValueConfig {
    /// Values larger than this many bytes are written to the blob store.
    pub inline_threshold: usize,
}

impl LargeValueConfig {
    /// Create a new policy with an explicit inline threshold.
    pub fn new(inline_threshold: usize) -> Self {
        Self { inline_threshold }
    }

    /// Configure the maximum value size stored directly in leaf nodes.
    pub fn with_inline_threshold(mut self, inline_threshold: usize) -> Self {
        self.inline_threshold = inline_threshold;
        self
    }
}

impl Default for LargeValueConfig {
    fn default() -> Self {
        Self {
            inline_threshold: DEFAULT_INLINE_VALUE_THRESHOLD,
        }
    }
}

/// Content-addressed blob storage used by large-value helpers.
pub trait BlobStore: Send + Sync {
    /// Error type for blob storage operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Load a blob by reference.
    fn get_blob(&self, reference: &BlobRef) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Store bytes and return their content-addressed reference.
    fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef, Self::Error>;

    /// Delete a blob. Deleting a missing blob is not an error.
    fn delete_blob(&self, reference: &BlobRef) -> Result<(), Self::Error>;
}

impl<T: BlobStore> BlobStore for Arc<T> {
    type Error = T::Error;

    fn get_blob(&self, reference: &BlobRef) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get_blob(reference)
    }

    fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef, Self::Error> {
        (**self).put_blob(bytes)
    }

    fn delete_blob(&self, reference: &BlobRef) -> Result<(), Self::Error> {
        (**self).delete_blob(reference)
    }
}

/// Blob stores that can enumerate known blob references.
///
/// This trait is separate from [`BlobStore`] so simple point-read blob stores
/// do not need to expose backend-wide scans. Implementations should return
/// content-addressed blob references, not temporary files or unrelated metadata.
pub trait BlobStoreScan: BlobStore {
    /// List all known blob references.
    ///
    /// Returned references should be sorted by raw CID bytes for deterministic
    /// garbage-collection planning.
    fn list_blob_refs(&self) -> Result<Vec<BlobRef>, Self::Error>;
}

impl<T: BlobStoreScan> BlobStoreScan for Arc<T> {
    fn list_blob_refs(&self) -> Result<Vec<BlobRef>, Self::Error> {
        (**self).list_blob_refs()
    }
}

/// Async content-addressed blob storage used by large-value helpers.
///
/// This trait is available behind the `async-store` feature and mirrors
/// [`BlobStore`] for object stores, remote caches, browser storage, and other
/// non-blocking blob backends. Like [`crate::AsyncStore`], it does not require
/// `Send` or `Sync` at the trait level so single-threaded WASM stores can
/// implement it.
#[cfg(feature = "async-store")]
#[allow(async_fn_in_trait)]
pub trait AsyncBlobStore {
    /// Error type for blob storage operations.
    type Error: std::error::Error + 'static;

    /// Load a blob by reference.
    async fn get_blob(&self, reference: &BlobRef) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Store bytes and return their content-addressed reference.
    async fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef, Self::Error>;

    /// Delete a blob. Deleting a missing blob is not an error.
    async fn delete_blob(&self, reference: &BlobRef) -> Result<(), Self::Error>;

    /// Maximum in-flight point reads for default ordered blob reads.
    fn read_parallelism(&self) -> usize {
        1
    }

    /// Retrieve multiple blobs while preserving request order.
    ///
    /// The default implementation deduplicates repeated references, performs
    /// point reads, and expands results back to the original request order. If
    /// [`AsyncBlobStore::read_parallelism`] is greater than one, point reads are
    /// overlapped up to that limit.
    async fn get_blobs_ordered(
        &self,
        references: &[BlobRef],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        async_get_blobs_ordered_with_limit(self, references, self.read_parallelism()).await
    }
}

#[cfg(feature = "async-store")]
async fn async_get_blobs_ordered_with_limit<S: AsyncBlobStore + ?Sized>(
    store: &S,
    references: &[BlobRef],
    max_in_flight: usize,
) -> Result<Vec<Option<Vec<u8>>>, S::Error> {
    if references.is_empty() {
        return Ok(Vec::new());
    }

    let plan = OrderedBlobReadPlan::new(references);
    let unique_values =
        async_get_blob_refs_ordered_unique_with_limit(store, plan.unique_refs(), max_in_flight)
            .await?;
    Ok(plan.expand_owned(unique_values))
}

#[cfg(feature = "async-store")]
async fn async_get_blob_refs_ordered_unique_with_limit<S: AsyncBlobStore + ?Sized>(
    store: &S,
    references: &[BlobRef],
    max_in_flight: usize,
) -> Result<Vec<Option<Vec<u8>>>, S::Error> {
    if references.is_empty() {
        return Ok(Vec::new());
    }

    let max_in_flight = max_in_flight.max(1);
    if references.len() < 2 || max_in_flight == 1 {
        let mut values = Vec::with_capacity(references.len());
        for reference in references {
            values.push(store.get_blob(reference).await?);
        }
        return Ok(values);
    }

    use futures_util::stream::{FuturesUnordered, StreamExt as _};

    let mut values = vec![None; references.len()];
    let mut next_idx = 0usize;
    let mut in_flight = FuturesUnordered::new();

    while next_idx < references.len() && in_flight.len() < max_in_flight {
        in_flight.push(async_get_blob_indexed(
            store,
            next_idx,
            references[next_idx].clone(),
        ));
        next_idx += 1;
    }

    while let Some((idx, result)) = in_flight.next().await {
        values[idx] = result?;

        if next_idx < references.len() {
            in_flight.push(async_get_blob_indexed(
                store,
                next_idx,
                references[next_idx].clone(),
            ));
            next_idx += 1;
        }
    }

    Ok(values)
}

#[cfg(feature = "async-store")]
async fn async_get_blob_indexed<S: AsyncBlobStore + ?Sized>(
    store: &S,
    idx: usize,
    reference: BlobRef,
) -> (usize, Result<Option<Vec<u8>>, S::Error>) {
    (idx, store.get_blob(&reference).await)
}

#[cfg(feature = "async-store")]
struct OrderedBlobReadPlan {
    unique_refs: Vec<BlobRef>,
    positions: Option<Vec<usize>>,
}

#[cfg(feature = "async-store")]
impl OrderedBlobReadPlan {
    fn new(references: &[BlobRef]) -> Self {
        if references.len() < 2 {
            return Self {
                unique_refs: references.to_vec(),
                positions: None,
            };
        }

        let mut unique_indexes = HashMap::with_capacity(references.len());
        let mut unique_refs = Vec::with_capacity(references.len());
        let mut positions: Option<Vec<usize>> = None;

        for reference in references {
            match unique_indexes.entry(reference.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => {
                    let positions =
                        positions.get_or_insert_with(|| (0..unique_refs.len()).collect());
                    positions.push(*entry.get());
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let unique_idx = unique_refs.len();
                    unique_refs.push(reference.clone());
                    if let Some(positions) = positions.as_mut() {
                        positions.push(unique_idx);
                    }
                    entry.insert(unique_idx);
                }
            }
        }

        Self {
            unique_refs,
            positions,
        }
    }

    fn unique_refs(&self) -> &[BlobRef] {
        &self.unique_refs
    }

    fn expand_owned<T: Clone>(&self, unique_values: Vec<Option<T>>) -> Vec<Option<T>> {
        debug_assert_eq!(self.unique_refs.len(), unique_values.len());
        match &self.positions {
            Some(positions) => positions
                .iter()
                .map(|&unique_idx| unique_values[unique_idx].clone())
                .collect(),
            None => unique_values,
        }
    }
}

/// Adapter that exposes an existing synchronous [`BlobStore`] as an
/// [`AsyncBlobStore`].
///
/// This adapter calls the synchronous blob store directly and does not spawn
/// blocking work. Use `TokioBlockingBlobStore` when a Tokio application needs
/// to adapt a blocking blob backend without stalling async worker threads.
#[cfg(feature = "async-store")]
#[derive(Clone, Debug)]
pub struct SyncBlobStoreAsAsync<S> {
    inner: S,
}

#[cfg(feature = "async-store")]
impl<S> SyncBlobStoreAsAsync<S> {
    /// Create a new adapter.
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    /// Borrow the wrapped blob store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Consume the adapter and return the wrapped blob store.
    pub fn into_inner(self) -> S {
        self.inner
    }
}

#[cfg(feature = "async-store")]
impl<S: BlobStore> AsyncBlobStore for SyncBlobStoreAsAsync<S> {
    type Error = S::Error;

    async fn get_blob(&self, reference: &BlobRef) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get_blob(reference)
    }

    async fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef, Self::Error> {
        self.inner.put_blob(bytes)
    }

    async fn delete_blob(&self, reference: &BlobRef) -> Result<(), Self::Error> {
        self.inner.delete_blob(reference)
    }
}

#[cfg(feature = "async-store")]
impl<T: AsyncBlobStore> AsyncBlobStore for Arc<T> {
    type Error = T::Error;

    async fn get_blob(&self, reference: &BlobRef) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get_blob(reference).await
    }

    async fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef, Self::Error> {
        (**self).put_blob(bytes).await
    }

    async fn delete_blob(&self, reference: &BlobRef) -> Result<(), Self::Error> {
        (**self).delete_blob(reference).await
    }

    fn read_parallelism(&self) -> usize {
        (**self).read_parallelism()
    }

    async fn get_blobs_ordered(
        &self,
        references: &[BlobRef],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        (**self).get_blobs_ordered(references).await
    }
}

/// Error returned by [`TokioBlockingBlobStore`].
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub enum TokioBlockingBlobStoreError<E> {
    /// The wrapped synchronous blob store returned an error.
    Store(E),
    /// Tokio failed to complete the blocking task, usually because it panicked
    /// or the runtime is shutting down.
    Join(tokio::task::JoinError),
}

#[cfg(feature = "tokio")]
impl<E: std::fmt::Display> std::fmt::Display for TokioBlockingBlobStoreError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(err) => write!(f, "blob store error: {err}"),
            Self::Join(err) => write!(f, "tokio blocking task failed: {err}"),
        }
    }
}

#[cfg(feature = "tokio")]
impl<E> std::error::Error for TokioBlockingBlobStoreError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(err) => Some(err),
            Self::Join(err) => Some(err),
        }
    }
}

/// Tokio-backed adapter that exposes a blocking [`BlobStore`] as an
/// [`AsyncBlobStore`].
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub struct TokioBlockingBlobStore<S> {
    inner: Arc<S>,
}

#[cfg(feature = "tokio")]
impl<S> Clone for TokioBlockingBlobStore<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(feature = "tokio")]
impl<S> TokioBlockingBlobStore<S> {
    /// Create an adapter from an owned blob store.
    pub fn new(inner: S) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Create an adapter from an already shared blob store.
    pub fn from_arc(inner: Arc<S>) -> Self {
        Self { inner }
    }

    /// Borrow the wrapped blob store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Clone the shared wrapped blob-store handle.
    pub fn shared(&self) -> Arc<S> {
        self.inner.clone()
    }
}

#[cfg(feature = "tokio")]
async fn spawn_blob_blocking<S, F, R>(
    store: Arc<S>,
    operation: F,
) -> Result<R, TokioBlockingBlobStoreError<S::Error>>
where
    S: BlobStore + 'static,
    F: FnOnce(Arc<S>) -> Result<R, S::Error> + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(move || operation(store))
        .await
        .map_err(TokioBlockingBlobStoreError::Join)?
        .map_err(TokioBlockingBlobStoreError::Store)
}

#[cfg(feature = "tokio")]
impl<S> AsyncBlobStore for TokioBlockingBlobStore<S>
where
    S: BlobStore + 'static,
{
    type Error = TokioBlockingBlobStoreError<S::Error>;

    async fn get_blob(&self, reference: &BlobRef) -> Result<Option<Vec<u8>>, Self::Error> {
        let reference = reference.clone();
        spawn_blob_blocking(self.inner.clone(), move |store| store.get_blob(&reference)).await
    }

    async fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef, Self::Error> {
        let bytes = bytes.to_vec();
        spawn_blob_blocking(self.inner.clone(), move |store| store.put_blob(&bytes)).await
    }

    async fn delete_blob(&self, reference: &BlobRef) -> Result<(), Self::Error> {
        let reference = reference.clone();
        spawn_blob_blocking(self.inner.clone(), move |store| {
            store.delete_blob(&reference)
        })
        .await
    }
}

/// In-memory blob store for tests and lightweight applications.
#[derive(Debug, Default)]
pub struct MemBlobStore {
    data: RwLock<HashMap<Cid, Vec<u8>>>,
}

impl MemBlobStore {
    /// Create an empty in-memory blob store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of stored blobs.
    pub fn len(&self) -> Result<usize, MemBlobStoreError> {
        self.data
            .read()
            .map(|data| data.len())
            .map_err(|err| MemBlobStoreError(format!("lock poisoned: {err}")))
    }

    /// Whether the store contains no blobs.
    pub fn is_empty(&self) -> Result<bool, MemBlobStoreError> {
        self.len().map(|len| len == 0)
    }
}

/// Error type for [`MemBlobStore`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemBlobStoreError(String);

impl std::fmt::Display for MemBlobStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemBlobStore error: {}", self.0)
    }
}

impl std::error::Error for MemBlobStoreError {}

impl BlobStore for MemBlobStore {
    type Error = MemBlobStoreError;

    fn get_blob(&self, reference: &BlobRef) -> Result<Option<Vec<u8>>, Self::Error> {
        let data = self
            .data
            .read()
            .map_err(|err| MemBlobStoreError(format!("lock poisoned: {err}")))?;
        Ok(data.get(&reference.cid).cloned())
    }

    fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef, Self::Error> {
        let reference = BlobRef::from_bytes(bytes);
        let mut data = self
            .data
            .write()
            .map_err(|err| MemBlobStoreError(format!("lock poisoned: {err}")))?;
        data.entry(reference.cid.clone())
            .or_insert_with(|| bytes.to_vec());
        Ok(reference)
    }

    fn delete_blob(&self, reference: &BlobRef) -> Result<(), Self::Error> {
        let mut data = self
            .data
            .write()
            .map_err(|err| MemBlobStoreError(format!("lock poisoned: {err}")))?;
        data.remove(&reference.cid);
        Ok(())
    }
}

impl BlobStoreScan for MemBlobStore {
    fn list_blob_refs(&self) -> Result<Vec<BlobRef>, Self::Error> {
        let data = self
            .data
            .read()
            .map_err(|err| MemBlobStoreError(format!("lock poisoned: {err}")))?;
        let mut refs = data
            .iter()
            .map(|(cid, bytes)| BlobRef {
                cid: cid.clone(),
                len: bytes.len() as u64,
            })
            .collect::<Vec<_>>();
        sort_blob_refs(&mut refs);
        Ok(refs)
    }
}

/// Durable filesystem-backed blob store.
///
/// Blobs are stored under `blobs/sha256/aa/bb/<cid-hex>` below the configured
/// root directory. Writes are content-addressed and idempotent; a completed
/// write is published with an atomic rename inside the target directory.
#[derive(Clone, Debug)]
pub struct FileBlobStore {
    root: PathBuf,
}

impl FileBlobStore {
    /// Open or create a filesystem blob store rooted at `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self, FileBlobStoreError> {
        let root = root.into();
        let store = Self { root };
        store.ensure_namespace_dir()?;
        Ok(store)
    }

    /// Return the root directory for this blob store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Return the namespace directory that contains sharded blob files.
    pub fn namespace_dir(&self) -> PathBuf {
        self.root.join("blobs").join("sha256")
    }

    /// Return the filesystem path for a blob reference.
    pub fn path_for_ref(&self, reference: &BlobRef) -> PathBuf {
        self.blob_path(&reference.cid)
    }

    fn ensure_namespace_dir(&self) -> Result<(), FileBlobStoreError> {
        let path = self.namespace_dir();
        fs::create_dir_all(&path).map_err(|source| FileBlobStoreError::Io { path, source })
    }

    fn blob_path(&self, cid: &Cid) -> PathBuf {
        let hex = cid_hex(cid);
        self.namespace_dir()
            .join(&hex[0..2])
            .join(&hex[2..4])
            .join(hex)
    }

    fn temp_path_for(&self, path: &Path) -> PathBuf {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("blob");
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        path.with_file_name(format!(".{file_name}.{}.{}.tmp", std::process::id(), nanos))
    }

    fn read_blob_path(
        &self,
        reference: &BlobRef,
        path: &Path,
    ) -> Result<Option<Vec<u8>>, FileBlobStoreError> {
        match fs::read(path) {
            Ok(bytes) => {
                validate_file_blob(reference, path, &bytes)?;
                Ok(Some(bytes))
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(FileBlobStoreError::Io {
                path: path.to_path_buf(),
                source,
            }),
        }
    }
}

impl BlobStore for FileBlobStore {
    type Error = FileBlobStoreError;

    fn get_blob(&self, reference: &BlobRef) -> Result<Option<Vec<u8>>, Self::Error> {
        self.read_blob_path(reference, &self.blob_path(&reference.cid))
    }

    fn put_blob(&self, bytes: &[u8]) -> Result<BlobRef, Self::Error> {
        let reference = BlobRef::from_bytes(bytes);
        let path = self.blob_path(&reference.cid);

        if let Some(existing) = self.read_blob_path(&reference, &path)? {
            if existing.len() as u64 == reference.len {
                return Ok(reference);
            }
        }

        let Some(parent) = path.parent() else {
            return Err(FileBlobStoreError::InvalidPath {
                path,
                message: "blob path has no parent directory".to_string(),
            });
        };
        fs::create_dir_all(parent).map_err(|source| FileBlobStoreError::Io {
            path: parent.to_path_buf(),
            source,
        })?;

        let mut temp_path = self.temp_path_for(&path);
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
        {
            Ok(file) => file,
            Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                temp_path = self.temp_path_for(&path);
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temp_path)
                    .map_err(|source| FileBlobStoreError::Io {
                        path: temp_path.clone(),
                        source,
                    })?
            }
            Err(source) => {
                return Err(FileBlobStoreError::Io {
                    path: temp_path,
                    source,
                });
            }
        };

        if let Err(source) = file.write_all(bytes) {
            let _ = fs::remove_file(&temp_path);
            return Err(FileBlobStoreError::Io {
                path: temp_path,
                source,
            });
        }
        if let Err(source) = file.sync_all() {
            let _ = fs::remove_file(&temp_path);
            return Err(FileBlobStoreError::Io {
                path: temp_path,
                source,
            });
        }
        drop(file);

        if let Err(source) = fs::rename(&temp_path, &path) {
            let _ = fs::remove_file(&temp_path);
            return Err(FileBlobStoreError::Io { path, source });
        }

        Ok(reference)
    }

    fn delete_blob(&self, reference: &BlobRef) -> Result<(), Self::Error> {
        let path = self.blob_path(&reference.cid);
        match fs::remove_file(&path) {
            Ok(()) => {
                remove_empty_blob_dirs(&path, &self.namespace_dir());
                Ok(())
            }
            Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(FileBlobStoreError::Io { path, source }),
        }
    }
}

impl BlobStoreScan for FileBlobStore {
    fn list_blob_refs(&self) -> Result<Vec<BlobRef>, Self::Error> {
        let namespace = self.namespace_dir();
        if !namespace.exists() {
            return Ok(Vec::new());
        }

        let mut refs = Vec::new();
        for first in read_visible_dir_entries(&namespace)? {
            ensure_shard_dir(&first, 2)?;
            for second in read_visible_dir_entries(&first.path())? {
                ensure_shard_dir(&second, 2)?;
                for blob in read_visible_dir_entries(&second.path())? {
                    let path = blob.path();
                    let file_type = blob.file_type().map_err(|source| FileBlobStoreError::Io {
                        path: path.clone(),
                        source,
                    })?;
                    if !file_type.is_file() {
                        return Err(FileBlobStoreError::InvalidPath {
                            path,
                            message: "blob namespace entry is not a file".to_string(),
                        });
                    }

                    let name = entry_name(&blob)?;
                    let cid =
                        parse_cid_hex(&name).ok_or_else(|| FileBlobStoreError::InvalidPath {
                            path: path.clone(),
                            message: "blob filename is not a 64-byte hex CID".to_string(),
                        })?;
                    if name[0..2] != entry_name(&first)? || name[2..4] != entry_name(&second)? {
                        return Err(FileBlobStoreError::InvalidPath {
                            path,
                            message: "blob CID does not match shard directories".to_string(),
                        });
                    }

                    let len = blob
                        .metadata()
                        .map_err(|source| FileBlobStoreError::Io {
                            path: blob.path(),
                            source,
                        })?
                        .len();
                    refs.push(BlobRef { cid, len });
                }
            }
        }

        sort_blob_refs(&mut refs);
        Ok(refs)
    }
}

/// Error type for [`FileBlobStore`].
#[derive(Debug)]
pub enum FileBlobStoreError {
    /// Filesystem I/O failed for the given path.
    Io {
        /// Path involved in the failed operation.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },
    /// A file's bytes do not match its content-addressed blob reference.
    InvalidBlob {
        /// Path containing the invalid blob bytes.
        path: PathBuf,
        /// Validation failure description.
        message: String,
    },
    /// The blob store namespace contains an invalid path or filename.
    InvalidPath {
        /// Invalid path.
        path: PathBuf,
        /// Validation failure description.
        message: String,
    },
}

impl std::fmt::Display for FileBlobStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(
                    f,
                    "file blob store I/O error at {}: {source}",
                    path.display()
                )
            }
            Self::InvalidBlob { path, message } => {
                write!(f, "invalid file blob at {}: {message}", path.display())
            }
            Self::InvalidPath { path, message } => {
                write!(f, "invalid file blob path {}: {message}", path.display())
            }
        }
    }
}

impl std::error::Error for FileBlobStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::InvalidBlob { .. } | Self::InvalidPath { .. } => None,
        }
    }
}

fn validate_file_blob(
    reference: &BlobRef,
    path: &Path,
    bytes: &[u8],
) -> Result<(), FileBlobStoreError> {
    reference
        .validate_bytes(bytes)
        .map_err(|err| FileBlobStoreError::InvalidBlob {
            path: path.to_path_buf(),
            message: err.to_string(),
        })
}

fn read_visible_dir_entries(path: &Path) -> Result<Vec<DirEntry>, FileBlobStoreError> {
    let mut entries = Vec::new();
    let dir = fs::read_dir(path).map_err(|source| FileBlobStoreError::Io {
        path: path.to_path_buf(),
        source,
    })?;

    for entry in dir {
        let entry = entry.map_err(|source| FileBlobStoreError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        let name = entry_name(&entry)?;
        if name.starts_with('.') {
            continue;
        }
        entries.push(entry);
    }

    entries.sort_by_key(|entry| entry.file_name());
    Ok(entries)
}

fn ensure_shard_dir(entry: &DirEntry, len: usize) -> Result<(), FileBlobStoreError> {
    let path = entry.path();
    let file_type = entry.file_type().map_err(|source| FileBlobStoreError::Io {
        path: path.clone(),
        source,
    })?;
    if !file_type.is_dir() {
        return Err(FileBlobStoreError::InvalidPath {
            path,
            message: "blob shard entry is not a directory".to_string(),
        });
    }

    let name = entry_name(entry)?;
    if name.len() != len || !name.as_bytes().iter().all(|byte| is_lower_hex_byte(*byte)) {
        return Err(FileBlobStoreError::InvalidPath {
            path,
            message: "blob shard directory is not lowercase hex".to_string(),
        });
    }

    Ok(())
}

fn entry_name(entry: &DirEntry) -> Result<String, FileBlobStoreError> {
    entry
        .file_name()
        .into_string()
        .map_err(|name| FileBlobStoreError::InvalidPath {
            path: entry.path(),
            message: format!("path is not valid UTF-8: {name:?}"),
        })
}

fn remove_empty_blob_dirs(blob_path: &Path, namespace: &Path) {
    let Some(second) = blob_path.parent() else {
        return;
    };
    let _ = fs::remove_dir(second);
    let Some(first) = second.parent() else {
        return;
    };
    if first != namespace {
        let _ = fs::remove_dir(first);
    }
}

fn sort_blob_refs(blobs: &mut [BlobRef]) {
    blobs.sort_by(|left, right| left.cid.as_bytes().cmp(right.cid.as_bytes()));
}

fn cid_hex(cid: &Cid) -> String {
    let mut out = String::with_capacity(64);
    for byte in cid.as_bytes() {
        out.push(hex_char(byte >> 4));
        out.push(hex_char(byte & 0x0f));
    }
    out
}

fn parse_cid_hex(hex: &str) -> Option<Cid> {
    if hex.len() != 64 {
        return None;
    }

    let mut bytes = [0u8; 32];
    for (idx, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_value(pair[0])?;
        let low = hex_value(pair[1])?;
        bytes[idx] = (high << 4) | low;
    }
    Some(Cid(bytes))
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => (b'0' + value) as char,
        10..=15 => (b'a' + (value - 10)) as char,
        _ => unreachable!("nibble should be <= 15"),
    }
}

fn hex_value(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

fn is_lower_hex_byte(value: u8) -> bool {
    matches!(value, b'0'..=b'9' | b'a'..=b'f')
}

pub(crate) fn encode_stored_value<B>(
    blob_store: &B,
    value: Vec<u8>,
    config: &LargeValueConfig,
) -> Result<Vec<u8>, Error>
where
    B: BlobStore,
{
    if value.len() > config.inline_threshold {
        let reference = blob_store
            .put_blob(&value)
            .map_err(|err| Error::Store(Box::new(err)))?;
        validate_blob_reference(&reference, &value)?;
        return Ok(ValueRef::Blob(reference).to_bytes());
    }

    if ValueRef::inline_requires_escape(&value) {
        Ok(ValueRef::Inline(value).to_bytes())
    } else {
        Ok(value)
    }
}

pub(crate) fn resolve_stored_value<B>(blob_store: &B, stored: &[u8]) -> Result<Vec<u8>, Error>
where
    B: BlobStore,
{
    match ValueRef::from_stored_bytes(stored)? {
        ValueRef::Inline(value) => Ok(value),
        ValueRef::Blob(reference) => {
            let bytes = blob_store
                .get_blob(&reference)
                .map_err(|err| Error::Store(Box::new(err)))?
                .ok_or_else(|| Error::NotFound(reference.cid.clone()))?;
            validate_blob_reference(&reference, &bytes)?;
            Ok(bytes)
        }
    }
}

#[cfg(feature = "async-store")]
pub(crate) async fn encode_stored_value_async<B>(
    blob_store: &B,
    value: Vec<u8>,
    config: &LargeValueConfig,
) -> Result<Vec<u8>, Error>
where
    B: AsyncBlobStore,
    B::Error: Send + Sync,
{
    if value.len() > config.inline_threshold {
        let reference = blob_store
            .put_blob(&value)
            .await
            .map_err(|err| Error::Store(Box::new(err)))?;
        validate_blob_reference(&reference, &value)?;
        return Ok(ValueRef::Blob(reference).to_bytes());
    }

    if ValueRef::inline_requires_escape(&value) {
        Ok(ValueRef::Inline(value).to_bytes())
    } else {
        Ok(value)
    }
}

#[cfg(feature = "async-store")]
pub(crate) async fn resolve_stored_value_async<B>(
    blob_store: &B,
    stored: &[u8],
) -> Result<Vec<u8>, Error>
where
    B: AsyncBlobStore,
    B::Error: Send + Sync,
{
    match ValueRef::from_stored_bytes(stored)? {
        ValueRef::Inline(value) => Ok(value),
        ValueRef::Blob(reference) => {
            let bytes = blob_store
                .get_blob(&reference)
                .await
                .map_err(|err| Error::Store(Box::new(err)))?
                .ok_or_else(|| Error::NotFound(reference.cid.clone()))?;
            validate_blob_reference(&reference, &bytes)?;
            Ok(bytes)
        }
    }
}

fn write_header(out: &mut Vec<u8>, tag: u8) {
    out.extend_from_slice(VALUE_REF_MAGIC);
    out.push(VALUE_REF_VERSION);
    out.push(tag);
}

fn decode_value_ref(bytes: &[u8]) -> Result<ValueRef, Error> {
    if bytes.len() < VALUE_REF_HEADER_LEN {
        return Err(value_ref_error("value reference header is truncated"));
    }
    if bytes[..VALUE_REF_MAGIC.len()] != VALUE_REF_MAGIC[..] {
        return Err(value_ref_error("value reference missing PLVB magic"));
    }
    if bytes[4] != VALUE_REF_VERSION {
        return Err(value_ref_error(format!(
            "unsupported value reference version {}",
            bytes[4]
        )));
    }

    match bytes[5] {
        VALUE_REF_INLINE => decode_inline_ref(bytes),
        VALUE_REF_BLOB => decode_blob_ref(bytes),
        tag => Err(value_ref_error(format!(
            "unknown value reference tag {tag}"
        ))),
    }
}

fn decode_inline_ref(bytes: &[u8]) -> Result<ValueRef, Error> {
    let mut offset = VALUE_REF_HEADER_LEN;
    let len = read_u64(bytes, &mut offset, "inline value length")? as usize;
    if bytes.len().saturating_sub(offset) != len {
        return Err(value_ref_error(
            "inline value length does not match payload",
        ));
    }
    Ok(ValueRef::Inline(bytes[offset..].to_vec()))
}

fn decode_blob_ref(bytes: &[u8]) -> Result<ValueRef, Error> {
    let expected_len = VALUE_REF_HEADER_LEN + 32 + U64_LEN;
    if bytes.len() != expected_len {
        return Err(value_ref_error("blob reference length is invalid"));
    }

    let mut cid_bytes = [0u8; 32];
    cid_bytes.copy_from_slice(&bytes[VALUE_REF_HEADER_LEN..VALUE_REF_HEADER_LEN + 32]);
    let mut offset = VALUE_REF_HEADER_LEN + 32;
    let len = read_u64(bytes, &mut offset, "blob length")?;
    Ok(ValueRef::Blob(BlobRef {
        cid: Cid(cid_bytes),
        len,
    }))
}

fn read_u64(bytes: &[u8], offset: &mut usize, context: &str) -> Result<u64, Error> {
    let end = offset.saturating_add(U64_LEN);
    let Some(raw) = bytes.get(*offset..end) else {
        return Err(value_ref_error(format!("{context} is truncated")));
    };
    *offset = end;
    Ok(u64::from_be_bytes(raw.try_into().map_err(|_| {
        value_ref_error(format!("{context} has invalid length"))
    })?))
}

fn validate_blob_reference(reference: &BlobRef, bytes: &[u8]) -> Result<(), Error> {
    if reference.len != bytes.len() as u64 {
        return Err(value_ref_error("blob length does not match reference"));
    }

    let actual = Cid::from_bytes(bytes);
    if actual != reference.cid {
        return Err(value_ref_error("blob CID does not match reference"));
    }

    Ok(())
}

fn value_ref_error(message: impl Into<String>) -> Error {
    Error::Deserialize(format!("invalid value reference: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_ref_round_trips_inline_and_blob_refs() {
        let inline = ValueRef::Inline(b"PLVB-user-data".to_vec());
        assert_eq!(
            ValueRef::from_bytes(&inline.to_bytes()).unwrap(),
            inline.clone()
        );
        assert_eq!(
            ValueRef::from_stored_bytes(&inline.to_bytes()).unwrap(),
            inline
        );

        let blob = ValueRef::Blob(BlobRef::from_bytes(b"large"));
        assert_eq!(
            ValueRef::from_bytes(&blob.to_bytes()).unwrap(),
            blob.clone()
        );
        assert_eq!(ValueRef::from_stored_bytes(&blob.to_bytes()).unwrap(), blob);
    }

    #[test]
    fn stored_value_without_envelope_decodes_as_inline() {
        assert_eq!(
            ValueRef::from_stored_bytes(b"raw").unwrap(),
            ValueRef::Inline(b"raw".to_vec())
        );
    }

    #[test]
    fn mem_blob_store_is_content_addressed_and_idempotent() {
        let store = MemBlobStore::new();
        let first = store.put_blob(b"payload").unwrap();
        let second = store.put_blob(b"payload").unwrap();

        assert_eq!(first, second);
        assert_eq!(store.len().unwrap(), 1);
        assert_eq!(store.get_blob(&first).unwrap(), Some(b"payload".to_vec()));

        store.delete_blob(&first).unwrap();
        assert_eq!(store.get_blob(&first).unwrap(), None);
    }
}
