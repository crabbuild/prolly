//! Storage backend trait and implementations for Prolly Trees

mod file;
mod memory;
pub use file::{FileNodeStore, FileNodeStoreError};
pub use memory::{MemStore, MemStoreError};

use std::collections::{hash_map::Entry, HashMap};
use std::future::Future;
use std::sync::Arc;

use super::cid::Cid;
use super::engine::ready::{ready_only, ReadyOnly};
use super::manifest::{
    AsyncManifestStore, AsyncManifestStoreScan, ManifestStore, ManifestStoreScan, ManifestUpdate,
    NamedRootManifest, RootManifest,
};

pub(crate) struct OrderedBatchReadPlan<'a> {
    unique_keys: Vec<&'a [u8]>,
    positions: Option<Vec<usize>>,
}

impl<'a> OrderedBatchReadPlan<'a> {
    pub(crate) fn new(keys: &[&'a [u8]]) -> Self {
        if keys.len() < 2 {
            return Self {
                unique_keys: keys.to_vec(),
                positions: None,
            };
        }

        let mut unique_indexes = HashMap::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        let mut positions: Option<Vec<usize>> = None;

        for key in keys {
            match unique_indexes.entry(*key) {
                Entry::Occupied(entry) => {
                    let positions =
                        positions.get_or_insert_with(|| (0..unique_keys.len()).collect());
                    positions.push(*entry.get());
                }
                Entry::Vacant(entry) => {
                    let unique_idx = unique_keys.len();
                    unique_keys.push(*key);
                    if let Some(positions) = positions.as_mut() {
                        positions.push(unique_idx);
                    }
                    entry.insert(unique_idx);
                }
            }
        }

        Self {
            unique_keys,
            positions,
        }
    }

    pub(crate) fn unique_keys(&self) -> &[&'a [u8]] {
        &self.unique_keys
    }

    #[cfg(test)]
    pub(crate) fn is_identity(&self) -> bool {
        self.positions.is_none()
    }

    #[cfg(test)]
    pub(crate) fn expand<T: Clone>(&self, unique_values: &[Option<T>]) -> Vec<Option<T>> {
        debug_assert_eq!(self.unique_keys.len(), unique_values.len());
        match &self.positions {
            Some(positions) => positions
                .iter()
                .map(|&unique_idx| unique_values[unique_idx].clone())
                .collect(),
            None => unique_values.to_vec(),
        }
    }

    pub(crate) fn expand_owned<T: Clone>(&self, unique_values: Vec<Option<T>>) -> Vec<Option<T>> {
        debug_assert_eq!(self.unique_keys.len(), unique_values.len());
        match &self.positions {
            Some(positions) => positions
                .iter()
                .map(|&unique_idx| unique_values[unique_idx].clone())
                .collect(),
            None => unique_values,
        }
    }
}

/// Batch operation for atomic writes
#[derive(Debug, Clone)]
pub enum BatchOp<'a> {
    /// Insert or update a key-value pair
    Upsert { key: &'a [u8], value: &'a [u8] },
    /// Delete a key
    Delete { key: &'a [u8] },
}

/// Logical source of an immutable-node publication.
///
/// The origin is an advisory, runtime-only optimization signal. Stores must
/// use [`PublicationOrigin::General`] behavior for variants they do not
/// recognize, and no origin may weaken correctness, atomicity, durability, or
/// visibility guarantees.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum PublicationOrigin {
    /// No more specific publication context is available.
    #[default]
    General,
    /// A single logical key was inserted or updated.
    PointUpsert,
    /// A single logical key was deleted.
    PointDelete,
    /// Multiple logical mutations were applied together.
    BatchMutation,
    /// A tree was constructed or rebuilt.
    TreeBuild,
    /// Multiple trees were merged.
    Merge,
    /// A logical key range was deleted.
    RangeDelete,
    /// Existing immutable content was copied between stores.
    Replication,
    /// Derived or internal content was maintained.
    Maintenance,
}

/// Optional non-canonical metadata accompanying a node publication.
///
/// Hints may improve backend performance but are never required to read or
/// verify the published content-addressed nodes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodePublicationHint<'a> {
    namespace: &'a [u8],
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> NodePublicationHint<'a> {
    /// Construct a borrowed performance hint.
    #[inline]
    pub const fn new(namespace: &'a [u8], key: &'a [u8], value: &'a [u8]) -> Self {
        Self {
            namespace,
            key,
            value,
        }
    }

    /// Return the logical hint namespace.
    #[inline]
    pub const fn namespace(self) -> &'a [u8] {
        self.namespace
    }

    /// Return the hint key.
    #[inline]
    pub const fn key(self) -> &'a [u8] {
        self.key
    }

    /// Return the hint value.
    #[inline]
    pub const fn value(self) -> &'a [u8] {
        self.value
    }
}

/// Borrowed request to publish canonical immutable nodes.
///
/// [`NodePublication::origin`] is advisory only. A store may select a faster
/// implementation from it, but the request must retain the same node bytes,
/// hint behavior, correctness, durability, atomicity, and visibility as the
/// general publication path. Unknown origins must use that general path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NodePublication<'a> {
    entries: &'a [(&'a [u8], &'a [u8])],
    hint: Option<NodePublicationHint<'a>>,
    origin: PublicationOrigin,
}

impl<'a> NodePublication<'a> {
    /// Construct a node publication without a performance hint.
    #[inline]
    pub const fn new(entries: &'a [(&'a [u8], &'a [u8])], origin: PublicationOrigin) -> Self {
        Self {
            entries,
            hint: None,
            origin,
        }
    }

    /// Construct a node publication with a performance hint.
    #[inline]
    pub const fn with_hint(
        entries: &'a [(&'a [u8], &'a [u8])],
        hint: NodePublicationHint<'a>,
        origin: PublicationOrigin,
    ) -> Self {
        Self {
            entries,
            hint: Some(hint),
            origin,
        }
    }

    /// Return the content-addressed node entries.
    #[inline]
    pub const fn entries(self) -> &'a [(&'a [u8], &'a [u8])] {
        self.entries
    }

    /// Return the optional non-canonical performance hint.
    #[inline]
    pub const fn hint(self) -> Option<NodePublicationHint<'a>> {
        self.hint
    }

    /// Return the advisory logical publication origin.
    #[inline]
    pub const fn origin(self) -> PublicationOrigin {
        self.origin
    }
}

/// Ordered results from a retained immutable-byte batch read.
pub type SharedReadBatch = Vec<Option<Arc<[u8]>>>;

/// Storage backend trait for Prolly Trees
///
/// Keys are CID bytes, values are serialized nodes.
/// Implementations must be thread-safe (Send + Sync).
pub trait Store: Send + Sync {
    /// Error type for storage operations
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get value by key
    ///
    /// Returns `Ok(Some(value))` if key exists, `Ok(None)` if not found,
    /// or `Err` on storage failure.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Get immutable shared bytes without requiring the engine to copy an
    /// already-retained cache value. Stores that cannot retain values may use
    /// the default owned-read adapter.
    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        self.get(key)
            .map(|value| value.map(|bytes| Arc::from(bytes.into_boxed_slice())))
    }

    /// Retrieve unique keys in order as retained immutable byte buffers.
    fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<SharedReadBatch, Self::Error> {
        self.batch_get_ordered_unique(keys).map(|values| {
            values
                .into_iter()
                .map(|value| value.map(|bytes| Arc::from(bytes.into_boxed_slice())))
                .collect()
        })
    }

    /// Whether shared reads return an already-retained immutable allocation.
    fn has_native_shared_reads(&self) -> bool {
        false
    }

    /// Store key-value pair
    ///
    /// Inserts or updates the value for the given key.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    /// Delete key
    ///
    /// Removes the key if it exists. No error if key doesn't exist.
    fn delete(&self, key: &[u8]) -> Result<(), Self::Error>;

    /// Batch write operations (atomic if supported by backend)
    ///
    /// Applies all operations in the batch. Implementations should
    /// attempt to make this atomic when possible.
    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error>;

    /// Retrieve multiple keys in a single operation
    ///
    /// Returns a HashMap mapping each requested key to its value (if found).
    /// Keys that don't exist are simply not included in the result.
    ///
    /// The default implementation uses sequential gets, but implementations
    /// can override this for better performance.
    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let plan = OrderedBatchReadPlan::new(keys);
        let mut results = HashMap::with_capacity(plan.unique_keys().len());
        for key in plan.unique_keys() {
            if let Some(value) = self.get(key)? {
                results.insert(key.to_vec(), value);
            }
        }
        Ok(results)
    }

    /// Retrieve multiple keys in a single operation with order preservation
    ///
    /// Returns a Vec of `Option<Vec<u8>>` in the same order as the input keys.
    /// Each element is `Some(value)` if the key exists, or `None` if not found.
    ///
    /// This method is useful when the order of results must match the order
    /// of input keys, such as when prefetching nodes for batch operations.
    ///
    /// The default implementation uses sequential gets, but implementations
    /// with parallel I/O capabilities (e.g., network stores) can override
    /// this for better performance.
    ///
    /// # Arguments
    /// * `keys` - Slice of keys to retrieve
    ///
    /// # Returns
    /// Vector of `Option<Vec<u8>>` in the same order as input keys.
    /// `None` indicates the key was not found.
    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        if keys.len() < 2 {
            return keys.iter().map(|key| self.get(key)).collect();
        }

        let plan = OrderedBatchReadPlan::new(keys);
        let mut unique_values = Vec::with_capacity(plan.unique_keys().len());
        for key in plan.unique_keys() {
            unique_values.push(self.get(key)?);
        }
        Ok(plan.expand_owned(unique_values))
    }

    /// Retrieve unique keys in input order.
    ///
    /// This is a fast path for callers that have already deduplicated keys and
    /// still need order preservation. The default keeps efficient custom
    /// `batch_get_ordered` implementations for stores that prefer batched
    /// reads, while avoiding duplicate-planning overhead for point-read stores.
    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        if !self.prefers_batch_reads() {
            return keys.iter().map(|key| self.get(key)).collect();
        }

        self.batch_get_ordered(keys)
    }

    /// Whether this store has an efficient batched-read implementation.
    ///
    /// The prolly engine uses this to decide whether to prefetch many tree
    /// paths through `batch_get_ordered`. Stores that implement true multi-get,
    /// request coalescing, or parallel remote reads should return `true`.
    fn prefers_batch_reads(&self) -> bool {
        false
    }

    /// Store multiple key-value pairs in a single operation
    ///
    /// Writes all entries atomically when possible. The default implementation
    /// uses the existing batch method with Upsert operations.
    ///
    /// Implementations can override this for better performance.
    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let ops: Vec<BatchOp> = entries
            .iter()
            .map(|(k, v)| BatchOp::Upsert { key: k, value: v })
            .collect();
        self.batch(&ops)
    }

    /// Whether this store persists performance hints.
    fn supports_hints(&self) -> bool {
        false
    }

    /// Whether append-heavy writes should maintain the engine's rightmost-path hint.
    ///
    /// This is a measured performance preference, not a correctness capability.
    /// Stores should opt in only when they also support hints and path hydration
    /// saves more work than persisting one hint for each appended tree root.
    fn prefers_rightmost_path_hints(&self) -> bool {
        false
    }

    /// Retrieve an optional performance hint for a logical namespace and key.
    ///
    /// Hints are not part of the content-addressed tree semantics. Store
    /// implementations may ignore them and return `None`; callers must always
    /// have a correct fallback path.
    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let _ = (namespace, key);
        Ok(None)
    }

    /// Persist an optional performance hint for a logical namespace and key.
    ///
    /// The default implementation is a no-op so custom stores remain compatible.
    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let _ = (namespace, key, value);
        Ok(())
    }

    /// Store content-addressed nodes and one hint atomically when supported.
    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.batch_put(entries)?;
        self.put_hint(namespace, key, value)
    }

    /// Publish canonical immutable nodes with advisory logical context.
    ///
    /// The default preserves existing batch and optional-hint behavior. Store
    /// overrides may optimize by origin only when all general-path guarantees
    /// remain unchanged.
    #[inline]
    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        match publication.hint() {
            Some(hint) => self.batch_put_with_hint(
                publication.entries(),
                hint.namespace(),
                hint.key(),
                hint.value(),
            ),
            None => self.batch_put(publication.entries()),
        }
    }
}

/// Storage backends that can enumerate content-addressed node CIDs.
///
/// This trait is separate from [`Store`] so simple point-read stores do not
/// need to expose backend-wide scans. Implementations must return only node
/// CIDs, not performance hints, root manifests, or other metadata keys.
pub trait NodeStoreScan: Send + Sync {
    /// Error type for scan operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// List all content-addressed node CIDs currently known to the store.
    ///
    /// The returned CIDs should be sorted by raw CID bytes for deterministic GC
    /// planning. Implementations should return an error if the node namespace
    /// contains a malformed non-CID key.
    fn list_node_cids(&self) -> Result<Vec<Cid>, Self::Error>;
}

impl<T: NodeStoreScan> NodeStoreScan for Arc<T> {
    type Error = T::Error;

    fn list_node_cids(&self) -> Result<Vec<Cid>, Self::Error> {
        (**self).list_node_cids()
    }
}

pub(crate) fn cid_from_store_key(key: &[u8], context: impl AsRef<str>) -> Result<Cid, String> {
    let context = context.as_ref();
    if key.len() != 32 {
        return Err(format!(
            "{context} key has invalid CID length {}, expected 32",
            key.len()
        ));
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(key);
    Ok(Cid(bytes))
}

pub(crate) fn sort_cids(cids: &mut [Cid]) {
    cids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
}

/// Async storage backend trait for Prolly Trees.
///
/// This trait mirrors [`Store`] for remote, browser, object-store, and
/// background-agent workloads. It is part of the runtime-neutral core and
/// intentionally does not require a Tokio dependency.
///
/// The base trait does not require `Send` or `Sync` so single-threaded browser
/// stores can implement it. Async managers or native backends may add stronger
/// bounds when they need cross-thread execution.
#[allow(async_fn_in_trait)]
pub trait AsyncStore {
    /// Error type for storage operations.
    type Error: std::error::Error + 'static;

    /// Get value by key.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Async immutable shared-byte read.
    async fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        self.get(key)
            .await
            .map(|value| value.map(|bytes| Arc::from(bytes.into_boxed_slice())))
    }

    /// Async ordered retained-byte batch read for unique keys.
    async fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<SharedReadBatch, Self::Error> {
        self.batch_get_ordered_unique(keys).await.map(|values| {
            values
                .into_iter()
                .map(|value| value.map(|bytes| Arc::from(bytes.into_boxed_slice())))
                .collect()
        })
    }

    /// Whether async shared reads retain backend-owned immutable allocations.
    fn has_native_shared_reads(&self) -> bool {
        false
    }

    /// Store key-value pair.
    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    /// Delete key.
    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error>;

    /// Batch write operations.
    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error>;

    /// Retrieve multiple keys as a map.
    ///
    /// The default implementation uses [`AsyncStore::batch_get_ordered`] and
    /// returns only found keys.
    async fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let ordered = self.batch_get_ordered(keys).await?;
        let mut results = HashMap::with_capacity(ordered.len());
        for (key, value) in keys.iter().zip(ordered) {
            if let Some(value) = value {
                results.insert((*key).to_vec(), value);
            }
        }
        Ok(results)
    }

    /// Retrieve multiple keys while preserving request order.
    ///
    /// The default implementation deduplicates repeated keys, performs point
    /// reads, and expands results back to the original request order. If
    /// [`AsyncStore::read_parallelism`] is greater than one, point reads are
    /// overlapped up to that limit.
    async fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        async_batch_get_ordered_with_limit(self, keys, self.read_parallelism()).await
    }

    /// Retrieve unique keys in request order.
    ///
    /// Callers use this when they have already deduplicated keys. The default
    /// implementation avoids duplicate planning and still respects
    /// [`AsyncStore::read_parallelism`].
    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        async_batch_get_ordered_unique_with_limit(self, keys, self.read_parallelism()).await
    }

    /// Whether this store has an efficient native batched-read implementation.
    fn prefers_batch_reads(&self) -> bool {
        false
    }

    /// Maximum in-flight point reads for default ordered batch reads.
    ///
    /// Stores with true native multi-get should override
    /// [`AsyncStore::batch_get_ordered`] directly. Stores that only have async
    /// point reads can return a value greater than one here to overlap fetches.
    fn read_parallelism(&self) -> usize {
        1
    }

    /// Store multiple key-value pairs.
    async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let ops: Vec<BatchOp<'_>> = entries
            .iter()
            .map(|(key, value)| BatchOp::Upsert { key, value })
            .collect();
        self.batch(&ops).await
    }

    /// Whether this store persists performance hints.
    fn supports_hints(&self) -> bool {
        false
    }

    /// Whether append-heavy writes should maintain the engine's rightmost-path hint.
    ///
    /// This is a measured performance preference, not a correctness capability.
    /// Stores should opt in only when they also support hints and path hydration
    /// saves more work than persisting one hint for each appended tree root.
    fn prefers_rightmost_path_hints(&self) -> bool {
        false
    }

    /// Retrieve an optional performance hint.
    async fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let _ = (namespace, key);
        Ok(None)
    }

    /// Persist an optional performance hint.
    async fn put_hint(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        let _ = (namespace, key, value);
        Ok(())
    }

    /// Store content-addressed nodes and one hint atomically when supported.
    async fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.batch_put(entries).await?;
        self.put_hint(namespace, key, value).await
    }

    /// Publish canonical immutable nodes with advisory logical context.
    ///
    /// The default preserves existing async batch and optional-hint behavior.
    /// Store overrides may optimize by origin only when all general-path
    /// guarantees remain unchanged.
    #[inline]
    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        match publication.hint() {
            Some(hint) => {
                self.batch_put_with_hint(
                    publication.entries(),
                    hint.namespace(),
                    hint.key(),
                    hint.value(),
                )
                .await
            }
            None => self.batch_put(publication.entries()).await,
        }
    }
}
async fn async_batch_get_ordered_with_limit<S: AsyncStore + ?Sized>(
    store: &S,
    keys: &[&[u8]],
    max_in_flight: usize,
) -> Result<Vec<Option<Vec<u8>>>, S::Error> {
    if keys.len() < 2 {
        return async_batch_get_ordered_unique_with_limit(store, keys, max_in_flight).await;
    }

    let plan = OrderedBatchReadPlan::new(keys);
    let unique_values =
        async_batch_get_ordered_unique_with_limit(store, plan.unique_keys(), max_in_flight).await?;
    Ok(plan.expand_owned(unique_values))
}
async fn async_batch_get_ordered_unique_with_limit<S: AsyncStore + ?Sized>(
    store: &S,
    keys: &[&[u8]],
    max_in_flight: usize,
) -> Result<Vec<Option<Vec<u8>>>, S::Error> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let max_in_flight = max_in_flight.max(1);
    if keys.len() < 2 || max_in_flight == 1 {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(store.get(key).await?);
        }
        return Ok(values);
    }

    use futures_util::stream::{FuturesUnordered, StreamExt as _};

    let mut values = vec![None; keys.len()];
    let mut next_idx = 0;
    let mut in_flight = FuturesUnordered::new();

    while next_idx < keys.len() && in_flight.len() < max_in_flight {
        in_flight.push(async_get_indexed(store, next_idx, keys[next_idx]));
        next_idx += 1;
    }

    while let Some((idx, result)) = in_flight.next().await {
        values[idx] = result?;

        if next_idx < keys.len() {
            in_flight.push(async_get_indexed(store, next_idx, keys[next_idx]));
            next_idx += 1;
        }
    }

    Ok(values)
}
async fn async_get_indexed<S: AsyncStore + ?Sized>(
    store: &S,
    idx: usize,
    key: &[u8],
) -> (usize, Result<Option<Vec<u8>>, S::Error>) {
    (idx, store.get(key).await)
}

/// Adapter that exposes an existing synchronous [`Store`] as an [`AsyncStore`].
///
/// This adapter calls the synchronous store directly and does not spawn
/// blocking work. Runtime-specific `spawn_blocking` adapters can be layered on
/// top by applications that need them.
///
/// Arbitrary futures cannot opt into the internal ready-only contract:
///
/// ```compile_fail
/// use prolly::{MemStore, SyncStoreAsAsync};
///
/// let adapter = SyncStoreAsAsync::new(MemStore::new());
/// adapter.ready(std::future::pending::<()>());
/// ```
#[derive(Clone, Debug)]
pub struct SyncStoreAsAsync<S> {
    inner: S,
}
impl<S> SyncStoreAsAsync<S> {
    /// Create a new adapter.
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    /// Borrow the wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Consume the adapter and return the wrapped store.
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Mark an engine operation over this adapter as ready-only.
    pub(crate) fn ready<F: Future>(&self, future: F) -> ReadyOnly<F> {
        ready_only(future)
    }
}
impl<S: Store> AsyncStore for SyncStoreAsAsync<S> {
    type Error = S::Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key)
    }

    async fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        self.inner.get_shared(key)
    }

    async fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<SharedReadBatch, Self::Error> {
        self.inner.batch_get_shared_ordered_unique(keys)
    }

    fn has_native_shared_reads(&self) -> bool {
        self.inner.has_native_shared_reads()
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put(key, value)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(ops)
    }

    async fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        self.inner.batch_get(keys)
    }

    async fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.inner.batch_get_ordered(keys)
    }

    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.inner.batch_get_ordered_unique(keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        self.inner.prefers_batch_reads()
    }

    async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.inner.batch_put(entries)
    }

    fn supports_hints(&self) -> bool {
        self.inner.supports_hints()
    }

    fn prefers_rightmost_path_hints(&self) -> bool {
        self.inner.prefers_rightmost_path_hints()
    }

    async fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get_hint(namespace, key)
    }

    async fn put_hint(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.inner.put_hint(namespace, key, value)
    }

    async fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.inner
            .batch_put_with_hint(entries, namespace, key, value)
    }

    #[inline]
    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.inner.publish_nodes(publication)
    }
}
impl<S: ManifestStore> AsyncManifestStore for SyncStoreAsAsync<S> {
    type Error = S::Error;

    async fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        self.inner.get_root(name)
    }

    async fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        self.inner.put_root(name, manifest)
    }

    async fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete_root(name)
    }

    async fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        self.inner.compare_and_swap_root(name, expected, new)
    }
}
impl<S: ManifestStoreScan> AsyncManifestStoreScan for SyncStoreAsAsync<S> {
    async fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        self.inner.list_roots()
    }
}

/// Error returned by [`TokioBlockingStore`].
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub enum TokioBlockingStoreError<E> {
    /// The wrapped synchronous store returned an error.
    Store(E),
    /// Tokio failed to complete the blocking task, usually because it panicked
    /// or the runtime is shutting down.
    Join(tokio::task::JoinError),
}

#[cfg(feature = "tokio")]
impl<E: std::fmt::Display> std::fmt::Display for TokioBlockingStoreError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(err) => write!(f, "store error: {err}"),
            Self::Join(err) => write!(f, "tokio blocking task failed: {err}"),
        }
    }
}

#[cfg(feature = "tokio")]
impl<E> std::error::Error for TokioBlockingStoreError<E>
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

/// Tokio-backed adapter that exposes a blocking [`Store`] as an [`AsyncStore`].
///
/// Unlike [`SyncStoreAsAsync`], this adapter runs each synchronous store
/// operation on Tokio's blocking thread pool with `spawn_blocking`. Use it when
/// an async application needs to use a blocking backend such as SQLite or
/// RocksDB without stalling async worker threads.
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub struct TokioBlockingStore<S> {
    inner: std::sync::Arc<S>,
}

#[cfg(feature = "tokio")]
impl<S> Clone for TokioBlockingStore<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[cfg(feature = "tokio")]
impl<S> TokioBlockingStore<S> {
    /// Create an adapter from an owned store.
    pub fn new(inner: S) -> Self {
        Self {
            inner: std::sync::Arc::new(inner),
        }
    }

    /// Create an adapter from an already shared store.
    pub fn from_arc(inner: std::sync::Arc<S>) -> Self {
        Self { inner }
    }

    /// Borrow the wrapped store.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Clone the shared wrapped store handle.
    pub fn shared(&self) -> std::sync::Arc<S> {
        self.inner.clone()
    }
}

#[cfg(feature = "tokio")]
async fn spawn_store_blocking<S, F, R>(
    store: std::sync::Arc<S>,
    operation: F,
) -> Result<R, TokioBlockingStoreError<S::Error>>
where
    S: Store + 'static,
    F: FnOnce(std::sync::Arc<S>) -> Result<R, S::Error> + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(move || operation(store))
        .await
        .map_err(TokioBlockingStoreError::Join)?
        .map_err(TokioBlockingStoreError::Store)
}

#[cfg(feature = "tokio")]
impl<S> AsyncStore for TokioBlockingStore<S>
where
    S: Store + 'static,
{
    type Error = TokioBlockingStoreError<S::Error>;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let key = key.to_vec();
        spawn_store_blocking(self.inner.clone(), move |store| store.get(&key)).await
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let key = key.to_vec();
        let value = value.to_vec();
        spawn_store_blocking(self.inner.clone(), move |store| store.put(&key, &value)).await
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        let key = key.to_vec();
        spawn_store_blocking(self.inner.clone(), move |store| store.delete(&key)).await
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        let owned_ops = ops
            .iter()
            .map(|op| match op {
                BatchOp::Upsert { key, value } => (true, key.to_vec(), value.to_vec()),
                BatchOp::Delete { key } => (false, key.to_vec(), Vec::new()),
            })
            .collect::<Vec<_>>();

        spawn_store_blocking(self.inner.clone(), move |store| {
            let ops = owned_ops
                .iter()
                .map(|(is_upsert, key, value)| {
                    if *is_upsert {
                        BatchOp::Upsert {
                            key: key.as_slice(),
                            value: value.as_slice(),
                        }
                    } else {
                        BatchOp::Delete {
                            key: key.as_slice(),
                        }
                    }
                })
                .collect::<Vec<_>>();
            store.batch(&ops)
        })
        .await
    }

    async fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let keys = keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>();
        spawn_store_blocking(self.inner.clone(), move |store| {
            let keys = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
            store.batch_get(&keys)
        })
        .await
    }

    async fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let keys = keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>();
        spawn_store_blocking(self.inner.clone(), move |store| {
            let keys = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
            store.batch_get_ordered(&keys)
        })
        .await
    }

    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let keys = keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>();
        spawn_store_blocking(self.inner.clone(), move |store| {
            let keys = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
            store.batch_get_ordered_unique(&keys)
        })
        .await
    }

    fn prefers_batch_reads(&self) -> bool {
        self.inner.prefers_batch_reads()
    }

    async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let entries = entries
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect::<Vec<_>>();
        spawn_store_blocking(self.inner.clone(), move |store| {
            let entries = entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>();
            store.batch_put(&entries)
        })
        .await
    }

    fn supports_hints(&self) -> bool {
        self.inner.supports_hints()
    }

    fn prefers_rightmost_path_hints(&self) -> bool {
        self.inner.prefers_rightmost_path_hints()
    }

    async fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let namespace = namespace.to_vec();
        let key = key.to_vec();
        spawn_store_blocking(self.inner.clone(), move |store| {
            store.get_hint(&namespace, &key)
        })
        .await
    }

    async fn put_hint(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        let namespace = namespace.to_vec();
        let key = key.to_vec();
        let value = value.to_vec();
        spawn_store_blocking(self.inner.clone(), move |store| {
            store.put_hint(&namespace, &key, &value)
        })
        .await
    }

    async fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        let entries = entries
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect::<Vec<_>>();
        let namespace = namespace.to_vec();
        let key = key.to_vec();
        let value = value.to_vec();
        spawn_store_blocking(self.inner.clone(), move |store| {
            let entries = entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>();
            store.batch_put_with_hint(&entries, &namespace, &key, &value)
        })
        .await
    }

    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        let entries = publication
            .entries()
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect::<Vec<_>>();
        let hint = publication.hint().map(|hint| {
            (
                hint.namespace().to_vec(),
                hint.key().to_vec(),
                hint.value().to_vec(),
            )
        });
        let origin = publication.origin();

        spawn_store_blocking(self.inner.clone(), move |store| {
            let entries = entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>();
            let publication = match hint.as_ref() {
                Some((namespace, key, value)) => NodePublication::with_hint(
                    &entries,
                    NodePublicationHint::new(namespace, key, value),
                    origin,
                ),
                None => NodePublication::new(&entries, origin),
            };
            store.publish_nodes(publication)
        })
        .await
    }
}

#[cfg(feature = "tokio")]
async fn spawn_manifest_blocking<S, F, R>(
    store: std::sync::Arc<S>,
    operation: F,
) -> Result<R, TokioBlockingStoreError<<S as ManifestStore>::Error>>
where
    S: ManifestStore + 'static,
    F: FnOnce(std::sync::Arc<S>) -> Result<R, <S as ManifestStore>::Error> + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(move || operation(store))
        .await
        .map_err(TokioBlockingStoreError::Join)?
        .map_err(TokioBlockingStoreError::Store)
}

#[cfg(feature = "tokio")]
impl<S> AsyncManifestStore for TokioBlockingStore<S>
where
    S: ManifestStore + 'static,
{
    type Error = TokioBlockingStoreError<<S as ManifestStore>::Error>;

    async fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        let name = name.to_vec();
        spawn_manifest_blocking(self.inner.clone(), move |store| store.get_root(&name)).await
    }

    async fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        let name = name.to_vec();
        let manifest = manifest.clone();
        spawn_manifest_blocking(self.inner.clone(), move |store| {
            store.put_root(&name, &manifest)
        })
        .await
    }

    async fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        let name = name.to_vec();
        spawn_manifest_blocking(self.inner.clone(), move |store| store.delete_root(&name)).await
    }

    async fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let name = name.to_vec();
        let expected = expected.cloned();
        let new = new.cloned();
        spawn_manifest_blocking(self.inner.clone(), move |store| {
            store.compare_and_swap_root(&name, expected.as_ref(), new.as_ref())
        })
        .await
    }
}

#[cfg(feature = "tokio")]
impl<S> AsyncManifestStoreScan for TokioBlockingStore<S>
where
    S: ManifestStoreScan + 'static,
{
    async fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        spawn_manifest_blocking(self.inner.clone(), move |store| store.list_roots()).await
    }
}

/// Implement Store for `Arc<T>` where T: Store
/// This allows sharing a store between multiple Prolly instances
impl<T: Store> Store for std::sync::Arc<T> {
    type Error = T::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get(key)
    }

    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        (**self).get_shared(key)
    }

    fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<SharedReadBatch, Self::Error> {
        (**self).batch_get_shared_ordered_unique(keys)
    }

    fn has_native_shared_reads(&self) -> bool {
        (**self).has_native_shared_reads()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        (**self).put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        (**self).delete(key)
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        (**self).batch(ops)
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        (**self).batch_get(keys)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        (**self).batch_get_ordered(keys)
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        (**self).batch_get_ordered_unique(keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        (**self).prefers_batch_reads()
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        (**self).batch_put(entries)
    }

    fn supports_hints(&self) -> bool {
        (**self).supports_hints()
    }

    fn prefers_rightmost_path_hints(&self) -> bool {
        (**self).prefers_rightmost_path_hints()
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get_hint(namespace, key)
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        (**self).put_hint(namespace, key, value)
    }

    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        (**self).batch_put_with_hint(entries, namespace, key, value)
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        (**self).publish_nodes(publication)
    }
}

/// Implement `Store` for shared references.
///
/// This lets short-lived managers reuse an existing store without requiring
/// ownership or an `Arc`, while preserving backend-specific batch and hint
/// behavior instead of falling back to the trait defaults.
impl<T: Store + ?Sized> Store for &T {
    type Error = T::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get(key)
    }

    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        (**self).get_shared(key)
    }

    fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<SharedReadBatch, Self::Error> {
        (**self).batch_get_shared_ordered_unique(keys)
    }

    fn has_native_shared_reads(&self) -> bool {
        (**self).has_native_shared_reads()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        (**self).put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        (**self).delete(key)
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        (**self).batch(ops)
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        (**self).batch_get(keys)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        (**self).batch_get_ordered(keys)
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        (**self).batch_get_ordered_unique(keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        (**self).prefers_batch_reads()
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        (**self).batch_put(entries)
    }

    fn supports_hints(&self) -> bool {
        (**self).supports_hints()
    }

    fn prefers_rightmost_path_hints(&self) -> bool {
        (**self).prefers_rightmost_path_hints()
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get_hint(namespace, key)
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        (**self).put_hint(namespace, key, value)
    }

    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        (**self).batch_put_with_hint(entries, namespace, key, value)
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        (**self).publish_nodes(publication)
    }
}
impl<T: AsyncStore> AsyncStore for std::sync::Arc<T> {
    type Error = T::Error;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get(key).await
    }

    async fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        (**self).get_shared(key).await
    }

    async fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<SharedReadBatch, Self::Error> {
        (**self).batch_get_shared_ordered_unique(keys).await
    }

    fn has_native_shared_reads(&self) -> bool {
        (**self).has_native_shared_reads()
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        (**self).put(key, value).await
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        (**self).delete(key).await
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        (**self).batch(ops).await
    }

    async fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        (**self).batch_get(keys).await
    }

    async fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        (**self).batch_get_ordered(keys).await
    }

    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        (**self).batch_get_ordered_unique(keys).await
    }

    fn prefers_batch_reads(&self) -> bool {
        (**self).prefers_batch_reads()
    }

    fn read_parallelism(&self) -> usize {
        (**self).read_parallelism()
    }

    async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        (**self).batch_put(entries).await
    }

    fn supports_hints(&self) -> bool {
        (**self).supports_hints()
    }

    fn prefers_rightmost_path_hints(&self) -> bool {
        (**self).prefers_rightmost_path_hints()
    }

    async fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get_hint(namespace, key).await
    }

    async fn put_hint(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        (**self).put_hint(namespace, key, value).await
    }

    async fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        (**self)
            .batch_put_with_hint(entries, namespace, key, value)
            .await
    }

    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        (**self).publish_nodes(publication).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    };

    #[derive(Debug)]
    struct DefaultReadStoreError;

    impl std::fmt::Display for DefaultReadStoreError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("default read store error")
        }
    }

    impl std::error::Error for DefaultReadStoreError {}

    #[derive(Default)]
    struct DefaultReadStore {
        data: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        get_calls: AtomicUsize,
    }

    impl DefaultReadStore {
        fn with_entries(entries: &[(&[u8], &[u8])]) -> Self {
            let mut data = BTreeMap::new();
            for (key, value) in entries {
                data.insert(key.to_vec(), value.to_vec());
            }

            Self {
                data: Mutex::new(data),
                get_calls: AtomicUsize::new(0),
            }
        }
    }

    impl Store for DefaultReadStore {
        type Error = DefaultReadStoreError;

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self.data.lock().unwrap().get(key).cloned())
        }

        fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.data
                .lock()
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
            self.data.lock().unwrap().remove(key);
            Ok(())
        }

        fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
            let mut data = self.data.lock().unwrap();
            for op in ops {
                match op {
                    BatchOp::Upsert { key, value } => {
                        data.insert(key.to_vec(), value.to_vec());
                    }
                    BatchOp::Delete { key } => {
                        data.remove(*key);
                    }
                }
            }
            Ok(())
        }
    }

    type OwnedNodeEntry = (Vec<u8>, Vec<u8>);
    type OwnedPublication = Vec<OwnedNodeEntry>;
    type OwnedPublicationHint = (Vec<u8>, Vec<u8>, Vec<u8>);

    #[derive(Default)]
    struct DefaultPublicationStore {
        batch_calls: AtomicUsize,
        hinted_batch_calls: AtomicUsize,
        published_entries: Mutex<Vec<OwnedPublication>>,
        last_hint: Mutex<Option<OwnedPublicationHint>>,
    }

    impl DefaultPublicationStore {
        fn record_entries(&self, entries: &[(&[u8], &[u8])]) {
            self.published_entries.lock().unwrap().push(
                entries
                    .iter()
                    .map(|(key, value)| (key.to_vec(), value.to_vec()))
                    .collect(),
            );
        }
    }

    impl Store for DefaultPublicationStore {
        type Error = DefaultReadStoreError;

        fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(None)
        }

        fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
            Ok(())
        }

        fn delete(&self, _key: &[u8]) -> Result<(), Self::Error> {
            Ok(())
        }

        fn batch(&self, _ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
            Ok(())
        }

        fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            self.batch_calls.fetch_add(1, Ordering::Relaxed);
            self.record_entries(entries);
            Ok(())
        }

        fn batch_put_with_hint(
            &self,
            entries: &[(&[u8], &[u8])],
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            self.hinted_batch_calls.fetch_add(1, Ordering::Relaxed);
            self.record_entries(entries);
            *self.last_hint.lock().unwrap() =
                Some((namespace.to_vec(), key.to_vec(), value.to_vec()));
            Ok(())
        }
    }

    #[derive(Default)]
    struct ForwardingPublicationStore {
        publication_calls: AtomicUsize,
        last_origin: Mutex<Option<PublicationOrigin>>,
    }

    impl ForwardingPublicationStore {
        fn record_publication(&self, publication: NodePublication<'_>) {
            self.publication_calls.fetch_add(1, Ordering::Relaxed);
            *self.last_origin.lock().unwrap() = Some(publication.origin());
        }
    }

    impl Store for ForwardingPublicationStore {
        type Error = DefaultReadStoreError;

        fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(None)
        }

        fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
            Ok(())
        }

        fn delete(&self, _key: &[u8]) -> Result<(), Self::Error> {
            Ok(())
        }

        fn batch(&self, _ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
            Ok(())
        }

        fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
            self.record_publication(publication);
            Ok(())
        }
    }

    impl AsyncStore for ForwardingPublicationStore {
        type Error = DefaultReadStoreError;

        async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(None)
        }

        async fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn delete(&self, _key: &[u8]) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn batch(&self, _ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
            Ok(())
        }

        async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
            self.record_publication(publication);
            Ok(())
        }
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut future = Box::pin(future);

        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }
    struct YieldOnce {
        yielded: bool,
    }
    impl Future for YieldOnce {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.yielded {
                Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    struct DefaultAsyncReadStore {
        data: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        get_calls: AtomicUsize,
        in_flight: AtomicUsize,
        max_in_flight: AtomicUsize,
        read_parallelism: usize,
    }
    impl DefaultAsyncReadStore {
        fn with_entries(read_parallelism: usize, entries: &[(&[u8], &[u8])]) -> Self {
            let mut data = BTreeMap::new();
            for (key, value) in entries {
                data.insert(key.to_vec(), value.to_vec());
            }

            Self {
                data: Mutex::new(data),
                get_calls: AtomicUsize::new(0),
                in_flight: AtomicUsize::new(0),
                max_in_flight: AtomicUsize::new(0),
                read_parallelism,
            }
        }
    }
    impl AsyncStore for DefaultAsyncReadStore {
        type Error = DefaultReadStoreError;

        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.get_calls.fetch_add(1, Ordering::Relaxed);
            let current = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            self.max_in_flight.fetch_max(current, Ordering::Relaxed);

            YieldOnce { yielded: false }.await;

            let value = self.data.lock().unwrap().get(key).cloned();
            self.in_flight.fetch_sub(1, Ordering::Relaxed);
            Ok(value)
        }

        async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.data
                .lock()
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
            self.data.lock().unwrap().remove(key);
            Ok(())
        }

        async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
            let mut data = self.data.lock().unwrap();
            for op in ops {
                match op {
                    BatchOp::Upsert { key, value } => {
                        data.insert(key.to_vec(), value.to_vec());
                    }
                    BatchOp::Delete { key } => {
                        data.remove(*key);
                    }
                }
            }
            Ok(())
        }

        fn read_parallelism(&self) -> usize {
            self.read_parallelism
        }
    }

    #[test]
    fn ordered_batch_read_plan_keeps_unique_batches_identity() {
        let keys: Vec<&[u8]> = vec![b"a", b"b", b"missing"];
        let plan = OrderedBatchReadPlan::new(&keys);

        assert!(plan.is_identity());
        assert_eq!(
            plan.unique_keys(),
            &[b"a".as_slice(), b"b".as_slice(), b"missing".as_slice()]
        );

        let values = vec![Some(b"1".to_vec()), Some(b"2".to_vec()), None];
        let values_ptr = values.as_ptr();
        let expanded = plan.expand_owned(values);

        assert_eq!(expanded.as_ptr(), values_ptr);
        assert_eq!(
            expanded,
            vec![Some(b"1".to_vec()), Some(b"2".to_vec()), None]
        );
    }

    #[test]
    fn publication_defaults_dispatch_once_and_preserve_borrowed_context() {
        let store = DefaultPublicationStore::default();
        let entries = [(b"node".as_slice(), b"bytes".as_slice())];
        let hint = NodePublicationHint::new(b"rightmost", b"root", b"path");

        store
            .publish_nodes(NodePublication::new(
                &entries,
                PublicationOrigin::PointUpsert,
            ))
            .unwrap();
        store
            .publish_nodes(NodePublication::with_hint(
                &entries,
                hint,
                PublicationOrigin::TreeBuild,
            ))
            .unwrap();

        assert_eq!(store.batch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(store.hinted_batch_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            *store.last_hint.lock().unwrap(),
            Some((b"rightmost".to_vec(), b"root".to_vec(), b"path".to_vec()))
        );
        assert_eq!(
            *store.published_entries.lock().unwrap(),
            vec![
                vec![(b"node".to_vec(), b"bytes".to_vec())],
                vec![(b"node".to_vec(), b"bytes".to_vec())]
            ]
        );
        assert_eq!(hint.namespace(), b"rightmost");
        assert_eq!(hint.key(), b"root");
        assert_eq!(hint.value(), b"path");
        assert!(
            std::mem::size_of::<NodePublication<'static>>() <= std::mem::size_of::<[usize; 10]>()
        );
    }

    #[test]
    fn sync_store_as_async_publication_is_ready_on_first_poll() {
        let store = SyncStoreAsAsync::new(DefaultPublicationStore::default());
        let entries = [(b"node".as_slice(), b"bytes".as_slice())];
        let publication = NodePublication::new(&entries, PublicationOrigin::PointDelete);
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(AsyncStore::publish_nodes(&store, publication));

        assert!(matches!(
            future.as_mut().poll(&mut context),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(store.inner().batch_calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sync_reference_and_arc_publications_forward_exactly_once() {
        let store = Arc::new(ForwardingPublicationStore::default());
        let entries = [(b"node".as_slice(), b"bytes".as_slice())];

        <Arc<ForwardingPublicationStore> as Store>::publish_nodes(
            &store,
            NodePublication::new(&entries, PublicationOrigin::Replication),
        )
        .unwrap();
        let borrowed = store.as_ref();
        <&ForwardingPublicationStore as Store>::publish_nodes(
            &borrowed,
            NodePublication::new(&entries, PublicationOrigin::Maintenance),
        )
        .unwrap();

        assert_eq!(store.publication_calls.load(Ordering::Relaxed), 2);
        assert_eq!(
            *store.last_origin.lock().unwrap(),
            Some(PublicationOrigin::Maintenance)
        );
    }

    #[test]
    fn async_arc_publication_forwards_exactly_once() {
        let store = Arc::new(ForwardingPublicationStore::default());
        let entries = [(b"node".as_slice(), b"bytes".as_slice())];

        block_on(
            <Arc<ForwardingPublicationStore> as AsyncStore>::publish_nodes(
                &store,
                NodePublication::new(&entries, PublicationOrigin::BatchMutation),
            ),
        )
        .unwrap();

        assert_eq!(store.publication_calls.load(Ordering::Relaxed), 1);
        assert_eq!(
            *store.last_origin.lock().unwrap(),
            Some(PublicationOrigin::BatchMutation)
        );
    }

    #[test]
    fn ordered_batch_read_plan_deduplicates_and_expands_slots() {
        let keys: Vec<&[u8]> = vec![b"c", b"a", b"c", b"missing", b"missing", b"a"];
        let plan = OrderedBatchReadPlan::new(&keys);

        assert!(!plan.is_identity());
        assert_eq!(
            plan.unique_keys(),
            &[b"c".as_slice(), b"a".as_slice(), b"missing".as_slice()]
        );
        assert_eq!(
            plan.expand(&[Some(b"3".to_vec()), Some(b"1".to_vec()), None]),
            vec![
                Some(b"3".to_vec()),
                Some(b"1".to_vec()),
                Some(b"3".to_vec()),
                None,
                None,
                Some(b"1".to_vec())
            ]
        );
        assert_eq!(
            plan.expand_owned(vec![Some(b"3".to_vec()), Some(b"1".to_vec()), None]),
            vec![
                Some(b"3".to_vec()),
                Some(b"1".to_vec()),
                Some(b"3".to_vec()),
                None,
                None,
                Some(b"1".to_vec())
            ]
        );
    }

    #[test]
    fn default_batch_get_deduplicates_duplicate_keys() {
        let store = DefaultReadStore::with_entries(&[(b"a", b"1"), (b"b", b"2")]);
        let keys: Vec<&[u8]> = vec![b"a", b"a", b"missing", b"missing", b"b"];

        let values = store.batch_get(&keys).unwrap();

        assert_eq!(values.get(b"a".as_slice()), Some(&b"1".to_vec()));
        assert_eq!(values.get(b"b".as_slice()), Some(&b"2".to_vec()));
        assert!(!values.contains_key(b"missing".as_slice()));
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            3,
            "default batch_get should point-read each unique key at most once"
        );
    }

    #[test]
    fn default_batch_get_ordered_deduplicates_while_preserving_slots() {
        let store = DefaultReadStore::with_entries(&[(b"a", b"1"), (b"b", b"2")]);
        let keys: Vec<&[u8]> = vec![b"a", b"a", b"missing", b"missing", b"b"];

        let values = store.batch_get_ordered(&keys).unwrap();

        assert_eq!(
            values,
            vec![
                Some(b"1".to_vec()),
                Some(b"1".to_vec()),
                None,
                None,
                Some(b"2".to_vec())
            ]
        );
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            3,
            "default ordered batch reads should preserve duplicate result slots without duplicate point reads"
        );
    }

    #[test]
    fn default_unique_ordered_batch_reads_preserve_order_with_point_reads() {
        let store = DefaultReadStore::with_entries(&[(b"a", b"1"), (b"b", b"2")]);
        let keys: Vec<&[u8]> = vec![b"b", b"missing", b"a"];

        let values = store.batch_get_ordered_unique(&keys).unwrap();

        assert_eq!(values, vec![Some(b"2".to_vec()), None, Some(b"1".to_vec())]);
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            3,
            "unique ordered batch reads for point-read stores should read each requested key once"
        );
    }
    #[test]
    fn async_sync_store_adapter_preserves_default_store_behavior() {
        let store = DefaultReadStore::with_entries(&[(b"a", b"1"), (b"b", b"2")]);
        let store = SyncStoreAsAsync::new(store);

        block_on(async {
            store.put(b"c", b"3").await.unwrap();
            store
                .batch_put(&[(b"d".as_slice(), b"4".as_slice())])
                .await
                .unwrap();
            store.delete(b"b").await.unwrap();

            let keys: Vec<&[u8]> = vec![b"a", b"a", b"b", b"c", b"d"];
            let values = store.batch_get_ordered(&keys).await.unwrap();
            assert_eq!(
                values,
                vec![
                    Some(b"1".to_vec()),
                    Some(b"1".to_vec()),
                    None,
                    Some(b"3".to_vec()),
                    Some(b"4".to_vec())
                ]
            );

            let mapped = store.batch_get(&keys).await.unwrap();
            assert_eq!(mapped.get(b"a".as_slice()), Some(&b"1".to_vec()));
            assert_eq!(mapped.get(b"c".as_slice()), Some(&b"3".to_vec()));
            assert_eq!(mapped.get(b"d".as_slice()), Some(&b"4".to_vec()));
            assert!(!mapped.contains_key(b"b".as_slice()));
        });
    }
    #[test]
    fn async_default_ordered_batch_reads_deduplicate_duplicate_keys() {
        let store = DefaultAsyncReadStore::with_entries(1, &[(b"a", b"1"), (b"b", b"2")]);
        let keys: Vec<&[u8]> = vec![b"a", b"a", b"missing", b"missing", b"b"];

        let values = block_on(store.batch_get_ordered(&keys)).unwrap();

        assert_eq!(
            values,
            vec![
                Some(b"1".to_vec()),
                Some(b"1".to_vec()),
                None,
                None,
                Some(b"2".to_vec())
            ]
        );
        assert_eq!(
            store.get_calls.load(Ordering::Relaxed),
            3,
            "async ordered batch reads should point-read each unique key at most once"
        );
    }
    #[test]
    fn async_default_ordered_batch_reads_respect_read_parallelism() {
        let store = DefaultAsyncReadStore::with_entries(
            2,
            &[(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")],
        );
        let keys: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];

        let values = block_on(store.batch_get_ordered(&keys)).unwrap();

        assert_eq!(
            values,
            vec![
                Some(b"1".to_vec()),
                Some(b"2".to_vec()),
                Some(b"3".to_vec()),
                Some(b"4".to_vec())
            ]
        );
        assert_eq!(store.get_calls.load(Ordering::Relaxed), 4);
        assert_eq!(
            store.max_in_flight.load(Ordering::Relaxed),
            2,
            "default async batch reads should cap concurrent point reads"
        );
    }
    #[test]
    fn arc_async_store_forwards_ordered_reads() {
        let store = std::sync::Arc::new(DefaultAsyncReadStore::with_entries(
            2,
            &[(b"a", b"1"), (b"b", b"2")],
        ));
        let keys: Vec<&[u8]> = vec![b"b", b"a"];

        let values = block_on(store.batch_get_ordered(&keys)).unwrap();

        assert_eq!(values, vec![Some(b"2".to_vec()), Some(b"1".to_vec())]);
    }
}
