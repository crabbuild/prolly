//! Optimistic MVCC transaction support.
//!
//! Transactions run the normal prolly tree engine against an overlay store. New
//! content-addressed nodes and named-root writes stay in memory until commit.
//! Stores that implement [`TransactionalStore`] can then validate named-root
//! conditions, write staged nodes, and apply root writes in one atomic backend
//! transaction.

use std::any::type_name;
use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};

use super::error::{Error, Mutation};
use super::manifest::{ManifestStore, ManifestUpdate, NamedRootUpdate, RootManifest};
use super::store::{cid_from_store_key, BatchOp, NodePublication, OrderedBatchReadPlan, Store};
use super::tree::Tree;
use super::{Cid, Config, NodeCache, Prolly, ProllyMetrics};
use {
    super::manifest::AsyncManifestStore,
    super::store::{AsyncStore, SyncStoreAsAsync},
    super::AsyncProlly,
    std::future::Future,
    std::pin::Pin,
};

/// A named-root value that must still match at transaction commit time.
#[derive(Clone, Debug, PartialEq)]
pub struct RootCondition {
    /// Durable root name.
    pub name: Vec<u8>,
    /// Manifest observed by the transaction. `None` means the root was absent.
    pub expected: Option<RootManifest>,
}

impl RootCondition {
    /// Create a root validation condition.
    pub fn new(name: Vec<u8>, expected: Option<RootManifest>) -> Self {
        Self { name, expected }
    }
}

/// A named-root write staged by a transaction.
#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum RootWrite {
    /// Insert or replace a named root manifest.
    Put {
        /// Durable root name.
        name: Vec<u8>,
        /// Manifest to store under `name`.
        manifest: RootManifest,
    },
    /// Delete a named root.
    Delete {
        /// Durable root name.
        name: Vec<u8>,
    },
}

impl RootWrite {
    /// Root name affected by this write.
    pub fn name(&self) -> &[u8] {
        match self {
            Self::Put { name, .. } | Self::Delete { name } => name,
        }
    }

    /// Replacement manifest, or `None` for a delete.
    pub fn replacement(&self) -> Option<&RootManifest> {
        match self {
            Self::Put { manifest, .. } => Some(manifest),
            Self::Delete { .. } => None,
        }
    }
}

/// A content-addressed node write staged by a transaction.
#[derive(Clone, Debug, PartialEq)]
pub enum TransactionNodeWrite {
    /// Insert or replace bytes under a content-addressed key.
    Upsert { key: Vec<u8>, value: Vec<u8> },
    /// Delete bytes under a content-addressed key.
    Delete { key: Vec<u8> },
}

/// Details for a failed transaction validation.
#[derive(Clone, Debug, PartialEq)]
pub struct TransactionConflict {
    /// Durable root name that failed validation.
    pub name: Vec<u8>,
    /// Manifest expected by the transaction.
    pub expected: Option<RootManifest>,
    /// Manifest currently stored by the backend.
    pub current: Option<RootManifest>,
}

impl TransactionConflict {
    /// Create a conflict record.
    pub fn new(
        name: Vec<u8>,
        expected: Option<RootManifest>,
        current: Option<RootManifest>,
    ) -> Self {
        Self {
            name,
            expected,
            current,
        }
    }
}

/// Result of committing a transaction.
#[derive(Clone, Debug, PartialEq)]
pub enum TransactionUpdate {
    /// All staged writes were committed atomically.
    Applied {
        /// Number of staged node writes applied.
        nodes_written: usize,
        /// Number of staged named-root writes applied.
        roots_written: usize,
    },
    /// A named-root condition failed; no staged writes were applied.
    Conflict(Box<TransactionConflict>),
}

impl TransactionUpdate {
    /// Whether the transaction committed.
    pub fn is_applied(&self) -> bool {
        matches!(self, Self::Applied { .. })
    }

    /// Whether the transaction failed validation.
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict(_))
    }

    /// Conflict details, if validation failed.
    pub fn conflict(&self) -> Option<&TransactionConflict> {
        match self {
            Self::Applied { .. } => None,
            Self::Conflict(conflict) => Some(conflict),
        }
    }
}

/// Store support for strict atomic transaction commits.
pub trait TransactionalStore: Store + ManifestStore {
    /// Whether this backend satisfies the strict transaction contract.
    ///
    /// Returning `true` promises that root conditions are evaluated against one
    /// linearizable backend state across threads, handles, and processes that
    /// share the store; conflicting commits expose no root writes; all root
    /// writes in an applied commit become visible atomically; and every node
    /// referenced by an applied root is durably readable before success is
    /// acknowledged. Implementations may retain unreachable content-addressed
    /// node writes after an error because those nodes do not affect visibility.
    fn supports_transactions(&self) -> bool {
        false
    }

    /// Validate root conditions, write nodes, and atomically apply root writes.
    ///
    /// `Conflict` must leave every root unchanged. `Applied` must be returned
    /// only after the complete root-write set and all referenced node bytes meet
    /// the durability and visibility guarantees of [`Self::supports_transactions`].
    fn commit_transaction(
        &self,
        _node_writes: &[TransactionNodeWrite],
        _root_conditions: &[RootCondition],
        _root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        Err(Error::UnsupportedTransactions {
            store: type_name::<Self>(),
        })
    }
}

impl<T> TransactionalStore for Arc<T>
where
    T: TransactionalStore,
{
    fn supports_transactions(&self) -> bool {
        (**self).supports_transactions()
    }

    fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        (**self).commit_transaction(node_writes, root_conditions, root_writes)
    }
}

/// Async store support for strict atomic transaction commits.
#[allow(async_fn_in_trait)]
pub trait AsyncTransactionalStore: AsyncStore + AsyncManifestStore {
    /// Whether this backend can atomically commit staged nodes and roots.
    fn supports_transactions(&self) -> bool {
        false
    }

    /// Atomically validate root conditions, write nodes, and apply root writes.
    async fn commit_transaction(
        &self,
        _node_writes: &[TransactionNodeWrite],
        _root_conditions: &[RootCondition],
        _root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        Err(Error::UnsupportedTransactions {
            store: type_name::<Self>(),
        })
    }
}
impl<T> AsyncTransactionalStore for Arc<T>
where
    T: AsyncTransactionalStore,
{
    fn supports_transactions(&self) -> bool {
        (**self).supports_transactions()
    }

    async fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        (**self)
            .commit_transaction(node_writes, root_conditions, root_writes)
            .await
    }
}
impl<S> AsyncTransactionalStore for SyncStoreAsAsync<S>
where
    S: TransactionalStore,
{
    fn supports_transactions(&self) -> bool {
        self.inner().supports_transactions()
    }

    async fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        self.inner()
            .commit_transaction(node_writes, root_conditions, root_writes)
    }
}

#[cfg(feature = "tokio")]
impl<S> AsyncTransactionalStore for super::store::TokioBlockingStore<S>
where
    S: TransactionalStore + 'static,
{
    fn supports_transactions(&self) -> bool {
        self.inner().supports_transactions()
    }

    async fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        let store = self.shared();
        let node_writes = node_writes.to_vec();
        let root_conditions = root_conditions.to_vec();
        let root_writes = root_writes.to_vec();
        tokio::task::spawn_blocking(move || {
            store.commit_transaction(&node_writes, &root_conditions, &root_writes)
        })
        .await
        .map_err(|err| Error::Store(Box::new(err)))?
    }
}

#[derive(Debug)]
pub struct TransactionOverlayError {
    message: String,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl TransactionOverlayError {
    fn poisoned(err: impl fmt::Display) -> Self {
        Self {
            message: format!("transaction overlay lock poisoned: {err}"),
            source: None,
        }
    }

    fn store(err: impl StdError + Send + Sync + 'static) -> Self {
        Self {
            message: format!("base store error: {err}"),
            source: Some(Box::new(err)),
        }
    }
}

impl fmt::Display for TransactionOverlayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "transaction overlay error: {}", self.message)
    }
}

impl StdError for TransactionOverlayError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_ref()
            .map(|err| err.as_ref() as &(dyn StdError + 'static))
    }
}

#[derive(Default)]
struct TransactionState {
    node_writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    root_reads: BTreeMap<Vec<u8>, Option<RootManifest>>,
    root_writes: BTreeMap<Vec<u8>, RootWrite>,
}

impl TransactionState {
    fn node_writes(&self) -> Vec<TransactionNodeWrite> {
        self.node_writes
            .iter()
            .map(|(key, value)| match value {
                Some(value) => TransactionNodeWrite::Upsert {
                    key: key.clone(),
                    value: value.clone(),
                },
                None => TransactionNodeWrite::Delete { key: key.clone() },
            })
            .collect()
    }

    fn root_conditions(&self) -> Vec<RootCondition> {
        self.root_reads
            .iter()
            .map(|(name, expected)| RootCondition::new(name.clone(), expected.clone()))
            .collect()
    }

    fn root_writes(&self) -> Vec<RootWrite> {
        self.root_writes.values().cloned().collect()
    }
}

/// Store overlay used internally by [`ProllyTransaction`].
#[cfg(test)]
#[derive(Clone)]
pub struct TransactionOverlayStore<'a, S> {
    base: &'a S,
    state: Arc<Mutex<TransactionState>>,
}

#[cfg(test)]
impl<'a, S> TransactionOverlayStore<'a, S> {
    fn new(base: &'a S, state: Arc<Mutex<TransactionState>>) -> Self {
        Self { base, state }
    }

    fn lock(&self) -> Result<MutexGuard<'_, TransactionState>, TransactionOverlayError> {
        self.state.lock().map_err(TransactionOverlayError::poisoned)
    }
}

#[cfg(test)]
impl<S> Store for TransactionOverlayStore<'_, S>
where
    S: Store,
{
    type Error = TransactionOverlayError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let staged = self.lock()?.node_writes.get(key).cloned();
        match staged {
            Some(value) => Ok(value),
            None => self.base.get(key).map_err(TransactionOverlayError::store),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.lock()?
            .node_writes
            .insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.lock()?.node_writes.insert(key.to_vec(), None);
        Ok(())
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let mut state = self.lock()?;
        for op in ops {
            match op {
                BatchOp::Upsert { key, value } => {
                    state
                        .node_writes
                        .insert((*key).to_vec(), Some((*value).to_vec()));
                }
                BatchOp::Delete { key } => {
                    state.node_writes.insert((*key).to_vec(), None);
                }
            }
        }
        Ok(())
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.batch_put(publication.entries())
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        overlay_batch_get_ordered(self.base, &self.state, keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        self.base.prefers_batch_reads()
    }
}

#[cfg(test)]
impl<S> ManifestStore for TransactionOverlayStore<'_, S>
where
    S: Store + ManifestStore,
{
    type Error = TransactionOverlayError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        {
            let state = self.lock()?;
            if let Some(write) = state.root_writes.get(name) {
                return Ok(write.replacement().cloned());
            }
            if let Some(read) = state.root_reads.get(name) {
                return Ok(read.clone());
            }
        }

        let current = self
            .base
            .get_root(name)
            .map_err(TransactionOverlayError::store)?;
        let mut state = self.lock()?;
        if let Some(write) = state.root_writes.get(name) {
            return Ok(write.replacement().cloned());
        }
        Ok(state
            .root_reads
            .entry(name.to_vec())
            .or_insert(current)
            .clone())
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        self.lock()?.root_writes.insert(
            name.to_vec(),
            RootWrite::Put {
                name: name.to_vec(),
                manifest: manifest.clone(),
            },
        );
        Ok(())
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        self.lock()?.root_writes.insert(
            name.to_vec(),
            RootWrite::Delete {
                name: name.to_vec(),
            },
        );
        Ok(())
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let current = self.get_root(name)?;
        if current.as_ref() != expected {
            return Ok(ManifestUpdate::Conflict { current });
        }

        match new {
            Some(manifest) => self.put_root(name, manifest)?,
            None => self.delete_root(name)?,
        }
        Ok(ManifestUpdate::Applied)
    }
}

/// Owned store overlay used by [`OwnedProllyTransaction`].
#[cfg(test)]
#[derive(Clone)]
pub struct OwnedTransactionOverlayStore<S> {
    base: S,
    state: Arc<Mutex<TransactionState>>,
}

#[cfg(test)]
impl<S> OwnedTransactionOverlayStore<S> {
    fn new(base: S, state: Arc<Mutex<TransactionState>>) -> Self {
        Self { base, state }
    }

    fn lock(&self) -> Result<MutexGuard<'_, TransactionState>, TransactionOverlayError> {
        self.state.lock().map_err(TransactionOverlayError::poisoned)
    }
}

#[cfg(test)]
impl<S> Store for OwnedTransactionOverlayStore<S>
where
    S: Store,
{
    type Error = TransactionOverlayError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let staged = self.lock()?.node_writes.get(key).cloned();
        match staged {
            Some(value) => Ok(value),
            None => self.base.get(key).map_err(TransactionOverlayError::store),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.lock()?
            .node_writes
            .insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.lock()?.node_writes.insert(key.to_vec(), None);
        Ok(())
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let mut state = self.lock()?;
        for op in ops {
            match op {
                BatchOp::Upsert { key, value } => {
                    state
                        .node_writes
                        .insert((*key).to_vec(), Some((*value).to_vec()));
                }
                BatchOp::Delete { key } => {
                    state.node_writes.insert((*key).to_vec(), None);
                }
            }
        }
        Ok(())
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.batch_put(publication.entries())
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        overlay_batch_get_ordered(&self.base, &self.state, keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        self.base.prefers_batch_reads()
    }
}

#[cfg(test)]
fn overlay_batch_get_ordered<S: Store>(
    base: &S,
    state: &Arc<Mutex<TransactionState>>,
    keys: &[&[u8]],
) -> Result<Vec<Option<Vec<u8>>>, TransactionOverlayError> {
    let staged = {
        let state = state.lock().map_err(TransactionOverlayError::poisoned)?;
        keys.iter()
            .map(|key| state.node_writes.get(*key).cloned())
            .collect::<Vec<_>>()
    };

    let mut results = vec![None; keys.len()];
    let mut missing_keys = Vec::new();
    let mut missing_positions = Vec::new();
    for (position, staged_value) in staged.into_iter().enumerate() {
        match staged_value {
            Some(value) => results[position] = value,
            None => {
                missing_keys.push(keys[position]);
                missing_positions.push(position);
            }
        }
    }
    if missing_keys.is_empty() {
        return Ok(results);
    }

    let plan = OrderedBatchReadPlan::new(&missing_keys);
    let unique_values = base
        .batch_get_ordered_unique(plan.unique_keys())
        .map_err(TransactionOverlayError::store)?;
    let missing_values = plan.expand_owned(unique_values);
    for (position, value) in missing_positions.into_iter().zip(missing_values) {
        results[position] = value;
    }
    Ok(results)
}

#[cfg(test)]
impl<S> ManifestStore for OwnedTransactionOverlayStore<S>
where
    S: Store + ManifestStore,
{
    type Error = TransactionOverlayError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        {
            let state = self.lock()?;
            if let Some(write) = state.root_writes.get(name) {
                return Ok(write.replacement().cloned());
            }
            if let Some(read) = state.root_reads.get(name) {
                return Ok(read.clone());
            }
        }

        let current = self
            .base
            .get_root(name)
            .map_err(TransactionOverlayError::store)?;
        let mut state = self.lock()?;
        if let Some(write) = state.root_writes.get(name) {
            return Ok(write.replacement().cloned());
        }
        Ok(state
            .root_reads
            .entry(name.to_vec())
            .or_insert(current)
            .clone())
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        self.lock()?.root_writes.insert(
            name.to_vec(),
            RootWrite::Put {
                name: name.to_vec(),
                manifest: manifest.clone(),
            },
        );
        Ok(())
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        self.lock()?.root_writes.insert(
            name.to_vec(),
            RootWrite::Delete {
                name: name.to_vec(),
            },
        );
        Ok(())
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let current = self.get_root(name)?;
        if current.as_ref() != expected {
            return Ok(ManifestUpdate::Conflict { current });
        }

        match new {
            Some(manifest) => self.put_root(name, manifest)?,
            None => self.delete_root(name)?,
        }
        Ok(ManifestUpdate::Applied)
    }
}

/// Owned async store overlay used by [`OwnedAsyncProllyTransaction`].
#[derive(Clone)]
pub struct OwnedAsyncTransactionOverlayStore<S> {
    base: S,
    state: Arc<Mutex<TransactionState>>,
    committed_cache: Option<Arc<std::sync::RwLock<NodeCache>>>,
}
impl<S> OwnedAsyncTransactionOverlayStore<S> {
    #[cfg(test)]
    fn new(base: S, state: Arc<Mutex<TransactionState>>) -> Self {
        Self {
            base,
            state,
            committed_cache: None,
        }
    }

    fn with_committed_cache(
        base: S,
        state: Arc<Mutex<TransactionState>>,
        committed_cache: Arc<std::sync::RwLock<NodeCache>>,
    ) -> Self {
        Self {
            base,
            state,
            committed_cache: Some(committed_cache),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, TransactionState>, TransactionOverlayError> {
        self.state.lock().map_err(TransactionOverlayError::poisoned)
    }
}
impl<S> AsyncStore for OwnedAsyncTransactionOverlayStore<S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    type Error = TransactionOverlayError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let staged = self.lock()?.node_writes.get(key).cloned();
        match staged {
            Some(value) => Ok(value),
            None => match cached_node_bytes(self.committed_cache.as_ref(), key) {
                Some(value) => Ok(Some(value)),
                None => self
                    .base
                    .get(key)
                    .await
                    .map_err(TransactionOverlayError::store),
            },
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.lock()?
            .node_writes
            .insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.lock()?.node_writes.insert(key.to_vec(), None);
        Ok(())
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        let mut state = self.lock()?;
        for op in ops {
            match op {
                BatchOp::Upsert { key, value } => {
                    state
                        .node_writes
                        .insert((*key).to_vec(), Some((*value).to_vec()));
                }
                BatchOp::Delete { key } => {
                    state.node_writes.insert((*key).to_vec(), None);
                }
            }
        }
        Ok(())
    }

    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.batch_put(publication.entries()).await
    }

    async fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        async_overlay_batch_get_ordered(
            &self.base,
            &self.state,
            self.committed_cache.as_ref(),
            keys,
        )
        .await
    }

    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        async_overlay_batch_get_ordered(
            &self.base,
            &self.state,
            self.committed_cache.as_ref(),
            keys,
        )
        .await
    }

    fn prefers_batch_reads(&self) -> bool {
        self.base.prefers_batch_reads()
    }

    fn read_parallelism(&self) -> usize {
        self.base.read_parallelism()
    }
}
impl<S> AsyncManifestStore for OwnedAsyncTransactionOverlayStore<S>
where
    S: AsyncStore + AsyncManifestStore,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    type Error = TransactionOverlayError;

    async fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        {
            let state = self.lock()?;
            if let Some(write) = state.root_writes.get(name) {
                return Ok(write.replacement().cloned());
            }
            if let Some(read) = state.root_reads.get(name) {
                return Ok(read.clone());
            }
        }

        let current = self
            .base
            .get_root(name)
            .await
            .map_err(TransactionOverlayError::store)?;
        let mut state = self.lock()?;
        if let Some(write) = state.root_writes.get(name) {
            return Ok(write.replacement().cloned());
        }
        Ok(state
            .root_reads
            .entry(name.to_vec())
            .or_insert(current)
            .clone())
    }

    async fn get_roots_ordered(
        &self,
        names: &[&[u8]],
    ) -> Result<Vec<Option<RootManifest>>, Self::Error> {
        async_overlay_get_roots_ordered(&self.base, &self.state, names).await
    }

    async fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        self.lock()?.root_writes.insert(
            name.to_vec(),
            RootWrite::Put {
                name: name.to_vec(),
                manifest: manifest.clone(),
            },
        );
        Ok(())
    }

    async fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        self.lock()?.root_writes.insert(
            name.to_vec(),
            RootWrite::Delete {
                name: name.to_vec(),
            },
        );
        Ok(())
    }

    async fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let current = self.get_root(name).await?;
        if current.as_ref() != expected {
            return Ok(ManifestUpdate::Conflict { current });
        }

        match new {
            Some(manifest) => self.put_root(name, manifest).await?,
            None => self.delete_root(name).await?,
        }
        Ok(ManifestUpdate::Applied)
    }
}
async fn async_overlay_batch_get_ordered<S: AsyncStore>(
    base: &S,
    state: &Arc<Mutex<TransactionState>>,
    committed_cache: Option<&Arc<std::sync::RwLock<NodeCache>>>,
    keys: &[&[u8]],
) -> Result<Vec<Option<Vec<u8>>>, TransactionOverlayError>
where
    S::Error: Send + Sync,
{
    let staged = {
        let state = state.lock().map_err(TransactionOverlayError::poisoned)?;
        keys.iter()
            .map(|key| state.node_writes.get(*key).cloned())
            .collect::<Vec<_>>()
    };

    let mut results = vec![None; keys.len()];
    let mut missing_keys = Vec::new();
    let mut missing_positions = Vec::new();
    for (position, staged_value) in staged.into_iter().enumerate() {
        match staged_value {
            Some(value) => results[position] = value,
            None => match cached_node_bytes(committed_cache, keys[position]) {
                Some(value) => results[position] = Some(value),
                None => {
                    missing_keys.push(keys[position]);
                    missing_positions.push(position);
                }
            },
        }
    }
    if missing_keys.is_empty() {
        return Ok(results);
    }

    let plan = OrderedBatchReadPlan::new(&missing_keys);
    let unique_values = base
        .batch_get_ordered_unique(plan.unique_keys())
        .await
        .map_err(TransactionOverlayError::store)?;
    let missing_values = plan.expand_owned(unique_values);
    for (position, value) in missing_positions.into_iter().zip(missing_values) {
        results[position] = value;
    }
    Ok(results)
}

fn cached_node_bytes(
    cache: Option<&Arc<std::sync::RwLock<NodeCache>>>,
    key: &[u8],
) -> Option<Vec<u8>> {
    let cid = cid_from_store_key(key, "transaction cache lookup").ok()?;
    let node = cache?.read().ok()?.peek(&cid)?;
    let bytes = node.to_bytes();
    (Cid::from_bytes(&bytes) == cid).then_some(bytes)
}

fn promote_committed_nodes(
    cache: &Arc<std::sync::RwLock<NodeCache>>,
    metrics: &Arc<ProllyMetrics>,
    config: &Config,
    writes: &[TransactionNodeWrite],
) {
    let mut decoded = Vec::new();
    let mut contains_delete = false;
    for write in writes {
        match write {
            TransactionNodeWrite::Upsert { key, value } => {
                let Ok(cid) = cid_from_store_key(key, "committed transaction node") else {
                    debug_assert!(false, "transaction committed a malformed node key");
                    continue;
                };
                let Ok(node) = super::engine::validation::decode_owned(&cid, &config.format, value)
                else {
                    debug_assert!(false, "transaction committed malformed node bytes");
                    continue;
                };
                decoded.push((cid, Arc::new(node), value.len()));
            }
            TransactionNodeWrite::Delete { .. } => contains_delete = true,
        }
    }
    if let Ok(mut cache) = cache.write() {
        let mut evictions = if contains_delete { cache.clear() } else { 0 };
        for (cid, node, bytes) in decoded {
            evictions += cache.insert(cid, node, bytes);
        }
        metrics.add_cache_evictions(evictions);
    }
}

async fn async_overlay_get_roots_ordered<S: AsyncManifestStore>(
    base: &S,
    state: &Arc<Mutex<TransactionState>>,
    names: &[&[u8]],
) -> Result<Vec<Option<RootManifest>>, TransactionOverlayError>
where
    S::Error: Send + Sync,
{
    let mut results = vec![None; names.len()];
    let mut resolved = vec![false; names.len()];
    let mut missing_names = Vec::new();
    let mut missing_positions = Vec::new();
    {
        let state = state.lock().map_err(TransactionOverlayError::poisoned)?;
        for (position, name) in names.iter().enumerate() {
            if let Some(write) = state.root_writes.get(*name) {
                results[position] = write.replacement().cloned();
                resolved[position] = true;
            } else if let Some(read) = state.root_reads.get(*name) {
                results[position] = read.clone();
                resolved[position] = true;
            } else {
                missing_names.push(*name);
                missing_positions.push(position);
            }
        }
    }
    if missing_names.is_empty() {
        return Ok(results);
    }
    let observed = base
        .get_roots_ordered(&missing_names)
        .await
        .map_err(TransactionOverlayError::store)?;
    if observed.len() != missing_names.len() {
        return Err(TransactionOverlayError {
            message: "base manifest batch returned the wrong result count".into(),
            source: None,
        });
    }
    let mut state = state.lock().map_err(TransactionOverlayError::poisoned)?;
    for ((position, name), observed) in missing_positions
        .into_iter()
        .zip(missing_names)
        .zip(observed)
    {
        results[position] = if let Some(write) = state.root_writes.get(name) {
            write.replacement().cloned()
        } else {
            state
                .root_reads
                .entry(name.to_vec())
                .or_insert(observed)
                .clone()
        };
        resolved[position] = true;
    }
    debug_assert!(resolved.into_iter().all(|value| value));
    Ok(results)
}

/// Async store overlay used internally by [`AsyncProllyTransaction`].
#[derive(Clone)]
pub struct AsyncTransactionOverlayStore<'a, S> {
    base: &'a S,
    state: Arc<Mutex<TransactionState>>,
    committed_cache: Option<Arc<std::sync::RwLock<NodeCache>>>,
}
impl<'a, S> AsyncTransactionOverlayStore<'a, S> {
    #[cfg(test)]
    fn new(base: &'a S, state: Arc<Mutex<TransactionState>>) -> Self {
        Self {
            base,
            state,
            committed_cache: None,
        }
    }

    fn with_committed_cache(
        base: &'a S,
        state: Arc<Mutex<TransactionState>>,
        committed_cache: Arc<std::sync::RwLock<NodeCache>>,
    ) -> Self {
        Self {
            base,
            state,
            committed_cache: Some(committed_cache),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, TransactionState>, TransactionOverlayError> {
        self.state.lock().map_err(TransactionOverlayError::poisoned)
    }
}
impl<S> AsyncStore for AsyncTransactionOverlayStore<'_, S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    type Error = TransactionOverlayError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let staged = self.lock()?.node_writes.get(key).cloned();
        match staged {
            Some(value) => Ok(value),
            None => match cached_node_bytes(self.committed_cache.as_ref(), key) {
                Some(value) => Ok(Some(value)),
                None => self
                    .base
                    .get(key)
                    .await
                    .map_err(TransactionOverlayError::store),
            },
        }
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.lock()?
            .node_writes
            .insert(key.to_vec(), Some(value.to_vec()));
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.lock()?.node_writes.insert(key.to_vec(), None);
        Ok(())
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        let mut state = self.lock()?;
        for op in ops {
            match op {
                BatchOp::Upsert { key, value } => {
                    state
                        .node_writes
                        .insert((*key).to_vec(), Some((*value).to_vec()));
                }
                BatchOp::Delete { key } => {
                    state.node_writes.insert((*key).to_vec(), None);
                }
            }
        }
        Ok(())
    }

    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.batch_put(publication.entries()).await
    }

    async fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        async_overlay_batch_get_ordered(self.base, &self.state, self.committed_cache.as_ref(), keys)
            .await
    }

    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        async_overlay_batch_get_ordered(self.base, &self.state, self.committed_cache.as_ref(), keys)
            .await
    }

    fn prefers_batch_reads(&self) -> bool {
        self.base.prefers_batch_reads()
    }

    fn read_parallelism(&self) -> usize {
        self.base.read_parallelism()
    }
}
impl<S> AsyncManifestStore for AsyncTransactionOverlayStore<'_, S>
where
    S: AsyncStore + AsyncManifestStore,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    type Error = TransactionOverlayError;

    async fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        {
            let state = self.lock()?;
            if let Some(write) = state.root_writes.get(name) {
                return Ok(write.replacement().cloned());
            }
            if let Some(read) = state.root_reads.get(name) {
                return Ok(read.clone());
            }
        }

        let current = self
            .base
            .get_root(name)
            .await
            .map_err(TransactionOverlayError::store)?;
        let mut state = self.lock()?;
        if let Some(write) = state.root_writes.get(name) {
            return Ok(write.replacement().cloned());
        }
        Ok(state
            .root_reads
            .entry(name.to_vec())
            .or_insert(current)
            .clone())
    }

    async fn get_roots_ordered(
        &self,
        names: &[&[u8]],
    ) -> Result<Vec<Option<RootManifest>>, Self::Error> {
        async_overlay_get_roots_ordered(self.base, &self.state, names).await
    }

    async fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        self.lock()?.root_writes.insert(
            name.to_vec(),
            RootWrite::Put {
                name: name.to_vec(),
                manifest: manifest.clone(),
            },
        );
        Ok(())
    }

    async fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        self.lock()?.root_writes.insert(
            name.to_vec(),
            RootWrite::Delete {
                name: name.to_vec(),
            },
        );
        Ok(())
    }

    async fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let current = self.get_root(name).await?;
        if current.as_ref() != expected {
            return Ok(ManifestUpdate::Conflict { current });
        }

        match new {
            Some(manifest) => self.put_root(name, manifest).await?,
            None => self.delete_root(name).await?,
        }
        Ok(ManifestUpdate::Applied)
    }
}

/// A strict optimistic transaction over a [`Prolly`] manager.
pub struct ProllyTransaction<'a, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    inner: Option<AsyncProllyTransaction<'a, SyncStoreAsAsync<Arc<S>>>>,
    ready_store: SyncStoreAsAsync<Arc<S>>,
}

impl<'a, S> ProllyTransaction<'a, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    fn new(base: &'a Prolly<S>) -> Result<Self, Error> {
        let ready_store = base.engine.store.clone();
        let inner = AsyncProllyTransaction::new(&base.engine)?;
        Ok(Self {
            inner: Some(inner),
            ready_store,
        })
    }

    fn inner(&self) -> &AsyncProllyTransaction<'a, SyncStoreAsAsync<Arc<S>>> {
        self.inner.as_ref().expect("active transaction")
    }

    /// Create an empty tree using the base manager's config.
    pub fn create(&self) -> Tree {
        self.inner().create()
    }

    /// Get a value from a tree, including nodes staged in this transaction.
    pub fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let future = self.inner().get(tree, key);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Insert or update a key/value pair, staging rewritten nodes.
    pub fn put(&self, tree: &Tree, key: Vec<u8>, value: Vec<u8>) -> Result<Tree, Error> {
        let future = self.inner().put(tree, key, value);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Delete a key, staging rewritten nodes.
    pub fn delete(&self, tree: &Tree, key: &[u8]) -> Result<Tree, Error> {
        let future = self.inner().delete(tree, key);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Apply a batch of logical map mutations inside the transaction.
    pub fn batch(&self, tree: &Tree, mutations: Vec<Mutation>) -> Result<Tree, Error> {
        let future = self.inner().batch(tree, mutations);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Load a named root and add it to the transaction read set.
    pub fn load_named_root(&self, name: &[u8]) -> Result<Option<Tree>, Error> {
        let future = self.inner().load_named_root(name);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Stage an unconditional named-root publish.
    pub fn publish_named_root(&self, name: &[u8], tree: &Tree) -> Result<(), Error> {
        let future = self.inner().publish_named_root(name, tree);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Stage an unconditional named-root publish with an explicit timestamp.
    pub fn publish_named_root_at_millis(
        &self,
        name: &[u8],
        tree: &Tree,
        timestamp_millis: u64,
    ) -> Result<(), Error> {
        let future = self
            .inner()
            .publish_named_root_at_millis(name, tree, timestamp_millis);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Stage an unconditional named-root delete.
    pub fn delete_named_root(&self, name: &[u8]) -> Result<(), Error> {
        let future = self.inner().delete_named_root(name);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Stage a named-root CAS update.
    pub fn compare_and_swap_named_root(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
    ) -> Result<NamedRootUpdate, Error> {
        let future = self
            .inner()
            .compare_and_swap_named_root(name, expected, new);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Stage a named-root CAS update with explicit manifest timestamps.
    pub fn compare_and_swap_named_root_at_millis(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
        timestamp_millis: u64,
    ) -> Result<NamedRootUpdate, Error> {
        let future = self.inner().compare_and_swap_named_root_at_millis(
            name,
            expected,
            new,
            timestamp_millis,
        );
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Discard all staged writes. Dropping an uncommitted transaction has the
    /// same effect; this method is useful when callers want to be explicit.
    pub fn rollback(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.rollback();
        }
    }

    /// Commit staged node and named-root writes atomically.
    pub fn commit(mut self) -> Result<TransactionUpdate, Error> {
        let inner = self.inner.take().expect("active transaction");
        let future = inner.commit();
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }
}

/// A strict optimistic transaction that owns a cloned store handle.
///
/// This is useful for FFI bindings, where a transaction object needs to live
/// independently from a borrowed Rust stack frame. Normal Rust callers should
/// prefer [`Prolly::begin_transaction`] or [`Prolly::transaction`].
pub struct OwnedProllyTransaction<S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    inner: Option<OwnedAsyncProllyTransaction<SyncStoreAsAsync<Arc<S>>>>,
    ready_store: SyncStoreAsAsync<Arc<S>>,
}

impl<S> OwnedProllyTransaction<S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    fn new(base: &Prolly<S>) -> Result<Self, Error> {
        let ready_store = base.engine.store.clone();
        let inner = OwnedAsyncProllyTransaction::new(&base.engine)?;
        Ok(Self {
            inner: Some(inner),
            ready_store,
        })
    }

    fn inner(&self) -> &OwnedAsyncProllyTransaction<SyncStoreAsAsync<Arc<S>>> {
        self.inner.as_ref().expect("active transaction")
    }

    /// Create an empty tree using the base manager's config.
    pub fn create(&self) -> Tree {
        self.inner().create()
    }

    /// Get a value from a tree, including nodes staged in this transaction.
    pub fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let future = self.inner().get(tree, key);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Insert or update a key/value pair, staging rewritten nodes.
    pub fn put(&self, tree: &Tree, key: Vec<u8>, value: Vec<u8>) -> Result<Tree, Error> {
        let future = self.inner().put(tree, key, value);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Delete a key, staging rewritten nodes.
    pub fn delete(&self, tree: &Tree, key: &[u8]) -> Result<Tree, Error> {
        let future = self.inner().delete(tree, key);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Apply a batch of logical map mutations inside the transaction.
    pub fn batch(&self, tree: &Tree, mutations: Vec<Mutation>) -> Result<Tree, Error> {
        let future = self.inner().batch(tree, mutations);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Load a named root and add it to the transaction read set.
    pub fn load_named_root(&self, name: &[u8]) -> Result<Option<Tree>, Error> {
        let future = self.inner().load_named_root(name);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Stage an unconditional named-root publish.
    pub fn publish_named_root(&self, name: &[u8], tree: &Tree) -> Result<(), Error> {
        let future = self.inner().publish_named_root(name, tree);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Stage an unconditional named-root delete.
    pub fn delete_named_root(&self, name: &[u8]) -> Result<(), Error> {
        let future = self.inner().delete_named_root(name);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Stage a named-root CAS update.
    pub fn compare_and_swap_named_root(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
    ) -> Result<NamedRootUpdate, Error> {
        let future = self
            .inner()
            .compare_and_swap_named_root(name, expected, new);
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Stage a named-root CAS update with explicit manifest timestamps.
    pub fn compare_and_swap_named_root_at_millis(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
        timestamp_millis: u64,
    ) -> Result<NamedRootUpdate, Error> {
        let future = self.inner().compare_and_swap_named_root_at_millis(
            name,
            expected,
            new,
            timestamp_millis,
        );
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }

    /// Discard all staged writes. Dropping an uncommitted transaction has the
    /// same effect; this method is useful when callers want to be explicit.
    pub fn rollback(mut self) {
        if let Some(inner) = self.inner.take() {
            inner.rollback();
        }
    }

    /// Commit staged node and named-root writes atomically.
    pub fn commit(mut self) -> Result<TransactionUpdate, Error> {
        let inner = self.inner.take().expect("active transaction");
        let future = inner.commit();
        super::engine::ready::run_ready(self.ready_store.ready(future))
    }
}

/// A strict optimistic async transaction that owns a cloned store handle.
///
/// This variant is intended for FFI bindings and other APIs that cannot keep a
/// borrow of an [`AsyncProlly`] manager alive across asynchronous calls.
pub struct OwnedAsyncProllyTransaction<S>
where
    S: AsyncStore + AsyncManifestStore + AsyncTransactionalStore,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    base_store: S,
    base_cache: Arc<std::sync::RwLock<NodeCache>>,
    base_metrics: Arc<ProllyMetrics>,
    base_config: Config,
    state: Arc<Mutex<TransactionState>>,
    manager: AsyncProlly<OwnedAsyncTransactionOverlayStore<S>>,
    completed: bool,
}
impl<S> OwnedAsyncProllyTransaction<S>
where
    S: AsyncStore + AsyncManifestStore + AsyncTransactionalStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    fn new(base: &AsyncProlly<S>) -> Result<Self, Error> {
        if !base.store.supports_transactions() {
            return Err(Error::UnsupportedTransactions {
                store: type_name::<S>(),
            });
        }

        let base_store = base.store.clone();
        let base_cache = base.node_cache.clone();
        let base_metrics = base.metrics.clone();
        let base_config = base.config.clone();
        let state = Arc::new(Mutex::new(TransactionState::default()));
        let overlay = OwnedAsyncTransactionOverlayStore::with_committed_cache(
            base_store.clone(),
            state.clone(),
            base_cache.clone(),
        );
        let manager = AsyncProlly::new(overlay, base.config.clone());
        Ok(Self {
            base_store,
            base_cache,
            base_metrics,
            base_config,
            state,
            manager,
            completed: false,
        })
    }

    /// Create an empty tree using the base manager's config.
    pub fn create(&self) -> Tree {
        self.manager.create()
    }

    /// Get a value from a tree, including nodes staged in this transaction.
    pub async fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.manager.get(tree, key).await
    }

    /// Insert or update a key/value pair, staging rewritten nodes.
    pub async fn put(&self, tree: &Tree, key: Vec<u8>, value: Vec<u8>) -> Result<Tree, Error> {
        self.manager.put(tree, key, value).await
    }

    /// Delete a key, staging rewritten nodes.
    pub async fn delete(&self, tree: &Tree, key: &[u8]) -> Result<Tree, Error> {
        self.manager.delete(tree, key).await
    }

    /// Apply a batch of logical map mutations inside the transaction.
    pub async fn batch(&self, tree: &Tree, mutations: Vec<Mutation>) -> Result<Tree, Error> {
        self.manager.batch(tree, mutations).await
    }

    /// Load a named root and add it to the transaction read set.
    pub async fn load_named_root(&self, name: &[u8]) -> Result<Option<Tree>, Error> {
        self.manager.load_named_root(name).await
    }

    /// Load and pin named roots in caller order, batching backend reads when
    /// supported by the manifest store.
    pub async fn load_named_roots_ordered(
        &self,
        names: &[&[u8]],
    ) -> Result<Vec<Option<Tree>>, Error> {
        self.manager.load_named_roots_ordered(names).await
    }

    /// Stage an unconditional named-root publish.
    pub async fn publish_named_root(&self, name: &[u8], tree: &Tree) -> Result<(), Error> {
        self.manager.publish_named_root(name, tree).await
    }

    /// Stage an unconditional named-root publish with an explicit timestamp.
    pub async fn publish_named_root_at_millis(
        &self,
        name: &[u8],
        tree: &Tree,
        timestamp_millis: u64,
    ) -> Result<(), Error> {
        self.manager
            .publish_named_root_at_millis(name, tree, timestamp_millis)
            .await
    }

    /// Stage an unconditional named-root delete.
    pub async fn delete_named_root(&self, name: &[u8]) -> Result<(), Error> {
        self.manager.delete_named_root(name).await
    }

    /// Stage a named-root CAS update.
    pub async fn compare_and_swap_named_root(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
    ) -> Result<NamedRootUpdate, Error> {
        self.manager
            .compare_and_swap_named_root(name, expected, new)
            .await
    }

    /// Stage a named-root CAS update with explicit manifest timestamps.
    pub async fn compare_and_swap_named_root_at_millis(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
        timestamp_millis: u64,
    ) -> Result<NamedRootUpdate, Error> {
        self.manager
            .compare_and_swap_named_root_at_millis(name, expected, new, timestamp_millis)
            .await
    }

    /// Discard all staged writes.
    pub fn rollback(mut self) {
        self.completed = true;
    }

    /// Commit staged node and named-root writes atomically.
    pub async fn commit(mut self) -> Result<TransactionUpdate, Error> {
        let (node_writes, root_conditions, root_writes) = {
            let state = self
                .state
                .lock()
                .map_err(|err| Error::Store(Box::new(TransactionOverlayError::poisoned(err))))?;
            (
                state.node_writes(),
                state.root_conditions(),
                state.root_writes(),
            )
        };

        let update = self
            .base_store
            .commit_transaction(&node_writes, &root_conditions, &root_writes)
            .await?;
        if update.is_applied() {
            promote_committed_nodes(
                &self.base_cache,
                &self.base_metrics,
                &self.base_config,
                &node_writes,
            );
        }
        self.completed = true;
        Ok(update)
    }
}

/// A strict optimistic transaction over an [`AsyncProlly`] manager.
pub struct AsyncProllyTransaction<'a, S>
where
    S: AsyncStore + AsyncManifestStore + AsyncTransactionalStore,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    base: &'a AsyncProlly<S>,
    state: Arc<Mutex<TransactionState>>,
    manager: AsyncProlly<AsyncTransactionOverlayStore<'a, S>>,
    completed: bool,
}
impl<'a, S> AsyncProllyTransaction<'a, S>
where
    S: AsyncStore + AsyncManifestStore + AsyncTransactionalStore,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    fn new(base: &'a AsyncProlly<S>) -> Result<Self, Error> {
        if !base.store.supports_transactions() {
            return Err(Error::UnsupportedTransactions {
                store: type_name::<S>(),
            });
        }

        let state = Arc::new(Mutex::new(TransactionState::default()));
        let overlay = AsyncTransactionOverlayStore::with_committed_cache(
            &base.store,
            state.clone(),
            base.node_cache.clone(),
        );
        let manager = AsyncProlly::new(overlay, base.config.clone());
        Ok(Self {
            base,
            state,
            manager,
            completed: false,
        })
    }

    /// Create an empty tree using the base manager's config.
    pub fn create(&self) -> Tree {
        self.manager.create()
    }

    /// Get a value from a tree, including nodes staged in this transaction.
    pub async fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.manager.get(tree, key).await
    }

    /// Read several keys from one transaction-visible tree in caller order.
    #[doc(hidden)]
    pub async fn get_many<K: AsRef<[u8]>>(
        &self,
        tree: &Tree,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        self.manager.get_many(tree, keys).await
    }

    /// Insert or update a key/value pair, staging rewritten nodes.
    pub async fn put(&self, tree: &Tree, key: Vec<u8>, value: Vec<u8>) -> Result<Tree, Error> {
        self.manager.put(tree, key, value).await
    }

    /// Delete a key, staging rewritten nodes.
    pub async fn delete(&self, tree: &Tree, key: &[u8]) -> Result<Tree, Error> {
        self.manager.delete(tree, key).await
    }

    /// Apply a batch of logical map mutations inside the transaction.
    pub async fn batch(&self, tree: &Tree, mutations: Vec<Mutation>) -> Result<Tree, Error> {
        self.manager.batch(tree, mutations).await
    }

    /// Load a named root and add it to the transaction read set.
    pub async fn load_named_root(&self, name: &[u8]) -> Result<Option<Tree>, Error> {
        self.manager.load_named_root(name).await
    }

    /// Load and pin named roots in caller order, batching backend reads when
    /// supported by the manifest store.
    pub async fn load_named_roots_ordered(
        &self,
        names: &[&[u8]],
    ) -> Result<Vec<Option<Tree>>, Error> {
        self.manager.load_named_roots_ordered(names).await
    }

    /// Stage an unconditional named-root publish.
    pub async fn publish_named_root(&self, name: &[u8], tree: &Tree) -> Result<(), Error> {
        self.manager.publish_named_root(name, tree).await
    }

    /// Stage an unconditional named-root publish with an explicit timestamp.
    pub async fn publish_named_root_at_millis(
        &self,
        name: &[u8],
        tree: &Tree,
        timestamp_millis: u64,
    ) -> Result<(), Error> {
        self.manager
            .publish_named_root_at_millis(name, tree, timestamp_millis)
            .await
    }

    /// Stage an unconditional named-root delete.
    pub async fn delete_named_root(&self, name: &[u8]) -> Result<(), Error> {
        self.manager.delete_named_root(name).await
    }

    /// Stage a named-root CAS update.
    pub async fn compare_and_swap_named_root(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
    ) -> Result<NamedRootUpdate, Error> {
        self.manager
            .compare_and_swap_named_root(name, expected, new)
            .await
    }

    /// Stage a named-root CAS update with explicit manifest timestamps.
    pub async fn compare_and_swap_named_root_at_millis(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
        timestamp_millis: u64,
    ) -> Result<NamedRootUpdate, Error> {
        self.manager
            .compare_and_swap_named_root_at_millis(name, expected, new, timestamp_millis)
            .await
    }

    /// Discard all staged writes. Dropping an uncommitted transaction has the
    /// same effect; this method is useful when callers want to be explicit.
    pub fn rollback(mut self) {
        self.completed = true;
    }

    /// Commit staged node and named-root writes atomically.
    pub async fn commit(mut self) -> Result<TransactionUpdate, Error> {
        let (node_writes, root_conditions, root_writes) = {
            let state = self
                .state
                .lock()
                .map_err(|err| Error::Store(Box::new(TransactionOverlayError::poisoned(err))))?;
            (
                state.node_writes(),
                state.root_conditions(),
                state.root_writes(),
            )
        };

        let update = self
            .base
            .store
            .commit_transaction(&node_writes, &root_conditions, &root_writes)
            .await?;
        if update.is_applied() {
            promote_committed_nodes(
                &self.base.node_cache,
                &self.base.metrics,
                &self.base.config,
                &node_writes,
            );
        }
        self.completed = true;
        Ok(update)
    }
}
impl<S> Drop for AsyncProllyTransaction<'_, S>
where
    S: AsyncStore + AsyncManifestStore + AsyncTransactionalStore,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    fn drop(&mut self) {
        if !self.completed {
            // Staged writes live only in the overlay, so rollback is just drop.
            self.completed = true;
        }
    }
}

impl<S> Drop for OwnedAsyncProllyTransaction<S>
where
    S: AsyncStore + AsyncManifestStore + AsyncTransactionalStore,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    fn drop(&mut self) {
        if !self.completed {
            // Staged writes live only in the overlay, so rollback is just drop.
            self.completed = true;
        }
    }
}

impl<S> Prolly<S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    /// Start a strict optimistic transaction.
    pub fn begin_transaction(&self) -> Result<ProllyTransaction<'_, S>, Error> {
        ProllyTransaction::new(self)
    }

    /// Start a strict optimistic transaction that owns a cloned store handle.
    ///
    /// This variant is intended for FFI bindings and other APIs that cannot
    /// hold Rust borrows across calls.
    pub fn begin_owned_transaction(&self) -> Result<OwnedProllyTransaction<S>, Error> {
        OwnedProllyTransaction::new(self)
    }

    /// Run a closure in a transaction, committing on success and rolling back
    /// automatically when the closure returns an error or commit validation
    /// fails.
    pub fn transaction<T>(
        &self,
        f: impl FnOnce(&mut ProllyTransaction<'_, S>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let mut tx = self.begin_transaction()?;
        let value = f(&mut tx)?;
        match tx.commit()? {
            TransactionUpdate::Applied { .. } => Ok(value),
            TransactionUpdate::Conflict(conflict) => Err(Error::TransactionConflict(conflict)),
        }
    }
}
impl<S> AsyncProlly<S>
where
    S: AsyncStore + AsyncManifestStore + AsyncTransactionalStore,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    /// Start a strict optimistic async transaction.
    pub fn begin_transaction(&self) -> Result<AsyncProllyTransaction<'_, S>, Error> {
        AsyncProllyTransaction::new(self)
    }

    /// Start a strict optimistic async transaction that owns a cloned store
    /// handle and can therefore outlive a borrow of this manager.
    pub fn begin_owned_transaction(&self) -> Result<OwnedAsyncProllyTransaction<S>, Error>
    where
        S: Clone,
    {
        OwnedAsyncProllyTransaction::new(self)
    }

    /// Run a boxed future in a transaction, committing on success and rolling
    /// back automatically when the future returns an error or commit validation
    /// fails.
    pub async fn transaction<T, F>(&self, f: F) -> Result<T, Error>
    where
        F: for<'tx> FnOnce(
            &'tx mut AsyncProllyTransaction<'_, S>,
        ) -> Pin<Box<dyn Future<Output = Result<T, Error>> + 'tx>>,
    {
        let mut tx = self.begin_transaction()?;
        let value = f(&mut tx).await?;
        match tx.commit().await? {
            TransactionUpdate::Applied { .. } => Ok(value),
            TransactionUpdate::Conflict(conflict) => Err(Error::TransactionConflict(conflict)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::store::{
        MemStore, MemStoreError, NodePublicationHint, PublicationOrigin, SyncStoreAsAsync,
    };
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{
        future::Future,
        task::{Context, Poll},
    };

    #[derive(Clone, Default)]
    struct CountingBatchStore {
        inner: Arc<MemStore>,
        point_reads: Arc<AtomicUsize>,
        batch_reads: Arc<AtomicUsize>,
        batch_keys: Arc<AtomicUsize>,
        publication_calls: Arc<AtomicUsize>,
        root_reads: Arc<AtomicUsize>,
    }

    impl Store for CountingBatchStore {
        type Error = MemStoreError;

        fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.point_reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get(key)
        }

        fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.inner.put(key, value)
        }

        fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
            self.inner.delete(key)
        }

        fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
            self.inner.batch(ops)
        }

        fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            self.batch_reads.fetch_add(1, Ordering::Relaxed);
            self.batch_keys.fetch_add(keys.len(), Ordering::Relaxed);
            self.inner.batch_get_ordered(keys)
        }

        fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
            self.inner.batch_get(keys)
        }

        fn prefers_batch_reads(&self) -> bool {
            true
        }

        fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
            self.publication_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.publish_nodes(publication)
        }
    }

    impl ManifestStore for CountingBatchStore {
        type Error = MemStoreError;

        fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
            self.root_reads.fetch_add(1, Ordering::Relaxed);
            self.inner.get_root(name)
        }

        fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
            self.inner.put_root(name, manifest)
        }

        fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
            self.inner.delete_root(name)
        }

        fn compare_and_swap_root(
            &self,
            name: &[u8],
            expected: Option<&RootManifest>,
            new: Option<&RootManifest>,
        ) -> Result<ManifestUpdate, Self::Error> {
            self.inner.compare_and_swap_root(name, expected, new)
        }
    }

    impl TransactionalStore for CountingBatchStore {
        fn supports_transactions(&self) -> bool {
            true
        }

        fn commit_transaction(
            &self,
            node_writes: &[TransactionNodeWrite],
            root_conditions: &[RootCondition],
            root_writes: &[RootWrite],
        ) -> Result<TransactionUpdate, Error> {
            self.inner
                .commit_transaction(node_writes, root_conditions, root_writes)
        }
    }

    fn seed(store: &CountingBatchStore) {
        store.inner.put(b"a", b"base-a").unwrap();
        store.inner.put(b"b", b"base-b").unwrap();
        store.inner.put(b"c", b"base-c").unwrap();
    }

    #[test]
    fn transaction_overlay_reuses_the_first_pinned_root_read() {
        let base = CountingBatchStore::default();
        let first =
            RootManifest::from_tree(&Tree::new(crate::Config::default())).with_updated_at_millis(1);
        let moved = first.clone().with_updated_at_millis(2);
        base.inner.put_root(b"root", &first).unwrap();
        let overlay =
            TransactionOverlayStore::new(&base, Arc::new(Mutex::new(TransactionState::default())));

        assert_eq!(overlay.get_root(b"root").unwrap(), Some(first.clone()));
        base.inner.put_root(b"root", &moved).unwrap();
        assert_eq!(overlay.get_root(b"root").unwrap(), Some(first));
        assert_eq!(base.root_reads.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn async_transaction_overlay_reuses_the_first_pinned_root_read() {
        block_on(async {
            let base = CountingBatchStore::default();
            let first = RootManifest::from_tree(&Tree::new(crate::Config::default()))
                .with_updated_at_millis(1);
            let moved = first.clone().with_updated_at_millis(2);
            base.inner.put_root(b"root", &first).unwrap();
            let adapter = SyncStoreAsAsync::new(Arc::new(base.clone()));
            let overlay = AsyncTransactionOverlayStore::new(
                &adapter,
                Arc::new(Mutex::new(TransactionState::default())),
            );

            assert_eq!(
                AsyncManifestStore::get_root(&overlay, b"root")
                    .await
                    .unwrap(),
                Some(first.clone())
            );
            base.inner.put_root(b"root", &moved).unwrap();
            assert_eq!(
                AsyncManifestStore::get_root(&overlay, b"root")
                    .await
                    .unwrap(),
                Some(first)
            );
            assert_eq!(base.root_reads.load(Ordering::Relaxed), 1);
        });
    }

    #[test]
    fn async_transaction_overlay_batch_preserves_order_missing_and_pins() {
        block_on(async {
            let base = CountingBatchStore::default();
            let first = RootManifest::from_tree(&Tree::new(crate::Config::default()))
                .with_updated_at_millis(1);
            let moved = first.clone().with_updated_at_millis(2);
            base.inner.put_root(b"root", &first).unwrap();
            let adapter = SyncStoreAsAsync::new(Arc::new(base.clone()));
            let overlay = AsyncTransactionOverlayStore::new(
                &adapter,
                Arc::new(Mutex::new(TransactionState::default())),
            );
            let names = [
                b"root".as_slice(),
                b"missing".as_slice(),
                b"root".as_slice(),
            ];

            assert_eq!(
                AsyncManifestStore::get_roots_ordered(&overlay, &names)
                    .await
                    .unwrap(),
                vec![Some(first.clone()), None, Some(first.clone())]
            );
            base.inner.put_root(b"root", &moved).unwrap();
            assert_eq!(
                AsyncManifestStore::get_roots_ordered(&overlay, &names)
                    .await
                    .unwrap(),
                vec![Some(first.clone()), None, Some(first)]
            );
            assert_eq!(base.root_reads.load(Ordering::Relaxed), 3);
        });
    }

    fn assert_overlay_reads<S: Store<Error = TransactionOverlayError>>(overlay: &S) {
        overlay.put(b"a", b"staged-a").unwrap();
        overlay.delete(b"b").unwrap();
        let values = overlay
            .batch_get_ordered(&[b"a", b"c", b"a", b"b", b"missing"])
            .unwrap();
        assert_eq!(
            values,
            vec![
                Some(b"staged-a".to_vec()),
                Some(b"base-c".to_vec()),
                Some(b"staged-a".to_vec()),
                None,
                None,
            ]
        );
    }

    fn assert_overlay_absorbs_publication<S: Store<Error = TransactionOverlayError>>(overlay: &S) {
        let entries = [(b"staged-node".as_slice(), b"node-bytes".as_slice())];
        overlay
            .publish_nodes(NodePublication::with_hint(
                &entries,
                NodePublicationHint::new(b"namespace", b"key", b"hint"),
                PublicationOrigin::PointUpsert,
            ))
            .unwrap();
        assert_eq!(
            overlay.get(b"staged-node").unwrap(),
            Some(b"node-bytes".to_vec())
        );
        assert!(!overlay.supports_hints());
    }

    async fn assert_async_overlay_absorbs_publication<
        S: AsyncStore<Error = TransactionOverlayError>,
    >(
        overlay: &S,
    ) {
        let entries = [(b"staged-node".as_slice(), b"node-bytes".as_slice())];
        overlay
            .publish_nodes(NodePublication::with_hint(
                &entries,
                NodePublicationHint::new(b"namespace", b"key", b"hint"),
                PublicationOrigin::PointUpsert,
            ))
            .await
            .unwrap();
        assert_eq!(
            overlay.get(b"staged-node").await.unwrap(),
            Some(b"node-bytes".to_vec())
        );
        assert!(!overlay.supports_hints());
    }

    #[test]
    fn sync_transaction_overlays_absorb_publication_context() {
        let base = CountingBatchStore::default();
        let overlay =
            TransactionOverlayStore::new(&base, Arc::new(Mutex::new(TransactionState::default())));
        assert_overlay_absorbs_publication(&overlay);
        assert_eq!(base.publication_calls.load(Ordering::Relaxed), 0);

        let base = CountingBatchStore::default();
        let counters = base.clone();
        let overlay = OwnedTransactionOverlayStore::new(
            base,
            Arc::new(Mutex::new(TransactionState::default())),
        );
        assert_overlay_absorbs_publication(&overlay);
        assert_eq!(counters.publication_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn async_transaction_overlays_absorb_publication_context() {
        block_on(async {
            let base = CountingBatchStore::default();
            let counters = base.clone();
            let base = SyncStoreAsAsync::new(base);
            let overlay = AsyncTransactionOverlayStore::new(
                &base,
                Arc::new(Mutex::new(TransactionState::default())),
            );
            assert_async_overlay_absorbs_publication(&overlay).await;
            assert_eq!(counters.publication_calls.load(Ordering::Relaxed), 0);

            let base = CountingBatchStore::default();
            let counters = base.clone();
            let overlay = OwnedAsyncTransactionOverlayStore::new(
                SyncStoreAsAsync::new(base),
                Arc::new(Mutex::new(TransactionState::default())),
            );
            assert_async_overlay_absorbs_publication(&overlay).await;
            assert_eq!(counters.publication_calls.load(Ordering::Relaxed), 0);
        });
    }

    #[test]
    fn borrowed_overlay_batch_reads_preserve_order_and_staged_values() {
        let base = CountingBatchStore::default();
        seed(&base);
        let overlay =
            TransactionOverlayStore::new(&base, Arc::new(Mutex::new(TransactionState::default())));
        assert_overlay_reads(&overlay);
        assert_eq!(base.point_reads.load(Ordering::Relaxed), 0);
        assert_eq!(base.batch_reads.load(Ordering::Relaxed), 1);
        assert_eq!(base.batch_keys.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn owned_overlay_batch_reads_preserve_order_and_staged_values() {
        let base = CountingBatchStore::default();
        seed(&base);
        let counters = base.clone();
        let overlay = OwnedTransactionOverlayStore::new(
            base,
            Arc::new(Mutex::new(TransactionState::default())),
        );
        assert_overlay_reads(&overlay);
        assert_eq!(counters.point_reads.load(Ordering::Relaxed), 0);
        assert_eq!(counters.batch_reads.load(Ordering::Relaxed), 1);
        assert_eq!(counters.batch_keys.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn transaction_cas_preserves_explicit_manifest_timestamp() {
        let store = Arc::new(MemStore::new());
        let engine = Prolly::new(store.clone(), crate::prolly::Config::default());
        let tree = engine
            .put(&engine.create(), b"key".to_vec(), b"value".to_vec())
            .unwrap();
        let tx = engine.begin_transaction().unwrap();
        assert!(matches!(
            tx.compare_and_swap_named_root_at_millis(b"root", None, Some(&tree), 42_000)
                .unwrap(),
            NamedRootUpdate::Applied
        ));
        assert!(matches!(
            tx.commit().unwrap(),
            TransactionUpdate::Applied { .. }
        ));
        let manifest = store.get_root(b"root").unwrap().unwrap();
        assert_eq!(manifest.created_at_millis, Some(42_000));
        assert_eq!(manifest.updated_at_millis, Some(42_000));
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }
    #[test]
    fn async_overlay_batch_reads_use_one_base_batch_without_holding_staged_state() {
        use crate::prolly::store::SyncStoreAsAsync;

        let base = CountingBatchStore::default();
        seed(&base);
        let counters = base.clone();
        let base = SyncStoreAsAsync::new(base);
        let overlay = AsyncTransactionOverlayStore::new(
            &base,
            Arc::new(Mutex::new(TransactionState::default())),
        );
        block_on(async {
            overlay.put(b"a", b"staged-a").await.unwrap();
            overlay.delete(b"b").await.unwrap();
            let values = overlay
                .batch_get_ordered(&[b"a", b"c", b"a", b"b", b"missing"])
                .await
                .unwrap();
            assert_eq!(
                values,
                vec![
                    Some(b"staged-a".to_vec()),
                    Some(b"base-c".to_vec()),
                    Some(b"staged-a".to_vec()),
                    None,
                    None,
                ]
            );
        });
        assert_eq!(counters.point_reads.load(Ordering::Relaxed), 0);
        assert_eq!(counters.batch_reads.load(Ordering::Relaxed), 1);
        assert_eq!(counters.batch_keys.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn async_overlay_unique_batch_reads_use_one_base_batch() {
        let base = CountingBatchStore::default();
        seed(&base);
        let counters = base.clone();
        let base = SyncStoreAsAsync::new(base);
        let overlay = AsyncTransactionOverlayStore::new(
            &base,
            Arc::new(Mutex::new(TransactionState::default())),
        );
        block_on(async {
            let values = overlay
                .batch_get_ordered_unique(&[b"a", b"b", b"missing"])
                .await
                .unwrap();
            assert_eq!(
                values,
                vec![Some(b"base-a".to_vec()), Some(b"base-b".to_vec()), None]
            );
        });
        assert_eq!(counters.point_reads.load(Ordering::Relaxed), 0);
        assert_eq!(counters.batch_reads.load(Ordering::Relaxed), 1);
        assert_eq!(counters.batch_keys.load(Ordering::Relaxed), 3);
    }

    #[test]
    fn committed_transaction_nodes_are_promoted_but_rollback_nodes_are_isolated() {
        let store = Arc::new(MemStore::new());
        let engine = Prolly::new(store, crate::Config::default());

        let rolled_back = {
            let tx = engine.begin_transaction().unwrap();
            let tree = tx
                .put(&tx.create(), b"rolled-back".to_vec(), b"value".to_vec())
                .unwrap();
            tx.rollback();
            tree
        };
        assert_eq!(engine.cache_len(), 0);
        assert!(matches!(
            engine.get(&rolled_back, b"rolled-back"),
            Err(Error::NotFound(_))
        ));

        let committed = {
            let tx = engine.begin_transaction().unwrap();
            let tree = tx
                .put(&tx.create(), b"committed".to_vec(), b"value".to_vec())
                .unwrap();
            assert!(matches!(
                tx.commit().unwrap(),
                TransactionUpdate::Applied { .. }
            ));
            tree
        };
        assert!(engine.cache_len() > 0);
        assert_eq!(
            engine.get(&committed, b"committed").unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn conflicting_transaction_does_not_promote_uncommitted_nodes() {
        let store = Arc::new(MemStore::new());
        let engine = Prolly::new(store.clone(), crate::Config::default());
        let initial = engine
            .put(&engine.create(), b"base".to_vec(), b"value".to_vec())
            .unwrap();
        engine
            .compare_and_swap_named_root(b"root", None, Some(&initial))
            .unwrap();
        engine.clear_cache();

        let tx = engine.begin_transaction().unwrap();
        let observed = tx.load_named_root(b"root").unwrap().unwrap();
        let candidate = tx
            .put(&observed, b"candidate".to_vec(), b"value".to_vec())
            .unwrap();
        tx.compare_and_swap_named_root(b"root", Some(&observed), Some(&candidate))
            .unwrap();

        let moved = engine
            .put(&initial, b"winner".to_vec(), b"value".to_vec())
            .unwrap();
        assert!(matches!(
            engine.compare_and_swap_named_root(b"root", Some(&initial), Some(&moved)),
            Ok(NamedRootUpdate::Applied)
        ));
        engine.clear_cache();
        assert!(matches!(
            tx.commit().unwrap(),
            TransactionUpdate::Conflict(_)
        ));
        assert!(matches!(
            engine.get(&candidate, b"candidate"),
            Err(Error::NotFound(_))
        ));
    }

    #[test]
    fn owned_commit_promotes_cache_for_later_borrowed_transactions() {
        block_on(async {
            let store = CountingBatchStore::default();
            let counters = store.clone();
            let engine = AsyncProlly::new(SyncStoreAsAsync::new(store), crate::Config::default());
            let committed = {
                let tx = engine.begin_owned_transaction().unwrap();
                let tree = tx
                    .put(&tx.create(), b"committed".to_vec(), b"value".to_vec())
                    .await
                    .unwrap();
                assert!(matches!(
                    tx.commit().await.unwrap(),
                    TransactionUpdate::Applied { .. }
                ));
                tree
            };
            counters.point_reads.store(0, Ordering::Relaxed);
            counters.batch_reads.store(0, Ordering::Relaxed);

            let tx = engine.begin_transaction().unwrap();
            assert_eq!(
                tx.get(&committed, b"committed").await.unwrap(),
                Some(b"value".to_vec())
            );
            tx.rollback();
            assert_eq!(counters.point_reads.load(Ordering::Relaxed), 0);
            assert_eq!(counters.batch_reads.load(Ordering::Relaxed), 0);

            let rolled_back = {
                let tx = engine.begin_owned_transaction().unwrap();
                let tree = tx
                    .put(&committed, b"rolled-back".to_vec(), b"value".to_vec())
                    .await
                    .unwrap();
                tx.rollback();
                tree
            };
            engine.clear_cache();
            assert!(matches!(
                engine.get(&rolled_back, b"rolled-back").await,
                Err(Error::NotFound(_))
            ));
        });
    }
}
