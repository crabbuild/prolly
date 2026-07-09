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
use super::store::{BatchOp, Store};
use super::tree::Tree;
use super::Prolly;
#[cfg(feature = "async-store")]
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
    Conflict(TransactionConflict),
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
    /// Whether this backend can atomically commit staged nodes and roots.
    fn supports_transactions(&self) -> bool {
        false
    }

    /// Atomically validate root conditions, write nodes, and apply root writes.
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
#[cfg(feature = "async-store")]
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

#[cfg(feature = "async-store")]
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

#[cfg(feature = "async-store")]
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
#[derive(Clone)]
pub struct TransactionOverlayStore<'a, S> {
    base: &'a S,
    state: Arc<Mutex<TransactionState>>,
}

impl<'a, S> TransactionOverlayStore<'a, S> {
    fn new(base: &'a S, state: Arc<Mutex<TransactionState>>) -> Self {
        Self { base, state }
    }

    fn lock(&self) -> Result<MutexGuard<'_, TransactionState>, TransactionOverlayError> {
        self.state.lock().map_err(TransactionOverlayError::poisoned)
    }
}

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
}

impl<S> ManifestStore for TransactionOverlayStore<'_, S>
where
    S: Store + ManifestStore,
{
    type Error = TransactionOverlayError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        if let Some(write) = self.lock()?.root_writes.get(name).cloned() {
            return Ok(write.replacement().cloned());
        }

        let current = self
            .base
            .get_root(name)
            .map_err(TransactionOverlayError::store)?;
        let mut state = self.lock()?;
        state
            .root_reads
            .entry(name.to_vec())
            .or_insert_with(|| current.clone());
        Ok(current)
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
#[derive(Clone)]
pub struct OwnedTransactionOverlayStore<S> {
    base: S,
    state: Arc<Mutex<TransactionState>>,
}

impl<S> OwnedTransactionOverlayStore<S> {
    fn new(base: S, state: Arc<Mutex<TransactionState>>) -> Self {
        Self { base, state }
    }

    fn lock(&self) -> Result<MutexGuard<'_, TransactionState>, TransactionOverlayError> {
        self.state.lock().map_err(TransactionOverlayError::poisoned)
    }
}

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
}

impl<S> ManifestStore for OwnedTransactionOverlayStore<S>
where
    S: Store + ManifestStore,
{
    type Error = TransactionOverlayError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        if let Some(write) = self.lock()?.root_writes.get(name).cloned() {
            return Ok(write.replacement().cloned());
        }

        let current = self
            .base
            .get_root(name)
            .map_err(TransactionOverlayError::store)?;
        let mut state = self.lock()?;
        state
            .root_reads
            .entry(name.to_vec())
            .or_insert_with(|| current.clone());
        Ok(current)
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

/// Async store overlay used internally by [`AsyncProllyTransaction`].
#[cfg(feature = "async-store")]
#[derive(Clone)]
pub struct AsyncTransactionOverlayStore<'a, S> {
    base: &'a S,
    state: Arc<Mutex<TransactionState>>,
}

#[cfg(feature = "async-store")]
impl<'a, S> AsyncTransactionOverlayStore<'a, S> {
    fn new(base: &'a S, state: Arc<Mutex<TransactionState>>) -> Self {
        Self { base, state }
    }

    fn lock(&self) -> Result<MutexGuard<'_, TransactionState>, TransactionOverlayError> {
        self.state.lock().map_err(TransactionOverlayError::poisoned)
    }
}

#[cfg(feature = "async-store")]
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
            None => self
                .base
                .get(key)
                .await
                .map_err(TransactionOverlayError::store),
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
}

#[cfg(feature = "async-store")]
impl<S> AsyncManifestStore for AsyncTransactionOverlayStore<'_, S>
where
    S: AsyncStore + AsyncManifestStore,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    type Error = TransactionOverlayError;

    async fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        if let Some(write) = self.lock()?.root_writes.get(name).cloned() {
            return Ok(write.replacement().cloned());
        }

        let current = self
            .base
            .get_root(name)
            .await
            .map_err(TransactionOverlayError::store)?;
        let mut state = self.lock()?;
        state
            .root_reads
            .entry(name.to_vec())
            .or_insert_with(|| current.clone());
        Ok(current)
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
    base: &'a Prolly<S>,
    state: Arc<Mutex<TransactionState>>,
    manager: Prolly<TransactionOverlayStore<'a, S>>,
    completed: bool,
}

impl<'a, S> ProllyTransaction<'a, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    fn new(base: &'a Prolly<S>) -> Result<Self, Error> {
        if !base.store.supports_transactions() {
            return Err(Error::UnsupportedTransactions {
                store: type_name::<S>(),
            });
        }

        let state = Arc::new(Mutex::new(TransactionState::default()));
        let overlay = TransactionOverlayStore::new(&base.store, state.clone());
        let manager = Prolly::new(overlay, base.config.clone());
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
    pub fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.manager.get(tree, key)
    }

    /// Insert or update a key/value pair, staging rewritten nodes.
    pub fn put(&self, tree: &Tree, key: Vec<u8>, value: Vec<u8>) -> Result<Tree, Error> {
        self.manager.put(tree, key, value)
    }

    /// Delete a key, staging rewritten nodes.
    pub fn delete(&self, tree: &Tree, key: &[u8]) -> Result<Tree, Error> {
        self.manager.delete(tree, key)
    }

    /// Apply a batch of logical map mutations inside the transaction.
    pub fn batch(&self, tree: &Tree, mutations: Vec<Mutation>) -> Result<Tree, Error> {
        self.manager.batch(tree, mutations)
    }

    /// Load a named root and add it to the transaction read set.
    pub fn load_named_root(&self, name: &[u8]) -> Result<Option<Tree>, Error> {
        self.manager.load_named_root(name)
    }

    /// Stage an unconditional named-root publish.
    pub fn publish_named_root(&self, name: &[u8], tree: &Tree) -> Result<(), Error> {
        self.manager.publish_named_root(name, tree)
    }

    /// Stage an unconditional named-root delete.
    pub fn delete_named_root(&self, name: &[u8]) -> Result<(), Error> {
        self.manager.delete_named_root(name)
    }

    /// Stage a named-root CAS update.
    pub fn compare_and_swap_named_root(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
    ) -> Result<NamedRootUpdate, Error> {
        self.manager
            .compare_and_swap_named_root(name, expected, new)
    }

    /// Discard all staged writes. Dropping an uncommitted transaction has the
    /// same effect; this method is useful when callers want to be explicit.
    pub fn rollback(mut self) {
        self.completed = true;
    }

    /// Commit staged node and named-root writes atomically.
    pub fn commit(mut self) -> Result<TransactionUpdate, Error> {
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

        let update =
            self.base
                .store
                .commit_transaction(&node_writes, &root_conditions, &root_writes)?;
        self.completed = true;
        Ok(update)
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
    base_store: S,
    state: Arc<Mutex<TransactionState>>,
    manager: Prolly<OwnedTransactionOverlayStore<S>>,
    completed: bool,
}

impl<S> OwnedProllyTransaction<S>
where
    S: Store + ManifestStore + TransactionalStore + Clone,
{
    fn new(base: &Prolly<S>) -> Result<Self, Error> {
        if !base.store.supports_transactions() {
            return Err(Error::UnsupportedTransactions {
                store: type_name::<S>(),
            });
        }

        let base_store = base.store.clone();
        let state = Arc::new(Mutex::new(TransactionState::default()));
        let overlay = OwnedTransactionOverlayStore::new(base_store.clone(), state.clone());
        let manager = Prolly::new(overlay, base.config.clone());
        Ok(Self {
            base_store,
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
    pub fn get(&self, tree: &Tree, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.manager.get(tree, key)
    }

    /// Insert or update a key/value pair, staging rewritten nodes.
    pub fn put(&self, tree: &Tree, key: Vec<u8>, value: Vec<u8>) -> Result<Tree, Error> {
        self.manager.put(tree, key, value)
    }

    /// Delete a key, staging rewritten nodes.
    pub fn delete(&self, tree: &Tree, key: &[u8]) -> Result<Tree, Error> {
        self.manager.delete(tree, key)
    }

    /// Apply a batch of logical map mutations inside the transaction.
    pub fn batch(&self, tree: &Tree, mutations: Vec<Mutation>) -> Result<Tree, Error> {
        self.manager.batch(tree, mutations)
    }

    /// Load a named root and add it to the transaction read set.
    pub fn load_named_root(&self, name: &[u8]) -> Result<Option<Tree>, Error> {
        self.manager.load_named_root(name)
    }

    /// Stage an unconditional named-root publish.
    pub fn publish_named_root(&self, name: &[u8], tree: &Tree) -> Result<(), Error> {
        self.manager.publish_named_root(name, tree)
    }

    /// Stage an unconditional named-root delete.
    pub fn delete_named_root(&self, name: &[u8]) -> Result<(), Error> {
        self.manager.delete_named_root(name)
    }

    /// Stage a named-root CAS update.
    pub fn compare_and_swap_named_root(
        &self,
        name: &[u8],
        expected: Option<&Tree>,
        new: Option<&Tree>,
    ) -> Result<NamedRootUpdate, Error> {
        self.manager
            .compare_and_swap_named_root(name, expected, new)
    }

    /// Discard all staged writes. Dropping an uncommitted transaction has the
    /// same effect; this method is useful when callers want to be explicit.
    pub fn rollback(mut self) {
        self.completed = true;
    }

    /// Commit staged node and named-root writes atomically.
    pub fn commit(mut self) -> Result<TransactionUpdate, Error> {
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

        let update =
            self.base_store
                .commit_transaction(&node_writes, &root_conditions, &root_writes)?;
        self.completed = true;
        Ok(update)
    }
}

/// A strict optimistic transaction over an [`AsyncProlly`] manager.
#[cfg(feature = "async-store")]
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

#[cfg(feature = "async-store")]
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
        let overlay = AsyncTransactionOverlayStore::new(&base.store, state.clone());
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

    /// Stage an unconditional named-root publish.
    pub async fn publish_named_root(&self, name: &[u8], tree: &Tree) -> Result<(), Error> {
        self.manager.publish_named_root(name, tree).await
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
        self.completed = true;
        Ok(update)
    }
}

#[cfg(feature = "async-store")]
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

impl<S> Drop for ProllyTransaction<'_, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    fn drop(&mut self) {
        if !self.completed {
            // Staged writes live only in the overlay, so rollback is just drop.
            self.completed = true;
        }
    }
}

impl<S> Drop for OwnedProllyTransaction<S>
where
    S: Store + ManifestStore + TransactionalStore,
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
    pub fn begin_owned_transaction(&self) -> Result<OwnedProllyTransaction<S>, Error>
    where
        S: Clone,
    {
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

#[cfg(feature = "async-store")]
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
