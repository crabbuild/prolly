//! Remote storage adapter contracts for `prolly-map`.
//!
//! The core crate intentionally keeps cloud SDK and database driver dependencies
//! out of `prolly-map`. Provider-specific crates own concrete clients and
//! implement [`RemoteStoreBackend`], while this module provides the shared
//! async store wrapper, manifest encoding, CID verification, and conformance
//! helpers.

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use super::cid::Cid;
use super::config::Config;
use super::error::Error;
use super::manifest::{
    AsyncManifestStore, AsyncManifestStoreScan, ManifestUpdate, NamedRootManifest, RootManifest,
};
use super::store::{AsyncStore, BatchOp, NodePublication, NodePublicationHint, PublicationOrigin};
use super::transaction::{
    AsyncTransactionalStore, RootCondition, RootWrite, TransactionConflict, TransactionNodeWrite,
    TransactionUpdate,
};
#[cfg(feature = "tokio")]
use super::{
    manifest::{ManifestStore, ManifestStoreScan},
    store::{NodeStoreScan, Store},
};

/// Batch operation passed to a remote backend.
#[derive(Debug, Clone, Copy)]
pub enum RemoteBatchOp<'a> {
    /// Insert or update a content-addressed node.
    Upsert { key: &'a [u8], value: &'a [u8] },
    /// Delete a content-addressed node.
    Delete { key: &'a [u8] },
}

/// Raw named root manifest returned by a backend scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteNamedRoot {
    /// Durable root name.
    pub name: Vec<u8>,
    /// Serialized [`RootManifest`] bytes.
    pub manifest: Vec<u8>,
}

impl RemoteNamedRoot {
    /// Create a raw named root record.
    pub fn new(name: Vec<u8>, manifest: Vec<u8>) -> Self {
        Self { name, manifest }
    }
}

/// Result of a backend-level root compare-and-swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteManifestUpdate {
    /// The expected manifest matched and the update was applied.
    Applied,
    /// The expected manifest did not match the current manifest bytes.
    Conflict {
        /// Current serialized manifest stored under the requested name.
        current: Option<Vec<u8>>,
    },
}

/// Serialized named-root value that must still match at transaction commit time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteRootCondition {
    /// Durable root name.
    pub name: Vec<u8>,
    /// Serialized manifest observed by the transaction.
    pub expected: Option<Vec<u8>>,
}

impl RemoteRootCondition {
    /// Create a serialized root validation condition.
    pub fn new(name: Vec<u8>, expected: Option<Vec<u8>>) -> Self {
        Self { name, expected }
    }
}

/// Serialized named-root write staged by a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteRootWrite {
    /// Insert or replace a serialized named root manifest.
    Put {
        /// Durable root name.
        name: Vec<u8>,
        /// Serialized manifest to store under `name`.
        manifest: Vec<u8>,
    },
    /// Delete a named root.
    Delete {
        /// Durable root name.
        name: Vec<u8>,
    },
}

/// Details for a failed backend-level transaction validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteTransactionConflict {
    /// Durable root name that failed validation.
    pub name: Vec<u8>,
    /// Serialized manifest expected by the transaction.
    pub expected: Option<Vec<u8>>,
    /// Serialized manifest currently stored by the backend.
    pub current: Option<Vec<u8>>,
}

impl RemoteTransactionConflict {
    /// Create a backend transaction conflict record.
    pub fn new(name: Vec<u8>, expected: Option<Vec<u8>>, current: Option<Vec<u8>>) -> Self {
        Self {
            name,
            expected,
            current,
        }
    }
}

/// Result of a backend-level transaction commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemoteTransactionUpdate {
    /// The transaction was applied.
    Applied,
    /// A root condition failed; no writes were applied.
    Conflict(RemoteTransactionConflict),
}

/// Backend capability contract used by all provider adapters.
///
/// Implementations should map these operations to native provider primitives:
///
/// - SQL stores should use transactions for `batch_nodes` and root CAS.
/// - DynamoDB should use conditional writes for root CAS.
/// - Cosmos DB should use partitioned documents and ETag or conditional writes.
/// - Spanner should use read/write transactions for root CAS.
/// - Redis should use Lua or `WATCH`/`MULTI` for root CAS and requires durable
///   persistence if used as a primary store.
#[allow(async_fn_in_trait)]
pub trait RemoteStoreBackend: Send + Sync {
    /// Backend error type.
    type Error: StdError + Send + Sync + 'static;

    /// Read one content-addressed node by CID bytes.
    async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Store one content-addressed node by CID bytes.
    async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    /// Delete one content-addressed node by CID bytes.
    async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error>;

    /// Apply node writes/deletes. Implementations should make this atomic when
    /// the provider supports it.
    async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
        for op in ops {
            match op {
                RemoteBatchOp::Upsert { key, value } => self.put_node(key, value).await?,
                RemoteBatchOp::Delete { key } => self.delete_node(key).await?,
            }
        }
        Ok(())
    }

    /// Read unique node keys in request order.
    async fn batch_get_nodes_ordered(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let mut values = Vec::with_capacity(keys.len());
        for key in keys {
            values.push(self.get_node(key).await?);
        }
        Ok(values)
    }

    /// Store multiple content-addressed nodes.
    async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let ops = entries
            .iter()
            .map(|(key, value)| RemoteBatchOp::Upsert { key, value })
            .collect::<Vec<_>>();
        self.batch_nodes(&ops).await
    }

    /// List all content-addressed node CIDs.
    ///
    /// Implementations must return only node CIDs, not hints, root manifests,
    /// or provider metadata. Results should be sorted by raw CID bytes for
    /// deterministic retention and GC planning.
    async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error>;

    /// Whether this backend has native or efficiently coalesced batch reads.
    fn prefers_batch_reads(&self) -> bool {
        false
    }

    /// Maximum in-flight reads for default async traversal paths.
    fn read_parallelism(&self) -> usize {
        1
    }

    /// Whether this backend persists optional performance hints.
    fn supports_hints(&self) -> bool {
        false
    }

    /// Whether append-heavy writes should maintain rightmost-path hints.
    fn prefers_rightmost_path_hints(&self) -> bool {
        false
    }

    /// Read an optional performance hint.
    async fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let _ = (namespace, key);
        Ok(None)
    }

    /// Write an optional performance hint.
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
    async fn batch_put_nodes_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.batch_put_nodes(entries).await?;
        self.put_hint(namespace, key, value).await
    }

    /// Publish canonical immutable nodes with advisory logical context.
    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        match publication.hint() {
            Some(hint) => {
                self.batch_put_nodes_with_hint(
                    publication.entries(),
                    hint.namespace(),
                    hint.key(),
                    hint.value(),
                )
                .await
            }
            None => self.batch_put_nodes(publication.entries()).await,
        }
    }

    /// Read a serialized root manifest.
    async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error>;

    /// Write a serialized root manifest unconditionally.
    async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error>;

    /// Delete a root manifest.
    async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error>;

    /// Compare-and-swap a serialized root manifest.
    async fn compare_and_swap_root_manifest(
        &self,
        name: &[u8],
        expected: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<RemoteManifestUpdate, Self::Error>;

    /// List serialized root manifests sorted by raw name bytes.
    async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error>;

    /// Whether this backend can atomically validate root conditions and commit
    /// staged node/root writes.
    fn supports_transactions(&self) -> bool {
        false
    }

    /// Atomically validate serialized root conditions, write nodes, and apply
    /// serialized root writes. Implementations should return
    /// [`RemoteTransactionUpdate::Conflict`] without applying any writes when a
    /// condition fails.
    async fn commit_transaction(
        &self,
        _node_writes: &[RemoteBatchOp<'_>],
        _root_conditions: &[RemoteRootCondition],
        _root_writes: &[RemoteRootWrite],
    ) -> Result<RemoteTransactionUpdate, Self::Error> {
        unreachable!("remote backend did not advertise transaction support")
    }
}

impl<T: RemoteStoreBackend> RemoteStoreBackend for Arc<T> {
    type Error = T::Error;

    async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get_node(key).await
    }

    async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        (**self).put_node(key, value).await
    }

    async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
        (**self).delete_node(key).await
    }

    async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
        (**self).batch_nodes(ops).await
    }

    async fn batch_get_nodes_ordered(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        (**self).batch_get_nodes_ordered(keys).await
    }

    async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        (**self).batch_put_nodes(entries).await
    }

    async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        (**self).list_node_cids().await
    }

    fn prefers_batch_reads(&self) -> bool {
        (**self).prefers_batch_reads()
    }

    fn read_parallelism(&self) -> usize {
        (**self).read_parallelism()
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

    async fn batch_put_nodes_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        (**self)
            .batch_put_nodes_with_hint(entries, namespace, key, value)
            .await
    }

    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        (**self).publish_nodes(publication).await
    }

    async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get_root_manifest(name).await
    }

    async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
        (**self).put_root_manifest(name, manifest).await
    }

    async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
        (**self).delete_root_manifest(name).await
    }

    async fn compare_and_swap_root_manifest(
        &self,
        name: &[u8],
        expected: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<RemoteManifestUpdate, Self::Error> {
        (**self)
            .compare_and_swap_root_manifest(name, expected, new)
            .await
    }

    async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error> {
        (**self).list_root_manifests().await
    }

    fn supports_transactions(&self) -> bool {
        (**self).supports_transactions()
    }

    async fn commit_transaction(
        &self,
        node_writes: &[RemoteBatchOp<'_>],
        root_conditions: &[RemoteRootCondition],
        root_writes: &[RemoteRootWrite],
    ) -> Result<RemoteTransactionUpdate, Self::Error> {
        (**self)
            .commit_transaction(node_writes, root_conditions, root_writes)
            .await
    }
}

/// Configuration shared by remote adapters.
#[derive(Debug, Clone)]
pub struct RemoteStoreConfig {
    /// Verify fetched and stored node bytes against their CID key.
    pub verify_node_cids: bool,
}

impl Default for RemoteStoreConfig {
    fn default() -> Self {
        Self {
            verify_node_cids: true,
        }
    }
}

/// Generic adapter over a remote backend.
#[derive(Debug, Clone)]
pub struct RemoteProllyStore<B> {
    backend: B,
    config: RemoteStoreConfig,
}

impl<B> RemoteProllyStore<B> {
    /// Create an adapter with default configuration.
    pub fn new(backend: B) -> Self {
        Self::with_config(backend, RemoteStoreConfig::default())
    }

    /// Create an adapter with explicit configuration.
    pub fn with_config(backend: B, config: RemoteStoreConfig) -> Self {
        Self { backend, config }
    }

    /// Borrow the backend.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Consume the adapter and return the backend.
    pub fn into_backend(self) -> B {
        self.backend
    }
}

/// Synchronous facade for a Tokio-based [`RemoteProllyStore`].
///
/// The facade owns a multi-thread Tokio runtime shared by all clones and
/// forwards native remote batches, hints, manifest operations, scans, and
/// strict transactions without changing their semantics. Calls made from an
/// existing Tokio runtime are executed on a scoped helper thread, avoiding
/// nested-runtime panics while preserving the synchronous API contract.
///
/// Provider crates expose concrete aliases such as `SyncPostgresStore` and
/// `SyncDynamoDbStore`. Construct the provider backend normally, wrap it here,
/// and pass the result to [`crate::Prolly::new`] before calling
/// [`crate::Prolly::indexed_map`].
#[cfg(feature = "tokio")]
#[derive(Clone, Debug)]
pub struct BlockingRemoteProllyStore<B> {
    inner: RemoteProllyStore<B>,
    runtime: Arc<BlockingRemoteRuntime>,
}

#[cfg(feature = "tokio")]
impl<B> BlockingRemoteProllyStore<B> {
    /// Create a synchronous remote store with default remote configuration.
    pub fn new(backend: B) -> Result<Self, std::io::Error> {
        Self::with_config(backend, RemoteStoreConfig::default())
    }

    /// Create a synchronous remote store with explicit remote configuration.
    pub fn with_config(backend: B, config: RemoteStoreConfig) -> Result<Self, std::io::Error> {
        let runtime = create_blocking_remote_runtime()?;
        Ok(Self {
            inner: RemoteProllyStore::with_config(backend, config),
            runtime: Arc::new(BlockingRemoteRuntime::new(runtime)),
        })
    }

    /// Build a provider backend on the facade's owned runtime.
    ///
    /// Prefer this constructor when the provider's connection or client setup
    /// is asynchronous. Runtime-owned background tasks then remain alive for
    /// exactly as long as the synchronous store and all of its clones.
    pub fn build<E, F, Fut>(builder: F) -> Result<Self, BlockingRemoteBuildError<E>>
    where
        E: StdError + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<B, E>>,
        B: Send,
    {
        Self::build_with_config(builder, RemoteStoreConfig::default())
    }

    /// Build a provider backend on the owned runtime with explicit remote
    /// configuration.
    pub fn build_with_config<E, F, Fut>(
        builder: F,
        config: RemoteStoreConfig,
    ) -> Result<Self, BlockingRemoteBuildError<E>>
    where
        E: StdError + Send + Sync + 'static,
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<B, E>>,
        B: Send,
    {
        let runtime = Arc::new(BlockingRemoteRuntime::new(
            create_blocking_remote_runtime().map_err(BlockingRemoteBuildError::Runtime)?,
        ));
        let backend = if tokio::runtime::Handle::try_current().is_ok() {
            std::thread::scope(|scope| {
                scope
                    .spawn(|| runtime.block_on(builder()))
                    .join()
                    .map_err(|_| BlockingRemoteBuildError::RuntimeBridgePanicked)?
                    .map_err(BlockingRemoteBuildError::Backend)
            })?
        } else {
            runtime
                .block_on(builder())
                .map_err(BlockingRemoteBuildError::Backend)?
        };
        Ok(Self {
            inner: RemoteProllyStore::with_config(backend, config),
            runtime,
        })
    }

    /// Borrow the wrapped asynchronous store.
    pub fn async_store(&self) -> &RemoteProllyStore<B> {
        &self.inner
    }

    /// Borrow the provider backend.
    pub fn backend(&self) -> &B {
        self.inner.backend()
    }
}

#[cfg(feature = "tokio")]
fn create_blocking_remote_runtime() -> Result<tokio::runtime::Runtime, std::io::Error> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
}

#[cfg(feature = "tokio")]
#[derive(Debug)]
struct BlockingRemoteRuntime {
    runtime: Option<tokio::runtime::Runtime>,
}

#[cfg(feature = "tokio")]
impl BlockingRemoteRuntime {
    fn new(runtime: tokio::runtime::Runtime) -> Self {
        Self {
            runtime: Some(runtime),
        }
    }

    fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.runtime
            .as_ref()
            .expect("blocking remote runtime is active")
            .block_on(future)
    }
}

#[cfg(feature = "tokio")]
impl Drop for BlockingRemoteRuntime {
    fn drop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        if tokio::runtime::Handle::try_current().is_ok() {
            // Tokio forbids dropping a runtime from one of its async contexts
            // because shutdown may block. Move the final drop to a scoped OS
            // thread so provider tasks are cancelled and joined instead of
            // being leaked through `shutdown_background`.
            let _ = std::thread::scope(|scope| scope.spawn(move || drop(runtime)).join());
        } else {
            drop(runtime);
        }
    }
}

/// Error returned while constructing a synchronous remote store.
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub enum BlockingRemoteBuildError<E> {
    /// The owned Tokio runtime could not be created.
    Runtime(std::io::Error),
    /// Provider-specific asynchronous initialization failed.
    Backend(E),
    /// A scoped runtime bridge thread panicked during initialization.
    RuntimeBridgePanicked,
}

#[cfg(feature = "tokio")]
impl<E: fmt::Display> fmt::Display for BlockingRemoteBuildError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(err) => write!(formatter, "failed to create remote runtime: {err}"),
            Self::Backend(err) => write!(formatter, "remote backend initialization failed: {err}"),
            Self::RuntimeBridgePanicked => {
                formatter.write_str("synchronous remote initialization bridge panicked")
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl<E: StdError + 'static> StdError for BlockingRemoteBuildError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Runtime(err) => Some(err),
            Self::Backend(err) => Some(err),
            Self::RuntimeBridgePanicked => None,
        }
    }
}

/// Error returned by synchronous remote-store operations.
#[cfg(feature = "tokio")]
#[derive(Debug)]
pub enum BlockingRemoteStoreError<E> {
    /// The shared asynchronous adapter rejected or failed the operation.
    Remote(RemoteAdapterError<E>),
    /// A scoped runtime bridge thread panicked.
    RuntimeBridgePanicked,
}

#[cfg(feature = "tokio")]
impl<E: fmt::Display> fmt::Display for BlockingRemoteStoreError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Remote(err) => write!(formatter, "{err}"),
            Self::RuntimeBridgePanicked => {
                formatter.write_str("synchronous remote runtime bridge panicked")
            }
        }
    }
}

#[cfg(feature = "tokio")]
impl<E: StdError + 'static> StdError for BlockingRemoteStoreError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Remote(err) => Some(err),
            Self::RuntimeBridgePanicked => None,
        }
    }
}

#[cfg(feature = "tokio")]
impl<B> BlockingRemoteProllyStore<B>
where
    B: RemoteStoreBackend + Clone + 'static,
{
    fn run<T, F, Fut>(&self, operation: F) -> Result<T, BlockingRemoteStoreError<B::Error>>
    where
        T: Send,
        F: FnOnce(RemoteProllyStore<B>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, RemoteAdapterError<B::Error>>>,
    {
        let store = self.inner.clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            return std::thread::scope(|scope| {
                scope
                    .spawn(|| self.runtime.block_on(operation(store)))
                    .join()
                    .map_err(|_| BlockingRemoteStoreError::RuntimeBridgePanicked)?
                    .map_err(BlockingRemoteStoreError::Remote)
            });
        }
        self.runtime
            .block_on(operation(store))
            .map_err(BlockingRemoteStoreError::Remote)
    }

    fn run_transaction<T, F, Fut>(&self, operation: F) -> Result<T, Error>
    where
        T: Send,
        F: FnOnce(RemoteProllyStore<B>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, Error>>,
    {
        let store = self.inner.clone();
        if tokio::runtime::Handle::try_current().is_ok() {
            return std::thread::scope(|scope| {
                scope
                    .spawn(|| self.runtime.block_on(operation(store)))
                    .join()
                    .map_err(|_| {
                        Error::Store(Box::new(
                            BlockingRemoteStoreError::<B::Error>::RuntimeBridgePanicked,
                        ))
                    })?
            });
        }
        self.runtime.block_on(operation(store))
    }
}

#[cfg(feature = "tokio")]
#[derive(Clone)]
enum OwnedBatchOp {
    Upsert { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[cfg(feature = "tokio")]
impl<B> Store for BlockingRemoteProllyStore<B>
where
    B: RemoteStoreBackend + Clone + 'static,
{
    type Error = BlockingRemoteStoreError<B::Error>;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let key = key.to_vec();
        self.run(move |store| async move { AsyncStore::get(&store, &key).await })
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let key = key.to_vec();
        let value = value.to_vec();
        self.run(move |store| async move { AsyncStore::put(&store, &key, &value).await })
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        let key = key.to_vec();
        self.run(move |store| async move { AsyncStore::delete(&store, &key).await })
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        let ops = ops
            .iter()
            .map(|op| match op {
                BatchOp::Upsert { key, value } => OwnedBatchOp::Upsert {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
                BatchOp::Delete { key } => OwnedBatchOp::Delete { key: key.to_vec() },
            })
            .collect::<Vec<_>>();
        self.run(move |store| async move {
            let borrowed = ops
                .iter()
                .map(|op| match op {
                    OwnedBatchOp::Upsert { key, value } => BatchOp::Upsert { key, value },
                    OwnedBatchOp::Delete { key } => BatchOp::Delete { key },
                })
                .collect::<Vec<_>>();
            AsyncStore::batch(&store, &borrowed).await
        })
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let keys = keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>();
        self.run(move |store| async move {
            let borrowed = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
            AsyncStore::batch_get_ordered(&store, &borrowed).await
        })
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let keys = keys.iter().map(|key| key.to_vec()).collect::<Vec<_>>();
        self.run(move |store| async move {
            let borrowed = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
            AsyncStore::batch_get_ordered_unique(&store, &borrowed).await
        })
    }

    fn prefers_batch_reads(&self) -> bool {
        AsyncStore::prefers_batch_reads(&self.inner)
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let entries = entries
            .iter()
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect::<Vec<_>>();
        self.run(move |store| async move {
            let borrowed = entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>();
            AsyncStore::batch_put(&store, &borrowed).await
        })
    }

    fn supports_hints(&self) -> bool {
        AsyncStore::supports_hints(&self.inner)
    }

    fn prefers_rightmost_path_hints(&self) -> bool {
        AsyncStore::prefers_rightmost_path_hints(&self.inner)
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let namespace = namespace.to_vec();
        let key = key.to_vec();
        self.run(move |store| async move { AsyncStore::get_hint(&store, &namespace, &key).await })
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let namespace = namespace.to_vec();
        let key = key.to_vec();
        let value = value.to_vec();
        self.run(move |store| async move {
            AsyncStore::put_hint(&store, &namespace, &key, &value).await
        })
    }

    fn batch_put_with_hint(
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
        self.run(move |store| async move {
            let borrowed = entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>();
            AsyncStore::batch_put_with_hint(&store, &borrowed, &namespace, &key, &value).await
        })
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
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
        self.run(move |store| async move {
            let borrowed = entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>();
            let publication = match hint.as_ref() {
                Some((namespace, key, value)) => NodePublication::with_hint(
                    &borrowed,
                    NodePublicationHint::new(namespace, key, value),
                    origin,
                ),
                None => NodePublication::new(&borrowed, origin),
            };
            AsyncStore::publish_nodes(&store, publication).await
        })
    }
}

#[cfg(feature = "tokio")]
impl<B> ManifestStore for BlockingRemoteProllyStore<B>
where
    B: RemoteStoreBackend + Clone + 'static,
{
    type Error = BlockingRemoteStoreError<B::Error>;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        let name = name.to_vec();
        self.run(move |store| async move { AsyncManifestStore::get_root(&store, &name).await })
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        let name = name.to_vec();
        let manifest = manifest.clone();
        self.run(move |store| async move {
            AsyncManifestStore::put_root(&store, &name, &manifest).await
        })
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        let name = name.to_vec();
        self.run(move |store| async move { AsyncManifestStore::delete_root(&store, &name).await })
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let name = name.to_vec();
        let expected = expected.cloned();
        let new = new.cloned();
        self.run(move |store| async move {
            AsyncManifestStore::compare_and_swap_root(
                &store,
                &name,
                expected.as_ref(),
                new.as_ref(),
            )
            .await
        })
    }
}

#[cfg(feature = "tokio")]
impl<B> ManifestStoreScan for BlockingRemoteProllyStore<B>
where
    B: RemoteStoreBackend + Clone + 'static,
{
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        self.run(move |store| async move { AsyncManifestStoreScan::list_roots(&store).await })
    }
}

#[cfg(feature = "tokio")]
impl<B> NodeStoreScan for BlockingRemoteProllyStore<B>
where
    B: RemoteStoreBackend + Clone + 'static,
{
    type Error = BlockingRemoteStoreError<B::Error>;

    fn list_node_cids(&self) -> Result<Vec<Cid>, Self::Error> {
        let mut cids = self.run(move |store| async move {
            store
                .backend()
                .list_node_cids()
                .await
                .map_err(RemoteAdapterError::Backend)
        })?;
        let mut decoded = Vec::with_capacity(cids.len());
        for key in cids.drain(..) {
            let bytes: [u8; 32] = key.as_slice().try_into().map_err(|_| {
                BlockingRemoteStoreError::Remote(RemoteAdapterError::InvalidCidLength {
                    len: key.len(),
                })
            })?;
            decoded.push(Cid(bytes));
        }
        decoded.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        Ok(decoded)
    }
}

#[cfg(feature = "tokio")]
impl<B> super::transaction::TransactionalStore for BlockingRemoteProllyStore<B>
where
    B: RemoteStoreBackend + Clone + 'static,
{
    fn supports_transactions(&self) -> bool {
        AsyncTransactionalStore::supports_transactions(&self.inner)
    }

    fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        let node_writes = node_writes.to_vec();
        let root_conditions = root_conditions.to_vec();
        let root_writes = root_writes.to_vec();
        self.run_transaction(move |store| async move {
            AsyncTransactionalStore::commit_transaction(
                &store,
                &node_writes,
                &root_conditions,
                &root_writes,
            )
            .await
        })
    }
}

#[cfg(feature = "tokio")]
impl<B> super::secondary_index::IndexedStore for BlockingRemoteProllyStore<B> where
    B: RemoteStoreBackend + Clone + 'static
{
}

impl<B: RemoteStoreBackend> AsyncStore for RemoteProllyStore<B> {
    type Error = RemoteAdapterError<B::Error>;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let value = self.backend.get_node(key).await.map_err(backend_error)?;
        if let Some(bytes) = value.as_ref() {
            self.verify_node(key, bytes)?;
        }
        Ok(value)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.verify_node(key, value)?;
        self.backend
            .put_node(key, value)
            .await
            .map_err(backend_error)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.backend.delete_node(key).await.map_err(backend_error)
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        for op in ops {
            if let BatchOp::Upsert { key, value } = op {
                self.verify_node(key, value)?;
            }
        }
        let remote_ops = ops
            .iter()
            .map(|op| match op {
                BatchOp::Upsert { key, value } => RemoteBatchOp::Upsert { key, value },
                BatchOp::Delete { key } => RemoteBatchOp::Delete { key },
            })
            .collect::<Vec<_>>();
        self.backend
            .batch_nodes(&remote_ops)
            .await
            .map_err(backend_error)
    }

    async fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let values = self
            .backend
            .batch_get_nodes_ordered(keys)
            .await
            .map_err(backend_error)?;
        self.verify_batch(keys, &values)?;
        Ok(values)
    }

    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let values = self
            .backend
            .batch_get_nodes_ordered(keys)
            .await
            .map_err(backend_error)?;
        self.verify_batch(keys, &values)?;
        Ok(values)
    }

    fn prefers_batch_reads(&self) -> bool {
        self.backend.prefers_batch_reads()
    }

    fn read_parallelism(&self) -> usize {
        self.backend.read_parallelism()
    }

    async fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        for (key, value) in entries {
            self.verify_node(key, value)?;
        }
        self.backend
            .batch_put_nodes(entries)
            .await
            .map_err(backend_error)
    }

    fn supports_hints(&self) -> bool {
        self.backend.supports_hints()
    }

    fn prefers_rightmost_path_hints(&self) -> bool {
        self.backend.prefers_rightmost_path_hints()
    }

    async fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.backend
            .get_hint(namespace, key)
            .await
            .map_err(backend_error)
    }

    async fn put_hint(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.backend
            .put_hint(namespace, key, value)
            .await
            .map_err(backend_error)
    }

    async fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        for (key, value) in entries {
            self.verify_node(key, value)?;
        }
        self.backend
            .batch_put_nodes_with_hint(entries, namespace, key, value)
            .await
            .map_err(backend_error)
    }

    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        for (key, value) in publication.entries() {
            verify_node_cid::<B::Error>(key, value)?;
        }
        self.backend
            .publish_nodes(publication)
            .await
            .map_err(backend_error)
    }
}

impl<B: RemoteStoreBackend> AsyncManifestStore for RemoteProllyStore<B> {
    type Error = RemoteAdapterError<B::Error>;

    async fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        self.backend
            .get_root_manifest(name)
            .await
            .map_err(backend_error)?
            .as_deref()
            .map(decode_root_manifest)
            .transpose()
    }

    async fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        let bytes = encode_root_manifest(manifest)?;
        self.backend
            .put_root_manifest(name, &bytes)
            .await
            .map_err(backend_error)
    }

    async fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        self.backend
            .delete_root_manifest(name)
            .await
            .map_err(backend_error)
    }

    async fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let expected_bytes = expected.map(encode_root_manifest).transpose()?;
        let new_bytes = new.map(encode_root_manifest).transpose()?;
        let update = self
            .backend
            .compare_and_swap_root_manifest(name, expected_bytes.as_deref(), new_bytes.as_deref())
            .await
            .map_err(backend_error)?;
        match update {
            RemoteManifestUpdate::Applied => Ok(ManifestUpdate::Applied),
            RemoteManifestUpdate::Conflict { current } => Ok(ManifestUpdate::Conflict {
                current: current.as_deref().map(decode_root_manifest).transpose()?,
            }),
        }
    }
}

impl<B: RemoteStoreBackend> AsyncManifestStoreScan for RemoteProllyStore<B> {
    async fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        let mut roots = self
            .backend
            .list_root_manifests()
            .await
            .map_err(backend_error)?
            .into_iter()
            .map(|root| {
                let manifest = decode_root_manifest(&root.manifest)?;
                Ok(NamedRootManifest::new(root.name, manifest))
            })
            .collect::<Result<Vec<_>, RemoteAdapterError<B::Error>>>()?;
        roots.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(roots)
    }
}

impl<B: RemoteStoreBackend> AsyncTransactionalStore for RemoteProllyStore<B> {
    fn supports_transactions(&self) -> bool {
        self.backend.supports_transactions()
    }

    async fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        if !self.backend.supports_transactions() {
            return Err(Error::UnsupportedTransactions {
                store: std::any::type_name::<B>(),
            });
        }

        for write in node_writes {
            if let TransactionNodeWrite::Upsert { key, value } = write {
                self.verify_node::<B::Error>(key, value)
                    .map_err(|err| Error::Store(Box::new(err)))?;
            }
        }

        let remote_node_writes = node_writes
            .iter()
            .map(|write| match write {
                TransactionNodeWrite::Upsert { key, value } => RemoteBatchOp::Upsert {
                    key: key.as_slice(),
                    value: value.as_slice(),
                },
                TransactionNodeWrite::Delete { key } => RemoteBatchOp::Delete {
                    key: key.as_slice(),
                },
            })
            .collect::<Vec<_>>();
        let remote_root_conditions = root_conditions
            .iter()
            .map(|condition| {
                encode_optional_root_manifest::<B::Error>(&condition.expected)
                    .map(|expected| RemoteRootCondition::new(condition.name.clone(), expected))
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| Error::Store(Box::new(err)))?;
        let remote_root_writes = root_writes
            .iter()
            .map(|write| match write {
                RootWrite::Put { name, manifest } => encode_root_manifest::<B::Error>(manifest)
                    .map(|manifest| RemoteRootWrite::Put {
                        name: name.clone(),
                        manifest,
                    }),
                RootWrite::Delete { name } => Ok(RemoteRootWrite::Delete { name: name.clone() }),
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|err| Error::Store(Box::new(err)))?;

        let update = self
            .backend
            .commit_transaction(
                &remote_node_writes,
                &remote_root_conditions,
                &remote_root_writes,
            )
            .await
            .map_err(|err| Error::Store(Box::new(RemoteAdapterError::Backend(err))))?;

        match update {
            RemoteTransactionUpdate::Applied => Ok(TransactionUpdate::Applied {
                nodes_written: node_writes.len(),
                roots_written: root_writes.len(),
            }),
            RemoteTransactionUpdate::Conflict(conflict) => {
                let expected = conflict
                    .expected
                    .as_deref()
                    .map(decode_root_manifest::<B::Error>)
                    .transpose()
                    .map_err(|err| Error::Store(Box::new(err)))?;
                let current = conflict
                    .current
                    .as_deref()
                    .map(decode_root_manifest::<B::Error>)
                    .transpose()
                    .map_err(|err| Error::Store(Box::new(err)))?;
                Ok(TransactionUpdate::Conflict(Box::new(
                    TransactionConflict::new(conflict.name, expected, current),
                )))
            }
        }
    }
}

impl<B> RemoteProllyStore<B> {
    fn verify_node<E>(&self, key: &[u8], bytes: &[u8]) -> Result<(), RemoteAdapterError<E>>
    where
        E: StdError + Send + Sync + 'static,
    {
        if self.config.verify_node_cids {
            verify_node_cid(key, bytes)?;
        }
        Ok(())
    }

    fn verify_batch<E>(
        &self,
        keys: &[&[u8]],
        values: &[Option<Vec<u8>>],
    ) -> Result<(), RemoteAdapterError<E>>
    where
        E: StdError + Send + Sync + 'static,
    {
        if !self.config.verify_node_cids {
            return Ok(());
        }
        for (key, value) in keys.iter().zip(values) {
            if let Some(bytes) = value {
                verify_node_cid(key, bytes)?;
            }
        }
        Ok(())
    }
}

/// Error returned by a remote adapter.
#[derive(Debug)]
pub enum RemoteAdapterError<E> {
    /// The provider backend returned an error.
    Backend(E),
    /// A serialized root manifest was invalid.
    RootManifest(String),
    /// A node key was not a raw 32-byte CID.
    InvalidCidLength { len: usize },
    /// Node bytes did not hash to the CID key they were stored under.
    CidMismatch {
        /// Expected CID bytes from the storage key.
        expected: Vec<u8>,
        /// Actual CID bytes computed from the stored value.
        actual: Vec<u8>,
    },
}

impl<E: fmt::Display> fmt::Display for RemoteAdapterError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Backend(err) => write!(f, "remote backend error: {err}"),
            Self::RootManifest(err) => write!(f, "root manifest error: {err}"),
            Self::InvalidCidLength { len } => {
                write!(f, "invalid CID key length {len}, expected 32")
            }
            Self::CidMismatch { .. } => f.write_str("stored node bytes did not match CID key"),
        }
    }
}

impl<E: StdError + 'static> StdError for RemoteAdapterError<E> {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Backend(err) => Some(err),
            _ => None,
        }
    }
}

fn backend_error<E>(err: E) -> RemoteAdapterError<E> {
    RemoteAdapterError::Backend(err)
}

fn encode_root_manifest<E>(manifest: &RootManifest) -> Result<Vec<u8>, RemoteAdapterError<E>>
where
    E: StdError + Send + Sync + 'static,
{
    manifest
        .to_bytes()
        .map_err(|err| RemoteAdapterError::RootManifest(err.to_string()))
}

fn encode_optional_root_manifest<E>(
    manifest: &Option<RootManifest>,
) -> Result<Option<Vec<u8>>, RemoteAdapterError<E>>
where
    E: StdError + Send + Sync + 'static,
{
    manifest.as_ref().map(encode_root_manifest).transpose()
}

fn decode_root_manifest<E>(bytes: &[u8]) -> Result<RootManifest, RemoteAdapterError<E>>
where
    E: StdError + Send + Sync + 'static,
{
    RootManifest::from_bytes(bytes).map_err(|err| RemoteAdapterError::RootManifest(err.to_string()))
}

fn verify_node_cid<E>(key: &[u8], bytes: &[u8]) -> Result<(), RemoteAdapterError<E>>
where
    E: StdError + Send + Sync + 'static,
{
    if key.len() != 32 {
        return Err(RemoteAdapterError::InvalidCidLength { len: key.len() });
    }

    let actual = Cid::from_bytes(bytes);
    if actual.as_bytes() != key {
        return Err(RemoteAdapterError::CidMismatch {
            expected: key.to_vec(),
            actual: actual.as_bytes().to_vec(),
        });
    }

    Ok(())
}

/// Reusable backend conformance checks for provider implementations.
///
/// Provider crates should run this against their SDK-backed implementation in
/// addition to any provider-specific transactional tests.
pub mod conformance {
    use std::fmt::Debug;

    use super::*;

    /// Assert the common backend contract needed by remote prolly adapters.
    pub async fn assert_remote_backend_contract<B>(backend: &B)
    where
        B: RemoteStoreBackend,
        B::Error: Debug,
    {
        let alpha = b"alpha-node";
        let beta = b"beta-node";
        let gamma = b"gamma-node";
        let alpha_cid = Cid::from_bytes(alpha);
        let beta_cid = Cid::from_bytes(beta);
        let gamma_cid = Cid::from_bytes(gamma);
        let missing_cid = Cid::from_bytes(b"missing");

        assert_eq!(backend.get_node(alpha_cid.as_bytes()).await.unwrap(), None);
        backend.put_node(alpha_cid.as_bytes(), alpha).await.unwrap();
        backend.put_node(beta_cid.as_bytes(), beta).await.unwrap();

        let ordered_keys = vec![
            beta_cid.as_bytes(),
            missing_cid.as_bytes(),
            alpha_cid.as_bytes(),
            beta_cid.as_bytes(),
        ];
        assert_eq!(
            backend
                .batch_get_nodes_ordered(&ordered_keys)
                .await
                .unwrap(),
            vec![
                Some(beta.to_vec()),
                None,
                Some(alpha.to_vec()),
                Some(beta.to_vec())
            ]
        );

        backend
            .batch_nodes(&[
                RemoteBatchOp::Upsert {
                    key: alpha_cid.as_bytes(),
                    value: alpha,
                },
                RemoteBatchOp::Upsert {
                    key: alpha_cid.as_bytes(),
                    value: alpha,
                },
                RemoteBatchOp::Delete {
                    key: beta_cid.as_bytes(),
                },
                RemoteBatchOp::Upsert {
                    key: gamma_cid.as_bytes(),
                    value: gamma,
                },
            ])
            .await
            .unwrap();
        assert_eq!(
            backend.get_node(alpha_cid.as_bytes()).await.unwrap(),
            Some(alpha.to_vec())
        );
        assert_eq!(backend.get_node(beta_cid.as_bytes()).await.unwrap(), None);
        assert_eq!(
            backend.get_node(gamma_cid.as_bytes()).await.unwrap(),
            Some(gamma.to_vec())
        );

        for origin in [
            PublicationOrigin::General,
            PublicationOrigin::PointUpsert,
            PublicationOrigin::PointDelete,
            PublicationOrigin::BatchMutation,
            PublicationOrigin::TreeBuild,
            PublicationOrigin::Merge,
            PublicationOrigin::RangeDelete,
            PublicationOrigin::Replication,
            PublicationOrigin::Maintenance,
        ] {
            let entries = [(alpha_cid.as_bytes(), alpha.as_slice())];
            backend
                .publish_nodes(NodePublication::new(&entries, origin))
                .await
                .unwrap();
            assert_eq!(
                backend.get_node(alpha_cid.as_bytes()).await.unwrap(),
                Some(alpha.to_vec())
            );
        }

        let entries = [(gamma_cid.as_bytes(), gamma.as_slice())];
        let hint = NodePublicationHint::new(b"publication", b"rightmost", gamma_cid.as_bytes());
        backend
            .publish_nodes(NodePublication::with_hint(
                &entries,
                hint,
                PublicationOrigin::Maintenance,
            ))
            .await
            .unwrap();
        assert_eq!(
            backend.get_node(gamma_cid.as_bytes()).await.unwrap(),
            Some(gamma.to_vec())
        );
        if backend.supports_hints() {
            assert_eq!(
                backend
                    .get_hint(b"publication", b"rightmost")
                    .await
                    .unwrap(),
                Some(gamma_cid.as_bytes().to_vec())
            );
        }

        backend
            .put_hint(b"scan", b"rightmost", b"hint")
            .await
            .unwrap();

        let config = Config::default();
        let main_v1 = RootManifest::new(Some(Cid::from_bytes(b"main-v1")), config.clone())
            .to_bytes()
            .unwrap();
        let main_v2 = RootManifest::new(Some(Cid::from_bytes(b"main-v2")), config)
            .to_bytes()
            .unwrap();

        assert_eq!(backend.get_root_manifest(b"main").await.unwrap(), None);
        assert!(matches!(
            backend
                .compare_and_swap_root_manifest(b"main", None, Some(&main_v1))
                .await
                .unwrap(),
            RemoteManifestUpdate::Applied
        ));
        assert_eq!(
            backend.get_root_manifest(b"main").await.unwrap(),
            Some(main_v1.clone())
        );

        assert_eq!(
            backend
                .compare_and_swap_root_manifest(b"main", None, Some(&main_v2))
                .await
                .unwrap(),
            RemoteManifestUpdate::Conflict {
                current: Some(main_v1.clone())
            }
        );
        assert!(matches!(
            backend
                .compare_and_swap_root_manifest(b"main", Some(&main_v1), Some(&main_v2))
                .await
                .unwrap(),
            RemoteManifestUpdate::Applied
        ));

        backend.put_root_manifest(b"zeta", &main_v1).await.unwrap();
        backend.put_root_manifest(b"alpha", &main_v2).await.unwrap();
        let mut roots = backend.list_root_manifests().await.unwrap();
        roots.sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(
            roots
                .iter()
                .map(|root| root.name.clone())
                .collect::<Vec<_>>(),
            vec![b"alpha".to_vec(), b"main".to_vec(), b"zeta".to_vec()]
        );

        let listed_cids = backend.list_node_cids().await.unwrap();
        let mut expected_cids = vec![alpha_cid.as_bytes().to_vec(), gamma_cid.as_bytes().to_vec()];
        expected_cids.sort();
        assert_eq!(listed_cids, expected_cids);
    }

    /// Assert the optional backend transaction contract. Providers that return
    /// `true` from `supports_transactions` should run this in addition to the
    /// base backend contract.
    pub async fn assert_remote_backend_transaction_contract<B>(backend: &B)
    where
        B: RemoteStoreBackend,
        B::Error: Debug,
    {
        assert!(backend.supports_transactions());

        let config = Config::default();
        let main_v1 = RootManifest::new(Some(Cid::from_bytes(b"txn-main-v1")), config.clone())
            .to_bytes()
            .unwrap();
        let main_v2 = RootManifest::new(Some(Cid::from_bytes(b"txn-main-v2")), config)
            .to_bytes()
            .unwrap();
        let alpha = b"transaction-alpha-node";
        let beta = b"transaction-beta-node";
        let alpha_cid = Cid::from_bytes(alpha);
        let beta_cid = Cid::from_bytes(beta);

        let update = backend
            .commit_transaction(
                &[RemoteBatchOp::Upsert {
                    key: alpha_cid.as_bytes(),
                    value: alpha,
                }],
                &[RemoteRootCondition::new(b"txn/main".to_vec(), None)],
                &[RemoteRootWrite::Put {
                    name: b"txn/main".to_vec(),
                    manifest: main_v1.clone(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(update, RemoteTransactionUpdate::Applied);
        assert_eq!(
            backend.get_node(alpha_cid.as_bytes()).await.unwrap(),
            Some(alpha.to_vec())
        );
        assert_eq!(
            backend.get_root_manifest(b"txn/main").await.unwrap(),
            Some(main_v1.clone())
        );

        let update = backend
            .commit_transaction(
                &[RemoteBatchOp::Upsert {
                    key: beta_cid.as_bytes(),
                    value: beta,
                }],
                &[RemoteRootCondition::new(b"txn/main".to_vec(), None)],
                &[RemoteRootWrite::Put {
                    name: b"txn/main".to_vec(),
                    manifest: main_v2.clone(),
                }],
            )
            .await
            .unwrap();
        assert_eq!(
            update,
            RemoteTransactionUpdate::Conflict(RemoteTransactionConflict::new(
                b"txn/main".to_vec(),
                None,
                Some(main_v1.clone())
            ))
        );
        assert_eq!(backend.get_node(beta_cid.as_bytes()).await.unwrap(), None);
        assert_eq!(
            backend.get_root_manifest(b"txn/main").await.unwrap(),
            Some(main_v1)
        );
    }

    /// Assert that a transaction-capable backend supports the complete native
    /// asynchronous `AsyncProlly::indexed_map` workflow.
    pub async fn assert_remote_backend_async_indexed_map_contract<B>(backend: B)
    where
        B: RemoteStoreBackend + Clone,
        B::Error: Debug,
    {
        use crate::{
            AsyncProlly, IndexedMapUpdate, Mutation, SecondaryIndex, SecondaryIndexRegistry,
        };

        let store = RemoteProllyStore::new(backend);
        assert!(AsyncTransactionalStore::supports_transactions(&store));
        let engine = AsyncProlly::new(store, Config::default());
        let registry = SecondaryIndexRegistry::new()
            .register(
                SecondaryIndex::non_unique(
                    "by-status",
                    1,
                    "remote-async-contract.by-status/v1",
                    |_, value| Ok(vec![value.to_vec()]),
                )
                .unwrap(),
            )
            .unwrap();
        let indexed = engine
            .indexed_map(b"remote-async-indexed-contract", registry)
            .await
            .unwrap();
        indexed.put(b"user-1", b"active").await.unwrap();
        indexed.ensure_index(b"by-status").await.unwrap();
        let stale = indexed.put(b"user-2", b"pending").await.unwrap().source.id;
        assert_eq!(
            indexed
                .snapshot()
                .await
                .unwrap()
                .index(b"by-status")
                .unwrap()
                .primary_keys(b"pending")
                .await
                .unwrap(),
            vec![b"user-2".to_vec()]
        );

        indexed.put(b"user-3", b"active").await.unwrap();
        let update = indexed
            .apply_if(
                Some(&stale),
                vec![Mutation::Upsert {
                    key: b"must-not-publish".to_vec(),
                    val: b"active".to_vec(),
                }],
            )
            .await
            .unwrap();
        assert!(matches!(update, IndexedMapUpdate::Conflict { .. }));
        assert_eq!(indexed.get(b"must-not-publish").await.unwrap(), None);
        let snapshot = indexed.snapshot().await.unwrap();
        assert!(indexed
            .verify_all(snapshot.source_version())
            .await
            .unwrap()
            .iter()
            .all(super::super::secondary_index::IndexVerification::is_valid));
    }

    /// Assert that a transaction-capable remote backend supports the complete
    /// synchronous `Prolly::indexed_map` API through the shared blocking facade.
    #[cfg(feature = "tokio")]
    pub fn assert_remote_backend_indexed_map_contract<B>(backend: B)
    where
        B: RemoteStoreBackend + Clone + 'static,
        B::Error: Debug,
    {
        use crate::{IndexedMapUpdate, Mutation, Prolly, SecondaryIndex, SecondaryIndexRegistry};

        let store = BlockingRemoteProllyStore::new(backend).unwrap();
        assert!(super::super::transaction::TransactionalStore::supports_transactions(&store));
        let engine = Prolly::new(store, Config::default());
        let registry = SecondaryIndexRegistry::new()
            .register(
                SecondaryIndex::non_unique(
                    "by-status",
                    1,
                    "remote-contract.by-status/v1",
                    |_, value| Ok(vec![value.to_vec()]),
                )
                .unwrap(),
            )
            .unwrap();
        let indexed = engine
            .indexed_map(b"remote-indexed-contract", registry)
            .unwrap();
        indexed.put(b"user-1", b"active").unwrap();
        indexed.ensure_index(b"by-status").unwrap();
        let stale = indexed.put(b"user-2", b"pending").unwrap().source.id;
        assert_eq!(
            indexed
                .snapshot()
                .unwrap()
                .index(b"by-status")
                .unwrap()
                .primary_keys(b"pending")
                .unwrap(),
            vec![b"user-2".to_vec()]
        );

        indexed.put(b"user-3", b"active").unwrap();
        let update = indexed
            .apply_if(
                Some(&stale),
                vec![Mutation::Upsert {
                    key: b"must-not-publish".to_vec(),
                    val: b"active".to_vec(),
                }],
            )
            .unwrap();
        assert!(matches!(update, IndexedMapUpdate::Conflict { .. }));
        assert_eq!(indexed.get(b"must-not-publish").unwrap(), None);
        assert!(indexed
            .verify_all(indexed.snapshot().unwrap().source_version())
            .unwrap()
            .iter()
            .all(super::super::secondary_index::IndexVerification::is_valid));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::sync::Mutex;
    use std::task::{Context, Poll};

    use super::*;
    use crate::{AsyncProlly, Config};

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

    #[derive(Debug)]
    struct MemoryBackendError(String);

    impl fmt::Display for MemoryBackendError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl StdError for MemoryBackendError {}

    #[derive(Default)]
    struct MemoryBackend {
        nodes: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        hints: Mutex<BTreeMap<HintKey, Vec<u8>>>,
        roots: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        publications: Mutex<Vec<PublicationOrigin>>,
    }

    type HintKey = (Vec<u8>, Vec<u8>);

    impl RemoteStoreBackend for MemoryBackend {
        type Error = MemoryBackendError;

        async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self.nodes.lock().unwrap().get(key).cloned())
        }

        async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.nodes
                .lock()
                .unwrap()
                .insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
            self.nodes.lock().unwrap().remove(key);
            Ok(())
        }

        async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
            let mut nodes = self.nodes.lock().unwrap();
            for op in ops {
                match op {
                    RemoteBatchOp::Upsert { key, value } => {
                        nodes.insert((*key).to_vec(), (*value).to_vec());
                    }
                    RemoteBatchOp::Delete { key } => {
                        nodes.remove(*key);
                    }
                }
            }
            Ok(())
        }

        async fn batch_get_nodes_ordered(
            &self,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            let nodes = self.nodes.lock().unwrap();
            Ok(keys.iter().map(|key| nodes.get(*key).cloned()).collect())
        }

        fn prefers_batch_reads(&self) -> bool {
            true
        }

        fn supports_hints(&self) -> bool {
            true
        }

        async fn get_hint(
            &self,
            namespace: &[u8],
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self
                .hints
                .lock()
                .unwrap()
                .get(&(namespace.to_vec(), key.to_vec()))
                .cloned())
        }

        async fn put_hint(
            &self,
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            self.hints
                .lock()
                .unwrap()
                .insert((namespace.to_vec(), key.to_vec()), value.to_vec());
            Ok(())
        }

        async fn batch_put_nodes_with_hint(
            &self,
            entries: &[(&[u8], &[u8])],
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            {
                let mut nodes = self.nodes.lock().unwrap();
                for (key, value) in entries {
                    nodes.insert((*key).to_vec(), (*value).to_vec());
                }
            }
            self.put_hint(namespace, key, value).await
        }

        async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
            self.publications.lock().unwrap().push(publication.origin());
            match publication.hint() {
                Some(hint) => {
                    self.batch_put_nodes_with_hint(
                        publication.entries(),
                        hint.namespace(),
                        hint.key(),
                        hint.value(),
                    )
                    .await
                }
                None => self.batch_put_nodes(publication.entries()).await,
            }
        }

        async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
            Ok(self.nodes.lock().unwrap().keys().cloned().collect())
        }

        async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self.roots.lock().unwrap().get(name).cloned())
        }

        async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
            self.roots
                .lock()
                .unwrap()
                .insert(name.to_vec(), manifest.to_vec());
            Ok(())
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            self.roots.lock().unwrap().remove(name);
            Ok(())
        }

        async fn compare_and_swap_root_manifest(
            &self,
            name: &[u8],
            expected: Option<&[u8]>,
            new: Option<&[u8]>,
        ) -> Result<RemoteManifestUpdate, Self::Error> {
            let mut roots = self.roots.lock().unwrap();
            let current = roots.get(name).cloned();
            if current.as_deref() != expected {
                return Ok(RemoteManifestUpdate::Conflict { current });
            }
            match new {
                Some(bytes) => {
                    roots.insert(name.to_vec(), bytes.to_vec());
                }
                None => {
                    roots.remove(name);
                }
            }
            Ok(RemoteManifestUpdate::Applied)
        }

        fn supports_transactions(&self) -> bool {
            true
        }

        async fn commit_transaction(
            &self,
            node_writes: &[RemoteBatchOp<'_>],
            root_conditions: &[RemoteRootCondition],
            root_writes: &[RemoteRootWrite],
        ) -> Result<RemoteTransactionUpdate, Self::Error> {
            let mut nodes = self.nodes.lock().unwrap();
            let mut roots = self.roots.lock().unwrap();

            for condition in root_conditions {
                let current = roots.get(&condition.name).cloned();
                if current != condition.expected {
                    return Ok(RemoteTransactionUpdate::Conflict(
                        RemoteTransactionConflict::new(
                            condition.name.clone(),
                            condition.expected.clone(),
                            current,
                        ),
                    ));
                }
            }

            for write in node_writes {
                match write {
                    RemoteBatchOp::Upsert { key, value } => {
                        nodes.insert((*key).to_vec(), (*value).to_vec());
                    }
                    RemoteBatchOp::Delete { key } => {
                        nodes.remove(*key);
                    }
                }
            }

            for write in root_writes {
                match write {
                    RemoteRootWrite::Put { name, manifest } => {
                        roots.insert(name.clone(), manifest.clone());
                    }
                    RemoteRootWrite::Delete { name } => {
                        roots.remove(name);
                    }
                }
            }

            Ok(RemoteTransactionUpdate::Applied)
        }

        async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error> {
            Ok(self
                .roots
                .lock()
                .unwrap()
                .iter()
                .map(|(name, manifest)| RemoteNamedRoot::new(name.clone(), manifest.clone()))
                .collect())
        }
    }

    #[test]
    fn remote_adapter_verifies_node_cids() {
        block_on(async {
            let store = RemoteProllyStore::new(MemoryBackend::default());
            let cid = Cid::from_bytes(b"expected bytes");
            let err = store.put(cid.as_bytes(), b"wrong bytes").await.unwrap_err();
            assert!(matches!(err, RemoteAdapterError::CidMismatch { .. }));
        });
    }

    #[test]
    fn remote_publication_preserves_origin_and_always_verifies_cids() {
        block_on(async {
            let backend = Arc::new(MemoryBackend::default());
            let store = RemoteProllyStore::new(backend.clone());
            let bytes = b"published-node";
            let cid = Cid::from_bytes(bytes);
            let entries = [(cid.as_bytes(), bytes.as_slice())];
            let hint = NodePublicationHint::new(b"namespace", b"rightmost", cid.as_bytes());
            store
                .publish_nodes(NodePublication::with_hint(
                    &entries,
                    hint,
                    PublicationOrigin::Maintenance,
                ))
                .await
                .unwrap();

            assert_eq!(
                *backend.publications.lock().unwrap(),
                vec![PublicationOrigin::Maintenance]
            );
            assert_eq!(
                backend.get_node(cid.as_bytes()).await.unwrap(),
                Some(bytes.to_vec())
            );
            assert_eq!(
                backend.get_hint(b"namespace", b"rightmost").await.unwrap(),
                Some(cid.as_bytes().to_vec())
            );

            let invalid_entries = [(cid.as_bytes(), b"wrong-bytes".as_slice())];
            let error = store
                .publish_nodes(NodePublication::new(
                    &invalid_entries,
                    PublicationOrigin::PointUpsert,
                ))
                .await
                .unwrap_err();
            assert!(matches!(error, RemoteAdapterError::CidMismatch { .. }));
            assert_eq!(backend.publications.lock().unwrap().len(), 1);

            let unchecked_backend = Arc::new(MemoryBackend::default());
            let unchecked = RemoteProllyStore::with_config(
                unchecked_backend.clone(),
                RemoteStoreConfig {
                    verify_node_cids: false,
                },
            );
            let error = unchecked
                .publish_nodes(NodePublication::new(
                    &invalid_entries,
                    PublicationOrigin::PointUpsert,
                ))
                .await
                .unwrap_err();
            assert!(matches!(error, RemoteAdapterError::CidMismatch { .. }));
            assert!(unchecked_backend.publications.lock().unwrap().is_empty());
        });
    }

    #[cfg(feature = "tokio")]
    #[test]
    fn blocking_remote_store_supports_indexed_map_inside_and_outside_tokio() {
        use crate::{Prolly, SecondaryIndexRegistry};

        let backend = Arc::new(MemoryBackend::default());
        let store = BlockingRemoteProllyStore::new(backend).unwrap();
        let engine = Prolly::new(store, Config::default());
        let indexed = engine
            .indexed_map(b"remote-users", SecondaryIndexRegistry::new())
            .unwrap();
        indexed.put(b"outside", b"runtime").unwrap();
        assert_eq!(indexed.get(b"outside").unwrap(), Some(b"runtime".to_vec()));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            indexed.put(b"inside", b"runtime").unwrap();
            assert_eq!(indexed.get(b"inside").unwrap(), Some(b"runtime".to_vec()));

            let nested = BlockingRemoteProllyStore::build(|| async {
                Ok::<_, MemoryBackendError>(Arc::new(MemoryBackend::default()))
            })
            .unwrap();
            drop(nested);
        });
    }

    #[test]
    fn memory_backend_satisfies_remote_backend_contract() {
        block_on(async {
            let backend = MemoryBackend::default();
            conformance::assert_remote_backend_contract(&backend).await;
        });
    }

    #[test]
    fn memory_backend_satisfies_remote_transaction_contract() {
        block_on(async {
            let backend = MemoryBackend::default();
            conformance::assert_remote_backend_transaction_contract(&backend).await;
        });
    }

    #[test]
    fn memory_backend_satisfies_async_indexed_map_contract() {
        block_on(async {
            let backend = Arc::new(MemoryBackend::default());
            conformance::assert_remote_backend_async_indexed_map_contract(backend).await;
        });
    }

    #[test]
    fn remote_adapter_supports_async_prolly_named_roots() {
        block_on(async {
            let store = Arc::new(MemoryBackend::default());
            let adapter = RemoteProllyStore::new(store);
            let prolly = AsyncProlly::new(adapter, Config::default());

            let empty = prolly.create();
            let first = prolly
                .put(&empty, b"k".to_vec(), b"v1".to_vec())
                .await
                .unwrap();
            let second = prolly
                .put(&first, b"k".to_vec(), b"v2".to_vec())
                .await
                .unwrap();

            assert!(prolly
                .compare_and_swap_named_root(b"main", None, Some(&first))
                .await
                .unwrap()
                .is_applied());
            let conflict = prolly
                .compare_and_swap_named_root(b"main", None, Some(&second))
                .await
                .unwrap();
            assert!(conflict.is_conflict());
            assert!(prolly
                .compare_and_swap_named_root(b"main", Some(&first), Some(&second))
                .await
                .unwrap()
                .is_applied());

            assert_eq!(
                prolly.load_named_root(b"main").await.unwrap(),
                Some(second.clone())
            );
            assert_eq!(
                prolly.get(&second, b"k").await.unwrap(),
                Some(b"v2".to_vec())
            );
            assert_eq!(
                prolly
                    .list_named_roots()
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|root| root.name)
                    .collect::<Vec<_>>(),
                vec![b"main".to_vec()]
            );
        });
    }

    #[test]
    fn remote_adapter_supports_async_prolly_transactions() {
        block_on(async {
            let store = Arc::new(MemoryBackend::default());
            let adapter = RemoteProllyStore::new(store);
            let prolly = AsyncProlly::new(adapter, Config::default());

            let (source, by_status) = prolly
                .transaction(|tx| {
                    Box::pin(async move {
                        let source = tx
                            .put(
                                &tx.create(),
                                b"ticket/123/status".to_vec(),
                                b"open".to_vec(),
                            )
                            .await?;
                        let by_status = tx
                            .put(
                                &tx.create(),
                                b"by_status/open/123".to_vec(),
                                b"ticket/123".to_vec(),
                            )
                            .await?;
                        tx.publish_named_root(b"tickets/source/current", &source)
                            .await?;
                        tx.publish_named_root(b"tickets/view/by-status/current", &by_status)
                            .await?;
                        Ok((source, by_status))
                    })
                })
                .await
                .unwrap();

            assert_eq!(
                prolly
                    .load_named_root(b"tickets/source/current")
                    .await
                    .unwrap(),
                Some(source.clone())
            );
            assert_eq!(
                prolly
                    .load_named_root(b"tickets/view/by-status/current")
                    .await
                    .unwrap(),
                Some(by_status.clone())
            );
            assert_eq!(
                prolly.get(&source, b"ticket/123/status").await.unwrap(),
                Some(b"open".to_vec())
            );
            assert_eq!(
                prolly.get(&by_status, b"by_status/open/123").await.unwrap(),
                Some(b"ticket/123".to_vec())
            );
        });
    }

    #[test]
    fn owned_async_transaction_outlives_manager_borrow() {
        block_on(async {
            let store = Arc::new(MemoryBackend::default());
            let transaction = {
                let adapter = RemoteProllyStore::new(store.clone());
                let prolly = AsyncProlly::new(adapter, Config::default());
                prolly.begin_owned_transaction().unwrap()
            };

            let tree = transaction
                .put(&transaction.create(), b"a".to_vec(), b"1".to_vec())
                .await
                .unwrap();
            transaction
                .publish_named_root(b"main", &tree)
                .await
                .unwrap();
            assert!(matches!(
                transaction.commit().await.unwrap(),
                crate::prolly::transaction::TransactionUpdate::Applied { .. }
            ));

            let adapter = RemoteProllyStore::new(store);
            let prolly = AsyncProlly::new(adapter, Config::default());
            assert_eq!(prolly.load_named_root(b"main").await.unwrap(), Some(tree));
        });
    }

    #[test]
    fn dropping_owned_async_transaction_discards_overlay() {
        block_on(async {
            let store = Arc::new(MemoryBackend::default());
            let adapter = RemoteProllyStore::new(store);
            let prolly = AsyncProlly::new(adapter, Config::default());
            let transaction = prolly.begin_owned_transaction().unwrap();
            let tree = transaction
                .put(&transaction.create(), b"a".to_vec(), b"1".to_vec())
                .await
                .unwrap();
            transaction
                .publish_named_root(b"main", &tree)
                .await
                .unwrap();
            drop(transaction);

            assert_eq!(prolly.load_named_root(b"main").await.unwrap(), None);
        });
    }
}
