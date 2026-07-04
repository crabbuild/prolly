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
use super::manifest::{
    AsyncManifestStore, AsyncManifestStoreScan, ManifestUpdate, NamedRootManifest, RootManifest,
};
use super::store::{AsyncStore, BatchOp};

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
        hints: Mutex<BTreeMap<(Vec<u8>, Vec<u8>), Vec<u8>>>,
        roots: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
    }

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
            let mut nodes = self.nodes.lock().unwrap();
            for (key, value) in entries {
                nodes.insert((*key).to_vec(), (*value).to_vec());
            }
            drop(nodes);
            self.put_hint(namespace, key, value).await
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
    fn memory_backend_satisfies_remote_backend_contract() {
        block_on(async {
            let backend = MemoryBackend::default();
            conformance::assert_remote_backend_contract(&backend).await;
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
}
