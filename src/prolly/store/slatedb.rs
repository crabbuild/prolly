//! SlateDB storage backend implementation.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use futures_util::stream::{self, StreamExt};
use slatedb::bytes::Bytes;
use slatedb::config::Settings;
use slatedb::object_store::ObjectStore;
use slatedb::{Db, WriteBatch};
use tokio::runtime::{Builder, Runtime};

use super::super::manifest::{
    sort_named_root_manifests, ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRootManifest,
    RootManifest,
};
use super::{cid_from_store_key, sort_cids, BatchOp, NodeStoreScan, OrderedBatchReadPlan, Store};

const NODE_PREFIX: &[u8] = b"node:";
const HINT_PREFIX: &[u8] = b"hint:";
const ROOT_PREFIX: &[u8] = b"root:";

/// Configuration options for [`SlateDbStore`].
#[derive(Debug, Clone)]
pub struct SlateDbStoreConfig {
    /// SlateDB engine settings.
    pub settings: Settings,
    /// Flush writes to object storage before returning from write operations.
    pub flush_after_write: bool,
    /// Close the SlateDB instance when this store is dropped.
    pub close_on_drop: bool,
    /// Maximum number of concurrent reads used by `batch_get` operations.
    pub read_parallelism: usize,
}

impl Default for SlateDbStoreConfig {
    fn default() -> Self {
        Self {
            settings: Settings::default(),
            flush_after_write: true,
            close_on_drop: true,
            read_parallelism: 64,
        }
    }
}

/// Error type for SlateDB store operations.
#[derive(Debug)]
pub struct SlateDbStoreError {
    message: String,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl SlateDbStoreError {
    /// Create a new error with a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// Create a new error with a source error.
    pub fn with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    fn from_slatedb(err: slatedb::Error, context: impl Into<String>) -> Self {
        Self::with_source(format!("{}: {}", context.into(), err), err)
    }
}

impl std::fmt::Display for SlateDbStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SlateDB error: {}", self.message)
    }
}

impl std::error::Error for SlateDbStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

impl From<slatedb::Error> for SlateDbStoreError {
    fn from(err: slatedb::Error) -> Self {
        Self::from_slatedb(err, "SlateDB operation failed")
    }
}

/// SlateDB-backed storage backend for Prolly Trees.
///
/// SlateDB is async-first; this adapter owns a private Tokio runtime so it can
/// implement the synchronous [`Store`] trait used by the rest of the prolly
/// tree engine.
pub struct SlateDbStore {
    db: Db,
    runtime: Runtime,
    manifest_lock: Mutex<()>,
    flush_after_write: bool,
    close_on_drop: bool,
    read_parallelism: usize,
}

impl SlateDbStore {
    /// Open or create a SlateDB database at `path` in the provided object store.
    pub fn open(
        path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
    ) -> Result<Self, SlateDbStoreError> {
        Self::open_with_config(path, object_store, SlateDbStoreConfig::default())
    }

    /// Open or create a SlateDB database with custom configuration.
    pub fn open_with_config(
        path: impl Into<String>,
        object_store: Arc<dyn ObjectStore>,
        config: SlateDbStoreConfig,
    ) -> Result<Self, SlateDbStoreError> {
        let runtime = Builder::new_multi_thread()
            .thread_name("prolly-slatedb")
            .enable_all()
            .build()
            .map_err(|e| SlateDbStoreError::with_source("failed to create Tokio runtime", e))?;

        let path = path.into();
        let settings = config.settings;
        let db = runtime
            .block_on(async move {
                Db::builder(path, object_store)
                    .with_settings(settings)
                    .build()
                    .await
            })
            .map_err(|e| SlateDbStoreError::from_slatedb(e, "failed to open SlateDB"))?;

        Ok(Self {
            db,
            runtime,
            manifest_lock: Mutex::new(()),
            flush_after_write: config.flush_after_write,
            close_on_drop: config.close_on_drop,
            read_parallelism: config.read_parallelism.max(1),
        })
    }

    /// Flush outstanding writes to object storage.
    pub fn flush(&self) -> Result<(), SlateDbStoreError> {
        self.block_on(self.db.flush(), "failed to flush SlateDB")
    }

    fn block_on<T, F>(&self, future: F, context: &'static str) -> Result<T, SlateDbStoreError>
    where
        F: Future<Output = Result<T, slatedb::Error>>,
    {
        self.runtime
            .block_on(future)
            .map_err(|e| SlateDbStoreError::from_slatedb(e, context))
    }

    fn flush_after_write_if_configured(&self) -> Result<(), SlateDbStoreError> {
        if self.flush_after_write {
            self.flush()?;
        }
        Ok(())
    }

    fn write_batch(
        &self,
        batch: WriteBatch,
        context: &'static str,
    ) -> Result<(), SlateDbStoreError> {
        self.block_on(async { self.db.write(batch).await.map(|_| ()) }, context)?;
        self.flush_after_write_if_configured()
    }

    fn batch_read_ordered(
        &self,
        storage_keys: Vec<Vec<u8>>,
        context: &'static str,
    ) -> Result<Vec<Option<Vec<u8>>>, SlateDbStoreError> {
        let len = storage_keys.len();
        if len == 0 {
            return Ok(Vec::new());
        }

        if len == 1 {
            let key = storage_keys.into_iter().next().expect("one key");
            return self
                .block_on(async { self.db.get(key).await }, context)
                .map(|value| vec![value.map(|bytes| bytes.to_vec())]);
        }

        let db = self.db.clone();
        let parallelism = self.read_parallelism;

        self.block_on(
            async move {
                let indexed_values = stream::iter(storage_keys.into_iter().enumerate())
                    .map(|(idx, key)| {
                        let db = db.clone();
                        async move {
                            db.get(key)
                                .await
                                .map(|value| (idx, value.map(|bytes| bytes.to_vec())))
                        }
                    })
                    .buffer_unordered(parallelism)
                    .collect::<Vec<_>>()
                    .await;

                let mut ordered = vec![None; len];
                for result in indexed_values {
                    let (idx, value) = result?;
                    ordered[idx] = Some(value);
                }

                Ok(ordered
                    .into_iter()
                    .map(|value| value.expect("all batch reads must fill their result slot"))
                    .collect())
            },
            context,
        )
    }
}

impl Drop for SlateDbStore {
    fn drop(&mut self) {
        if self.close_on_drop {
            let _ = self.runtime.block_on(self.db.close());
        }
    }
}

impl Store for SlateDbStore {
    type Error = SlateDbStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let key = node_key(key);
        self.block_on(async { self.db.get(key).await }, "failed to read key")
            .map(|value| value.map(|bytes| bytes.to_vec()))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let key = Bytes::from(node_key(key));
        let value = Bytes::copy_from_slice(value);
        self.block_on(
            async { self.db.put_bytes(key, value).await.map(|_| ()) },
            "failed to write key",
        )?;
        self.flush_after_write_if_configured()
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        let key = node_key(key);
        self.block_on(
            async { self.db.delete(key).await.map(|_| ()) },
            "failed to delete key",
        )?;
        self.flush_after_write_if_configured()
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        if ops.is_empty() {
            return Ok(());
        }

        let mut batch = WriteBatch::new();
        for op in ops {
            match op {
                BatchOp::Upsert { key, value } => {
                    batch.put(node_key(key), value);
                }
                BatchOp::Delete { key } => {
                    batch.delete(node_key(key));
                }
            }
        }

        self.write_batch(batch, "batch operation failed")
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        if keys.is_empty() {
            return Ok(HashMap::new());
        }

        if keys.len() == 1 {
            let mut results = HashMap::with_capacity(1);
            if let Some(value) = self.get(keys[0])? {
                results.insert(keys[0].to_vec(), value);
            }
            return Ok(results);
        }

        let plan = OrderedBatchReadPlan::new(keys);
        let storage_keys = plan
            .unique_keys()
            .iter()
            .map(|key| node_key(key))
            .collect::<Vec<_>>();
        let values = self.batch_read_ordered(storage_keys, "failed to read keys in batch")?;

        let mut results = HashMap::with_capacity(plan.unique_keys().len());
        for (key, value) in plan.unique_keys().iter().zip(values) {
            if let Some(value) = value {
                results.insert(key.to_vec(), value);
            }
        }
        Ok(results)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        if keys.len() == 1 {
            return Ok(vec![self.get(keys[0])?]);
        }

        let plan = OrderedBatchReadPlan::new(keys);
        let storage_keys = plan
            .unique_keys()
            .iter()
            .map(|key| node_key(key))
            .collect::<Vec<_>>();
        let values =
            self.batch_read_ordered(storage_keys, "failed to read keys in ordered batch")?;
        Ok(plan.expand_owned(values))
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let storage_keys = keys.iter().map(|key| node_key(key)).collect::<Vec<_>>();
        self.batch_read_ordered(storage_keys, "failed to read keys in unique ordered batch")
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut batch = WriteBatch::new();
        for (key, value) in entries {
            batch.put(node_key(key), value);
        }

        self.write_batch(batch, "batch put operation failed")
    }

    fn supports_hints(&self) -> bool {
        true
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let key = hint_key(namespace, key);
        self.block_on(async { self.db.get(key).await }, "failed to read hint")
            .map(|value| value.map(|bytes| bytes.to_vec()))
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let key = Bytes::from(hint_key(namespace, key));
        let value = Bytes::copy_from_slice(value);
        self.block_on(
            async { self.db.put_bytes(key, value).await.map(|_| ()) },
            "failed to write hint",
        )?;
        self.flush_after_write_if_configured()
    }

    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        let mut batch = WriteBatch::new();
        for (key, value) in entries {
            batch.put(node_key(key), value);
        }
        batch.put(hint_key(namespace, key), value);

        self.write_batch(batch, "batch put with hint operation failed")
    }
}

impl NodeStoreScan for SlateDbStore {
    type Error = SlateDbStoreError;

    fn list_node_cids(&self) -> Result<Vec<super::super::cid::Cid>, Self::Error> {
        self.block_on(
            async {
                let mut iter = self.db.scan_prefix(NODE_PREFIX, ..).await?;
                let mut cids = Vec::new();
                while let Some(kv) = iter.next().await? {
                    let key = kv.key.as_ref();
                    let cid = key
                        .strip_prefix(NODE_PREFIX)
                        .ok_or_else(|| {
                            slatedb::Error::invalid(
                                "SlateDB node scan returned key without node prefix".to_string(),
                            )
                        })
                        .and_then(|key| {
                            cid_from_store_key(key, "SlateDB node").map_err(slatedb::Error::invalid)
                        })?;
                    cids.push(cid);
                }
                sort_cids(&mut cids);
                Ok(cids)
            },
            "failed to list node CIDs",
        )
    }
}

impl ManifestStore for SlateDbStore {
    type Error = SlateDbStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        let key = root_key(name);
        let bytes = self
            .block_on(
                async { self.db.get(key).await },
                "failed to read root manifest",
            )?
            .map(|bytes| bytes.to_vec());
        decode_root_manifest(bytes)
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        let _guard = self
            .manifest_lock
            .lock()
            .map_err(|e| SlateDbStoreError::new(format!("manifest lock poisoned: {e}")))?;

        let key = Bytes::from(root_key(name));
        let bytes = encode_root_manifest(manifest)?;
        let value = Bytes::from(bytes);
        self.block_on(
            async { self.db.put_bytes(key, value).await.map(|_| ()) },
            "failed to write root manifest",
        )?;
        self.flush_after_write_if_configured()
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        let _guard = self
            .manifest_lock
            .lock()
            .map_err(|e| SlateDbStoreError::new(format!("manifest lock poisoned: {e}")))?;

        let key = root_key(name);
        self.block_on(
            async { self.db.delete(key).await.map(|_| ()) },
            "failed to delete root manifest",
        )?;
        self.flush_after_write_if_configured()
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let _guard = self
            .manifest_lock
            .lock()
            .map_err(|e| SlateDbStoreError::new(format!("manifest lock poisoned: {e}")))?;

        let key = root_key(name);
        let current_bytes = self
            .block_on(
                async { self.db.get(key.clone()).await },
                "failed to read root manifest",
            )?
            .map(|bytes| bytes.to_vec());
        let current = decode_root_manifest(current_bytes)?;
        if current.as_ref() != expected {
            return Ok(ManifestUpdate::Conflict { current });
        }

        match new {
            Some(manifest) => {
                let key = Bytes::from(key);
                let value = Bytes::from(encode_root_manifest(manifest)?);
                self.block_on(
                    async { self.db.put_bytes(key, value).await.map(|_| ()) },
                    "failed to write root manifest",
                )?;
            }
            None => {
                self.block_on(
                    async { self.db.delete(key).await.map(|_| ()) },
                    "failed to delete root manifest",
                )?;
            }
        }

        self.flush_after_write_if_configured()?;
        Ok(ManifestUpdate::Applied)
    }
}

impl ManifestStoreScan for SlateDbStore {
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        let raw_roots = self.block_on(
            async {
                let mut iter = self.db.scan_prefix(ROOT_PREFIX, ..).await?;
                let mut roots = Vec::new();
                while let Some(kv) = iter.next().await? {
                    let key = kv.key.as_ref();
                    let name = key
                        .strip_prefix(ROOT_PREFIX)
                        .ok_or_else(|| {
                            slatedb::Error::invalid(
                                "SlateDB root scan returned key without root prefix".to_string(),
                            )
                        })?
                        .to_vec();
                    roots.push((name, kv.value.to_vec()));
                }
                Ok(roots)
            },
            "failed to list root manifests",
        )?;

        let mut roots = raw_roots
            .into_iter()
            .map(|(name, bytes)| {
                let manifest = RootManifest::from_bytes(&bytes)
                    .map_err(|err| SlateDbStoreError::new(err.to_string()))?;
                Ok(NamedRootManifest::new(name, manifest))
            })
            .collect::<Result<Vec<_>, SlateDbStoreError>>()?;
        sort_named_root_manifests(&mut roots);
        Ok(roots)
    }
}

fn node_key(key: &[u8]) -> Vec<u8> {
    let mut storage_key = Vec::with_capacity(NODE_PREFIX.len() + key.len());
    storage_key.extend_from_slice(NODE_PREFIX);
    storage_key.extend_from_slice(key);
    storage_key
}

fn hint_key(namespace: &[u8], key: &[u8]) -> Vec<u8> {
    let mut storage_key = Vec::with_capacity(HINT_PREFIX.len() + 4 + namespace.len() + key.len());
    storage_key.extend_from_slice(HINT_PREFIX);
    storage_key.extend_from_slice(&(namespace.len() as u32).to_be_bytes());
    storage_key.extend_from_slice(namespace);
    storage_key.extend_from_slice(key);
    storage_key
}

fn root_key(name: &[u8]) -> Vec<u8> {
    let mut storage_key = Vec::with_capacity(ROOT_PREFIX.len() + name.len());
    storage_key.extend_from_slice(ROOT_PREFIX);
    storage_key.extend_from_slice(name);
    storage_key
}

fn encode_root_manifest(manifest: &RootManifest) -> Result<Vec<u8>, SlateDbStoreError> {
    manifest
        .to_bytes()
        .map_err(|e| SlateDbStoreError::new(format!("failed to encode root manifest: {e}")))
}

fn decode_root_manifest(bytes: Option<Vec<u8>>) -> Result<Option<RootManifest>, SlateDbStoreError> {
    bytes
        .as_deref()
        .map(RootManifest::from_bytes)
        .transpose()
        .map_err(|e| SlateDbStoreError::new(format!("failed to decode root manifest: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::{Config, Prolly};

    fn in_memory_store(path: &str) -> SlateDbStore {
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        SlateDbStore::open(path, object_store).unwrap()
    }

    #[test]
    fn slatedb_store_put_get_delete() {
        let store = in_memory_store("test_put_get_delete");

        store.put(b"key", b"value").unwrap();
        assert_eq!(store.get(b"key").unwrap(), Some(b"value".to_vec()));

        store.delete(b"key").unwrap();
        assert_eq!(store.get(b"key").unwrap(), None);
    }

    #[test]
    fn slatedb_store_batch_is_order_preserving_for_reads() {
        let store = in_memory_store("test_batch_reads");
        let ops = vec![
            BatchOp::Upsert {
                key: b"a",
                value: b"1",
            },
            BatchOp::Upsert {
                key: b"b",
                value: b"2",
            },
            BatchOp::Upsert {
                key: b"c",
                value: b"3",
            },
        ];

        store.batch(&ops).unwrap();

        let keys: Vec<&[u8]> = vec![b"c", b"missing", b"a", b"c", b"missing", b"b"];
        assert_eq!(
            store.batch_get_ordered(&keys).unwrap(),
            vec![
                Some(b"3".to_vec()),
                None,
                Some(b"1".to_vec()),
                Some(b"3".to_vec()),
                None,
                Some(b"2".to_vec())
            ]
        );
    }

    #[test]
    fn slatedb_store_fast_paths_empty_and_single_batch_reads() {
        let store = in_memory_store("test_empty_single_batch_reads");
        let empty: Vec<&[u8]> = Vec::new();

        assert_eq!(store.batch_get_ordered(&empty).unwrap(), Vec::new());
        assert!(store.batch_get(&empty).unwrap().is_empty());

        store.put(b"a", b"1").unwrap();
        let existing: Vec<&[u8]> = vec![b"a"];
        let missing: Vec<&[u8]> = vec![b"missing"];

        assert_eq!(
            store.batch_get_ordered(&existing).unwrap(),
            vec![Some(b"1".to_vec())]
        );
        assert_eq!(store.batch_get_ordered(&missing).unwrap(), vec![None]);

        let values = store.batch_get(&existing).unwrap();
        assert_eq!(values.get(b"a".as_slice()), Some(&b"1".to_vec()));
        assert!(store.batch_get(&missing).unwrap().is_empty());
    }

    #[test]
    fn slatedb_store_persists_hints_separately_from_nodes() {
        let store = in_memory_store("test_hints");

        store.put_hint(b"rightmost", b"root", b"hint-v1").unwrap();
        assert_eq!(
            store.get_hint(b"rightmost", b"root").unwrap(),
            Some(b"hint-v1".to_vec())
        );
        assert_eq!(store.get_hint(b"rightmost", b"missing").unwrap(), None);
        assert_eq!(store.get(b"root").unwrap(), None);

        store.put_hint(b"rightmost", b"root", b"hint-v2").unwrap();
        assert_eq!(
            store.get_hint(b"rightmost", b"root").unwrap(),
            Some(b"hint-v2".to_vec())
        );
    }

    #[test]
    fn slatedb_store_reopens_from_same_object_store() {
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(slatedb::object_store::memory::InMemory::new());
        let path = "test_reopen";
        {
            let store = SlateDbStore::open(path, object_store.clone()).unwrap();
            store.put(b"key", b"value").unwrap();
        }

        let store = SlateDbStore::open(path, object_store).unwrap();
        assert_eq!(store.get(b"key").unwrap(), Some(b"value".to_vec()));
    }

    #[test]
    fn slatedb_store_supports_prolly_tree_round_trip() {
        let store = in_memory_store("test_prolly_round_trip");
        let config = Config::default();
        let prolly = Prolly::new(store, config);
        let tree = prolly.create();
        let tree = prolly
            .put(&tree, b"name".to_vec(), b"Alice".to_vec())
            .unwrap();
        let tree = prolly.put(&tree, b"age".to_vec(), b"30".to_vec()).unwrap();

        assert_eq!(prolly.get(&tree, b"name").unwrap(), Some(b"Alice".to_vec()));
        assert_eq!(prolly.get(&tree, b"age").unwrap(), Some(b"30".to_vec()));
    }
}
