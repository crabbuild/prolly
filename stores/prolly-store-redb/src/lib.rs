#![doc = include_str!("../README.md")]

use std::collections::HashMap;
use std::error::Error as StdError;
use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};

use prolly::{
    BatchOp, Cid, Error, ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRootManifest,
    NodeStoreScan, RootCondition, RootManifest, RootWrite, Store, TransactionConflict,
    TransactionNodeWrite, TransactionUpdate, TransactionalStore,
};

pub use redb::Durability;

const NODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_nodes");
const ROOTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_roots");
const HINTS: TableDefinition<(&[u8], &[u8]), &[u8]> = TableDefinition::new("prolly_hints");

/// Configuration for [`RedbStore`].
#[derive(Debug, Clone, Copy)]
pub struct RedbStoreConfig {
    /// Maximum bytes redb may use for its in-memory page cache.
    pub cache_size_bytes: usize,
    /// Durability applied to every committed write transaction.
    pub durability: Durability,
}

impl Default for RedbStoreConfig {
    fn default() -> Self {
        Self {
            cache_size_bytes: 1024 * 1024 * 1024,
            durability: Durability::Immediate,
        }
    }
}

/// Error returned by [`RedbStore`] operations.
#[derive(Debug)]
pub struct RedbStoreError {
    message: String,
    source: Option<Box<dyn StdError + Send + Sync>>,
}

impl RedbStoreError {
    fn message(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    fn with_source(
        context: impl Into<String>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        let context = context.into();
        Self {
            message: format!("{context}: {source}"),
            source: Some(Box::new(source)),
        }
    }
}

impl std::fmt::Display for RedbStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "redb store error: {}", self.message)
    }
}

impl StdError for RedbStoreError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

/// Persistent synchronous prolly store backed by redb.
///
/// The store keeps nodes, named roots, and advisory hints in separate redb
/// tables. All mutation methods commit through redb transactions, and strict
/// prolly transactions atomically validate roots and update nodes and roots.
pub struct RedbStore {
    db: Database,
    durability: Durability,
}

impl std::fmt::Debug for RedbStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedbStore")
            .field("durability", &self.durability)
            .finish_non_exhaustive()
    }
}

impl RedbStore {
    /// Open an existing redb database or create one with default settings.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RedbStoreError> {
        Self::open_with_config(path, RedbStoreConfig::default())
    }

    /// Open an existing redb database or create one with `config`.
    pub fn open_with_config(
        path: impl AsRef<Path>,
        config: RedbStoreConfig,
    ) -> Result<Self, RedbStoreError> {
        let mut builder = Database::builder();
        builder.set_cache_size(config.cache_size_bytes);
        let db = builder.create(path.as_ref()).map_err(|error| {
            RedbStoreError::with_source(
                format!("failed to open database at {:?}", path.as_ref()),
                error,
            )
        })?;
        let store = Self {
            db,
            durability: config.durability,
        };
        store.initialize_tables()?;
        Ok(store)
    }

    fn initialize_tables(&self) -> Result<(), RedbStoreError> {
        let transaction = self.begin_write("failed to begin table initialization")?;
        {
            let _nodes = transaction.open_table(NODES).map_err(|error| {
                RedbStoreError::with_source("failed to initialize node table", error)
            })?;
            let _roots = transaction.open_table(ROOTS).map_err(|error| {
                RedbStoreError::with_source("failed to initialize root table", error)
            })?;
            let _hints = transaction.open_table(HINTS).map_err(|error| {
                RedbStoreError::with_source("failed to initialize hint table", error)
            })?;
        }
        transaction.commit().map_err(|error| {
            RedbStoreError::with_source("failed to commit table initialization", error)
        })
    }

    fn begin_write(&self, context: &str) -> Result<WriteTransaction, RedbStoreError> {
        let mut transaction = self
            .db
            .begin_write()
            .map_err(|error| RedbStoreError::with_source(context, error))?;
        transaction
            .set_durability(self.durability)
            .map_err(|error| {
                RedbStoreError::with_source("failed to configure transaction durability", error)
            })?;
        Ok(transaction)
    }

    fn read_nodes_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, RedbStoreError> {
        let transaction = self.db.begin_read().map_err(|error| {
            RedbStoreError::with_source("failed to begin node read transaction", error)
        })?;
        let table = transaction.open_table(NODES).map_err(|error| {
            RedbStoreError::with_source("failed to open node table for reading", error)
        })?;
        keys.iter()
            .map(|key| {
                table
                    .get(*key)
                    .map(|value| value.map(|guard| guard.value().to_vec()))
                    .map_err(|error| RedbStoreError::with_source("failed to read node", error))
            })
            .collect()
    }
}

impl Store for RedbStore {
    type Error = RedbStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let transaction = self.db.begin_read().map_err(|error| {
            RedbStoreError::with_source("failed to begin node read transaction", error)
        })?;
        let table = transaction.open_table(NODES).map_err(|error| {
            RedbStoreError::with_source("failed to open node table for reading", error)
        })?;
        table
            .get(key)
            .map(|value| value.map(|guard| guard.value().to_vec()))
            .map_err(|error| RedbStoreError::with_source("failed to read node", error))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let transaction = self.begin_write("failed to begin node write transaction")?;
        {
            let mut table = transaction.open_table(NODES).map_err(|error| {
                RedbStoreError::with_source("failed to open node table for writing", error)
            })?;
            table
                .insert(key, value)
                .map_err(|error| RedbStoreError::with_source("failed to write node", error))?;
        }
        transaction
            .commit()
            .map_err(|error| RedbStoreError::with_source("failed to commit node write", error))
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        let transaction = self.begin_write("failed to begin node delete transaction")?;
        {
            let mut table = transaction.open_table(NODES).map_err(|error| {
                RedbStoreError::with_source("failed to open node table for deletion", error)
            })?;
            table
                .remove(key)
                .map_err(|error| RedbStoreError::with_source("failed to delete node", error))?;
        }
        transaction
            .commit()
            .map_err(|error| RedbStoreError::with_source("failed to commit node deletion", error))
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let transaction = self.begin_write("failed to begin node batch transaction")?;
        {
            let mut table = transaction.open_table(NODES).map_err(|error| {
                RedbStoreError::with_source("failed to open node table for batch", error)
            })?;
            for op in ops {
                match op {
                    BatchOp::Upsert { key, value } => {
                        table.insert(*key, *value).map_err(|error| {
                            RedbStoreError::with_source("failed to write node in batch", error)
                        })?;
                    }
                    BatchOp::Delete { key } => {
                        table.remove(*key).map_err(|error| {
                            RedbStoreError::with_source("failed to delete node in batch", error)
                        })?;
                    }
                }
            }
        }
        transaction
            .commit()
            .map_err(|error| RedbStoreError::with_source("failed to commit node batch", error))
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let values = self.read_nodes_ordered(keys)?;
        Ok(keys
            .iter()
            .zip(values)
            .filter_map(|(key, value)| value.map(|value| (key.to_vec(), value)))
            .collect())
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.read_nodes_ordered(keys)
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.read_nodes_ordered(keys)
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let transaction = self.begin_write("failed to begin node batch-put transaction")?;
        {
            let mut table = transaction.open_table(NODES).map_err(|error| {
                RedbStoreError::with_source("failed to open node table for batch put", error)
            })?;
            for (key, value) in entries {
                table.insert(*key, *value).map_err(|error| {
                    RedbStoreError::with_source("failed to write node in batch put", error)
                })?;
            }
        }
        transaction
            .commit()
            .map_err(|error| RedbStoreError::with_source("failed to commit node batch put", error))
    }

    fn supports_hints(&self) -> bool {
        true
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let transaction = self.db.begin_read().map_err(|error| {
            RedbStoreError::with_source("failed to begin hint read transaction", error)
        })?;
        let table = transaction.open_table(HINTS).map_err(|error| {
            RedbStoreError::with_source("failed to open hint table for reading", error)
        })?;
        table
            .get((namespace, key))
            .map(|value| value.map(|guard| guard.value().to_vec()))
            .map_err(|error| RedbStoreError::with_source("failed to read hint", error))
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let transaction = self.begin_write("failed to begin hint write transaction")?;
        {
            let mut table = transaction.open_table(HINTS).map_err(|error| {
                RedbStoreError::with_source("failed to open hint table for writing", error)
            })?;
            table
                .insert((namespace, key), value)
                .map_err(|error| RedbStoreError::with_source("failed to write hint", error))?;
        }
        transaction
            .commit()
            .map_err(|error| RedbStoreError::with_source("failed to commit hint write", error))
    }

    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        let transaction = self.begin_write("failed to begin node and hint transaction")?;
        {
            let mut nodes = transaction.open_table(NODES).map_err(|error| {
                RedbStoreError::with_source("failed to open node table for publication", error)
            })?;
            let mut hints = transaction.open_table(HINTS).map_err(|error| {
                RedbStoreError::with_source("failed to open hint table for publication", error)
            })?;
            for (node_key, node_value) in entries {
                nodes.insert(*node_key, *node_value).map_err(|error| {
                    RedbStoreError::with_source("failed to publish node", error)
                })?;
            }
            hints
                .insert((namespace, key), value)
                .map_err(|error| RedbStoreError::with_source("failed to publish hint", error))?;
        }
        transaction.commit().map_err(|error| {
            RedbStoreError::with_source("failed to commit node and hint publication", error)
        })
    }
}

impl NodeStoreScan for RedbStore {
    type Error = RedbStoreError;

    fn list_node_cids(&self) -> Result<Vec<Cid>, Self::Error> {
        let transaction = self.db.begin_read().map_err(|error| {
            RedbStoreError::with_source("failed to begin node scan transaction", error)
        })?;
        let table = transaction.open_table(NODES).map_err(|error| {
            RedbStoreError::with_source("failed to open node table for scanning", error)
        })?;
        let iterator = table
            .iter()
            .map_err(|error| RedbStoreError::with_source("failed to iterate node table", error))?;
        let mut cids = Vec::new();
        for entry in iterator {
            let (key, _) = entry.map_err(|error| {
                RedbStoreError::with_source("failed to read node scan entry", error)
            })?;
            let raw = key.value();
            let bytes: [u8; 32] = raw.try_into().map_err(|_| {
                RedbStoreError::message(format!(
                    "node key has invalid CID length {}, expected 32",
                    raw.len()
                ))
            })?;
            cids.push(Cid(bytes));
        }
        cids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        Ok(cids)
    }
}

impl ManifestStore for RedbStore {
    type Error = RedbStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        let transaction = self.db.begin_read().map_err(|error| {
            RedbStoreError::with_source("failed to begin root read transaction", error)
        })?;
        let table = transaction.open_table(ROOTS).map_err(|error| {
            RedbStoreError::with_source("failed to open root table for reading", error)
        })?;
        let value = table
            .get(name)
            .map_err(|error| RedbStoreError::with_source("failed to read root manifest", error))?;
        value
            .map(|guard| decode_root_manifest(guard.value()))
            .transpose()
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        let bytes = encode_root_manifest(manifest)?;
        let transaction = self.begin_write("failed to begin root write transaction")?;
        {
            let mut table = transaction.open_table(ROOTS).map_err(|error| {
                RedbStoreError::with_source("failed to open root table for writing", error)
            })?;
            table.insert(name, bytes.as_slice()).map_err(|error| {
                RedbStoreError::with_source("failed to write root manifest", error)
            })?;
        }
        transaction
            .commit()
            .map_err(|error| RedbStoreError::with_source("failed to commit root write", error))
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        let transaction = self.begin_write("failed to begin root delete transaction")?;
        {
            let mut table = transaction.open_table(ROOTS).map_err(|error| {
                RedbStoreError::with_source("failed to open root table for deletion", error)
            })?;
            table.remove(name).map_err(|error| {
                RedbStoreError::with_source("failed to delete root manifest", error)
            })?;
        }
        transaction
            .commit()
            .map_err(|error| RedbStoreError::with_source("failed to commit root deletion", error))
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let new_bytes = new.map(encode_root_manifest).transpose()?;
        let transaction = self.begin_write("failed to begin root CAS transaction")?;
        {
            let mut table = transaction.open_table(ROOTS).map_err(|error| {
                RedbStoreError::with_source("failed to open root table for CAS", error)
            })?;
            let current = {
                let value = table.get(name).map_err(|error| {
                    RedbStoreError::with_source("failed to read root during CAS", error)
                })?;
                value
                    .map(|guard| decode_root_manifest(guard.value()))
                    .transpose()?
            };
            if current.as_ref() != expected {
                return Ok(ManifestUpdate::Conflict { current });
            }
            match new_bytes.as_deref() {
                Some(bytes) => {
                    table.insert(name, bytes).map_err(|error| {
                        RedbStoreError::with_source("failed to write root during CAS", error)
                    })?;
                }
                None => {
                    table.remove(name).map_err(|error| {
                        RedbStoreError::with_source("failed to delete root during CAS", error)
                    })?;
                }
            }
        }
        transaction
            .commit()
            .map_err(|error| RedbStoreError::with_source("failed to commit root CAS", error))?;
        Ok(ManifestUpdate::Applied)
    }
}

impl ManifestStoreScan for RedbStore {
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        let transaction = self.db.begin_read().map_err(|error| {
            RedbStoreError::with_source("failed to begin root scan transaction", error)
        })?;
        let table = transaction.open_table(ROOTS).map_err(|error| {
            RedbStoreError::with_source("failed to open root table for scanning", error)
        })?;
        let iterator = table
            .iter()
            .map_err(|error| RedbStoreError::with_source("failed to iterate root table", error))?;
        let mut roots = Vec::new();
        for entry in iterator {
            let (name, manifest) = entry.map_err(|error| {
                RedbStoreError::with_source("failed to read root scan entry", error)
            })?;
            roots.push(NamedRootManifest::new(
                name.value().to_vec(),
                decode_root_manifest(manifest.value())?,
            ));
        }
        roots.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(roots)
    }
}

impl TransactionalStore for RedbStore {
    fn supports_transactions(&self) -> bool {
        true
    }

    fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        let transaction = self
            .begin_write("failed to begin strict transaction")
            .map_err(store_error)?;
        {
            let mut nodes = transaction.open_table(NODES).map_err(|error| {
                store_error(RedbStoreError::with_source(
                    "failed to open node table for strict transaction",
                    error,
                ))
            })?;
            let mut roots = transaction.open_table(ROOTS).map_err(|error| {
                store_error(RedbStoreError::with_source(
                    "failed to open root table for strict transaction",
                    error,
                ))
            })?;

            for condition in root_conditions {
                let current = {
                    let value = roots.get(condition.name.as_slice()).map_err(|error| {
                        store_error(RedbStoreError::with_source(
                            "failed to read root condition",
                            error,
                        ))
                    })?;
                    value
                        .map(|guard| decode_root_manifest(guard.value()))
                        .transpose()
                        .map_err(store_error)?
                };
                if current != condition.expected {
                    return Ok(TransactionUpdate::Conflict(Box::new(
                        TransactionConflict::new(
                            condition.name.clone(),
                            condition.expected.clone(),
                            current,
                        ),
                    )));
                }
            }

            for write in node_writes {
                match write {
                    TransactionNodeWrite::Upsert { key, value } => {
                        nodes
                            .insert(key.as_slice(), value.as_slice())
                            .map_err(|error| {
                                store_error(RedbStoreError::with_source(
                                    "failed to write node in strict transaction",
                                    error,
                                ))
                            })?;
                    }
                    TransactionNodeWrite::Delete { key } => {
                        nodes.remove(key.as_slice()).map_err(|error| {
                            store_error(RedbStoreError::with_source(
                                "failed to delete node in strict transaction",
                                error,
                            ))
                        })?;
                    }
                }
            }

            for write in root_writes {
                match write {
                    RootWrite::Put { name, manifest } => {
                        let bytes = encode_root_manifest(manifest).map_err(store_error)?;
                        roots
                            .insert(name.as_slice(), bytes.as_slice())
                            .map_err(|error| {
                                store_error(RedbStoreError::with_source(
                                    "failed to write root in strict transaction",
                                    error,
                                ))
                            })?;
                    }
                    RootWrite::Delete { name } => {
                        roots.remove(name.as_slice()).map_err(|error| {
                            store_error(RedbStoreError::with_source(
                                "failed to delete root in strict transaction",
                                error,
                            ))
                        })?;
                    }
                }
            }
        }
        transaction.commit().map_err(|error| {
            store_error(RedbStoreError::with_source(
                "failed to commit strict transaction",
                error,
            ))
        })?;
        Ok(TransactionUpdate::Applied {
            nodes_written: node_writes.len(),
            roots_written: root_writes.len(),
        })
    }
}

fn encode_root_manifest(manifest: &RootManifest) -> Result<Vec<u8>, RedbStoreError> {
    manifest
        .to_bytes()
        .map_err(|error| RedbStoreError::with_source("failed to encode root manifest", error))
}

fn decode_root_manifest(bytes: &[u8]) -> Result<RootManifest, RedbStoreError> {
    RootManifest::from_bytes(bytes)
        .map_err(|error| RedbStoreError::with_source("failed to decode root manifest", error))
}

fn store_error(error: RedbStoreError) -> Error {
    Error::Store(Box::new(error))
}
