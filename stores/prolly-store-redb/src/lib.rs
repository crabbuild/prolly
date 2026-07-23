#![doc = include_str!("../README.md")]

use std::collections::{hash_map::Entry, HashMap};
use std::error::Error as StdError;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use ahash::AHashMap;
use parking_lot::{Mutex, RwLock};
use redb::{
    Database, ReadableDatabase, ReadableTable, ReadableTableMetadata, TableDefinition,
    WriteTransaction,
};

use prolly::{
    BatchOp, Cid, Error, ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRootManifest,
    NodeStoreScan, RootCondition, RootManifest, RootWrite, Store, TransactionConflict,
    TransactionNodeWrite, TransactionUpdate, TransactionalStore,
};

pub use redb::Durability;

const V2_NODES: TableDefinition<&[u8], (u8, &[u8])> = TableDefinition::new("prolly_nodes_v2");
const NODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_nodes");
const ROOTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_roots");
const HINTS: TableDefinition<(&[u8], &[u8]), &[u8]> = TableDefinition::new("prolly_hints");

const NODE_ENCODING_RAW: u8 = 0;
const NODE_ENCODING_LZ4: u8 = 1;
const NODE_ENVELOPE_MAGIC: &[u8; 4] = b"PRN1";
const NODE_ENVELOPE_HEADER_BYTES: usize = NODE_ENVELOPE_MAGIC.len() + 1;
const MIN_COMPRESSIBLE_NODE_BYTES: usize = 8 * 1024;
const DEFAULT_NODE_READ_CACHE_SIZE_BYTES: usize = 128 * 1024 * 1024;

type V2NodeTable<'txn> = redb::Table<'txn, &'static [u8], (u8, &'static [u8])>;

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

/// Extended adapter options that preserve [`RedbStoreConfig`] compatibility.
///
/// Redb's page cache stores encoded database pages, while the node read cache
/// retains decoded immutable prolly nodes. Their memory budgets are separate.
#[derive(Debug, Clone, Copy)]
pub struct RedbStoreOptions {
    /// Redb database configuration.
    pub database: RedbStoreConfig,
    /// Maximum decoded node bytes retained for native shared reads.
    ///
    /// Set this to zero to disable retention without changing read semantics.
    pub node_read_cache_size_bytes: usize,
    /// Transparently LZ4-compress large nodes when it reduces their size.
    pub compress_nodes: bool,
}

impl Default for RedbStoreOptions {
    fn default() -> Self {
        Self {
            database: RedbStoreConfig::default(),
            node_read_cache_size_bytes: DEFAULT_NODE_READ_CACHE_SIZE_BYTES,
            compress_nodes: true,
        }
    }
}

struct NodeReadCache {
    values: AHashMap<Vec<u8>, Arc<[u8]>>,
    retained_bytes: usize,
    max_bytes: usize,
    generation: u64,
}

impl NodeReadCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            values: AHashMap::new(),
            retained_bytes: 0,
            max_bytes,
            generation: 0,
        }
    }

    fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        self.values.get(key).cloned()
    }

    fn insert(&mut self, key: &[u8], value: Arc<[u8]>) {
        if self.max_bytes == 0 || value.len() > self.max_bytes {
            return;
        }
        if let Some(previous) = self.values.remove(key) {
            self.retained_bytes = self.retained_bytes.saturating_sub(previous.len());
        }
        if self.retained_bytes.saturating_add(value.len()) > self.max_bytes {
            self.values.clear();
            self.retained_bytes = 0;
        }
        self.retained_bytes = self.retained_bytes.saturating_add(value.len());
        self.values.insert(key.to_vec(), value);
    }

    fn remove(&mut self, key: &[u8]) {
        if let Some(value) = self.values.remove(key) {
            self.retained_bytes = self.retained_bytes.saturating_sub(value.len());
        }
    }

    fn invalidate<'a>(&mut self, keys: impl IntoIterator<Item = &'a [u8]>) {
        self.generation = self.generation.wrapping_add(1);
        for key in keys {
            self.remove(key);
        }
    }
}

struct OrderedBatchReadPlan<'a> {
    unique_keys: Vec<&'a [u8]>,
    positions: Option<Vec<usize>>,
}

impl<'a> OrderedBatchReadPlan<'a> {
    fn new(keys: &[&'a [u8]]) -> Self {
        let mut unique_indexes = AHashMap::with_capacity(keys.len());
        let mut unique_keys = Vec::with_capacity(keys.len());
        let mut positions = None;
        for key in keys {
            match unique_indexes.entry(*key) {
                Entry::Occupied(entry) => positions
                    .get_or_insert_with(|| (0..unique_keys.len()).collect::<Vec<_>>())
                    .push(*entry.get()),
                Entry::Vacant(entry) => {
                    let index = unique_keys.len();
                    unique_keys.push(*key);
                    if let Some(positions) = positions.as_mut() {
                        positions.push(index);
                    }
                    entry.insert(index);
                }
            }
        }
        Self {
            unique_keys,
            positions,
        }
    }

    fn unique_keys(&self) -> &[&'a [u8]] {
        &self.unique_keys
    }

    fn expand<T: Clone>(&self, values: Vec<Option<T>>) -> Vec<Option<T>> {
        match &self.positions {
            Some(positions) => positions
                .iter()
                .map(|&index| values[index].clone())
                .collect(),
            None => values,
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
    node_read_cache: Mutex<NodeReadCache>,
    node_read_cache_enabled: bool,
    node_read_transaction: RwLock<Option<redb::ReadTransaction>>,
    compress_nodes: bool,
    v2_nodes_present: AtomicBool,
}

impl std::fmt::Debug for RedbStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RedbStore")
            .field("durability", &self.durability)
            .field(
                "node_read_cache_size_bytes",
                &self.node_read_cache.lock().max_bytes,
            )
            .field("compress_nodes", &self.compress_nodes)
            .field(
                "v2_nodes_present",
                &self.v2_nodes_present.load(Ordering::Relaxed),
            )
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
        Self::open_with_options(
            path,
            RedbStoreOptions {
                database: config,
                ..RedbStoreOptions::default()
            },
        )
    }

    /// Open an existing database or create one with database and adapter tuning.
    pub fn open_with_options(
        path: impl AsRef<Path>,
        options: RedbStoreOptions,
    ) -> Result<Self, RedbStoreError> {
        let mut builder = Database::builder();
        builder.set_cache_size(options.database.cache_size_bytes);
        let db = builder.create(path.as_ref()).map_err(|error| {
            RedbStoreError::with_source(
                format!("failed to open database at {:?}", path.as_ref()),
                error,
            )
        })?;
        let store = Self {
            db,
            durability: options.database.durability,
            node_read_cache: Mutex::new(NodeReadCache::new(options.node_read_cache_size_bytes)),
            node_read_cache_enabled: options.node_read_cache_size_bytes != 0,
            node_read_transaction: RwLock::new(None),
            compress_nodes: options.compress_nodes,
            v2_nodes_present: AtomicBool::new(false),
        };
        store.initialize_tables()?;
        Ok(store)
    }

    /// Reclaim free pages and compact the database file where possible.
    ///
    /// Returns `true` when redb performed compaction and `false` when the file
    /// cannot be compacted further. The store must be mutably borrowed so no
    /// concurrent operation can begin while redb rewrites the file.
    pub fn compact(&mut self) -> Result<bool, RedbStoreError> {
        *self.node_read_transaction.write() = None;
        self.db
            .compact()
            .map_err(|error| RedbStoreError::with_source("failed to compact database", error))
    }

    fn initialize_tables(&self) -> Result<(), RedbStoreError> {
        let transaction = self.begin_write("failed to begin table initialization")?;
        let v2_nodes_present;
        {
            let v2_nodes = transaction.open_table(V2_NODES).map_err(|error| {
                RedbStoreError::with_source("failed to initialize v2 node table", error)
            })?;
            v2_nodes_present = !v2_nodes.is_empty().map_err(|error| {
                RedbStoreError::with_source("failed to inspect v2 node table", error)
            })?;
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
        })?;
        self.v2_nodes_present
            .store(v2_nodes_present, Ordering::Relaxed);
        Ok(())
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

    fn open_v2_nodes_for_write<'txn>(
        &self,
        transaction: &'txn WriteTransaction,
        operation: &str,
    ) -> Result<Option<V2NodeTable<'txn>>, RedbStoreError> {
        if !self.v2_nodes_present.load(Ordering::Relaxed) {
            return Ok(None);
        }
        transaction.open_table(V2_NODES).map(Some).map_err(|error| {
            RedbStoreError::with_source(
                format!("failed to open v2 node table for {operation}"),
                error,
            )
        })
    }

    fn read_nodes_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Arc<[u8]>>>, RedbStoreError> {
        loop {
            let mut values = vec![None; keys.len()];
            let mut missing = Vec::new();
            let read_generation = {
                let cache = self.node_read_cache.lock();
                for (position, key) in keys.iter().enumerate() {
                    match cache.get(key) {
                        Some(value) => values[position] = Some(value),
                        None => missing.push((position, *key)),
                    }
                }
                cache.generation
            };
            if missing.is_empty() {
                return Ok(values);
            }

            self.ensure_node_read_transaction()?;
            let transaction_guard = self.node_read_transaction.read();
            let transaction = transaction_guard
                .as_ref()
                .expect("node read transaction was initialized");
            let table = transaction.open_table(NODES).map_err(|error| {
                RedbStoreError::with_source("failed to open node table for reading", error)
            })?;
            let v2 = self
                .v2_nodes_present
                .load(Ordering::Relaxed)
                .then(|| {
                    transaction.open_table(V2_NODES).map_err(|error| {
                        RedbStoreError::with_source(
                            "failed to open v2 node table for reading",
                            error,
                        )
                    })
                })
                .transpose()?;
            if !missing.windows(2).all(|pair| pair[0].1 <= pair[1].1) {
                missing.sort_unstable_by(|left, right| left.1.cmp(right.1));
            }
            let mut loaded_values: Vec<Option<Arc<[u8]>>> = Vec::with_capacity(missing.len());
            for (_, key) in &missing {
                let value = if let Some(value) = table
                    .get(*key)
                    .map_err(|error| RedbStoreError::with_source("failed to read node", error))?
                {
                    Some(Arc::from(decode_stored_node(value.value())?))
                } else if let Some(value) = v2
                    .as_ref()
                    .map(|v2| {
                        v2.get(*key).map_err(|error| {
                            RedbStoreError::with_source("failed to read v2 node", error)
                        })
                    })
                    .transpose()?
                    .flatten()
                {
                    let (encoding, bytes) = value.value();
                    Some(Arc::from(decode_v2_stored_node(encoding, bytes)?))
                } else {
                    None
                };
                loaded_values.push(value);
            }
            drop(v2);
            drop(table);
            drop(transaction_guard);

            let mut cache = self.node_read_cache.lock();
            if cache.generation != read_generation {
                continue;
            }
            for ((position, key), loaded) in missing.into_iter().zip(loaded_values) {
                if let Some(loaded) = loaded {
                    cache.insert(key, loaded.clone());
                    values[position] = Some(loaded);
                }
            }
            return Ok(values);
        }
    }

    fn read_nodes_owned_ordered_unique_uncached(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, RedbStoreError> {
        self.ensure_node_read_transaction()?;
        let transaction_guard = self.node_read_transaction.read();
        let transaction = transaction_guard
            .as_ref()
            .expect("node read transaction was initialized");
        let table = transaction.open_table(NODES).map_err(|error| {
            RedbStoreError::with_source("failed to open node table for reading", error)
        })?;
        let v2 = self
            .v2_nodes_present
            .load(Ordering::Relaxed)
            .then(|| {
                transaction.open_table(V2_NODES).map_err(|error| {
                    RedbStoreError::with_source("failed to open v2 node table for reading", error)
                })
            })
            .transpose()?;
        let mut order = (0..keys.len()).collect::<Vec<_>>();
        if !keys.windows(2).all(|pair| pair[0] <= pair[1]) {
            order.sort_unstable_by(|&left, &right| keys[left].cmp(keys[right]));
        }
        let mut values = vec![None; keys.len()];
        for index in order {
            let key = keys[index];
            values[index] = if let Some(value) = table
                .get(key)
                .map_err(|error| RedbStoreError::with_source("failed to read node", error))?
            {
                decode_stored_node(value.value()).map(Some)
            } else if let Some(value) = v2
                .as_ref()
                .map(|v2| {
                    v2.get(key).map_err(|error| {
                        RedbStoreError::with_source("failed to read v2 node", error)
                    })
                })
                .transpose()?
                .flatten()
            {
                let (encoding, bytes) = value.value();
                decode_v2_stored_node(encoding, bytes).map(Some)
            } else {
                Ok(None)
            }?;
        }
        Ok(values)
    }

    fn node_read_cache_enabled(&self) -> bool {
        self.node_read_cache_enabled
    }

    fn ensure_node_read_transaction(&self) -> Result<(), RedbStoreError> {
        if self.node_read_transaction.read().is_some() {
            return Ok(());
        }
        let mut retained = self.node_read_transaction.write();
        if retained.is_none() {
            *retained = Some(self.db.begin_read().map_err(|error| {
                RedbStoreError::with_source("failed to begin retained node read transaction", error)
            })?);
        }
        Ok(())
    }

    fn invalidate_node(&self, key: &[u8]) {
        *self.node_read_transaction.write() = None;
        self.node_read_cache.lock().invalidate([key]);
    }

    fn invalidate_nodes<'a>(&self, keys: impl IntoIterator<Item = &'a [u8]>) {
        *self.node_read_transaction.write() = None;
        self.node_read_cache.lock().invalidate(keys);
    }
}

impl Store for RedbStore {
    type Error = RedbStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        if self.node_read_cache_enabled() {
            self.get_shared(key)
                .map(|value| value.map(|value| value.as_ref().to_vec()))
        } else {
            self.read_nodes_owned_ordered_unique_uncached(&[key])
                .map(|mut values| values.pop().flatten())
        }
    }

    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        if self.node_read_cache_enabled() {
            self.read_nodes_shared_ordered_unique(&[key])
                .map(|mut values| values.pop().flatten())
        } else {
            self.read_nodes_owned_ordered_unique_uncached(&[key])
                .map(|mut values| values.pop().flatten().map(Arc::from))
        }
    }

    fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Self::Error> {
        if self.node_read_cache_enabled() {
            self.read_nodes_shared_ordered_unique(keys)
        } else {
            self.read_nodes_owned_ordered_unique_uncached(keys)
                .map(|values| {
                    values
                        .into_iter()
                        .map(|value| value.map(Arc::from))
                        .collect()
                })
        }
    }

    fn has_native_shared_reads(&self) -> bool {
        self.node_read_cache_enabled()
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let transaction = self.begin_write("failed to begin node write transaction")?;
        {
            let mut table = transaction.open_table(NODES).map_err(|error| {
                RedbStoreError::with_source("failed to open node table for writing", error)
            })?;
            let mut v2 = self.open_v2_nodes_for_write(&transaction, "writing")?;
            let mut compression_scratch = Vec::new();
            insert_stored_node(
                &mut table,
                key,
                value,
                self.compress_nodes,
                &mut compression_scratch,
            )
            .map_err(|error| RedbStoreError::with_source("failed to write node", error))?;
            if let Some(v2) = v2.as_mut() {
                v2.remove(key).map_err(|error| {
                    RedbStoreError::with_source("failed to remove superseded v2 node", error)
                })?;
            }
        }
        transaction
            .commit()
            .map_err(|error| RedbStoreError::with_source("failed to commit node write", error))?;
        self.invalidate_node(key);
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        let transaction = self.begin_write("failed to begin node delete transaction")?;
        {
            let mut table = transaction.open_table(NODES).map_err(|error| {
                RedbStoreError::with_source("failed to open node table for deletion", error)
            })?;
            let mut v2 = self.open_v2_nodes_for_write(&transaction, "deletion")?;
            table
                .remove(key)
                .map_err(|error| RedbStoreError::with_source("failed to delete node", error))?;
            if let Some(v2) = v2.as_mut() {
                v2.remove(key).map_err(|error| {
                    RedbStoreError::with_source("failed to delete v2 node", error)
                })?;
            }
        }
        transaction.commit().map_err(|error| {
            RedbStoreError::with_source("failed to commit node deletion", error)
        })?;
        self.invalidate_node(key);
        Ok(())
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let transaction = self.begin_write("failed to begin node batch transaction")?;
        {
            let mut table = transaction.open_table(NODES).map_err(|error| {
                RedbStoreError::with_source("failed to open node table for batch", error)
            })?;
            let mut v2 = self.open_v2_nodes_for_write(&transaction, "batch")?;
            let mut compression_scratch = Vec::new();
            for op in ops {
                match op {
                    BatchOp::Upsert { key, value } => {
                        insert_stored_node(
                            &mut table,
                            key,
                            value,
                            self.compress_nodes,
                            &mut compression_scratch,
                        )
                        .map_err(|error| {
                            RedbStoreError::with_source("failed to write node in batch", error)
                        })?;
                        if let Some(v2) = v2.as_mut() {
                            v2.remove(*key).map_err(|error| {
                                RedbStoreError::with_source(
                                    "failed to remove superseded v2 node in batch",
                                    error,
                                )
                            })?;
                        }
                    }
                    BatchOp::Delete { key } => {
                        table.remove(*key).map_err(|error| {
                            RedbStoreError::with_source("failed to delete node in batch", error)
                        })?;
                        if let Some(v2) = v2.as_mut() {
                            v2.remove(*key).map_err(|error| {
                                RedbStoreError::with_source(
                                    "failed to delete v2 node in batch",
                                    error,
                                )
                            })?;
                        }
                    }
                }
            }
        }
        transaction
            .commit()
            .map_err(|error| RedbStoreError::with_source("failed to commit node batch", error))?;
        self.invalidate_nodes(ops.iter().map(|op| match op {
            BatchOp::Upsert { key, .. } | BatchOp::Delete { key } => *key,
        }));
        Ok(())
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let plan = OrderedBatchReadPlan::new(keys);
        let values = self.batch_get_ordered_unique(plan.unique_keys())?;
        let mut results = HashMap::with_capacity(plan.unique_keys().len());
        for (key, value) in plan.unique_keys().iter().zip(values) {
            if let Some(value) = value {
                results.insert(key.to_vec(), value);
            }
        }
        Ok(results)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let plan = OrderedBatchReadPlan::new(keys);
        let values = self.batch_get_ordered_unique(plan.unique_keys())?;
        Ok(plan.expand(values))
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        if self.node_read_cache_enabled() {
            self.read_nodes_shared_ordered_unique(keys).map(|values| {
                values
                    .into_iter()
                    .map(|value| value.map(|value| value.as_ref().to_vec()))
                    .collect()
            })
        } else {
            self.read_nodes_owned_ordered_unique_uncached(keys)
        }
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
            let mut v2 = self.open_v2_nodes_for_write(&transaction, "batch put")?;
            let mut order = (0..entries.len()).collect::<Vec<_>>();
            order.sort_by(|&left, &right| entries[left].0.cmp(entries[right].0));
            let mut compression_scratch = Vec::new();
            for index in order {
                let (key, value) = entries[index];
                insert_stored_node(
                    &mut table,
                    key,
                    value,
                    self.compress_nodes,
                    &mut compression_scratch,
                )
                .map_err(|error| {
                    RedbStoreError::with_source("failed to write node in batch put", error)
                })?;
                if let Some(v2) = v2.as_mut() {
                    v2.remove(key).map_err(|error| {
                        RedbStoreError::with_source(
                            "failed to remove superseded v2 node in batch put",
                            error,
                        )
                    })?;
                }
            }
        }
        transaction.commit().map_err(|error| {
            RedbStoreError::with_source("failed to commit node batch put", error)
        })?;
        self.invalidate_nodes(entries.iter().map(|(key, _)| *key));
        Ok(())
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
            let mut v2 = self.open_v2_nodes_for_write(&transaction, "publication")?;
            let mut hints = transaction.open_table(HINTS).map_err(|error| {
                RedbStoreError::with_source("failed to open hint table for publication", error)
            })?;
            let mut order = (0..entries.len()).collect::<Vec<_>>();
            order.sort_by(|&left, &right| entries[left].0.cmp(entries[right].0));
            let mut compression_scratch = Vec::new();
            for index in order {
                let (node_key, node_value) = entries[index];
                insert_stored_node(
                    &mut nodes,
                    node_key,
                    node_value,
                    self.compress_nodes,
                    &mut compression_scratch,
                )
                .map_err(|error| RedbStoreError::with_source("failed to publish node", error))?;
                if let Some(v2) = v2.as_mut() {
                    v2.remove(node_key).map_err(|error| {
                        RedbStoreError::with_source(
                            "failed to remove superseded v2 node during publication",
                            error,
                        )
                    })?;
                }
            }
            hints
                .insert((namespace, key), value)
                .map_err(|error| RedbStoreError::with_source("failed to publish hint", error))?;
        }
        transaction.commit().map_err(|error| {
            RedbStoreError::with_source("failed to commit node and hint publication", error)
        })?;
        self.invalidate_nodes(entries.iter().map(|(key, _)| *key));
        Ok(())
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
        let mut cids = Vec::new();
        for entry in table
            .iter()
            .map_err(|error| RedbStoreError::with_source("failed to iterate node table", error))?
        {
            let (key, _) = entry.map_err(|error| {
                RedbStoreError::with_source("failed to read node scan entry", error)
            })?;
            cids.push(cid_from_store_key(key.value())?);
        }
        if self.v2_nodes_present.load(Ordering::Relaxed) {
            let v2 = transaction.open_table(V2_NODES).map_err(|error| {
                RedbStoreError::with_source("failed to open v2 node table for scanning", error)
            })?;
            for entry in v2.iter().map_err(|error| {
                RedbStoreError::with_source("failed to iterate v2 node table", error)
            })? {
                let (key, _) = entry.map_err(|error| {
                    RedbStoreError::with_source("failed to read v2 node scan entry", error)
                })?;
                cids.push(cid_from_store_key(key.value())?);
            }
        }
        cids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        cids.dedup();
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
            let mut v2_nodes = self
                .open_v2_nodes_for_write(&transaction, "strict transaction")
                .map_err(store_error)?;
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

            let mut compression_scratch = Vec::new();
            for write in node_writes {
                match write {
                    TransactionNodeWrite::Upsert { key, value } => {
                        insert_stored_node(
                            &mut nodes,
                            key.as_slice(),
                            value,
                            self.compress_nodes,
                            &mut compression_scratch,
                        )
                        .map_err(|error| {
                            store_error(RedbStoreError::with_source(
                                "failed to write node in strict transaction",
                                error,
                            ))
                        })?;
                        if let Some(v2_nodes) = v2_nodes.as_mut() {
                            v2_nodes.remove(key.as_slice()).map_err(|error| {
                                store_error(RedbStoreError::with_source(
                                    "failed to remove superseded v2 node in strict transaction",
                                    error,
                                ))
                            })?;
                        }
                    }
                    TransactionNodeWrite::Delete { key } => {
                        nodes.remove(key.as_slice()).map_err(|error| {
                            store_error(RedbStoreError::with_source(
                                "failed to delete node in strict transaction",
                                error,
                            ))
                        })?;
                        if let Some(v2_nodes) = v2_nodes.as_mut() {
                            v2_nodes.remove(key.as_slice()).map_err(|error| {
                                store_error(RedbStoreError::with_source(
                                    "failed to delete v2 node in strict transaction",
                                    error,
                                ))
                            })?;
                        }
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
        self.invalidate_nodes(node_writes.iter().map(|write| match write {
            TransactionNodeWrite::Upsert { key, .. } | TransactionNodeWrite::Delete { key } => {
                key.as_slice()
            }
        }));
        Ok(TransactionUpdate::Applied {
            nodes_written: node_writes.len(),
            roots_written: root_writes.len(),
        })
    }
}

fn cid_from_store_key(key: &[u8]) -> Result<Cid, RedbStoreError> {
    let bytes: [u8; 32] = key.try_into().map_err(|_| {
        RedbStoreError::message(format!(
            "node key has invalid CID length {}, expected 32",
            key.len()
        ))
    })?;
    Ok(Cid(bytes))
}

fn insert_stored_node(
    table: &mut redb::Table<'_, &[u8], &[u8]>,
    key: &[u8],
    node: &[u8],
    compress: bool,
    scratch: &mut Vec<u8>,
) -> redb::Result {
    if compress && node.len() >= MIN_COMPRESSIBLE_NODE_BYTES && node.len() <= u32::MAX as usize {
        let compressed_header_bytes = NODE_ENVELOPE_HEADER_BYTES + 4;
        let maximum =
            compressed_header_bytes + lz4_flex::block::get_maximum_output_size(node.len());
        scratch.clear();
        scratch.resize(maximum, 0);
        scratch[..NODE_ENVELOPE_MAGIC.len()].copy_from_slice(NODE_ENVELOPE_MAGIC);
        scratch[NODE_ENVELOPE_MAGIC.len()] = NODE_ENCODING_LZ4;
        scratch[NODE_ENVELOPE_HEADER_BYTES..compressed_header_bytes]
            .copy_from_slice(&(node.len() as u32).to_le_bytes());
        let compressed_len =
            lz4_flex::block::compress_into(node, &mut scratch[compressed_header_bytes..])
                .expect("maximum LZ4 output size is sufficient");
        let stored_len = compressed_header_bytes + compressed_len;
        if stored_len < NODE_ENVELOPE_HEADER_BYTES + node.len() {
            scratch.truncate(stored_len);
            table.insert(key, scratch.as_slice())?;
            return Ok(());
        }
    }

    let mut stored = table.insert_reserve(key, NODE_ENVELOPE_HEADER_BYTES + node.len())?;
    let stored = stored.as_mut();
    stored[..NODE_ENVELOPE_MAGIC.len()].copy_from_slice(NODE_ENVELOPE_MAGIC);
    stored[NODE_ENVELOPE_MAGIC.len()] = NODE_ENCODING_RAW;
    stored[NODE_ENVELOPE_HEADER_BYTES..].copy_from_slice(node);
    Ok(())
}

fn decode_stored_node(stored: &[u8]) -> Result<Vec<u8>, RedbStoreError> {
    let Some(envelope) = stored.strip_prefix(NODE_ENVELOPE_MAGIC) else {
        return Ok(stored.to_vec());
    };
    let (&encoding, node) = envelope
        .split_first()
        .ok_or_else(|| RedbStoreError::message("stored node is missing its encoding byte"))?;
    match encoding {
        NODE_ENCODING_RAW => Ok(node.to_vec()),
        NODE_ENCODING_LZ4 => lz4_flex::decompress_size_prepended(node)
            .map_err(|error| RedbStoreError::with_source("failed to decompress node", error)),
        other => Err(RedbStoreError::message(format!(
            "unsupported node encoding {other}"
        ))),
    }
}

fn decode_v2_stored_node(encoding: u8, node: &[u8]) -> Result<Vec<u8>, RedbStoreError> {
    match encoding {
        NODE_ENCODING_RAW => Ok(node.to_vec()),
        NODE_ENCODING_LZ4 => lz4_flex::decompress_size_prepended(node)
            .map_err(|error| RedbStoreError::with_source("failed to decompress v2 node", error)),
        other => Err(RedbStoreError::message(format!(
            "unsupported v2 node encoding {other}"
        ))),
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
