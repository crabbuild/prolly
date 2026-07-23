#![doc = include_str!("../README.md")]

use std::cell::Cell;
use std::collections::{hash_map::Entry, HashMap};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use lru::LruCache;
use parking_lot::{Condvar, Mutex, MutexGuard};
use rusqlite::OpenFlags;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use prolly::{
    BatchOp, Cid, Error, ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRootManifest,
    NodePublication, NodeStoreScan, PublicationOrigin, RootCondition, RootManifest, RootWrite,
    Store, TransactionConflict, TransactionNodeWrite, TransactionUpdate, TransactionalStore,
};

const DEFAULT_NODE_CACHE_SHARDS: usize = 16;
static NEXT_SQLITE_READER_SLOT: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SQLITE_READER_SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
}

struct NodeReadCacheShard {
    values: LruCache<Vec<u8>, Arc<[u8]>>,
    retained_bytes: usize,
    max_bytes: usize,
}

impl NodeReadCacheShard {
    fn new(max_bytes: usize) -> Self {
        Self {
            values: LruCache::unbounded(),
            retained_bytes: 0,
            max_bytes,
        }
    }

    fn get(&mut self, key: &[u8]) -> Option<Arc<[u8]>> {
        self.values.get(key).cloned()
    }

    fn insert(&mut self, key: &[u8], value: Arc<[u8]>) -> usize {
        if self.max_bytes == 0 || value.len() > self.max_bytes {
            return 0;
        }
        let mut evictions = 0usize;
        let previous = self.values.pop(key);
        self.retained_bytes = self
            .retained_bytes
            .saturating_sub(previous.as_ref().map_or(0, |entry| entry.len()));
        let target_bytes = self.max_bytes.saturating_sub(value.len());
        while self.retained_bytes > target_bytes {
            if let Some((_, evicted)) = self.values.pop_lru() {
                self.retained_bytes = self.retained_bytes.saturating_sub(evicted.len());
                evictions = evictions.saturating_add(1);
            } else {
                break;
            }
        }
        self.retained_bytes = self.retained_bytes.saturating_add(value.len());
        self.values.put(key.to_vec(), value);
        evictions
    }

    fn insert_immutable(&mut self, key: &[u8], value: Arc<[u8]>) -> usize {
        if self.max_bytes == 0 || value.len() > self.max_bytes {
            return 0;
        }
        if self.get(key).is_some() {
            return 0;
        }
        self.insert(key, value)
    }

    fn remove(&mut self, key: &[u8]) {
        if let Some(value) = self.values.pop(key) {
            self.retained_bytes = self.retained_bytes.saturating_sub(value.len());
        }
    }
}

struct ShardedNodeReadCache {
    shards: Box<[Mutex<NodeReadCacheShard>]>,
    metrics: Arc<SqliteMetricsInner>,
}

impl ShardedNodeReadCache {
    fn new(max_bytes: usize, shard_count: usize, metrics: Arc<SqliteMetricsInner>) -> Self {
        let shard_count = if max_bytes == 0 {
            1
        } else {
            shard_count.max(1)
        };
        let shard_bytes = max_bytes.div_ceil(shard_count);
        let shards = (0..shard_count)
            .map(|_| Mutex::new(NodeReadCacheShard::new(shard_bytes)))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { shards, metrics }
    }

    fn shard_index(&self, key: &[u8]) -> usize {
        let hash = key
            .iter()
            .take(8)
            .fold(0xcbf29ce484222325u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
            });
        hash as usize % self.shards.len()
    }

    fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        let value = self.shards[self.shard_index(key)].lock().get(key);
        if value.is_some() {
            self.metrics.node_cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .node_cache_misses
                .fetch_add(1, Ordering::Relaxed);
        }
        value
    }

    fn insert(&self, key: &[u8], value: Arc<[u8]>) {
        let evictions = self.shards[self.shard_index(key)].lock().insert(key, value);
        self.metrics
            .node_cache_evictions
            .fetch_add(evictions as u64, Ordering::Relaxed);
    }

    fn insert_immutable(&self, key: &[u8], value: Arc<[u8]>) {
        let evictions = self.shards[self.shard_index(key)]
            .lock()
            .insert_immutable(key, value);
        self.metrics
            .node_cache_evictions
            .fetch_add(evictions as u64, Ordering::Relaxed);
    }

    fn remove(&self, key: &[u8]) {
        self.shards[self.shard_index(key)].lock().remove(key);
    }

    fn retained_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().retained_bytes)
            .sum()
    }

    fn entries(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.lock().values.len())
            .sum()
    }
}

#[derive(Default)]
struct SqliteMetricsInner {
    node_cache_hits: AtomicU64,
    node_cache_misses: AtomicU64,
    node_cache_evictions: AtomicU64,
    sql_reads: AtomicU64,
    write_transactions: AtomicU64,
    published_nodes: AtomicU64,
    grouped_publications: AtomicU64,
    checkpoint_attempts: AtomicU64,
    checkpoint_busy: AtomicU64,
    checkpointed_frames: AtomicU64,
    compressed_input_bytes: AtomicU64,
    compressed_stored_bytes: AtomicU64,
}

/// Runtime counters for the SQLite adapter and SQLite pager.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqliteStoreMetrics {
    /// Decoded-node reads served by the adapter cache.
    pub node_cache_hits: u64,
    /// Decoded-node reads not found in the adapter cache.
    pub node_cache_misses: u64,
    /// Decoded nodes removed to keep cache shards within their bounds.
    pub node_cache_evictions: u64,
    /// Decoded nodes currently retained by all cache shards.
    pub node_cache_entries: usize,
    /// Decoded node payload bytes currently retained by all cache shards.
    pub node_cache_retained_bytes: usize,
    /// SQLite node-read statements executed after cache misses.
    pub sql_reads: u64,
    /// Successfully committed SQLite write transactions.
    pub write_transactions: u64,
    /// Immutable nodes submitted by successfully committed publications.
    pub published_nodes: u64,
    /// Publications committed alongside another publication by group commit.
    pub grouped_publications: u64,
    /// Explicit and background WAL checkpoint attempts.
    pub checkpoint_attempts: u64,
    /// Checkpoint attempts that could not complete because a reader was active.
    pub checkpoint_busy: u64,
    /// Frames reported as checkpointed by the latest checkpoint attempt.
    pub checkpointed_frames: u64,
    /// Uncompressed input bytes considered by successful node publications.
    pub compressed_input_bytes: u64,
    /// Stored bytes produced for those node publications.
    pub compressed_stored_bytes: u64,
    /// SQLite page-cache hits reported across all open connections.
    pub sqlite_page_cache_hits: u64,
    /// SQLite page-cache misses reported across all open connections.
    pub sqlite_page_cache_misses: u64,
    /// SQLite page-cache writes reported across all open connections.
    pub sqlite_page_cache_writes: u64,
    /// SQLite page-cache spills reported across all open connections.
    pub sqlite_page_cache_spills: u64,
}

struct OrderedBatchReadPlan<'a> {
    unique_keys: Vec<&'a [u8]>,
    positions: Option<Vec<usize>>,
}

impl<'a> OrderedBatchReadPlan<'a> {
    fn new(keys: &[&'a [u8]]) -> Self {
        let mut unique_indexes = HashMap::with_capacity(keys.len());
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

    fn expand_owned<T: Clone>(&self, values: Vec<Option<T>>) -> Vec<Option<T>> {
        match &self.positions {
            Some(positions) => positions
                .iter()
                .map(|&index| values[index].clone())
                .collect(),
            None => values,
        }
    }
}

fn cid_from_store_key(key: &[u8], context: &str) -> Result<Cid, String> {
    let bytes: [u8; 32] = key.try_into().map_err(|_| {
        format!(
            "{context} key has invalid CID length {}, expected 32",
            key.len()
        )
    })?;
    Ok(Cid(bytes))
}

fn sort_cids(cids: &mut [Cid]) {
    cids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
}

fn sort_named_root_manifests(roots: &mut [NamedRootManifest]) {
    roots.sort_by(|left, right| left.name.cmp(&right.name));
}

const CREATE_TABLE_SQL: &str = "\
CREATE TABLE IF NOT EXISTS prolly_nodes (
    cid      BLOB PRIMARY KEY NOT NULL,
    encoding INTEGER NOT NULL DEFAULT 0,
    node     BLOB NOT NULL
);";

const CREATE_HINTS_TABLE_SQL: &str = "\
CREATE TABLE IF NOT EXISTS prolly_hints (
    namespace BLOB NOT NULL,
    key       BLOB NOT NULL,
    value     BLOB NOT NULL,
    PRIMARY KEY (namespace, key)
) WITHOUT ROWID;";

const CREATE_ROOTS_TABLE_SQL: &str = "\
CREATE TABLE IF NOT EXISTS prolly_roots (
    name     BLOB PRIMARY KEY NOT NULL,
    manifest BLOB NOT NULL
) WITHOUT ROWID;";

const SELECT_SQL: &str = "SELECT encoding, node FROM prolly_nodes WHERE cid = ?1";
const SELECT_NODE_CIDS_SQL: &str = "SELECT cid FROM prolly_nodes ORDER BY cid";
const UPSERT_SQL: &str = "\
INSERT INTO prolly_nodes (cid, encoding, node)
VALUES (?1, ?2, ?3)
ON CONFLICT(cid) DO UPDATE SET encoding = excluded.encoding, node = excluded.node";
const INSERT_IMMUTABLE_SQL: &str = "\
INSERT OR IGNORE INTO prolly_nodes (cid, encoding, node)
VALUES (?1, ?2, ?3)";
const DELETE_SQL: &str = "DELETE FROM prolly_nodes WHERE cid = ?1";
const UPSERT_HINT_SQL: &str = "\
INSERT INTO prolly_hints (namespace, key, value)
VALUES (?1, ?2, ?3)
ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value";
const SELECT_ROOT_SQL: &str = "SELECT manifest FROM prolly_roots WHERE name = ?1";
const SELECT_ROOTS_SQL: &str = "SELECT name, manifest FROM prolly_roots ORDER BY name";
const UPSERT_ROOT_SQL: &str = "\
INSERT INTO prolly_roots (name, manifest)
VALUES (?1, ?2)
ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest";
const DELETE_ROOT_SQL: &str = "DELETE FROM prolly_roots WHERE name = ?1";

/// Configuration options for [`SqliteStore`].
#[derive(Debug, Clone)]
pub struct SqliteStoreConfig {
    /// Busy timeout in milliseconds for contended SQLite locks.
    pub busy_timeout_ms: u64,
    /// Enable WAL journaling for file-backed databases.
    pub enable_wal: bool,
    /// Set SQLite synchronous mode to NORMAL when applying default pragmas.
    pub synchronous_normal: bool,
    /// Page size requested when creating a new SQLite database.
    ///
    /// Existing databases retain the page size stored in their file header.
    pub page_size_bytes: u32,
    /// Maximum bytes retained in SQLite's page cache for this connection.
    ///
    /// A larger cache prevents dirty B-tree pages from being spilled and
    /// rewritten repeatedly during large prolly-node publications.
    pub page_cache_size_bytes: u64,
    /// Number of WAL pages that triggers an automatic checkpoint.
    ///
    /// This should be large enough that a normal node publication commits
    /// without performing checkpoint I/O in its latency-sensitive path.
    pub wal_autocheckpoint_pages: u32,
    /// Maximum decoded node bytes retained by the adapter.
    ///
    /// This cache complements SQLite's encoded page cache and lets immutable
    /// nodes written earlier in the process satisfy later shared reads without
    /// another SQL lookup or decompression.
    pub node_read_cache_size_bytes: usize,
    /// Minimum serialized node size considered for LZ4 compression.
    ///
    /// Smaller values favor database size and cold I/O; larger values avoid
    /// compression CPU for latency-sensitive in-process publication.
    pub node_compression_min_bytes: usize,
    /// Maximum database bytes SQLite may access through memory-mapped reads.
    ///
    /// This avoids per-page read syscalls for large immutable prolly nodes while
    /// retaining SQLite's normal transactional and durability guarantees.
    pub mmap_size_bytes: u64,
    /// Number of read-only SQLite connections used for concurrent cache misses,
    /// scans, and manifest reads. In-memory and externally supplied connections
    /// use the writer connection regardless of this value.
    pub reader_connections: usize,
    /// Let the first thread that reads from a file-backed store reuse the writer
    /// connection. This preserves the single-thread page and statement cache
    /// fast path while other threads use the read-only pool.
    pub primary_reader_uses_writer: bool,
    /// Number of independently locked decoded-node cache shards.
    pub node_read_cache_shards: usize,
    /// Maximum SQLite variables used by one native multi-key read statement.
    pub max_batch_select_keys: usize,
    /// Run passive WAL checkpoints on a dedicated connection instead of in the
    /// latency-sensitive commit path.
    pub background_checkpoints: bool,
    /// Run a passive checkpoint after the WAL file reaches this allocation.
    pub checkpoint_wal_bytes: u64,
    /// Maximum delay between background checkpoint inspections.
    pub checkpoint_interval_ms: u64,
    /// Maximum retained WAL allocation after the log is reset.
    pub journal_size_limit_bytes: u64,
    /// Optional window used to combine concurrent immutable-node publications
    /// into one durable SQLite transaction. Zero disables group commit.
    pub group_commit_delay_micros: u64,
    /// Maximum number of nodes combined in one grouped publication transaction.
    pub group_commit_max_nodes: usize,
}

impl Default for SqliteStoreConfig {
    fn default() -> Self {
        Self {
            busy_timeout_ms: 5_000,
            enable_wal: true,
            synchronous_normal: true,
            page_size_bytes: 64 * 1024,
            page_cache_size_bytes: 64 * 1024 * 1024,
            wal_autocheckpoint_pages: 32 * 1024,
            node_read_cache_size_bytes: 128 * 1024 * 1024,
            node_compression_min_bytes: 8 * 1024,
            mmap_size_bytes: 256 * 1024 * 1024,
            reader_connections: 4,
            primary_reader_uses_writer: true,
            node_read_cache_shards: DEFAULT_NODE_CACHE_SHARDS,
            max_batch_select_keys: 128,
            background_checkpoints: true,
            checkpoint_wal_bytes: 64 * 1024 * 1024,
            checkpoint_interval_ms: 1_000,
            journal_size_limit_bytes: 256 * 1024 * 1024,
            group_commit_delay_micros: 0,
            group_commit_max_nodes: 16_384,
        }
    }
}

/// Error type for SQLite store operations.
#[derive(Debug)]
pub struct SqliteStoreError {
    message: String,
    source: Option<rusqlite::Error>,
}

impl SqliteStoreError {
    /// Create a new error with a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
        }
    }

    /// Create a new error from a rusqlite error.
    pub fn from_sqlite(err: rusqlite::Error, context: impl Into<String>) -> Self {
        Self {
            message: format!("{}: {}", context.into(), err),
            source: Some(err),
        }
    }
}

impl std::fmt::Display for SqliteStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SQLite error: {}", self.message)
    }
}

impl std::error::Error for SqliteStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

impl From<rusqlite::Error> for SqliteStoreError {
    fn from(err: rusqlite::Error) -> Self {
        Self {
            message: err.to_string(),
            source: Some(err),
        }
    }
}

enum ReadConnectionGuard<'a> {
    Writer(MutexGuard<'a, Connection>),
    Reader(MutexGuard<'a, Connection>),
}

impl Deref for ReadConnectionGuard<'_> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Writer(conn) | Self::Reader(conn) => conn,
        }
    }
}

/// WAL checkpoint modes exposed by [`SqliteStore::checkpoint`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteCheckpointMode {
    Passive,
    Full,
    Restart,
    Truncate,
}

impl SqliteCheckpointMode {
    const fn sql(self) -> &'static str {
        match self {
            Self::Passive => "PRAGMA wal_checkpoint(PASSIVE)",
            Self::Full => "PRAGMA wal_checkpoint(FULL)",
            Self::Restart => "PRAGMA wal_checkpoint(RESTART)",
            Self::Truncate => "PRAGMA wal_checkpoint(TRUNCATE)",
        }
    }
}

/// Result returned by a WAL checkpoint operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqliteCheckpointStats {
    /// Whether SQLite reported a busy reader or writer during the checkpoint.
    pub busy: bool,
    /// Frames present in the WAL when SQLite inspected it.
    pub log_frames: u64,
    /// Frames already copied back to the database file.
    pub checkpointed_frames: u64,
}

/// File and freelist statistics useful for maintenance decisions.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SqliteStorageStats {
    /// Configured database page size.
    pub page_size_bytes: u64,
    /// Total pages currently allocated in the database file.
    pub page_count: u64,
    /// Allocated pages currently present on SQLite's freelist.
    pub freelist_pages: u64,
    /// Logical database allocation (`page_size_bytes * page_count`).
    pub database_bytes: u64,
    /// Reclaimable database allocation (`page_size_bytes * freelist_pages`).
    pub free_bytes: u64,
    /// Bytes currently allocated to the WAL file, including retained capacity.
    pub wal_bytes: u64,
}

enum CheckpointSignal {
    WriteCommitted,
    Shutdown,
}

struct CheckpointWorker {
    sender: mpsc::Sender<CheckpointSignal>,
    handle: Option<JoinHandle<()>>,
}

impl CheckpointWorker {
    fn notify_write(&self) {
        let _ = self.sender.send(CheckpointSignal::WriteCommitted);
    }
}

impl Drop for CheckpointWorker {
    fn drop(&mut self) {
        let _ = self.sender.send(CheckpointSignal::Shutdown);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct OwnedPublication {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    hint: Option<(Vec<u8>, Vec<u8>, Vec<u8>)>,
    origin: PublicationOrigin,
}

struct PublicationCompletion {
    result: Mutex<Option<Result<(), String>>>,
    ready: Condvar,
}

struct PendingPublication {
    publication: OwnedPublication,
    completion: Arc<PublicationCompletion>,
}

#[derive(Default)]
struct PublicationGroupState {
    queue: Vec<PendingPublication>,
    flushing: bool,
}

struct PublicationGroupCommit {
    delay: Duration,
    max_nodes: usize,
    state: Mutex<PublicationGroupState>,
}

/// SQLite-backed storage backend for Prolly Trees.
///
/// This store persists content-addressed nodes in a single SQLite table and
/// supports atomic batch operations through transactions.
pub struct SqliteStore {
    writer: Mutex<Connection>,
    readers: Box<[Mutex<Connection>]>,
    primary_reader_thread: OnceLock<thread::ThreadId>,
    primary_reader_uses_writer: bool,
    node_read_cache: ShardedNodeReadCache,
    node_compression_min_bytes: usize,
    max_batch_select_keys: usize,
    metrics: Arc<SqliteMetricsInner>,
    checkpoint_worker: Option<CheckpointWorker>,
    group_commit: Option<PublicationGroupCommit>,
    wal_path: Option<PathBuf>,
}

/// Identity obtained from SQLite's actual open main-database file descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SqliteMainFileIdentity {
    /// Filesystem device containing the open database file.
    pub device: u64,
    /// Filesystem inode of the open database file.
    pub inode: u64,
    /// Length observed through the open descriptor.
    pub length: u64,
}

impl SqliteStore {
    /// Open or create a SQLite database at the given path with default config.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, SqliteStoreError> {
        Self::open_with_config(path, SqliteStoreConfig::default())
    }

    /// Open or create a SQLite database with custom configuration.
    pub fn open_with_config<P: AsRef<Path>>(
        path: P,
        config: SqliteStoreConfig,
    ) -> Result<Self, SqliteStoreError> {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|e| {
            SqliteStoreError::from_sqlite(e, format!("Failed to open database at {path:?}"))
        })?;
        Self::from_connection(conn, config.clone())?.attach_file_runtime(path, &config, None)
    }

    /// Open an existing SQLite database with default runtime configuration.
    ///
    /// Unlike [`Self::open`], this never creates the database file and does not
    /// execute schema DDL. Callers must validate the required schema before
    /// using this path.
    pub fn open_existing<P: AsRef<Path>>(path: P) -> Result<Self, SqliteStoreError> {
        Self::open_existing_verified(path, |_| Ok(()))
    }

    /// Open an existing database with explicit runtime configuration.
    pub fn open_existing_with_config<P: AsRef<Path>>(
        path: P,
        config: SqliteStoreConfig,
    ) -> Result<Self, SqliteStoreError> {
        Self::open_existing_verified_with_config(path, config, |_| Ok(()))
    }

    /// Open an existing database and verify SQLite's actual main-file handle
    /// before executing any pragma, schema statement, or other SQL.
    #[cfg(unix)]
    pub fn open_existing_verified<P, F>(path: P, verifier: F) -> Result<Self, SqliteStoreError>
    where
        P: AsRef<Path>,
        F: FnOnce(SqliteMainFileIdentity) -> Result<(), SqliteStoreError>,
    {
        Self::open_existing_verified_with_config(path, SqliteStoreConfig::default(), verifier)
    }

    /// Open and verify an existing database with explicit runtime tuning.
    #[cfg(unix)]
    pub fn open_existing_verified_with_config<P, F>(
        path: P,
        config: SqliteStoreConfig,
        verifier: F,
    ) -> Result<Self, SqliteStoreError>
    where
        P: AsRef<Path>,
        F: FnOnce(SqliteMainFileIdentity) -> Result<(), SqliteStoreError>,
    {
        let path = path.as_ref().to_path_buf();
        let conn = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|error| {
            SqliteStoreError::from_sqlite(
                error,
                format!("Failed to open existing database at {path:?}"),
            )
        })?;
        let identity = sqlite_main_file_identity(&conn)?;
        verifier(identity)?;
        Self::from_existing_connection(conn, config.clone())?.attach_file_runtime(
            path,
            &config,
            Some(identity),
        )
    }

    /// Non-Unix platforms cannot currently prove the SQLite VFS handle's
    /// identity before SQL runs, so verified opens fail closed there.
    #[cfg(not(unix))]
    pub fn open_existing_verified<P, F>(_path: P, _verifier: F) -> Result<Self, SqliteStoreError>
    where
        P: AsRef<Path>,
        F: FnOnce(SqliteMainFileIdentity) -> Result<(), SqliteStoreError>,
    {
        Err(SqliteStoreError::new(
            "verified existing SQLite opens are unsupported on this platform",
        ))
    }

    /// Non-Unix platforms cannot verify SQLite's actual main-file handle.
    #[cfg(not(unix))]
    pub fn open_existing_verified_with_config<P, F>(
        _path: P,
        _config: SqliteStoreConfig,
        _verifier: F,
    ) -> Result<Self, SqliteStoreError>
    where
        P: AsRef<Path>,
        F: FnOnce(SqliteMainFileIdentity) -> Result<(), SqliteStoreError>,
    {
        Err(SqliteStoreError::new(
            "verified existing SQLite opens are unsupported on this platform",
        ))
    }

    /// Create an in-memory SQLite store.
    pub fn open_in_memory() -> Result<Self, SqliteStoreError> {
        let conn = Connection::open_in_memory()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to open in-memory database"))?;
        Self::from_connection(conn, SqliteStoreConfig::default())
    }

    fn from_connection(
        conn: Connection,
        config: SqliteStoreConfig,
    ) -> Result<Self, SqliteStoreError> {
        Self::validate_config(&config)?;
        // Applied before WAL or schema creation so new stores use pages large
        // enough to pack several compressed prolly nodes per B-tree page.
        conn.pragma_update(None, "page_size", config.page_size_bytes)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set page_size"))?;
        Self::apply_runtime_config(&conn, &config)?;
        conn.execute_batch(CREATE_TABLE_SQL)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to initialize schema"))?;
        ensure_node_encoding_column(&conn)?;
        conn.execute_batch(CREATE_HINTS_TABLE_SQL)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to initialize hint schema"))?;
        conn.execute_batch(CREATE_ROOTS_TABLE_SQL)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to initialize root schema"))?;
        let metrics = Arc::new(SqliteMetricsInner::default());
        Ok(Self {
            writer: Mutex::new(conn),
            readers: Box::new([]),
            primary_reader_thread: OnceLock::new(),
            primary_reader_uses_writer: config.primary_reader_uses_writer,
            node_read_cache: ShardedNodeReadCache::new(
                config.node_read_cache_size_bytes,
                config.node_read_cache_shards,
                metrics.clone(),
            ),
            node_compression_min_bytes: config.node_compression_min_bytes,
            max_batch_select_keys: config.max_batch_select_keys.max(1),
            metrics,
            checkpoint_worker: None,
            group_commit: (config.group_commit_delay_micros > 0).then(|| PublicationGroupCommit {
                delay: Duration::from_micros(config.group_commit_delay_micros),
                max_nodes: config.group_commit_max_nodes.max(1),
                state: Mutex::new(PublicationGroupState::default()),
            }),
            wal_path: None,
        })
    }

    fn from_existing_connection(
        conn: Connection,
        config: SqliteStoreConfig,
    ) -> Result<Self, SqliteStoreError> {
        Self::validate_config(&config)?;
        Self::apply_runtime_config(&conn, &config)?;
        let metrics = Arc::new(SqliteMetricsInner::default());
        Ok(Self {
            writer: Mutex::new(conn),
            readers: Box::new([]),
            primary_reader_thread: OnceLock::new(),
            primary_reader_uses_writer: config.primary_reader_uses_writer,
            node_read_cache: ShardedNodeReadCache::new(
                config.node_read_cache_size_bytes,
                config.node_read_cache_shards,
                metrics.clone(),
            ),
            node_compression_min_bytes: config.node_compression_min_bytes,
            max_batch_select_keys: config.max_batch_select_keys.max(1),
            metrics,
            checkpoint_worker: None,
            group_commit: (config.group_commit_delay_micros > 0).then(|| PublicationGroupCommit {
                delay: Duration::from_micros(config.group_commit_delay_micros),
                max_nodes: config.group_commit_max_nodes.max(1),
                state: Mutex::new(PublicationGroupState::default()),
            }),
            wal_path: None,
        })
    }

    fn validate_config(config: &SqliteStoreConfig) -> Result<(), SqliteStoreError> {
        if !(512..=65_536).contains(&config.page_size_bytes)
            || !config.page_size_bytes.is_power_of_two()
        {
            return Err(SqliteStoreError::new(
                "page_size_bytes must be a power of two from 512 through 65536",
            ));
        }
        if config.reader_connections > 64 {
            return Err(SqliteStoreError::new(
                "reader_connections must not exceed 64",
            ));
        }
        if !(1..=256).contains(&config.node_read_cache_shards) {
            return Err(SqliteStoreError::new(
                "node_read_cache_shards must be from 1 through 256",
            ));
        }
        if config.max_batch_select_keys == 0 {
            return Err(SqliteStoreError::new(
                "max_batch_select_keys must be greater than zero",
            ));
        }
        if config.group_commit_delay_micros > 0 && config.group_commit_max_nodes == 0 {
            return Err(SqliteStoreError::new(
                "group_commit_max_nodes must be greater than zero when group commit is enabled",
            ));
        }
        Ok(())
    }

    fn attach_file_runtime(
        mut self,
        path: PathBuf,
        config: &SqliteStoreConfig,
        expected_identity: Option<SqliteMainFileIdentity>,
    ) -> Result<Self, SqliteStoreError> {
        let mut readers = Vec::with_capacity(config.reader_connections);
        for _ in 0..config.reader_connections {
            let reader = Connection::open_with_flags(
                &path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to open SQLite read connection")
            })?;
            #[cfg(unix)]
            if let Some(expected) = expected_identity {
                let actual = sqlite_main_file_identity(&reader)?;
                if actual.device != expected.device || actual.inode != expected.inode {
                    return Err(SqliteStoreError::new(
                        "SQLite read connection resolved to a different database file",
                    ));
                }
            }
            Self::apply_reader_runtime_config(&reader, config)?;
            readers.push(Mutex::new(reader));
        }
        self.readers = readers.into_boxed_slice();
        self.wal_path = Some(wal_path_for(&path));
        if config.enable_wal && config.background_checkpoints {
            self.writer
                .lock()
                .pragma_update(None, "wal_autocheckpoint", 0)
                .map_err(|error| {
                    SqliteStoreError::from_sqlite(error, "Failed to disable writer autocheckpoint")
                })?;
            self.checkpoint_worker =
                Some(spawn_checkpoint_worker(path, config, self.metrics.clone())?);
        }
        Ok(self)
    }

    fn apply_runtime_config(
        conn: &Connection,
        config: &SqliteStoreConfig,
    ) -> Result<(), SqliteStoreError> {
        conn.busy_timeout(Duration::from_millis(config.busy_timeout_ms))
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set busy timeout"))?;

        if config.enable_wal {
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to enable WAL mode"))?;
        }
        if config.synchronous_normal {
            conn.pragma_update(None, "synchronous", "NORMAL")
                .map_err(|e| {
                    SqliteStoreError::from_sqlite(e, "Failed to set synchronous=NORMAL")
                })?;
        }
        conn.pragma_update(None, "temp_store", "MEMORY")
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set temp_store=MEMORY"))?;
        let cache_size_kib = config
            .page_cache_size_bytes
            .div_ceil(1024)
            .min(i64::MAX as u64);
        let cache_size_kib = -(cache_size_kib as i64);
        conn.pragma_update(None, "cache_size", cache_size_kib)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set cache_size"))?;
        conn.pragma_update(None, "wal_autocheckpoint", config.wal_autocheckpoint_pages)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set WAL autocheckpoint"))?;
        conn.pragma_update(
            None,
            "journal_size_limit",
            config.journal_size_limit_bytes.min(i64::MAX as u64) as i64,
        )
        .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set journal_size_limit"))?;
        conn.pragma_update(None, "mmap_size", config.mmap_size_bytes)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set mmap_size"))?;
        Ok(())
    }

    fn apply_reader_runtime_config(
        conn: &Connection,
        config: &SqliteStoreConfig,
    ) -> Result<(), SqliteStoreError> {
        conn.busy_timeout(Duration::from_millis(config.busy_timeout_ms))
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set reader busy timeout"))?;
        conn.pragma_update(None, "query_only", true)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to enable query_only"))?;
        let reader_cache_bytes = config
            .page_cache_size_bytes
            .checked_div(config.reader_connections.max(1) as u64)
            .unwrap_or(config.page_cache_size_bytes)
            .max(1024 * 1024);
        let cache_kib = -(reader_cache_bytes.div_ceil(1024).min(i64::MAX as u64) as i64);
        conn.pragma_update(None, "cache_size", cache_kib)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set reader cache_size"))?;
        conn.pragma_update(None, "mmap_size", config.mmap_size_bytes)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set reader mmap_size"))?;
        Ok(())
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, SqliteStoreError> {
        Ok(self.writer.lock())
    }

    fn read_connection(&self) -> Result<ReadConnectionGuard<'_>, SqliteStoreError> {
        if self.readers.is_empty() {
            return Ok(ReadConnectionGuard::Writer(self.writer.lock()));
        }
        if self.primary_reader_uses_writer {
            let current = thread::current().id();
            let primary = self.primary_reader_thread.get_or_init(|| current);
            if *primary == current {
                return Ok(ReadConnectionGuard::Writer(self.writer.lock()));
            }
        }
        let index = SQLITE_READER_SLOT.with(|slot| {
            let current = slot.get();
            if current == usize::MAX {
                let assigned = NEXT_SQLITE_READER_SLOT.fetch_add(1, Ordering::Relaxed);
                slot.set(assigned);
                assigned
            } else {
                current
            }
        }) % self.readers.len();
        Ok(ReadConnectionGuard::Reader(self.readers[index].lock()))
    }

    fn write_committed(&self) {
        self.metrics
            .write_transactions
            .fetch_add(1, Ordering::Relaxed);
        if let Some(worker) = &self.checkpoint_worker {
            worker.notify_write();
        }
    }

    fn record_compression(&self, input_len: usize, encoding: i64, stored_len: usize) {
        if encoding == NODE_ENCODING_LZ4 {
            self.metrics
                .compressed_input_bytes
                .fetch_add(input_len as u64, Ordering::Relaxed);
            self.metrics
                .compressed_stored_bytes
                .fetch_add(stored_len as u64, Ordering::Relaxed);
        }
    }

    fn own_publication(publication: NodePublication<'_>) -> OwnedPublication {
        OwnedPublication {
            entries: publication
                .entries()
                .iter()
                .map(|(key, value)| (key.to_vec(), value.to_vec()))
                .collect(),
            hint: publication.hint().map(|hint| {
                (
                    hint.namespace().to_vec(),
                    hint.key().to_vec(),
                    hint.value().to_vec(),
                )
            }),
            origin: publication.origin(),
        }
    }

    fn publish_owned_batch(
        &self,
        publications: &[&OwnedPublication],
    ) -> Result<(), SqliteStoreError> {
        let mut conn = self.connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to start grouped node publication")
            })?;
        {
            let mut insert = tx.prepare_cached(INSERT_IMMUTABLE_SQL).map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to prepare grouped node publication")
            })?;
            let mut compression_scratch = Vec::new();
            for publication in publications {
                let mut ordered = publication.entries.iter().collect::<Vec<_>>();
                ordered.sort_by(|left, right| left.0.cmp(&right.0));
                for (key, input) in ordered {
                    let (encoding, stored) = encode_stored_node_into(
                        input,
                        &mut compression_scratch,
                        self.node_compression_min_bytes,
                    );
                    self.record_compression(input.len(), encoding, stored.len());
                    insert
                        .execute(params![key, encoding, stored])
                        .map_err(|error| {
                            SqliteStoreError::from_sqlite(
                                error,
                                "Failed to publish grouped immutable node",
                            )
                        })?;
                }
            }
        }
        for publication in publications {
            if let Some((namespace, key, value)) = &publication.hint {
                tx.execute(UPSERT_HINT_SQL, params![namespace, key, value])
                    .map_err(|error| {
                        SqliteStoreError::from_sqlite(
                            error,
                            "Failed to write grouped publication hint",
                        )
                    })?;
            }
        }
        tx.commit().map_err(|error| {
            SqliteStoreError::from_sqlite(error, "Failed to commit grouped node publication")
        })?;
        drop(conn);
        self.write_committed();
        self.metrics.grouped_publications.fetch_add(
            publications.len().saturating_sub(1) as u64,
            Ordering::Relaxed,
        );
        self.metrics.published_nodes.fetch_add(
            publications
                .iter()
                .map(|publication| publication.entries.len() as u64)
                .sum(),
            Ordering::Relaxed,
        );
        for publication in publications {
            if !matches!(
                publication.origin,
                PublicationOrigin::TreeBuild | PublicationOrigin::Merge
            ) {
                for (key, value) in &publication.entries {
                    self.node_read_cache
                        .insert_immutable(key, Arc::from(value.as_slice()));
                }
            }
        }
        Ok(())
    }

    fn publish_grouped(&self, publication: NodePublication<'_>) -> Result<(), SqliteStoreError> {
        let group = self
            .group_commit
            .as_ref()
            .expect("grouped publication requires configured coordinator");
        let completion = Arc::new(PublicationCompletion {
            result: Mutex::new(None),
            ready: Condvar::new(),
        });
        let leader = {
            let mut state = group.state.lock();
            state.queue.push(PendingPublication {
                publication: Self::own_publication(publication),
                completion: completion.clone(),
            });
            if state.flushing {
                false
            } else {
                state.flushing = true;
                true
            }
        };

        if leader {
            thread::sleep(group.delay);
            loop {
                let pending = {
                    let mut state = group.state.lock();
                    if state.queue.is_empty() {
                        state.flushing = false;
                        break;
                    }
                    let mut nodes = 0usize;
                    let take = state
                        .queue
                        .iter()
                        .take_while(|pending| {
                            if nodes > 0
                                && nodes.saturating_add(pending.publication.entries.len())
                                    > group.max_nodes
                            {
                                return false;
                            }
                            nodes = nodes.saturating_add(pending.publication.entries.len());
                            true
                        })
                        .count()
                        .max(1);
                    state.queue.drain(..take).collect::<Vec<_>>()
                };
                let publications = pending
                    .iter()
                    .map(|pending| &pending.publication)
                    .collect::<Vec<_>>();
                let result = self
                    .publish_owned_batch(&publications)
                    .map_err(|error| error.to_string());
                for pending in pending {
                    let mut slot = pending.completion.result.lock();
                    *slot = Some(result.clone());
                    pending.completion.ready.notify_one();
                }
            }
        }

        let mut result = completion.result.lock();
        while result.is_none() {
            completion.ready.wait(&mut result);
        }
        result
            .take()
            .expect("group publication completion must be populated")
            .map_err(SqliteStoreError::new)
    }

    /// Number of read-only connections available for concurrent reads.
    pub fn reader_connection_count(&self) -> usize {
        self.readers.len()
    }

    /// Return adapter and SQLite pager counters without resetting them.
    pub fn metrics(&self) -> Result<SqliteStoreMetrics, SqliteStoreError> {
        let mut pager = SqlitePagerMetrics::default();
        {
            let writer = self.writer.lock();
            pager.add_connection(&writer)?;
        }
        for reader in &self.readers {
            pager.add_connection(&reader.lock())?;
        }
        Ok(SqliteStoreMetrics {
            node_cache_hits: self.metrics.node_cache_hits.load(Ordering::Relaxed),
            node_cache_misses: self.metrics.node_cache_misses.load(Ordering::Relaxed),
            node_cache_evictions: self.metrics.node_cache_evictions.load(Ordering::Relaxed),
            node_cache_entries: self.node_read_cache.entries(),
            node_cache_retained_bytes: self.node_read_cache.retained_bytes(),
            sql_reads: self.metrics.sql_reads.load(Ordering::Relaxed),
            write_transactions: self.metrics.write_transactions.load(Ordering::Relaxed),
            published_nodes: self.metrics.published_nodes.load(Ordering::Relaxed),
            grouped_publications: self.metrics.grouped_publications.load(Ordering::Relaxed),
            checkpoint_attempts: self.metrics.checkpoint_attempts.load(Ordering::Relaxed),
            checkpoint_busy: self.metrics.checkpoint_busy.load(Ordering::Relaxed),
            checkpointed_frames: self.metrics.checkpointed_frames.load(Ordering::Relaxed),
            compressed_input_bytes: self.metrics.compressed_input_bytes.load(Ordering::Relaxed),
            compressed_stored_bytes: self.metrics.compressed_stored_bytes.load(Ordering::Relaxed),
            sqlite_page_cache_hits: pager.hits,
            sqlite_page_cache_misses: pager.misses,
            sqlite_page_cache_writes: pager.writes,
            sqlite_page_cache_spills: pager.spills,
        })
    }

    /// Run an explicit WAL checkpoint and return SQLite's frame counters.
    pub fn checkpoint(
        &self,
        mode: SqliteCheckpointMode,
    ) -> Result<SqliteCheckpointStats, SqliteStoreError> {
        let conn = self.connection()?;
        let stats = run_checkpoint(&conn, mode)?;
        record_checkpoint_metrics(&self.metrics, stats);
        Ok(stats)
    }

    /// Return database size, freelist, and WAL allocation statistics.
    pub fn storage_stats(&self) -> Result<SqliteStorageStats, SqliteStoreError> {
        let conn = self.read_connection()?;
        let page_size: u64 = conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to read page_size"))?;
        let page_count: u64 = conn
            .query_row("PRAGMA page_count", [], |row| row.get(0))
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to read page_count"))?;
        let freelist_pages: u64 = conn
            .query_row("PRAGMA freelist_count", [], |row| row.get(0))
            .map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to read freelist_count")
            })?;
        let wal_bytes = self
            .wal_path
            .as_ref()
            .and_then(|path| std::fs::metadata(path).ok())
            .map_or(0, |metadata| metadata.len());
        Ok(SqliteStorageStats {
            page_size_bytes: page_size,
            page_count,
            freelist_pages,
            database_bytes: page_size.saturating_mul(page_count),
            free_bytes: page_size.saturating_mul(freelist_pages),
            wal_bytes,
        })
    }

    /// Create a consistent online backup using SQLite's backup API.
    pub fn backup_to<P: AsRef<Path>>(&self, path: P) -> Result<(), SqliteStoreError> {
        let conn = self.read_connection()?;
        conn.backup(rusqlite::DatabaseName::Main, path, None)
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to back up database"))
    }

    /// Rebuild the database to reclaim free pages and reduce fragmentation.
    /// Callers should quiesce application traffic before invoking this method.
    pub fn compact(&self) -> Result<(), SqliteStoreError> {
        let conn = self.connection()?;
        conn.execute_batch("VACUUM")
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to compact database"))?;
        drop(conn);
        self.write_committed();
        Ok(())
    }

    /// Ask SQLite to perform bounded planner and connection maintenance.
    pub fn optimize(&self) -> Result<(), SqliteStoreError> {
        let conn = self.connection()?;
        conn.execute_batch("PRAGMA optimize")
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to optimize database"))
    }

    /// Run SQLite's fast structural consistency check.
    pub fn quick_check(&self) -> Result<(), SqliteStoreError> {
        let conn = self.read_connection()?;
        let result: String = conn
            .query_row("PRAGMA quick_check", [], |row| row.get(0))
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to check database"))?;
        if result == "ok" {
            Ok(())
        } else {
            Err(SqliteStoreError::new(format!(
                "SQLite quick_check failed: {result}"
            )))
        }
    }
}

#[derive(Default)]
struct SqlitePagerMetrics {
    hits: u64,
    misses: u64,
    writes: u64,
    spills: u64,
}

impl SqlitePagerMetrics {
    fn add_connection(&mut self, conn: &Connection) -> Result<(), SqliteStoreError> {
        self.hits = self.hits.saturating_add(connection_db_status(
            conn,
            rusqlite::ffi::SQLITE_DBSTATUS_CACHE_HIT,
        )?);
        self.misses = self.misses.saturating_add(connection_db_status(
            conn,
            rusqlite::ffi::SQLITE_DBSTATUS_CACHE_MISS,
        )?);
        self.writes = self.writes.saturating_add(connection_db_status(
            conn,
            rusqlite::ffi::SQLITE_DBSTATUS_CACHE_WRITE,
        )?);
        self.spills = self.spills.saturating_add(connection_db_status(
            conn,
            rusqlite::ffi::SQLITE_DBSTATUS_CACHE_SPILL,
        )?);
        Ok(())
    }
}

fn connection_db_status(conn: &Connection, operation: i32) -> Result<u64, SqliteStoreError> {
    let mut current = 0;
    let mut highwater = 0;
    // SAFETY: the connection remains locked for the call and both output
    // pointers refer to initialized integers valid for the duration of it.
    let code = unsafe {
        rusqlite::ffi::sqlite3_db_status(conn.handle(), operation, &mut current, &mut highwater, 0)
    };
    if code == rusqlite::ffi::SQLITE_OK {
        Ok(current.max(0) as u64)
    } else {
        Err(SqliteStoreError::new(format!(
            "sqlite3_db_status failed with code {code}"
        )))
    }
}

fn run_checkpoint(
    conn: &Connection,
    mode: SqliteCheckpointMode,
) -> Result<SqliteCheckpointStats, SqliteStoreError> {
    let (busy, log, checkpointed): (i64, i64, i64) = conn
        .query_row(mode.sql(), [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to checkpoint WAL"))?;
    Ok(SqliteCheckpointStats {
        busy: busy != 0,
        log_frames: log.max(0) as u64,
        checkpointed_frames: checkpointed.max(0) as u64,
    })
}

fn record_checkpoint_metrics(metrics: &SqliteMetricsInner, stats: SqliteCheckpointStats) {
    metrics.checkpoint_attempts.fetch_add(1, Ordering::Relaxed);
    if stats.busy {
        metrics.checkpoint_busy.fetch_add(1, Ordering::Relaxed);
    }
    metrics
        .checkpointed_frames
        .store(stats.checkpointed_frames, Ordering::Relaxed);
}

fn spawn_checkpoint_worker(
    path: PathBuf,
    config: &SqliteStoreConfig,
    metrics: Arc<SqliteMetricsInner>,
) -> Result<CheckpointWorker, SqliteStoreError> {
    let conn = Connection::open_with_flags(
        &path,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| {
        SqliteStoreError::from_sqlite(error, "Failed to open checkpoint connection")
    })?;
    conn.busy_timeout(Duration::from_millis(config.busy_timeout_ms))
        .map_err(|error| {
            SqliteStoreError::from_sqlite(error, "Failed to configure checkpoint timeout")
        })?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)
        .map_err(|error| {
            SqliteStoreError::from_sqlite(error, "Failed to disable checkpoint autocheckpoint")
        })?;
    conn.pragma_update(
        None,
        "journal_size_limit",
        config.journal_size_limit_bytes.min(i64::MAX as u64) as i64,
    )
    .map_err(|error| {
        SqliteStoreError::from_sqlite(error, "Failed to configure checkpoint journal limit")
    })?;
    let wal_path = wal_path_for(&path);
    let threshold = config.checkpoint_wal_bytes;
    let interval = Duration::from_millis(config.checkpoint_interval_ms.max(1));
    let (sender, receiver) = mpsc::channel();
    let handle = thread::Builder::new()
        .name("prolly-sqlite-checkpoint".to_string())
        .spawn(move || {
            let mut writes_pending = false;
            let mut next_inspection = Instant::now() + interval;
            loop {
                let signal = receiver
                    .recv_timeout(next_inspection.saturating_duration_since(Instant::now()));
                if matches!(signal, Ok(CheckpointSignal::Shutdown)) {
                    if let Ok(stats) = run_checkpoint(&conn, SqliteCheckpointMode::Truncate) {
                        record_checkpoint_metrics(&metrics, stats);
                    }
                    break;
                }
                if matches!(signal, Ok(CheckpointSignal::WriteCommitted)) {
                    writes_pending = true;
                }
                while let Ok(signal) = receiver.try_recv() {
                    if matches!(signal, CheckpointSignal::Shutdown) {
                        if let Ok(stats) = run_checkpoint(&conn, SqliteCheckpointMode::Truncate) {
                            record_checkpoint_metrics(&metrics, stats);
                        }
                        return;
                    }
                    writes_pending = true;
                }
                if Instant::now() < next_inspection {
                    continue;
                }
                next_inspection = Instant::now() + interval;
                if !writes_pending {
                    continue;
                }
                let wal_bytes = std::fs::metadata(&wal_path)
                    .map(|metadata| metadata.len())
                    .unwrap_or(0);
                if wal_bytes >= threshold {
                    if let Ok(stats) = run_checkpoint(&conn, SqliteCheckpointMode::Passive) {
                        record_checkpoint_metrics(&metrics, stats);
                        writes_pending = stats.busy || stats.checkpointed_frames < stats.log_frames;
                    }
                }
            }
        })
        .map_err(|error| {
            SqliteStoreError::new(format!("failed to spawn checkpoint worker: {error}"))
        })?;
    Ok(CheckpointWorker {
        sender,
        handle: Some(handle),
    })
}

fn wal_path_for(path: &Path) -> PathBuf {
    let mut wal_path = path.as_os_str().to_os_string();
    wal_path.push("-wal");
    PathBuf::from(wal_path)
}

fn ensure_node_encoding_column(conn: &Connection) -> Result<(), SqliteStoreError> {
    let has_encoding = {
        let mut stmt = conn
            .prepare("PRAGMA table_info(prolly_nodes)")
            .map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to inspect node schema")
            })?;
        let columns = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to query node schema"))?;
        let mut found = false;
        for column in columns {
            if column.map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to read node schema")
            })? == "encoding"
            {
                found = true;
                break;
            }
        }
        found
    };
    if !has_encoding {
        conn.execute(
            "ALTER TABLE prolly_nodes ADD COLUMN encoding INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .map_err(|error| {
            SqliteStoreError::from_sqlite(error, "Failed to migrate node encoding schema")
        })?;
    }
    Ok(())
}

const NODE_ENCODING_RAW: i64 = 0;
const NODE_ENCODING_LZ4: i64 = 1;
fn encode_stored_node(node: &[u8], min_compressible_bytes: usize) -> (i64, Vec<u8>) {
    let mut scratch = Vec::new();
    let (encoding, stored) = encode_stored_node_into(node, &mut scratch, min_compressible_bytes);
    (encoding, stored.to_vec())
}

fn encode_stored_node_into<'a>(
    node: &'a [u8],
    scratch: &'a mut Vec<u8>,
    min_compressible_bytes: usize,
) -> (i64, &'a [u8]) {
    if node.len() < min_compressible_bytes || node.len() > u32::MAX as usize {
        return (NODE_ENCODING_RAW, node);
    }
    let maximum = 4 + lz4_flex::block::get_maximum_output_size(node.len());
    scratch.clear();
    scratch.resize(maximum, 0);
    scratch[..4].copy_from_slice(&(node.len() as u32).to_le_bytes());
    let compressed_len = lz4_flex::block::compress_into(node, &mut scratch[4..])
        .expect("maximum LZ4 output size is sufficient");
    let stored_len = 4 + compressed_len;
    if stored_len >= node.len() {
        return (NODE_ENCODING_RAW, node);
    }
    scratch.truncate(stored_len);
    (NODE_ENCODING_LZ4, scratch)
}

fn decode_stored_node_ref(encoding: i64, node: &[u8]) -> Result<Vec<u8>, SqliteStoreError> {
    match encoding {
        NODE_ENCODING_RAW => Ok(node.to_vec()),
        NODE_ENCODING_LZ4 => lz4_flex::decompress_size_prepended(node)
            .map_err(|error| SqliteStoreError::new(format!("failed to decompress node: {error}"))),
        other => Err(SqliteStoreError::new(format!(
            "unsupported node encoding {other}"
        ))),
    }
}

fn select_nodes_ordered_unique(
    conn: &Connection,
    keys: &[&[u8]],
    configured_max_keys: usize,
) -> Result<Vec<Option<Vec<u8>>>, SqliteStoreError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    if keys.len() == 1 {
        let mut stmt = conn.prepare_cached(SELECT_SQL).map_err(|error| {
            SqliteStoreError::from_sqlite(error, "Failed to prepare point read")
        })?;
        let mut rows = stmt
            .query(params![keys[0]])
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to read key"))?;
        let value = match rows
            .next()
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to read key"))?
        {
            Some(row) => {
                let encoding = row.get::<_, i64>(0).map_err(|error| {
                    SqliteStoreError::from_sqlite(error, "Failed to read node encoding")
                })?;
                let node_value = row.get_ref(1).map_err(|error| {
                    SqliteStoreError::from_sqlite(error, "Failed to borrow node bytes")
                })?;
                let node = node_value.as_blob().map_err(|error| {
                    SqliteStoreError::new(format!("Failed to borrow node bytes: {error}"))
                })?;
                Some(decode_stored_node_ref(encoding, node)?)
            }
            None => None,
        };
        return Ok(vec![value]);
    }

    let positions = keys
        .iter()
        .enumerate()
        .map(|(index, key)| (*key, index))
        .collect::<HashMap<_, _>>();
    let mut values = vec![None; keys.len()];

    // SAFETY: the connection remains locked for this call and sqlite3_limit
    // only reads the current per-connection variable bound for a negative value.
    let sqlite_max_keys = unsafe {
        rusqlite::ffi::sqlite3_limit(
            conn.handle(),
            rusqlite::ffi::SQLITE_LIMIT_VARIABLE_NUMBER,
            -1,
        )
    }
    .max(1) as usize;
    let max_keys = configured_max_keys.max(1).min(sqlite_max_keys);
    for chunk in keys.chunks(max_keys) {
        let placeholders = (1..=chunk.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(",");
        let sql =
            format!("SELECT cid, encoding, node FROM prolly_nodes WHERE cid IN ({placeholders})");
        let mut stmt = conn.prepare_cached(&sql).map_err(|error| {
            SqliteStoreError::from_sqlite(error, "Failed to prepare multi-key read")
        })?;
        let mut rows = stmt
            .query(rusqlite::params_from_iter(chunk.iter().copied()))
            .map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to execute multi-key read")
            })?;
        while let Some(row) = rows.next().map_err(|error| {
            SqliteStoreError::from_sqlite(error, "Failed to read multi-key result")
        })? {
            let key_value = row.get_ref(0).map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to read multi-key result")
            })?;
            let key = key_value.as_blob().map_err(|error| {
                SqliteStoreError::new(format!("Failed to borrow node key: {error}"))
            })?;
            let encoding = row.get::<_, i64>(1).map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to read node encoding")
            })?;
            let node_value = row.get_ref(2).map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to borrow node bytes")
            })?;
            let node = node_value.as_blob().map_err(|error| {
                SqliteStoreError::new(format!("Failed to borrow node bytes: {error}"))
            })?;
            let index = positions.get(key).copied().ok_or_else(|| {
                SqliteStoreError::new("multi-key read returned an unrequested key")
            })?;
            values[index] = Some(decode_stored_node_ref(encoding, node)?);
        }
    }

    Ok(values)
}

#[cfg(unix)]
/// Inspect SQLite's actual open main-database descriptor without executing SQL.
pub fn sqlite_main_file_identity(
    conn: &Connection,
) -> Result<SqliteMainFileIdentity, SqliteStoreError> {
    use std::ffi::{c_int, c_void};
    use std::fs::File;
    use std::os::fd::BorrowedFd;
    use std::os::unix::fs::MetadataExt;

    // Every bundled Unix SQLite VFS begins its concrete `unixFile` with this
    // stable prefix. `SQLITE_FCNTL_FILE_POINTER` returns the actual main-file
    // object owned by this connection, not a pathname-derived approximation.
    #[repr(C)]
    struct UnixFilePrefix {
        methods: *const rusqlite::ffi::sqlite3_io_methods,
        vfs: *mut rusqlite::ffi::sqlite3_vfs,
        inode: *mut c_void,
        fd: c_int,
    }

    let mut sqlite_file: *mut rusqlite::ffi::sqlite3_file = std::ptr::null_mut();
    // SAFETY: `conn` remains alive for the call, `main` is NUL terminated, and
    // SQLite writes one sqlite3_file pointer into `sqlite_file` for this opcode.
    let rc = unsafe {
        rusqlite::ffi::sqlite3_file_control(
            conn.handle(),
            c"main".as_ptr(),
            rusqlite::ffi::SQLITE_FCNTL_FILE_POINTER,
            (&mut sqlite_file as *mut *mut rusqlite::ffi::sqlite3_file).cast(),
        )
    };
    if rc != rusqlite::ffi::SQLITE_OK || sqlite_file.is_null() {
        return Err(SqliteStoreError::new(format!(
            "SQLite did not expose its main-file handle (code {rc})"
        )));
    }
    // SAFETY: the bundled Unix VFS concrete file begins with UnixFilePrefix,
    // and SQLite retains the descriptor for the lifetime of this connection.
    let fd = unsafe { (*(sqlite_file.cast::<UnixFilePrefix>())).fd };
    // Duplicate the descriptor so the temporary File cannot close SQLite's
    // owned descriptor when it is dropped.
    let borrowed = unsafe { BorrowedFd::borrow_raw(fd) };
    let owned = borrowed.try_clone_to_owned().map_err(|error| {
        SqliteStoreError::new(format!(
            "failed to duplicate SQLite main-file handle: {error}"
        ))
    })?;
    let metadata = File::from(owned).metadata().map_err(|error| {
        SqliteStoreError::new(format!("failed to stat SQLite main-file handle: {error}"))
    })?;
    Ok(SqliteMainFileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        length: metadata.len(),
    })
}

impl Store for SqliteStore {
    type Error = SqliteStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        if let Some(value) = self.node_read_cache.get(key) {
            return Ok(Some(value.as_ref().to_vec()));
        }
        let conn = self.read_connection()?;
        self.metrics.sql_reads.fetch_add(1, Ordering::Relaxed);
        let mut stmt = conn
            .prepare_cached(SELECT_SQL)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to prepare point read"))?;
        let mut rows = stmt
            .query(params![key])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to read key"))?;
        let Some(row) = rows
            .next()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to read key"))?
        else {
            return Ok(None);
        };
        let encoding = row.get::<_, i64>(0).map_err(|error| {
            SqliteStoreError::from_sqlite(error, "Failed to read node encoding")
        })?;
        let node_value = row
            .get_ref(1)
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to borrow node bytes"))?;
        let node = node_value.as_blob().map_err(|error| {
            SqliteStoreError::new(format!("Failed to borrow node bytes: {error}"))
        })?;
        let decoded: Arc<[u8]> = Arc::from(decode_stored_node_ref(encoding, node)?);
        self.node_read_cache.insert(key, decoded.clone());
        Ok(Some(decoded.as_ref().to_vec()))
    }

    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        if let Some(value) = self.node_read_cache.get(key) {
            return Ok(Some(value));
        }
        let conn = self.read_connection()?;
        self.metrics.sql_reads.fetch_add(1, Ordering::Relaxed);
        let mut stmt = conn
            .prepare_cached(SELECT_SQL)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to prepare shared point read"))?;
        let mut rows = stmt
            .query(params![key])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to read shared key"))?;
        let Some(row) = rows
            .next()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to read shared key"))?
        else {
            return Ok(None);
        };
        let encoding = row.get::<_, i64>(0).map_err(|error| {
            SqliteStoreError::from_sqlite(error, "Failed to read node encoding")
        })?;
        let node_value = row
            .get_ref(1)
            .map_err(|error| SqliteStoreError::from_sqlite(error, "Failed to borrow node bytes"))?;
        let node = node_value.as_blob().map_err(|error| {
            SqliteStoreError::new(format!("Failed to borrow node bytes: {error}"))
        })?;
        let decoded: Arc<[u8]> = Arc::from(decode_stored_node_ref(encoding, node)?);
        self.node_read_cache.insert(key, decoded.clone());
        Ok(Some(decoded))
    }

    fn has_native_shared_reads(&self) -> bool {
        true
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let conn = self.connection()?;
        let (encoding, stored) = encode_stored_node(value, self.node_compression_min_bytes);
        self.record_compression(value.len(), encoding, stored.len());
        conn.execute(UPSERT_SQL, params![key, encoding, stored])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to write key"))?;
        drop(conn);
        self.write_committed();
        self.node_read_cache.insert(key, Arc::from(value));
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        let conn = self.connection()?;
        conn.execute(DELETE_SQL, params![key])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to delete key"))?;
        drop(conn);
        self.write_committed();
        self.node_read_cache.remove(key);
        Ok(())
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let mut conn = self.connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to start transaction"))?;

        {
            let mut upsert = tx
                .prepare_cached(UPSERT_SQL)
                .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to prepare batch write"))?;
            let mut delete = tx
                .prepare_cached(DELETE_SQL)
                .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to prepare batch delete"))?;
            let mut compression_scratch = Vec::new();

            for op in ops {
                match op {
                    BatchOp::Upsert { key, value } => {
                        let input_len = value.len();
                        let (encoding, stored) = encode_stored_node_into(
                            value,
                            &mut compression_scratch,
                            self.node_compression_min_bytes,
                        );
                        self.record_compression(input_len, encoding, stored.len());
                        upsert
                            .execute(params![key, encoding, stored])
                            .map_err(|e| {
                                SqliteStoreError::from_sqlite(e, "Failed to write key in batch")
                            })?;
                    }
                    BatchOp::Delete { key } => {
                        delete.execute(params![key]).map_err(|e| {
                            SqliteStoreError::from_sqlite(e, "Failed to delete key in batch")
                        })?;
                    }
                }
            }
        }

        tx.commit()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to commit transaction"))?;
        drop(conn);
        self.write_committed();
        for op in ops {
            match op {
                BatchOp::Upsert { key, .. } | BatchOp::Delete { key } => {
                    self.node_read_cache.remove(key)
                }
            }
        }
        Ok(())
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let plan = OrderedBatchReadPlan::new(keys);
        let values = self.batch_get_shared_ordered_unique(plan.unique_keys())?;
        let mut results = HashMap::with_capacity(plan.unique_keys().len());
        for (key, value) in plan.unique_keys().iter().zip(values) {
            if let Some(value) = value {
                results.insert(key.to_vec(), value.as_ref().to_vec());
            }
        }

        Ok(results)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let plan = OrderedBatchReadPlan::new(keys);
        let unique_values = self
            .batch_get_shared_ordered_unique(plan.unique_keys())?
            .into_iter()
            .map(|value| value.map(|value| value.as_ref().to_vec()))
            .collect();
        Ok(plan.expand_owned(unique_values))
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.batch_get_shared_ordered_unique(keys).map(|values| {
            values
                .into_iter()
                .map(|value| value.map(|value| value.as_ref().to_vec()))
                .collect()
        })
    }

    fn batch_get_shared_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Arc<[u8]>>>, Self::Error> {
        let mut values = vec![None; keys.len()];
        let mut missing = Vec::new();
        for (position, key) in keys.iter().enumerate() {
            match self.node_read_cache.get(key) {
                Some(value) => values[position] = Some(value),
                None => missing.push((position, *key)),
            }
        }
        if missing.is_empty() {
            return Ok(values);
        }
        let missing_keys = missing.iter().map(|(_, key)| *key).collect::<Vec<_>>();
        let conn = self.read_connection()?;
        self.metrics.sql_reads.fetch_add(1, Ordering::Relaxed);
        let loaded = select_nodes_ordered_unique(&conn, &missing_keys, self.max_batch_select_keys)?;
        drop(conn);
        for ((position, key), loaded) in missing.into_iter().zip(loaded) {
            if let Some(loaded) = loaded {
                let loaded: Arc<[u8]> = Arc::from(loaded);
                self.node_read_cache.insert(key, loaded.clone());
                values[position] = Some(loaded);
            }
        }
        Ok(values)
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let mut conn = self.connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to start transaction"))?;

        {
            let mut stmt = tx.prepare_cached(UPSERT_SQL).map_err(|e| {
                SqliteStoreError::from_sqlite(e, "Failed to prepare batch_put write")
            })?;
            let mut ordered = entries.iter().collect::<Vec<_>>();
            ordered.sort_by(|left, right| left.0.cmp(right.0));
            let mut compression_scratch = Vec::new();
            for &&(key, value) in &ordered {
                let input_len = value.len();
                let (encoding, stored) = encode_stored_node_into(
                    value,
                    &mut compression_scratch,
                    self.node_compression_min_bytes,
                );
                self.record_compression(input_len, encoding, stored.len());
                stmt.execute(params![key, encoding, stored]).map_err(|e| {
                    SqliteStoreError::from_sqlite(e, "Failed to write key in batch_put")
                })?;
            }
        }

        tx.commit()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to commit transaction"))?;
        drop(conn);
        self.write_committed();
        for &(key, _) in entries {
            self.node_read_cache.remove(key);
        }
        Ok(())
    }

    fn supports_hints(&self) -> bool {
        true
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let conn = self.read_connection()?;
        self.metrics.sql_reads.fetch_add(1, Ordering::Relaxed);
        conn.query_row(
            "SELECT value FROM prolly_hints WHERE namespace = ?1 AND key = ?2",
            params![namespace, key],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to read hint"))
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let conn = self.connection()?;
        conn.execute(
            "\
            INSERT INTO prolly_hints (namespace, key, value) \
            VALUES (?1, ?2, ?3) \
            ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value",
            params![namespace, key, value],
        )
        .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to write hint"))?;
        drop(conn);
        self.write_committed();
        Ok(())
    }

    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        let mut conn = self.connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to start transaction"))?;

        {
            let mut upsert_node = tx.prepare_cached(UPSERT_SQL).map_err(|e| {
                SqliteStoreError::from_sqlite(e, "Failed to prepare batch_put write")
            })?;
            let mut ordered = entries.iter().collect::<Vec<_>>();
            ordered.sort_by(|left, right| left.0.cmp(right.0));
            let mut compression_scratch = Vec::new();
            for &&(key, value) in &ordered {
                let input_len = value.len();
                let (encoding, stored) = encode_stored_node_into(
                    value,
                    &mut compression_scratch,
                    self.node_compression_min_bytes,
                );
                self.record_compression(input_len, encoding, stored.len());
                upsert_node
                    .execute(params![key, encoding, stored])
                    .map_err(|e| {
                        SqliteStoreError::from_sqlite(e, "Failed to write key in batch_put")
                    })?;
            }
        }

        tx.execute(
            "\
            INSERT INTO prolly_hints (namespace, key, value) \
            VALUES (?1, ?2, ?3) \
            ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value",
            params![namespace, key, value],
        )
        .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to write hint in batch_put"))?;

        tx.commit()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to commit transaction"))?;
        drop(conn);
        self.write_committed();
        for &(key, _) in entries {
            self.node_read_cache.remove(key);
        }
        Ok(())
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        if self.group_commit.is_some() {
            return self.publish_grouped(publication);
        }
        let mut conn = self.connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to start node publication")
            })?;
        {
            // Publication keys are content IDs for immutable nodes. Ignoring an
            // existing key avoids rewriting shared nodes carried into a new
            // tree while preserving the Store upsert contract on batch_put.
            let mut insert = tx.prepare_cached(INSERT_IMMUTABLE_SQL).map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to prepare node publication")
            })?;
            let mut ordered = publication.entries().iter().collect::<Vec<_>>();
            ordered.sort_by(|left, right| left.0.cmp(right.0));
            let mut compression_scratch = Vec::new();
            for &&(key, value) in &ordered {
                let input_len = value.len();
                let (encoding, stored) = encode_stored_node_into(
                    value,
                    &mut compression_scratch,
                    self.node_compression_min_bytes,
                );
                self.record_compression(input_len, encoding, stored.len());
                insert
                    .execute(params![key, encoding, stored])
                    .map_err(|error| {
                        SqliteStoreError::from_sqlite(error, "Failed to publish immutable node")
                    })?;
            }
        }
        if let Some(hint) = publication.hint() {
            tx.execute(
                UPSERT_HINT_SQL,
                params![hint.namespace(), hint.key(), hint.value()],
            )
            .map_err(|error| {
                SqliteStoreError::from_sqlite(error, "Failed to write publication hint")
            })?;
        }
        tx.commit().map_err(|error| {
            SqliteStoreError::from_sqlite(error, "Failed to commit node publication")
        })?;
        drop(conn);
        self.write_committed();
        self.metrics
            .published_nodes
            .fetch_add(publication.entries().len() as u64, Ordering::Relaxed);
        // Batch and merge branches are commonly read immediately. A full tree
        // build already leaves its decoded nodes in the manager cache, so
        // duplicating every serialized node here only adds publication cost.
        if !matches!(
            publication.origin(),
            PublicationOrigin::TreeBuild | PublicationOrigin::Merge
        ) {
            for &(key, value) in publication.entries() {
                self.node_read_cache.insert_immutable(key, Arc::from(value));
            }
        }
        Ok(())
    }
}

impl NodeStoreScan for SqliteStore {
    type Error = SqliteStoreError;

    fn list_node_cids(&self) -> Result<Vec<Cid>, Self::Error> {
        let conn = self.read_connection()?;
        self.metrics.sql_reads.fetch_add(1, Ordering::Relaxed);
        let mut stmt = conn
            .prepare_cached(SELECT_NODE_CIDS_SQL)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to prepare node CID listing"))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to list node CIDs"))?;

        let mut cids = Vec::new();
        for row in rows {
            let key = row
                .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to read listed node CID"))?;
            cids.push(cid_from_store_key(&key, "SQLite node").map_err(SqliteStoreError::new)?);
        }
        sort_cids(&mut cids);
        Ok(cids)
    }
}

impl ManifestStore for SqliteStore {
    type Error = SqliteStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        let conn = self.read_connection()?;
        self.metrics.sql_reads.fetch_add(1, Ordering::Relaxed);
        let bytes = conn
            .query_row(SELECT_ROOT_SQL, params![name], |row| row.get(0))
            .optional()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to read root manifest"))?;
        decode_root_manifest(bytes)
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        let conn = self.connection()?;
        let bytes = encode_root_manifest(manifest)?;
        conn.execute(UPSERT_ROOT_SQL, params![name, bytes])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to write root manifest"))?;
        drop(conn);
        self.write_committed();
        Ok(())
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        let conn = self.connection()?;
        conn.execute(DELETE_ROOT_SQL, params![name])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to delete root manifest"))?;
        drop(conn);
        self.write_committed();
        Ok(())
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let expected_bytes = expected.map(encode_root_manifest).transpose()?;
        let new_bytes = new.map(encode_root_manifest).transpose()?;

        let mut conn = self.connection()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to start root transaction"))?;

        let current_bytes = tx
            .query_row(SELECT_ROOT_SQL, params![name], |row| row.get(0))
            .optional()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to read root manifest"))?;

        if current_bytes.as_deref() != expected_bytes.as_deref() {
            return Ok(ManifestUpdate::Conflict {
                current: decode_root_manifest(current_bytes)?,
            });
        }

        match new_bytes {
            Some(bytes) => {
                tx.execute(UPSERT_ROOT_SQL, params![name, bytes])
                    .map_err(|e| {
                        SqliteStoreError::from_sqlite(e, "Failed to write root manifest")
                    })?;
            }
            None => {
                tx.execute(DELETE_ROOT_SQL, params![name]).map_err(|e| {
                    SqliteStoreError::from_sqlite(e, "Failed to delete root manifest")
                })?;
            }
        }

        tx.commit()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to commit root transaction"))?;
        drop(conn);
        self.write_committed();
        Ok(ManifestUpdate::Applied)
    }
}

impl ManifestStoreScan for SqliteStore {
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        let conn = self.read_connection()?;
        self.metrics.sql_reads.fetch_add(1, Ordering::Relaxed);
        let mut stmt = conn.prepare_cached(SELECT_ROOTS_SQL).map_err(|e| {
            SqliteStoreError::from_sqlite(e, "Failed to prepare root manifest listing")
        })?;
        let rows = stmt
            .query_map([], |row| {
                Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, Vec<u8>>(1)?))
            })
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to list root manifests"))?;

        let mut roots = Vec::new();
        for row in rows {
            let (name, bytes) = row.map_err(|e| {
                SqliteStoreError::from_sqlite(e, "Failed to read listed root manifest")
            })?;
            let manifest = RootManifest::from_bytes(&bytes)
                .map_err(|err| SqliteStoreError::new(err.to_string()))?;
            roots.push(NamedRootManifest::new(name, manifest));
        }
        sort_named_root_manifests(&mut roots);
        Ok(roots)
    }
}

impl TransactionalStore for SqliteStore {
    fn supports_transactions(&self) -> bool {
        true
    }

    fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, Error> {
        let mut conn = self
            .connection()
            .map_err(|err| Error::Store(Box::new(err)))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|err| {
                Error::Store(Box::new(SqliteStoreError::from_sqlite(
                    err,
                    "Failed to start transaction commit",
                )))
            })?;

        for condition in root_conditions {
            let current_bytes = tx
                .query_row(SELECT_ROOT_SQL, params![condition.name], |row| row.get(0))
                .optional()
                .map_err(|err| {
                    Error::Store(Box::new(SqliteStoreError::from_sqlite(
                        err,
                        "Failed to read root manifest during transaction commit",
                    )))
                })?;
            let current =
                decode_root_manifest(current_bytes).map_err(|err| Error::Store(Box::new(err)))?;
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

        {
            let mut upsert_node = tx.prepare_cached(UPSERT_SQL).map_err(|err| {
                Error::Store(Box::new(SqliteStoreError::from_sqlite(
                    err,
                    "Failed to prepare transaction node write",
                )))
            })?;
            let mut delete_node = tx.prepare_cached(DELETE_SQL).map_err(|err| {
                Error::Store(Box::new(SqliteStoreError::from_sqlite(
                    err,
                    "Failed to prepare transaction node delete",
                )))
            })?;
            for write in node_writes {
                match write {
                    TransactionNodeWrite::Upsert { key, value } => {
                        let (encoding, stored) =
                            encode_stored_node(value, self.node_compression_min_bytes);
                        self.record_compression(value.len(), encoding, stored.len());
                        upsert_node
                            .execute(params![key, encoding, stored])
                            .map_err(|err| {
                                Error::Store(Box::new(SqliteStoreError::from_sqlite(
                                    err,
                                    "Failed to write node during transaction commit",
                                )))
                            })?;
                    }
                    TransactionNodeWrite::Delete { key } => {
                        delete_node.execute(params![key]).map_err(|err| {
                            Error::Store(Box::new(SqliteStoreError::from_sqlite(
                                err,
                                "Failed to delete node during transaction commit",
                            )))
                        })?;
                    }
                }
            }
        }

        {
            let mut upsert_root = tx.prepare_cached(UPSERT_ROOT_SQL).map_err(|err| {
                Error::Store(Box::new(SqliteStoreError::from_sqlite(
                    err,
                    "Failed to prepare transaction root write",
                )))
            })?;
            let mut delete_root = tx.prepare_cached(DELETE_ROOT_SQL).map_err(|err| {
                Error::Store(Box::new(SqliteStoreError::from_sqlite(
                    err,
                    "Failed to prepare transaction root delete",
                )))
            })?;
            for write in root_writes {
                match write {
                    RootWrite::Put { name, manifest } => {
                        let bytes = encode_root_manifest(manifest)
                            .map_err(|err| Error::Store(Box::new(err)))?;
                        upsert_root.execute(params![name, bytes]).map_err(|err| {
                            Error::Store(Box::new(SqliteStoreError::from_sqlite(
                                err,
                                "Failed to write root during transaction commit",
                            )))
                        })?;
                    }
                    RootWrite::Delete { name } => {
                        delete_root.execute(params![name]).map_err(|err| {
                            Error::Store(Box::new(SqliteStoreError::from_sqlite(
                                err,
                                "Failed to delete root during transaction commit",
                            )))
                        })?;
                    }
                }
            }
        }

        tx.commit().map_err(|err| {
            Error::Store(Box::new(SqliteStoreError::from_sqlite(
                err,
                "Failed to commit transaction",
            )))
        })?;
        drop(conn);
        self.write_committed();
        for write in node_writes {
            match write {
                TransactionNodeWrite::Upsert { key, .. } | TransactionNodeWrite::Delete { key } => {
                    self.node_read_cache.remove(key)
                }
            }
        }
        Ok(TransactionUpdate::Applied {
            nodes_written: node_writes.len(),
            roots_written: root_writes.len(),
        })
    }
}

fn encode_root_manifest(manifest: &RootManifest) -> Result<Vec<u8>, SqliteStoreError> {
    manifest
        .to_bytes()
        .map_err(|e| SqliteStoreError::new(format!("failed to encode root manifest: {e}")))
}

fn decode_root_manifest(bytes: Option<Vec<u8>>) -> Result<Option<RootManifest>, SqliteStoreError> {
    bytes
        .as_deref()
        .map(RootManifest::from_bytes)
        .transpose()
        .map_err(|e| SqliteStoreError::new(format!("failed to decode root manifest: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_store_put_get_delete() {
        let store = SqliteStore::open_in_memory().unwrap();

        store.put(b"key", b"value").unwrap();
        assert_eq!(store.get(b"key").unwrap(), Some(b"value".to_vec()));

        store.delete(b"key").unwrap();
        assert_eq!(store.get(b"key").unwrap(), None);
    }

    #[test]
    fn sqlite_store_reuses_shared_node_reads() {
        let store = SqliteStore::open_in_memory().unwrap();
        store.put(b"key", b"value").unwrap();

        let first = store.get_shared(b"key").unwrap().unwrap();
        let second = store.get_shared(b"key").unwrap().unwrap();

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn sqlite_store_applies_publication_pragmas_and_rowid_schema() {
        let store = SqliteStore::open_in_memory().unwrap();
        let conn = store.connection().unwrap();
        let page_size: u32 = conn
            .query_row("PRAGMA page_size", [], |row| row.get(0))
            .unwrap();
        let cache_size: i64 = conn
            .query_row("PRAGMA cache_size", [], |row| row.get(0))
            .unwrap();
        let wal_autocheckpoint: u32 = conn
            .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
            .unwrap();
        let schema: String = conn
            .query_row(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'prolly_nodes'",
                [],
                |row| row.get(0),
            )
            .unwrap();

        assert_eq!(page_size, 64 * 1024);
        assert_eq!(cache_size, -64 * 1024);
        assert_eq!(wal_autocheckpoint, 32 * 1024);
        assert!(!schema.to_ascii_uppercase().contains("WITHOUT ROWID"));
    }

    #[test]
    fn sqlite_store_rejects_invalid_page_size() {
        let config = SqliteStoreConfig {
            page_size_bytes: 1_000,
            ..SqliteStoreConfig::default()
        };
        let error = SqliteStore::from_connection(Connection::open_in_memory().unwrap(), config)
            .err()
            .expect("invalid page size must fail");
        assert!(error.to_string().contains("page_size_bytes"));
    }

    #[test]
    fn sqlite_store_batch_is_order_preserving_for_reads() {
        let store = SqliteStore::open_in_memory().unwrap();
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
    fn sqlite_store_batch_put_updates_existing_keys() {
        let store = SqliteStore::open_in_memory().unwrap();

        store.put(b"a", b"old").unwrap();
        store
            .batch_put(&[(b"a".as_slice(), b"new".as_slice()), (b"b", b"2")])
            .unwrap();

        assert_eq!(store.get(b"a").unwrap(), Some(b"new".to_vec()));
        assert_eq!(store.get(b"b").unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn sqlite_store_persists_hints_separately_from_nodes() {
        let store = SqliteStore::open_in_memory().unwrap();

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
    fn sqlite_store_compresses_repetitive_nodes_transparently() {
        let store = SqliteStore::open_in_memory().unwrap();
        let node = vec![b'x'; 16 * 1024];

        store.put(b"compressed", &node).unwrap();

        let conn = store.connection().unwrap();
        let (encoding, stored_bytes): (i64, usize) = conn
            .query_row(
                "SELECT encoding, length(node) FROM prolly_nodes WHERE cid = ?1",
                params![b"compressed"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        drop(conn);
        assert_eq!(encoding, NODE_ENCODING_LZ4);
        assert!(stored_bytes < node.len());
        assert_eq!(store.get(b"compressed").unwrap(), Some(node));
    }

    #[test]
    fn sqlite_store_honors_node_compression_threshold() {
        let config = SqliteStoreConfig {
            node_compression_min_bytes: 32 * 1024,
            ..SqliteStoreConfig::default()
        };
        let store =
            SqliteStore::from_connection(Connection::open_in_memory().unwrap(), config).unwrap();
        let node = vec![b'x'; 16 * 1024];

        store.put(b"raw", &node).unwrap();

        let conn = store.connection().unwrap();
        let encoding: i64 = conn
            .query_row(
                "SELECT encoding FROM prolly_nodes WHERE cid = ?1",
                params![b"raw"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(encoding, NODE_ENCODING_RAW);
    }

    #[test]
    fn sqlite_store_migrates_legacy_raw_node_tables() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE prolly_nodes (
                cid BLOB PRIMARY KEY NOT NULL,
                node BLOB NOT NULL
            ) WITHOUT ROWID;
            INSERT INTO prolly_nodes (cid, node) VALUES (x'6c6567616379', x'726177');",
        )
        .unwrap();

        let store = SqliteStore::from_connection(conn, SqliteStoreConfig::default()).unwrap();

        assert_eq!(store.get(b"legacy").unwrap(), Some(b"raw".to_vec()));
        let conn = store.connection().unwrap();
        let encoding: i64 = conn
            .query_row(
                "SELECT encoding FROM prolly_nodes WHERE cid = ?1",
                params![b"legacy"],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(encoding, NODE_ENCODING_RAW);
    }

    #[test]
    fn file_store_opens_reader_pool_and_disables_writer_autocheckpoint() {
        let directory = tempfile::tempdir().unwrap();
        let config = SqliteStoreConfig {
            reader_connections: 3,
            background_checkpoints: true,
            ..SqliteStoreConfig::default()
        };
        let store =
            SqliteStore::open_with_config(directory.path().join("store.db"), config).unwrap();

        assert_eq!(store.reader_connection_count(), 3);
        let writer = store.connection().unwrap();
        let autocheckpoint: u32 = writer
            .query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))
            .unwrap();
        assert_eq!(autocheckpoint, 0);
    }

    #[test]
    fn primary_reader_reuses_writer_and_other_threads_use_the_pool() {
        let directory = tempfile::tempdir().unwrap();
        let config = SqliteStoreConfig {
            background_checkpoints: false,
            reader_connections: 2,
            ..SqliteStoreConfig::default()
        };
        let store = Arc::new(
            SqliteStore::open_with_config(directory.path().join("store.db"), config).unwrap(),
        );

        assert!(matches!(
            store.read_connection().unwrap(),
            ReadConnectionGuard::Writer(_)
        ));
        let other = {
            let store = store.clone();
            thread::spawn(move || {
                matches!(
                    store.read_connection().unwrap(),
                    ReadConnectionGuard::Reader(_)
                )
            })
        };
        assert!(other.join().unwrap());
    }

    #[test]
    fn reader_pool_serves_reads_while_the_writer_commits() {
        let directory = tempfile::tempdir().unwrap();
        let config = SqliteStoreConfig {
            background_checkpoints: false,
            node_read_cache_size_bytes: 0,
            reader_connections: 4,
            ..SqliteStoreConfig::default()
        };
        let store = Arc::new(
            SqliteStore::open_with_config(directory.path().join("store.db"), config).unwrap(),
        );
        store.put(b"stable", b"value").unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(5));
        let readers = (0..4)
            .map(|_| {
                let store = store.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    for _ in 0..100 {
                        assert_eq!(store.get(b"stable").unwrap(), Some(b"value".to_vec()));
                    }
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for index in 0u32..100 {
            store
                .put(&index.to_be_bytes(), &index.wrapping_add(1).to_be_bytes())
                .unwrap();
        }
        for reader in readers {
            reader.join().unwrap();
        }

        assert!(store.metrics().unwrap().sql_reads >= 400);
    }

    #[test]
    fn background_checkpoint_worker_processes_committed_wal_frames() {
        let directory = tempfile::tempdir().unwrap();
        let config = SqliteStoreConfig {
            background_checkpoints: true,
            checkpoint_interval_ms: 5,
            checkpoint_wal_bytes: 1,
            reader_connections: 1,
            ..SqliteStoreConfig::default()
        };
        let store =
            SqliteStore::open_with_config(directory.path().join("store.db"), config).unwrap();
        store.put(b"key", b"value").unwrap();

        let deadline = Instant::now() + Duration::from_secs(2);
        while store.metrics().unwrap().checkpoint_attempts == 0 && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(5));
        }

        let metrics = store.metrics().unwrap();
        assert!(metrics.checkpoint_attempts >= 1);
        assert!(metrics.checkpointed_frames >= 1);
    }

    #[test]
    fn adaptive_batch_reads_cross_the_legacy_256_key_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let config = SqliteStoreConfig {
            background_checkpoints: false,
            max_batch_select_keys: 512,
            node_read_cache_size_bytes: 0,
            reader_connections: 2,
            ..SqliteStoreConfig::default()
        };
        let store =
            SqliteStore::open_with_config(directory.path().join("store.db"), config).unwrap();
        let keys = (0u32..600)
            .map(|index| index.to_be_bytes().to_vec())
            .collect::<Vec<_>>();
        let values = (0u32..600)
            .map(|index| format!("value-{index}").into_bytes())
            .collect::<Vec<_>>();
        let entries = keys
            .iter()
            .zip(&values)
            .map(|(key, value)| (key.as_slice(), value.as_slice()))
            .collect::<Vec<_>>();
        store.batch_put(&entries).unwrap();
        let key_refs = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();

        let observed = store.batch_get_ordered_unique(&key_refs).unwrap();

        assert_eq!(observed.len(), values.len());
        assert!(observed
            .iter()
            .zip(values)
            .all(|(observed, expected)| observed.as_deref() == Some(expected.as_slice())));
    }

    #[test]
    fn sharded_cache_eviction_and_sqlite_metrics_are_reported() {
        let config = SqliteStoreConfig {
            node_read_cache_size_bytes: 16,
            node_read_cache_shards: 1,
            ..SqliteStoreConfig::default()
        };
        let store =
            SqliteStore::from_connection(Connection::open_in_memory().unwrap(), config).unwrap();
        store.put(b"a", b"123456789012").unwrap();
        store.put(b"b", b"abcdefghijkl").unwrap();
        assert_eq!(store.get(b"a").unwrap(), Some(b"123456789012".to_vec()));

        let metrics = store.metrics().unwrap();
        assert!(metrics.node_cache_evictions >= 1);
        assert!(metrics.node_cache_misses >= 1);
        assert!(metrics.sql_reads >= 1);
        assert!(metrics.sqlite_page_cache_hits + metrics.sqlite_page_cache_misses > 0);
    }

    #[test]
    fn group_commit_combines_concurrent_publications() {
        let directory = tempfile::tempdir().unwrap();
        let config = SqliteStoreConfig {
            background_checkpoints: false,
            group_commit_delay_micros: 20_000,
            group_commit_max_nodes: 64,
            reader_connections: 2,
            ..SqliteStoreConfig::default()
        };
        let store = Arc::new(
            SqliteStore::open_with_config(directory.path().join("store.db"), config).unwrap(),
        );
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let threads = (0u8..8)
            .map(|index| {
                let store = store.clone();
                let barrier = barrier.clone();
                thread::spawn(move || {
                    let key = vec![index; 32];
                    let value = vec![index.wrapping_add(1); 128];
                    let entries = [(key.as_slice(), value.as_slice())];
                    barrier.wait();
                    store
                        .publish_nodes(NodePublication::new(
                            &entries,
                            PublicationOrigin::PointUpsert,
                        ))
                        .unwrap();
                })
            })
            .collect::<Vec<_>>();
        for thread in threads {
            thread.join().unwrap();
        }

        let metrics = store.metrics().unwrap();
        assert_eq!(metrics.published_nodes, 8);
        assert!(metrics.grouped_publications >= 1);
        assert!(metrics.write_transactions < 8);
    }

    #[test]
    fn checkpoint_backup_compaction_and_quick_check_are_available() {
        let directory = tempfile::tempdir().unwrap();
        let database = directory.path().join("store.db");
        let backup = directory.path().join("backup.db");
        let config = SqliteStoreConfig {
            background_checkpoints: false,
            reader_connections: 2,
            ..SqliteStoreConfig::default()
        };
        let store = SqliteStore::open_with_config(&database, config).unwrap();
        store.put(b"key", b"value").unwrap();

        let checkpoint = store.checkpoint(SqliteCheckpointMode::Passive).unwrap();
        assert!(checkpoint.checkpointed_frames <= checkpoint.log_frames);
        store.quick_check().unwrap();
        store.backup_to(&backup).unwrap();
        store.compact().unwrap();
        let stats = store.storage_stats().unwrap();
        assert!(stats.page_count > 0);

        let backup_store = SqliteStore::open_existing(&backup).unwrap();
        assert_eq!(backup_store.get(b"key").unwrap(), Some(b"value".to_vec()));
    }
}
