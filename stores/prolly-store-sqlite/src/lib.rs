#![doc = include_str!("../README.md")]

use std::collections::{hash_map::Entry, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap;
use parking_lot::{Mutex, MutexGuard};
#[cfg(unix)]
use rusqlite::OpenFlags;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};

use prolly::{
    BatchOp, Cid, Error, ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRootManifest,
    NodePublication, NodeStoreScan, PublicationOrigin, RootCondition, RootManifest, RootWrite,
    Store, TransactionConflict, TransactionNodeWrite, TransactionUpdate, TransactionalStore,
};

struct NodeReadCache {
    values: AHashMap<Vec<u8>, Arc<[u8]>>,
    retained_bytes: usize,
    max_bytes: usize,
}

impl NodeReadCache {
    fn new(max_bytes: usize) -> Self {
        Self {
            values: AHashMap::new(),
            retained_bytes: 0,
            max_bytes,
        }
    }

    fn get(&self, key: &[u8]) -> Option<Arc<[u8]>> {
        self.values.get(key).cloned()
    }

    fn insert(&mut self, key: &[u8], value: Arc<[u8]>) {
        if self.max_bytes == 0 || value.len() > self.max_bytes {
            return;
        }
        let previous = self.values.remove(key);
        self.retained_bytes = self
            .retained_bytes
            .saturating_sub(previous.as_ref().map_or(0, |value| value.len()));
        if self.retained_bytes.saturating_add(value.len()) > self.max_bytes {
            self.values.clear();
            self.retained_bytes = 0;
        }
        self.retained_bytes = self.retained_bytes.saturating_add(value.len());
        self.values.insert(key.to_vec(), value);
    }

    fn insert_immutable(&mut self, key: &[u8], value: Arc<[u8]>) {
        if self.max_bytes == 0 || value.len() > self.max_bytes {
            return;
        }
        if self.retained_bytes.saturating_add(value.len()) > self.max_bytes {
            self.values.clear();
            self.retained_bytes = 0;
        }
        if let Entry::Vacant(entry) = self.values.entry(key.to_vec()) {
            self.retained_bytes = self.retained_bytes.saturating_add(value.len());
            entry.insert(value);
        }
    }

    fn remove(&mut self, key: &[u8]) {
        if let Some(value) = self.values.remove(key) {
            self.retained_bytes = self.retained_bytes.saturating_sub(value.len());
        }
    }
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
const MAX_BATCH_SELECT_KEYS: usize = 256;
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

/// SQLite-backed storage backend for Prolly Trees.
///
/// This store persists content-addressed nodes in a single SQLite table and
/// supports atomic batch operations through transactions.
pub struct SqliteStore {
    conn: Mutex<Connection>,
    node_read_cache: Mutex<NodeReadCache>,
    node_compression_min_bytes: usize,
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
        let conn = Connection::open(path.as_ref()).map_err(|e| {
            SqliteStoreError::from_sqlite(
                e,
                format!("Failed to open database at {:?}", path.as_ref()),
            )
        })?;
        Self::from_connection(conn, config)
    }

    /// Open an existing SQLite database with default runtime configuration.
    ///
    /// Unlike [`Self::open`], this never creates the database file and does not
    /// execute schema DDL. Callers must validate the required schema before
    /// using this path.
    pub fn open_existing<P: AsRef<Path>>(path: P) -> Result<Self, SqliteStoreError> {
        Self::open_existing_verified(path, |_| Ok(()))
    }

    /// Open an existing database and verify SQLite's actual main-file handle
    /// before executing any pragma, schema statement, or other SQL.
    #[cfg(unix)]
    pub fn open_existing_verified<P, F>(path: P, verifier: F) -> Result<Self, SqliteStoreError>
    where
        P: AsRef<Path>,
        F: FnOnce(SqliteMainFileIdentity) -> Result<(), SqliteStoreError>,
    {
        let conn = Connection::open_with_flags(
            path.as_ref(),
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|error| {
            SqliteStoreError::from_sqlite(
                error,
                format!("Failed to open existing database at {:?}", path.as_ref()),
            )
        })?;
        verifier(sqlite_main_file_identity(&conn)?)?;
        Self::from_existing_connection(conn, SqliteStoreConfig::default())
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
        if !(512..=65_536).contains(&config.page_size_bytes)
            || !config.page_size_bytes.is_power_of_two()
        {
            return Err(SqliteStoreError::new(
                "page_size_bytes must be a power of two from 512 through 65536",
            ));
        }
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
        Ok(Self {
            conn: Mutex::new(conn),
            node_read_cache: Mutex::new(NodeReadCache::new(config.node_read_cache_size_bytes)),
            node_compression_min_bytes: config.node_compression_min_bytes,
        })
    }

    fn from_existing_connection(
        conn: Connection,
        config: SqliteStoreConfig,
    ) -> Result<Self, SqliteStoreError> {
        Self::apply_runtime_config(&conn, &config)?;
        Ok(Self {
            conn: Mutex::new(conn),
            node_read_cache: Mutex::new(NodeReadCache::new(config.node_read_cache_size_bytes)),
            node_compression_min_bytes: config.node_compression_min_bytes,
        })
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
        conn.pragma_update(None, "mmap_size", config.mmap_size_bytes)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to set mmap_size"))?;
        Ok(())
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, SqliteStoreError> {
        Ok(self.conn.lock())
    }

    fn node_read_cache(&self) -> Result<MutexGuard<'_, NodeReadCache>, SqliteStoreError> {
        Ok(self.node_read_cache.lock())
    }

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
    let (encoding, stored) =
        encode_stored_node_into(node, &mut scratch, min_compressible_bytes);
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

    for chunk in keys.chunks(MAX_BATCH_SELECT_KEYS) {
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
        if let Some(value) = self.node_read_cache()?.get(key) {
            return Ok(Some(value.as_ref().to_vec()));
        }
        let conn = self.connection()?;
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
        self.node_read_cache()?.insert(key, decoded.clone());
        Ok(Some(decoded.as_ref().to_vec()))
    }

    fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        if let Some(value) = self.node_read_cache()?.get(key) {
            return Ok(Some(value));
        }
        let conn = self.connection()?;
        let mut stmt = conn.prepare_cached(SELECT_SQL).map_err(|e| {
            SqliteStoreError::from_sqlite(e, "Failed to prepare shared point read")
        })?;
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
        self.node_read_cache()?.insert(key, decoded.clone());
        Ok(Some(decoded))
    }

    fn has_native_shared_reads(&self) -> bool {
        true
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let conn = self.connection()?;
        let (encoding, stored) =
            encode_stored_node(value, self.node_compression_min_bytes);
        conn.execute(UPSERT_SQL, params![key, encoding, stored])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to write key"))?;
        self.node_read_cache()?.insert(key, Arc::from(value));
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        let conn = self.connection()?;
        conn.execute(DELETE_SQL, params![key])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to delete key"))?;
        self.node_read_cache()?.remove(key);
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
                        let (encoding, value) = encode_stored_node_into(
                            value,
                            &mut compression_scratch,
                            self.node_compression_min_bytes,
                        );
                        upsert.execute(params![key, encoding, value]).map_err(|e| {
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
        let mut cache = self.node_read_cache()?;
        for op in ops {
            match op {
                BatchOp::Upsert { key, .. } | BatchOp::Delete { key } => cache.remove(key),
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
        {
            let cache = self.node_read_cache()?;
            for (position, key) in keys.iter().enumerate() {
                match cache.get(key) {
                    Some(value) => values[position] = Some(value),
                    None => missing.push((position, *key)),
                }
            }
        }
        if missing.is_empty() {
            return Ok(values);
        }
        let missing_keys = missing.iter().map(|(_, key)| *key).collect::<Vec<_>>();
        let conn = self.connection()?;
        let loaded = select_nodes_ordered_unique(&conn, &missing_keys)?;
        let mut cache = self.node_read_cache()?;
        for ((position, key), loaded) in missing.into_iter().zip(loaded) {
            if let Some(loaded) = loaded {
                let loaded: Arc<[u8]> = Arc::from(loaded);
                cache.insert(key, loaded.clone());
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
                let (encoding, value) = encode_stored_node_into(
                    value,
                    &mut compression_scratch,
                    self.node_compression_min_bytes,
                );
                stmt.execute(params![key, encoding, value]).map_err(|e| {
                    SqliteStoreError::from_sqlite(e, "Failed to write key in batch_put")
                })?;
            }
        }

        tx.commit()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to commit transaction"))?;
        let mut cache = self.node_read_cache()?;
        for &(key, _) in entries {
            cache.remove(key);
        }
        Ok(())
    }

    fn supports_hints(&self) -> bool {
        true
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let conn = self.connection()?;
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
                let (encoding, value) = encode_stored_node_into(
                    value,
                    &mut compression_scratch,
                    self.node_compression_min_bytes,
                );
                upsert_node
                    .execute(params![key, encoding, value])
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
        let mut cache = self.node_read_cache()?;
        for &(key, _) in entries {
            cache.remove(key);
        }
        Ok(())
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
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
                let (encoding, value) = encode_stored_node_into(
                    value,
                    &mut compression_scratch,
                    self.node_compression_min_bytes,
                );
                insert
                    .execute(params![key, encoding, value])
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
        // Batch and merge branches are commonly read immediately. A full tree
        // build already leaves its decoded nodes in the manager cache, so
        // duplicating every serialized node here only adds publication cost.
        if !matches!(
            publication.origin(),
            PublicationOrigin::TreeBuild | PublicationOrigin::Merge
        ) {
            let mut cache = self.node_read_cache()?;
            for &(key, value) in publication.entries() {
                cache.insert_immutable(key, Arc::from(value));
            }
        }
        Ok(())
    }
}

impl NodeStoreScan for SqliteStore {
    type Error = SqliteStoreError;

    fn list_node_cids(&self) -> Result<Vec<Cid>, Self::Error> {
        let conn = self.connection()?;
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
        let conn = self.connection()?;
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
        Ok(())
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        let conn = self.connection()?;
        conn.execute(DELETE_ROOT_SQL, params![name])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to delete root manifest"))?;
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
        Ok(ManifestUpdate::Applied)
    }
}

impl ManifestStoreScan for SqliteStore {
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        let conn = self.connection()?;
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
                        let (encoding, value) =
                            encode_stored_node(value, self.node_compression_min_bytes);
                        upsert_node
                            .execute(params![key, encoding, value])
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
        let mut cache = self
            .node_read_cache()
            .map_err(|err| Error::Store(Box::new(err)))?;
        for write in node_writes {
            match write {
                TransactionNodeWrite::Upsert { key, .. } | TransactionNodeWrite::Delete { key } => {
                    cache.remove(key)
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
        let store = SqliteStore::from_connection(Connection::open_in_memory().unwrap(), config)
            .unwrap();
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
}
