//! SQLite storage backend implementation

use std::collections::{hash_map::Entry, HashMap};
use std::path::Path;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use rusqlite::{params, Connection, OptionalExtension};

use prolly::{
    BatchOp, Cid, Error, ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRootManifest,
    NodeStoreScan, RootCondition, RootManifest, RootWrite, Store, TransactionConflict,
    TransactionNodeWrite, TransactionUpdate, TransactionalStore,
};

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
    cid  BLOB PRIMARY KEY NOT NULL,
    node BLOB NOT NULL
) WITHOUT ROWID;";

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

const SELECT_SQL: &str = "SELECT node FROM prolly_nodes WHERE cid = ?1";
const SELECT_NODE_CIDS_SQL: &str = "SELECT cid FROM prolly_nodes ORDER BY cid";
const UPSERT_SQL: &str = "\
INSERT INTO prolly_nodes (cid, node)
VALUES (?1, ?2)
ON CONFLICT(cid) DO UPDATE SET node = excluded.node";
const DELETE_SQL: &str = "DELETE FROM prolly_nodes WHERE cid = ?1";
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
}

impl Default for SqliteStoreConfig {
    fn default() -> Self {
        Self {
            busy_timeout_ms: 5_000,
            enable_wal: true,
            synchronous_normal: true,
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
        conn.execute_batch(CREATE_TABLE_SQL)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to initialize schema"))?;
        conn.execute_batch(CREATE_HINTS_TABLE_SQL)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to initialize hint schema"))?;
        conn.execute_batch(CREATE_ROOTS_TABLE_SQL)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to initialize root schema"))?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, SqliteStoreError> {
        self.conn
            .lock()
            .map_err(|e| SqliteStoreError::new(format!("lock poisoned: {}", e)))
    }
}

impl Store for SqliteStore {
    type Error = SqliteStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let conn = self.connection()?;
        conn.query_row(SELECT_SQL, params![key], |row| row.get(0))
            .optional()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to read key"))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let conn = self.connection()?;
        conn.execute(UPSERT_SQL, params![key, value])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to write key"))?;
        Ok(())
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        let conn = self.connection()?;
        conn.execute(DELETE_SQL, params![key])
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to delete key"))?;
        Ok(())
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let mut conn = self.connection()?;
        let tx = conn
            .transaction()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to start transaction"))?;

        {
            let mut upsert = tx
                .prepare_cached(UPSERT_SQL)
                .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to prepare batch write"))?;
            let mut delete = tx
                .prepare_cached(DELETE_SQL)
                .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to prepare batch delete"))?;

            for op in ops {
                match op {
                    BatchOp::Upsert { key, value } => {
                        upsert.execute(params![key, value]).map_err(|e| {
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
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to commit transaction"))
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let conn = self.connection()?;
        let mut stmt = conn
            .prepare_cached(SELECT_SQL)
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to prepare batch read"))?;
        let plan = OrderedBatchReadPlan::new(keys);
        let mut results = HashMap::with_capacity(plan.unique_keys().len());

        for key in plan.unique_keys() {
            if let Some(value) = stmt
                .query_row(params![key], |row| row.get(0))
                .optional()
                .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to read key in batch"))?
            {
                results.insert(key.to_vec(), value);
            }
        }

        Ok(results)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare_cached(SELECT_SQL).map_err(|e| {
            SqliteStoreError::from_sqlite(e, "Failed to prepare ordered batch read")
        })?;
        let plan = OrderedBatchReadPlan::new(keys);
        let mut unique_values = Vec::with_capacity(plan.unique_keys().len());

        for key in plan.unique_keys() {
            let value = stmt
                .query_row(params![key], |row| row.get(0))
                .optional()
                .map_err(|e| {
                    SqliteStoreError::from_sqlite(e, "Failed to read key in ordered batch")
                })?;
            unique_values.push(value);
        }

        Ok(plan.expand_owned(unique_values))
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let conn = self.connection()?;
        let mut stmt = conn.prepare_cached(SELECT_SQL).map_err(|e| {
            SqliteStoreError::from_sqlite(e, "Failed to prepare unique ordered batch read")
        })?;
        let mut values = Vec::with_capacity(keys.len());

        for key in keys {
            let value = stmt
                .query_row(params![key], |row| row.get(0))
                .optional()
                .map_err(|e| {
                    SqliteStoreError::from_sqlite(e, "Failed to read key in unique ordered batch")
                })?;
            values.push(value);
        }

        Ok(values)
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let mut conn = self.connection()?;
        let tx = conn
            .transaction()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to start transaction"))?;

        {
            let mut stmt = tx.prepare_cached(UPSERT_SQL).map_err(|e| {
                SqliteStoreError::from_sqlite(e, "Failed to prepare batch_put write")
            })?;
            for (key, value) in entries {
                stmt.execute(params![key, value]).map_err(|e| {
                    SqliteStoreError::from_sqlite(e, "Failed to write key in batch_put")
                })?;
            }
        }

        tx.commit()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to commit transaction"))
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
            .transaction()
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to start transaction"))?;

        {
            let mut upsert_node = tx.prepare_cached(UPSERT_SQL).map_err(|e| {
                SqliteStoreError::from_sqlite(e, "Failed to prepare batch_put write")
            })?;
            for (key, value) in entries {
                upsert_node.execute(params![key, value]).map_err(|e| {
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
            .map_err(|e| SqliteStoreError::from_sqlite(e, "Failed to commit transaction"))
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
            .transaction()
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
        let tx = conn.transaction().map_err(|err| {
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
                return Ok(TransactionUpdate::Conflict(TransactionConflict::new(
                    condition.name.clone(),
                    condition.expected.clone(),
                    current,
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
                        upsert_node.execute(params![key, value]).map_err(|err| {
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
}
