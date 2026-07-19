//! Native Turso Database store adapter for `prolly-map`.

use std::collections::HashMap;
use std::fmt;
use std::path::{Path, PathBuf};

use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteRootCondition, RemoteRootWrite,
    RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
};
use turso::transaction::TransactionBehavior;
use turso::{Connection, Database, IntoParams};

pub use prolly::RemoteProllyStore;

/// A complete async prolly store backed by native Turso Database.
pub type TursoStore = RemoteProllyStore<TursoBackend>;

/// Native Turso backend used by [`TursoStore`].
#[derive(Clone)]
pub struct TursoBackend {
    database: DatabaseHandle,
}

#[derive(Clone)]
enum DatabaseHandle {
    Local(Database),
    #[cfg(feature = "sync")]
    Synced(turso::sync::Database),
}

impl TursoBackend {
    /// Open or create a local Turso database and initialize the prolly schema.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, TursoStoreError> {
        let path = database_path(path.as_ref())?;
        let database = turso::Builder::new_local(path).build().await?;
        Self::from_local_database(database).await
    }

    /// Create a backend from a caller-configured local Turso database.
    pub async fn from_local_database(database: Database) -> Result<Self, TursoStoreError> {
        let backend = Self {
            database: DatabaseHandle::Local(database),
        };
        backend.initialize_schema().await?;
        Ok(backend)
    }

    /// Open a local replica connected to Turso Cloud.
    ///
    /// Store reads and writes remain local. Call [`TursoBackend::push`] and
    /// [`TursoBackend::pull`] explicitly to synchronize with the remote.
    #[cfg(feature = "sync")]
    pub async fn open_synced(
        path: impl AsRef<Path>,
        remote_url: impl Into<String>,
        auth_token: impl Into<String>,
    ) -> Result<Self, TursoStoreError> {
        let path = database_path(path.as_ref())?;
        let database = turso::sync::Builder::new_remote(path)
            .with_remote_url(remote_url)
            .with_auth_token(auth_token)
            .build()
            .await?;
        Self::from_synced_database(database).await
    }

    /// Create a backend from a caller-configured synced Turso database.
    #[cfg(feature = "sync")]
    pub async fn from_synced_database(
        database: turso::sync::Database,
    ) -> Result<Self, TursoStoreError> {
        let backend = Self {
            database: DatabaseHandle::Synced(database),
        };
        backend.initialize_schema().await?;
        Ok(backend)
    }

    /// Return whether this backend has Turso Cloud synchronization configured.
    pub fn is_synced(&self) -> bool {
        match &self.database {
            DatabaseHandle::Local(_) => false,
            #[cfg(feature = "sync")]
            DatabaseHandle::Synced(_) => true,
        }
    }

    /// Push locally committed changes to Turso Cloud.
    #[cfg(feature = "sync")]
    pub async fn push(&self) -> Result<(), TursoStoreError> {
        match &self.database {
            DatabaseHandle::Synced(database) => Ok(database.push().await?),
            DatabaseHandle::Local(_) => Err(TursoStoreError::NotSynced),
        }
    }

    /// Pull and apply remote changes, returning whether any changes were applied.
    #[cfg(feature = "sync")]
    pub async fn pull(&self) -> Result<bool, TursoStoreError> {
        match &self.database {
            DatabaseHandle::Synced(database) => Ok(database.pull().await?),
            DatabaseHandle::Local(_) => Err(TursoStoreError::NotSynced),
        }
    }

    async fn connect(&self) -> Result<Connection, TursoStoreError> {
        match &self.database {
            DatabaseHandle::Local(database) => Ok(database.connect()?),
            #[cfg(feature = "sync")]
            DatabaseHandle::Synced(database) => Ok(database.connect().await?),
        }
    }

    async fn initialize_schema(&self) -> Result<(), TursoStoreError> {
        self.connect().await?.execute_batch(SCHEMA_SQL).await?;
        Ok(())
    }
}

impl fmt::Debug for TursoBackend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TursoBackend")
            .field("is_synced", &self.is_synced())
            .finish_non_exhaustive()
    }
}

/// Errors returned by the native Turso backend.
#[derive(Debug)]
pub enum TursoStoreError {
    /// Turso's string-based builder cannot represent the supplied path.
    InvalidPath(PathBuf),
    /// The native Turso engine rejected an operation.
    Turso(turso::Error),
    /// A cloud sync operation was requested on a local-only backend.
    #[cfg(feature = "sync")]
    NotSynced,
}

impl fmt::Display for TursoStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath(path) => write!(
                formatter,
                "Turso database path is not valid UTF-8: {}",
                path.display()
            ),
            Self::Turso(error) => write!(formatter, "Turso Database error: {error}"),
            #[cfg(feature = "sync")]
            Self::NotSynced => formatter.write_str("Turso Cloud sync is not configured"),
        }
    }
}

impl std::error::Error for TursoStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidPath(_) => None,
            Self::Turso(error) => Some(error),
            #[cfg(feature = "sync")]
            Self::NotSynced => None,
        }
    }
}

impl RemoteStoreBackend for TursoBackend {
    type Error = TursoStoreError;

    async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let connection = self.connect().await?;
        query_optional_blob(&connection, SELECT_NODE_SQL, (key,)).await
    }

    async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let connection = self.connect().await?;
        connection.execute(UPSERT_NODE_SQL, (key, value)).await?;
        Ok(())
    }

    async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
        let connection = self.connect().await?;
        connection.execute(DELETE_NODE_SQL, (key,)).await?;
        Ok(())
    }

    async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
        let mut connection = self.connect().await?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        apply_node_ops(&transaction, ops).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn batch_get_nodes_ordered(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let connection = self.connect().await?;
        let mut loaded = HashMap::with_capacity(keys.len());
        for chunk in keys.chunks(MAX_SQL_BATCH_KEYS) {
            let sql = select_nodes_sql(chunk.len());
            let params = chunk.iter().map(|key| key.to_vec()).collect::<Vec<_>>();
            let mut rows = connection.query(sql, params).await?;
            while let Some(row) = rows.next().await? {
                loaded.insert(row.get::<Vec<u8>>(0)?, row.get::<Vec<u8>>(1)?);
            }
        }
        Ok(keys.iter().map(|key| loaded.get(*key).cloned()).collect())
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        let mut connection = self.connect().await?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        apply_node_entries(&transaction, entries).await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        let connection = self.connect().await?;
        query_blob_column(&connection, SELECT_NODE_CIDS_SQL, ()).await
    }

    fn supports_hints(&self) -> bool {
        true
    }

    async fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let connection = self.connect().await?;
        query_optional_blob(&connection, SELECT_HINT_SQL, (namespace, key)).await
    }

    async fn put_hint(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        let connection = self.connect().await?;
        connection
            .execute(UPSERT_HINT_SQL, (namespace, key, value))
            .await?;
        Ok(())
    }

    async fn batch_put_nodes_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        let mut connection = self.connect().await?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        apply_node_entries(&transaction, entries).await?;
        transaction
            .execute(UPSERT_HINT_SQL, (namespace, key, value))
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let connection = self.connect().await?;
        query_optional_blob(&connection, SELECT_ROOT_SQL, (name,)).await
    }

    async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
        let connection = self.connect().await?;
        connection
            .execute(UPSERT_ROOT_SQL, (name, manifest))
            .await?;
        Ok(())
    }

    async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
        let connection = self.connect().await?;
        connection.execute(DELETE_ROOT_SQL, (name,)).await?;
        Ok(())
    }

    async fn compare_and_swap_root_manifest(
        &self,
        name: &[u8],
        expected: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<RemoteManifestUpdate, Self::Error> {
        let mut connection = self.connect().await?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;
        let current = query_optional_blob(&transaction, SELECT_ROOT_SQL, (name,)).await?;

        if current.as_deref() != expected {
            transaction.rollback().await?;
            return Ok(RemoteManifestUpdate::Conflict { current });
        }

        apply_root_write(&transaction, name, new).await?;
        transaction.commit().await?;
        Ok(RemoteManifestUpdate::Applied)
    }

    async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error> {
        let connection = self.connect().await?;
        let mut rows = connection.query(SELECT_ROOTS_SQL, ()).await?;
        let mut roots = Vec::new();
        while let Some(row) = rows.next().await? {
            roots.push(RemoteNamedRoot::new(row.get(0)?, row.get(1)?));
        }
        Ok(roots)
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
        let mut connection = self.connect().await?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .await?;

        for condition in root_conditions {
            let current =
                query_optional_blob(&transaction, SELECT_ROOT_SQL, (condition.name.as_slice(),))
                    .await?;
            if current.as_deref() != condition.expected.as_deref() {
                transaction.rollback().await?;
                return Ok(RemoteTransactionUpdate::Conflict(
                    RemoteTransactionConflict::new(
                        condition.name.clone(),
                        condition.expected.clone(),
                        current,
                    ),
                ));
            }
        }

        apply_node_ops(&transaction, node_writes).await?;
        for write in root_writes {
            match write {
                RemoteRootWrite::Put { name, manifest } => {
                    apply_root_write(&transaction, name, Some(manifest)).await?;
                }
                RemoteRootWrite::Delete { name } => {
                    apply_root_write(&transaction, name, None).await?;
                }
            }
        }

        transaction.commit().await?;
        Ok(RemoteTransactionUpdate::Applied)
    }
}

async fn query_optional_blob(
    connection: &Connection,
    sql: &str,
    params: impl IntoParams,
) -> Result<Option<Vec<u8>>, TursoStoreError> {
    let mut rows = connection.query(sql, params).await?;
    let value = rows.next().await?.map(|row| row.get(0)).transpose()?;
    while rows.next().await?.is_some() {}
    Ok(value)
}

async fn query_blob_column(
    connection: &Connection,
    sql: &str,
    params: impl IntoParams,
) -> Result<Vec<Vec<u8>>, TursoStoreError> {
    let mut rows = connection.query(sql, params).await?;
    let mut values = Vec::new();
    while let Some(row) = rows.next().await? {
        values.push(row.get(0)?);
    }
    Ok(values)
}

async fn apply_node_ops(
    connection: &Connection,
    ops: &[RemoteBatchOp<'_>],
) -> Result<(), TursoStoreError> {
    let mut upsert = connection.prepare(UPSERT_NODE_SQL).await?;
    let mut delete = connection.prepare(DELETE_NODE_SQL).await?;
    for op in ops {
        match op {
            RemoteBatchOp::Upsert { key, value } => {
                upsert.execute((*key, *value)).await?;
            }
            RemoteBatchOp::Delete { key } => {
                delete.execute((*key,)).await?;
            }
        }
    }
    Ok(())
}

async fn apply_node_entries(
    connection: &Connection,
    entries: &[(&[u8], &[u8])],
) -> Result<(), TursoStoreError> {
    for chunk in entries.chunks(MAX_SQL_BATCH_KEYS) {
        let sql = upsert_nodes_sql(chunk.len());
        let mut params = Vec::with_capacity(chunk.len() * 2);
        for (key, value) in chunk {
            params.push(key.to_vec());
            params.push(value.to_vec());
        }
        connection.execute(sql, params).await?;
    }
    Ok(())
}

fn select_nodes_sql(count: usize) -> String {
    let placeholders = (1..=count)
        .map(|index| format!("?{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("SELECT cid, node FROM prolly_nodes WHERE cid IN ({placeholders})")
}

fn upsert_nodes_sql(count: usize) -> String {
    let values = (0..count)
        .map(|index| format!("(?{}, ?{})", index * 2 + 1, index * 2 + 2))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO prolly_nodes (cid, node) VALUES {values} \
         ON CONFLICT(cid) DO UPDATE SET node = excluded.node"
    )
}

async fn apply_root_write(
    connection: &Connection,
    name: &[u8],
    manifest: Option<&[u8]>,
) -> Result<(), TursoStoreError> {
    match manifest {
        Some(manifest) => {
            connection
                .execute(UPSERT_ROOT_SQL, (name, manifest))
                .await?;
        }
        None => {
            connection.execute(DELETE_ROOT_SQL, (name,)).await?;
        }
    }
    Ok(())
}

impl From<turso::Error> for TursoStoreError {
    fn from(error: turso::Error) -> Self {
        Self::Turso(error)
    }
}

fn database_path(path: &Path) -> Result<&str, TursoStoreError> {
    path.to_str()
        .ok_or_else(|| TursoStoreError::InvalidPath(path.to_path_buf()))
}

const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS prolly_nodes (
  cid BLOB PRIMARY KEY,
  node BLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace BLOB NOT NULL,
  key BLOB NOT NULL,
  value BLOB NOT NULL,
  PRIMARY KEY(namespace, key)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name BLOB PRIMARY KEY,
  manifest BLOB NOT NULL
);";

const SELECT_NODE_SQL: &str = "SELECT node FROM prolly_nodes WHERE cid = ?1";
const MAX_SQL_BATCH_KEYS: usize = 256;
const UPSERT_NODE_SQL: &str = "\
INSERT INTO prolly_nodes (cid, node) VALUES (?1, ?2)
ON CONFLICT(cid) DO UPDATE SET node = excluded.node";
const DELETE_NODE_SQL: &str = "DELETE FROM prolly_nodes WHERE cid = ?1";
const SELECT_NODE_CIDS_SQL: &str = "SELECT cid FROM prolly_nodes ORDER BY cid";
const SELECT_HINT_SQL: &str = "SELECT value FROM prolly_hints WHERE namespace = ?1 AND key = ?2";
const UPSERT_HINT_SQL: &str = "\
INSERT INTO prolly_hints (namespace, key, value) VALUES (?1, ?2, ?3)
ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value";
const SELECT_ROOT_SQL: &str = "SELECT manifest FROM prolly_roots WHERE name = ?1";
const UPSERT_ROOT_SQL: &str = "\
INSERT INTO prolly_roots (name, manifest) VALUES (?1, ?2)
ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest";
const DELETE_ROOT_SQL: &str = "DELETE FROM prolly_roots WHERE name = ?1";
const SELECT_ROOTS_SQL: &str = "SELECT name, manifest FROM prolly_roots ORDER BY name";
