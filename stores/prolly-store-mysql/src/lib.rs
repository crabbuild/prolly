//! MySQL store adapter for prolly-map.

pub use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteStoreBackend,
};

/// MySQL adapter entry point.
pub mod mysql {
    use sqlx::{MySqlPool, Row};

    use crate::{RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteStoreBackend};

    /// Store adapter for MySQL-backed prolly nodes and roots.
    pub type MySqlStore = crate::RemoteProllyStore<MySqlBackend>;

    /// SQLx-backed MySQL backend.
    #[derive(Clone, Debug)]
    pub struct MySqlBackend {
        pool: MySqlPool,
    }

    impl MySqlBackend {
        /// Create a backend from an existing SQLx pool.
        pub fn new(pool: MySqlPool) -> Self {
            Self { pool }
        }

        /// Connect to MySQL using `database_url`.
        pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
            Ok(Self::new(MySqlPool::connect(database_url).await?))
        }

        /// Borrow the underlying pool.
        pub fn pool(&self) -> &MySqlPool {
            &self.pool
        }

        /// Create the required tables if they do not already exist.
        pub async fn initialize_schema(&self) -> Result<(), sqlx::Error> {
            execute_statements(&self.pool, MYSQL_SCHEMA).await
        }
    }

    impl RemoteStoreBackend for MySqlBackend {
        type Error = sqlx::Error;

        async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            sqlx::query("SELECT node FROM prolly_nodes WHERE cid = ?")
                .bind(key)
                .fetch_optional(&self.pool)
                .await?
                .map(|row| row.try_get("node"))
                .transpose()
        }

        async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            sqlx::query(
                "\
                INSERT INTO prolly_nodes (cid, node) VALUES (?, ?) \
                ON DUPLICATE KEY UPDATE node = VALUES(node)",
            )
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
            sqlx::query("DELETE FROM prolly_nodes WHERE cid = ?")
                .bind(key)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
            let mut tx = self.pool.begin().await?;
            for op in ops {
                match op {
                    RemoteBatchOp::Upsert { key, value } => {
                        sqlx::query(
                            "\
                            INSERT INTO prolly_nodes (cid, node) VALUES (?, ?) \
                            ON DUPLICATE KEY UPDATE node = VALUES(node)",
                        )
                        .bind(*key)
                        .bind(*value)
                        .execute(&mut *tx)
                        .await?;
                    }
                    RemoteBatchOp::Delete { key } => {
                        sqlx::query("DELETE FROM prolly_nodes WHERE cid = ?")
                            .bind(*key)
                            .execute(&mut *tx)
                            .await?;
                    }
                }
            }
            tx.commit().await
        }

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

        async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            let mut tx = self.pool.begin().await?;
            for (key, value) in entries {
                sqlx::query(
                    "\
                    INSERT INTO prolly_nodes (cid, node) VALUES (?, ?) \
                    ON DUPLICATE KEY UPDATE node = VALUES(node)",
                )
                .bind(*key)
                .bind(*value)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await
        }

        async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
            let rows = sqlx::query("SELECT cid FROM prolly_nodes ORDER BY cid")
                .fetch_all(&self.pool)
                .await?;
            rows.into_iter().map(|row| row.try_get("cid")).collect()
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
            sqlx::query("SELECT value FROM prolly_hints WHERE namespace = ? AND `key` = ?")
                .bind(namespace)
                .bind(key)
                .fetch_optional(&self.pool)
                .await?
                .map(|row| row.try_get("value"))
                .transpose()
        }

        async fn put_hint(
            &self,
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            sqlx::query(
                "\
                INSERT INTO prolly_hints (namespace, `key`, value) VALUES (?, ?, ?) \
                ON DUPLICATE KEY UPDATE value = VALUES(value)",
            )
            .bind(namespace)
            .bind(key)
            .bind(value)
            .execute(&self.pool)
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
            let mut tx = self.pool.begin().await?;
            for (key, value) in entries {
                sqlx::query(
                    "\
                    INSERT INTO prolly_nodes (cid, node) VALUES (?, ?) \
                    ON DUPLICATE KEY UPDATE node = VALUES(node)",
                )
                .bind(*key)
                .bind(*value)
                .execute(&mut *tx)
                .await?;
            }
            sqlx::query(
                "\
                INSERT INTO prolly_hints (namespace, `key`, value) VALUES (?, ?, ?) \
                ON DUPLICATE KEY UPDATE value = VALUES(value)",
            )
            .bind(namespace)
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await?;
            tx.commit().await
        }

        async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            sqlx::query("SELECT manifest FROM prolly_roots WHERE name = ?")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?
                .map(|row| row.try_get("manifest"))
                .transpose()
        }

        async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
            sqlx::query(
                "\
                INSERT INTO prolly_roots (name, manifest) VALUES (?, ?) \
                ON DUPLICATE KEY UPDATE manifest = VALUES(manifest)",
            )
            .bind(name)
            .bind(manifest)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            sqlx::query("DELETE FROM prolly_roots WHERE name = ?")
                .bind(name)
                .execute(&self.pool)
                .await?;
            Ok(())
        }

        async fn compare_and_swap_root_manifest(
            &self,
            name: &[u8],
            expected: Option<&[u8]>,
            new: Option<&[u8]>,
        ) -> Result<RemoteManifestUpdate, Self::Error> {
            let mut tx = self.pool.begin().await?;
            let applied = match (expected, new) {
                (None, Some(manifest)) => {
                    sqlx::query("INSERT IGNORE INTO prolly_roots (name, manifest) VALUES (?, ?)")
                        .bind(name)
                        .bind(manifest)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1
                }
                (Some(expected), Some(manifest)) => {
                    sqlx::query(
                        "UPDATE prolly_roots SET manifest = ? WHERE name = ? AND manifest = ?",
                    )
                    .bind(manifest)
                    .bind(name)
                    .bind(expected)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                        == 1
                }
                (Some(expected), None) => {
                    sqlx::query("DELETE FROM prolly_roots WHERE name = ? AND manifest = ?")
                        .bind(name)
                        .bind(expected)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1
                }
                (None, None) => {
                    sqlx::query("SELECT manifest FROM prolly_roots WHERE name = ? FOR UPDATE")
                        .bind(name)
                        .fetch_optional(&mut *tx)
                        .await?
                        .is_none()
                }
            };

            if applied {
                tx.commit().await?;
                return Ok(RemoteManifestUpdate::Applied);
            }

            let current = sqlx::query("SELECT manifest FROM prolly_roots WHERE name = ?")
                .bind(name)
                .fetch_optional(&mut *tx)
                .await?
                .map(|row| row.try_get("manifest"))
                .transpose()?;
            tx.rollback().await?;
            Ok(RemoteManifestUpdate::Conflict { current })
        }

        async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error> {
            let rows = sqlx::query("SELECT name, manifest FROM prolly_roots ORDER BY name")
                .fetch_all(&self.pool)
                .await?;
            rows.into_iter()
                .map(|row| {
                    Ok(RemoteNamedRoot::new(
                        row.try_get("name")?,
                        row.try_get("manifest")?,
                    ))
                })
                .collect()
        }
    }

    async fn execute_statements(pool: &MySqlPool, sql: &str) -> Result<(), sqlx::Error> {
        for statement in sql
            .split(';')
            .map(str::trim)
            .filter(|stmt| !stmt.is_empty())
        {
            sqlx::query(statement).execute(pool).await?;
        }
        Ok(())
    }

    /// Minimal table layout for MySQL implementations.
    pub const MYSQL_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS prolly_nodes (
  cid VARBINARY(32) PRIMARY KEY,
  node LONGBLOB NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace VARBINARY(255) NOT NULL,
  `key` VARBINARY(255) NOT NULL,
  value LONGBLOB NOT NULL,
  PRIMARY KEY(namespace, `key`)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name VARBINARY(255) PRIMARY KEY,
  manifest LONGBLOB NOT NULL
);";
}

pub use mysql::*;
