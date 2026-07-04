//! PostgreSQL store adapter for prolly-map.

pub use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteStoreBackend,
};

/// Postgres adapter entry point.
pub mod postgres {
    use sqlx::{PgPool, Row};

    use crate::{RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteStoreBackend};

    /// Store adapter for PostgreSQL-backed prolly nodes and roots.
    pub type PostgresStore = crate::RemoteProllyStore<PostgresBackend>;

    /// SQLx-backed PostgreSQL backend.
    #[derive(Clone, Debug)]
    pub struct PostgresBackend {
        pool: PgPool,
    }

    impl PostgresBackend {
        /// Create a backend from an existing SQLx pool.
        pub fn new(pool: PgPool) -> Self {
            Self { pool }
        }

        /// Connect to PostgreSQL using `database_url`.
        pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
            Ok(Self::new(PgPool::connect(database_url).await?))
        }

        /// Borrow the underlying pool.
        pub fn pool(&self) -> &PgPool {
            &self.pool
        }

        /// Create the required tables if they do not already exist.
        pub async fn initialize_schema(&self) -> Result<(), sqlx::Error> {
            execute_statements(&self.pool, POSTGRES_SCHEMA).await
        }
    }

    impl RemoteStoreBackend for PostgresBackend {
        type Error = sqlx::Error;

        async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            sqlx::query("SELECT node FROM prolly_nodes WHERE cid = $1")
                .bind(key)
                .fetch_optional(&self.pool)
                .await?
                .map(|row| row.try_get("node"))
                .transpose()
        }

        async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            sqlx::query(
                "\
                INSERT INTO prolly_nodes (cid, node) VALUES ($1, $2) \
                ON CONFLICT(cid) DO UPDATE SET node = excluded.node",
            )
            .bind(key)
            .bind(value)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
            sqlx::query("DELETE FROM prolly_nodes WHERE cid = $1")
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
                            INSERT INTO prolly_nodes (cid, node) VALUES ($1, $2) \
                            ON CONFLICT(cid) DO UPDATE SET node = excluded.node",
                        )
                        .bind(*key)
                        .bind(*value)
                        .execute(&mut *tx)
                        .await?;
                    }
                    RemoteBatchOp::Delete { key } => {
                        sqlx::query("DELETE FROM prolly_nodes WHERE cid = $1")
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
                    INSERT INTO prolly_nodes (cid, node) VALUES ($1, $2) \
                    ON CONFLICT(cid) DO UPDATE SET node = excluded.node",
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
            sqlx::query("SELECT value FROM prolly_hints WHERE namespace = $1 AND key = $2")
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
                INSERT INTO prolly_hints (namespace, key, value) VALUES ($1, $2, $3) \
                ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value",
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
                    INSERT INTO prolly_nodes (cid, node) VALUES ($1, $2) \
                    ON CONFLICT(cid) DO UPDATE SET node = excluded.node",
                )
                .bind(*key)
                .bind(*value)
                .execute(&mut *tx)
                .await?;
            }
            sqlx::query(
                "\
                INSERT INTO prolly_hints (namespace, key, value) VALUES ($1, $2, $3) \
                ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value",
            )
            .bind(namespace)
            .bind(key)
            .bind(value)
            .execute(&mut *tx)
            .await?;
            tx.commit().await
        }

        async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            sqlx::query("SELECT manifest FROM prolly_roots WHERE name = $1")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?
                .map(|row| row.try_get("manifest"))
                .transpose()
        }

        async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
            sqlx::query(
                "\
                INSERT INTO prolly_roots (name, manifest) VALUES ($1, $2) \
                ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest",
            )
            .bind(name)
            .bind(manifest)
            .execute(&self.pool)
            .await?;
            Ok(())
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            sqlx::query("DELETE FROM prolly_roots WHERE name = $1")
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
            sqlx::query("LOCK TABLE prolly_roots IN SHARE ROW EXCLUSIVE MODE")
                .execute(&mut *tx)
                .await?;

            let current = sqlx::query("SELECT manifest FROM prolly_roots WHERE name = $1")
                .bind(name)
                .fetch_optional(&mut *tx)
                .await?
                .map(|row| row.try_get("manifest"))
                .transpose()?;
            if current.as_deref() != expected {
                tx.rollback().await?;
                return Ok(RemoteManifestUpdate::Conflict { current });
            }

            match new {
                Some(manifest) => {
                    sqlx::query(
                        "\
                        INSERT INTO prolly_roots (name, manifest) VALUES ($1, $2) \
                        ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest",
                    )
                    .bind(name)
                    .bind(manifest)
                    .execute(&mut *tx)
                    .await?;
                }
                None => {
                    sqlx::query("DELETE FROM prolly_roots WHERE name = $1")
                        .bind(name)
                        .execute(&mut *tx)
                        .await?;
                }
            }

            tx.commit().await?;
            Ok(RemoteManifestUpdate::Applied)
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

    async fn execute_statements(pool: &PgPool, sql: &str) -> Result<(), sqlx::Error> {
        for statement in sql
            .split(';')
            .map(str::trim)
            .filter(|stmt| !stmt.is_empty())
        {
            sqlx::query(statement).execute(pool).await?;
        }
        Ok(())
    }

    /// Minimal table layout for PostgreSQL implementations.
    pub const POSTGRES_SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS prolly_nodes (
  cid bytea PRIMARY KEY,
  node bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace bytea NOT NULL,
  key bytea NOT NULL,
  value bytea NOT NULL,
  PRIMARY KEY(namespace, key)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name bytea PRIMARY KEY,
  manifest bytea NOT NULL
);";
}

pub use postgres::*;
