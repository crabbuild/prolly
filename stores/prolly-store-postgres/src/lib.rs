#![doc = include_str!("../README.md")]

pub use prolly::{
    BlockingRemoteBuildError, BlockingRemoteProllyStore, BlockingRemoteStoreError, RemoteBatchOp,
    RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteRootCondition, RemoteRootWrite,
    RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
};

/// Postgres adapter entry point.
pub mod postgres {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::num::NonZeroUsize;

    use sqlx::{PgConnection, PgPool, Row};

    use crate::{
        RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteRootCondition, RemoteRootWrite,
        RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
    };

    /// Store adapter for PostgreSQL-backed prolly nodes and roots.
    pub type PostgresStore = crate::RemoteProllyStore<PostgresBackend>;

    /// Synchronous PostgreSQL store supporting `Prolly::indexed_map`.
    pub type SyncPostgresStore = crate::BlockingRemoteProllyStore<PostgresBackend>;

    /// PostgreSQL adapter tuning that does not change stored data.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct PostgresBackendOptions {
        max_batch_items: NonZeroUsize,
    }

    impl PostgresBackendOptions {
        /// Create options with the maximum number of items sent in one SQL batch.
        pub const fn new(max_batch_items: NonZeroUsize) -> Self {
            Self { max_batch_items }
        }

        /// Maximum number of items sent in one SQL batch.
        pub const fn max_batch_items(self) -> usize {
            self.max_batch_items.get()
        }
    }

    impl Default for PostgresBackendOptions {
        fn default() -> Self {
            Self::new(NonZeroUsize::new(1_024).expect("1024 is nonzero"))
        }
    }

    /// SQLx-backed PostgreSQL backend.
    #[derive(Clone, Debug)]
    pub struct PostgresBackend {
        pool: PgPool,
        options: PostgresBackendOptions,
    }

    impl PostgresBackend {
        /// Create a backend from an existing SQLx pool.
        pub fn new(pool: PgPool) -> Self {
            Self::new_with_options(pool, PostgresBackendOptions::default())
        }

        /// Create a backend from an existing SQLx pool and adapter options.
        pub fn new_with_options(pool: PgPool, options: PostgresBackendOptions) -> Self {
            Self { pool, options }
        }

        /// Connect to PostgreSQL using `database_url`.
        pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
            Self::connect_with_options(database_url, PostgresBackendOptions::default()).await
        }

        /// Connect to PostgreSQL using `database_url` and adapter options.
        pub async fn connect_with_options(
            database_url: &str,
            options: PostgresBackendOptions,
        ) -> Result<Self, sqlx::Error> {
            Ok(Self::new_with_options(
                PgPool::connect(database_url).await?,
                options,
            ))
        }

        /// Borrow the underlying pool.
        pub fn pool(&self) -> &PgPool {
            &self.pool
        }

        /// Return this backend's adapter options.
        pub const fn options(&self) -> PostgresBackendOptions {
            self.options
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
            if ops.is_empty() {
                return Ok(());
            }
            let mut final_ops = HashMap::<&[u8], Option<&[u8]>>::with_capacity(ops.len());
            for op in ops {
                match op {
                    RemoteBatchOp::Upsert { key, value } => {
                        final_ops.insert(key, Some(value));
                    }
                    RemoteBatchOp::Delete { key } => {
                        final_ops.insert(key, None);
                    }
                }
            }
            let deletes = final_ops
                .iter()
                .filter_map(|(key, value)| value.is_none().then_some(*key))
                .collect::<Vec<_>>();
            let upserts = final_ops
                .iter()
                .filter_map(|(key, value)| value.map(|value| (*key, value)))
                .collect::<Vec<_>>();
            let mut tx = self.pool.begin().await?;
            delete_node_chunks(&mut tx, &deletes, self.options.max_batch_items()).await?;
            upsert_node_chunks(&mut tx, &upserts, self.options.max_batch_items()).await?;
            tx.commit().await
        }

        async fn batch_get_nodes_ordered(
            &self,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            if keys.is_empty() {
                return Ok(Vec::new());
            }
            let mut values = Vec::with_capacity(keys.len());
            for chunk in keys.chunks(self.options.max_batch_items()) {
                let requested = chunk.iter().map(|key| (*key).to_vec()).collect::<Vec<_>>();
                let rows = sqlx::query(
                    "\
                    SELECT requested.ord, nodes.node \
                    FROM unnest($1::bytea[]) WITH ORDINALITY AS requested(cid, ord) \
                    LEFT JOIN prolly_nodes AS nodes ON nodes.cid = requested.cid \
                    ORDER BY requested.ord",
                )
                .bind(requested)
                .fetch_all(&self.pool)
                .await?;
                for row in rows {
                    values.push(row.try_get::<Option<Vec<u8>>, _>("node")?);
                }
            }
            Ok(values)
        }

        async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            if entries.is_empty() {
                return Ok(());
            }
            let entries = deduplicate_entries(entries);
            let entries = entries.into_iter().collect::<Vec<_>>();
            let mut tx = self.pool.begin().await?;
            upsert_node_chunks(&mut tx, &entries, self.options.max_batch_items()).await?;
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
            let entries = deduplicate_entries(entries);
            let entries = entries.into_iter().collect::<Vec<_>>();
            let mut tx = self.pool.begin().await?;
            upsert_node_chunks(&mut tx, &entries, self.options.max_batch_items()).await?;
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
            let mut tx = self.pool.begin().await?;
            lock_root_names(&mut tx, &[name.to_vec()]).await?;
            upsert_root_chunks(&mut tx, &[(name, manifest)], self.options.max_batch_items())
                .await?;
            tx.commit().await
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            let mut tx = self.pool.begin().await?;
            lock_root_names(&mut tx, &[name.to_vec()]).await?;
            delete_root_chunks(&mut tx, &[name], self.options.max_batch_items()).await?;
            tx.commit().await
        }

        async fn compare_and_swap_root_manifest(
            &self,
            name: &[u8],
            expected: Option<&[u8]>,
            new: Option<&[u8]>,
        ) -> Result<RemoteManifestUpdate, Self::Error> {
            let mut tx = self.pool.begin().await?;
            lock_root_names(&mut tx, &[name.to_vec()]).await?;

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
                    upsert_root_chunks(
                        &mut tx,
                        &[(name, manifest)],
                        self.options.max_batch_items(),
                    )
                    .await?;
                }
                None => {
                    delete_root_chunks(&mut tx, &[name], self.options.max_batch_items()).await?;
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

        fn supports_transactions(&self) -> bool {
            true
        }

        async fn commit_transaction(
            &self,
            node_writes: &[RemoteBatchOp<'_>],
            root_conditions: &[RemoteRootCondition],
            root_writes: &[RemoteRootWrite],
        ) -> Result<RemoteTransactionUpdate, Self::Error> {
            let mut tx = self.pool.begin().await?;
            let root_names = root_names(root_conditions, root_writes);
            lock_root_names(&mut tx, &root_names).await?;
            let current_roots = read_root_manifests(&mut tx, &root_names).await?;

            for condition in root_conditions {
                let current = current_roots.get(&condition.name).cloned().unwrap_or(None);
                if current != condition.expected {
                    tx.rollback().await?;
                    return Ok(RemoteTransactionUpdate::Conflict(
                        RemoteTransactionConflict::new(
                            condition.name.clone(),
                            condition.expected.clone(),
                            current,
                        ),
                    ));
                }
            }

            let mut final_nodes =
                HashMap::<&[u8], Option<&[u8]>>::with_capacity(node_writes.len());
            for write in node_writes {
                match write {
                    RemoteBatchOp::Upsert { key, value } => {
                        final_nodes.insert(key, Some(value));
                    }
                    RemoteBatchOp::Delete { key } => {
                        final_nodes.insert(key, None);
                    }
                };
            }
            let node_deletes = final_nodes
                .iter()
                .filter_map(|(key, value)| value.is_none().then_some(*key))
                .collect::<Vec<_>>();
            let node_upserts = final_nodes
                .iter()
                .filter_map(|(key, value)| value.map(|value| (*key, value)))
                .collect::<Vec<_>>();
            delete_node_chunks(&mut tx, &node_deletes, self.options.max_batch_items()).await?;
            upsert_node_chunks(&mut tx, &node_upserts, self.options.max_batch_items()).await?;

            let mut final_roots = BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
            for write in root_writes {
                match write {
                    RemoteRootWrite::Put { name, manifest } => {
                        final_roots.insert(name.clone(), Some(manifest.clone()));
                    }
                    RemoteRootWrite::Delete { name } => {
                        final_roots.insert(name.clone(), None);
                    }
                }
            }
            let root_deletes = final_roots
                .iter()
                .filter_map(|(name, manifest)| manifest.is_none().then_some(name.as_slice()))
                .collect::<Vec<_>>();
            let root_upserts = final_roots
                .iter()
                .filter_map(|(name, manifest)| {
                    manifest
                        .as_deref()
                        .map(|manifest| (name.as_slice(), manifest))
                })
                .collect::<Vec<_>>();
            delete_root_chunks(&mut tx, &root_deletes, self.options.max_batch_items()).await?;
            upsert_root_chunks(&mut tx, &root_upserts, self.options.max_batch_items()).await?;

            tx.commit().await?;
            Ok(RemoteTransactionUpdate::Applied)
        }
    }

    async fn upsert_node_chunks(
        connection: &mut PgConnection,
        entries: &[(&[u8], &[u8])],
        max_batch_items: usize,
    ) -> Result<(), sqlx::Error> {
        for chunk in entries.chunks(max_batch_items) {
            let keys = chunk
                .iter()
                .map(|(key, _)| (*key).to_vec())
                .collect::<Vec<_>>();
            let values = chunk
                .iter()
                .map(|(_, value)| (*value).to_vec())
                .collect::<Vec<_>>();
            sqlx::query(
                "\
                INSERT INTO prolly_nodes (cid, node) \
                SELECT input.cid, input.node \
                FROM unnest($1::bytea[], $2::bytea[]) AS input(cid, node) \
                ON CONFLICT(cid) DO UPDATE SET node = excluded.node",
            )
            .bind(keys)
            .bind(values)
            .execute(&mut *connection)
            .await?;
        }
        Ok(())
    }

    fn deduplicate_entries<'a>(entries: &[(&'a [u8], &'a [u8])]) -> HashMap<&'a [u8], &'a [u8]> {
        entries.iter().copied().collect()
    }

    async fn delete_node_chunks(
        connection: &mut PgConnection,
        keys: &[&[u8]],
        max_batch_items: usize,
    ) -> Result<(), sqlx::Error> {
        for chunk in keys.chunks(max_batch_items) {
            let keys = chunk.iter().map(|key| (*key).to_vec()).collect::<Vec<_>>();
            sqlx::query("DELETE FROM prolly_nodes WHERE cid = ANY($1::bytea[])")
                .bind(keys)
                .execute(&mut *connection)
                .await?;
        }
        Ok(())
    }

    async fn upsert_root_chunks(
        connection: &mut PgConnection,
        entries: &[(&[u8], &[u8])],
        max_batch_items: usize,
    ) -> Result<(), sqlx::Error> {
        for chunk in entries.chunks(max_batch_items) {
            let names = chunk
                .iter()
                .map(|(name, _)| (*name).to_vec())
                .collect::<Vec<_>>();
            let manifests = chunk
                .iter()
                .map(|(_, manifest)| (*manifest).to_vec())
                .collect::<Vec<_>>();
            sqlx::query(
                "\
                INSERT INTO prolly_roots (name, manifest) \
                SELECT input.name, input.manifest \
                FROM unnest($1::bytea[], $2::bytea[]) AS input(name, manifest) \
                ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest",
            )
            .bind(names)
            .bind(manifests)
            .execute(&mut *connection)
            .await?;
        }
        Ok(())
    }

    async fn delete_root_chunks(
        connection: &mut PgConnection,
        names: &[&[u8]],
        max_batch_items: usize,
    ) -> Result<(), sqlx::Error> {
        for chunk in names.chunks(max_batch_items) {
            let names = chunk
                .iter()
                .map(|name| (*name).to_vec())
                .collect::<Vec<_>>();
            sqlx::query("DELETE FROM prolly_roots WHERE name = ANY($1::bytea[])")
                .bind(names)
                .execute(&mut *connection)
                .await?;
        }
        Ok(())
    }

    fn root_names(conditions: &[RemoteRootCondition], writes: &[RemoteRootWrite]) -> Vec<Vec<u8>> {
        let mut names = BTreeSet::new();
        names.extend(conditions.iter().map(|condition| condition.name.clone()));
        names.extend(writes.iter().map(|write| match write {
            RemoteRootWrite::Put { name, .. } | RemoteRootWrite::Delete { name } => name.clone(),
        }));
        names.into_iter().collect()
    }

    async fn lock_root_names(
        connection: &mut PgConnection,
        names: &[Vec<u8>],
    ) -> Result<(), sqlx::Error> {
        for name in names {
            sqlx::query(
                "\
                SELECT pg_advisory_xact_lock( \
                    hashtextextended('prolly-root-v1:' || encode($1::bytea, 'hex'), 0) \
                )",
            )
            .bind(name)
            .execute(&mut *connection)
            .await?;
        }
        Ok(())
    }

    async fn read_root_manifests(
        connection: &mut PgConnection,
        names: &[Vec<u8>],
    ) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>, sqlx::Error> {
        if names.is_empty() {
            return Ok(BTreeMap::new());
        }
        let rows = sqlx::query(
            "\
            SELECT requested.name, roots.manifest \
            FROM unnest($1::bytea[]) AS requested(name) \
            LEFT JOIN prolly_roots AS roots ON roots.name = requested.name",
        )
        .bind(names)
        .fetch_all(&mut *connection)
        .await?;
        rows.into_iter()
            .map(|row| {
                Ok((
                    row.try_get::<Vec<u8>, _>("name")?,
                    row.try_get::<Option<Vec<u8>>, _>("manifest")?,
                ))
            })
            .collect()
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
