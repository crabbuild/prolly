#![doc = include_str!("../README.md")]

pub use prolly::{
    BlockingRemoteBuildError, BlockingRemoteProllyStore, BlockingRemoteStoreError, RemoteBatchOp,
    RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteRootCondition, RemoteRootWrite,
    RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
};

/// MySQL adapter entry point.
pub mod mysql {
    use std::collections::{BTreeMap, BTreeSet, HashMap};
    use std::num::NonZeroUsize;

    use sqlx::{MySql, MySqlConnection, MySqlPool, QueryBuilder, Row};

    use crate::{
        RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteRootCondition, RemoteRootWrite,
        RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
    };

    /// Store adapter for MySQL-backed prolly nodes and roots.
    pub type MySqlStore = crate::RemoteProllyStore<MySqlBackend>;

    /// Synchronous MySQL store supporting `Prolly::indexed_map`.
    pub type SyncMySqlStore = crate::BlockingRemoteProllyStore<MySqlBackend>;

    /// MySQL adapter tuning that does not change stored data.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct MySqlBackendOptions {
        max_batch_items: NonZeroUsize,
    }

    impl MySqlBackendOptions {
        /// Create options with the maximum number of items sent in one SQL batch.
        pub const fn new(max_batch_items: NonZeroUsize) -> Self {
            Self { max_batch_items }
        }

        /// Maximum number of items sent in one SQL batch.
        pub const fn max_batch_items(self) -> usize {
            self.max_batch_items.get()
        }
    }

    impl Default for MySqlBackendOptions {
        fn default() -> Self {
            Self::new(NonZeroUsize::new(1_000).expect("1000 is nonzero"))
        }
    }

    /// SQLx-backed MySQL backend.
    #[derive(Clone, Debug)]
    pub struct MySqlBackend {
        pool: MySqlPool,
        options: MySqlBackendOptions,
    }

    impl MySqlBackend {
        /// Create a backend from an existing SQLx pool.
        pub fn new(pool: MySqlPool) -> Self {
            Self::new_with_options(pool, MySqlBackendOptions::default())
        }

        /// Create a backend from an existing SQLx pool and adapter options.
        pub fn new_with_options(pool: MySqlPool, options: MySqlBackendOptions) -> Self {
            Self { pool, options }
        }

        /// Connect to MySQL using `database_url`.
        pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
            Self::connect_with_options(database_url, MySqlBackendOptions::default()).await
        }

        /// Connect to MySQL using `database_url` and adapter options.
        pub async fn connect_with_options(
            database_url: &str,
            options: MySqlBackendOptions,
        ) -> Result<Self, sqlx::Error> {
            Ok(Self::new_with_options(
                MySqlPool::connect(database_url).await?,
                options,
            ))
        }

        /// Borrow the underlying pool.
        pub fn pool(&self) -> &MySqlPool {
            &self.pool
        }

        /// Return this backend's adapter options.
        pub const fn options(&self) -> MySqlBackendOptions {
            self.options
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
            for chunk in keys.chunks(safe_batch_items(self.options.max_batch_items(), 1)) {
                let mut builder =
                    QueryBuilder::<MySql>::new("SELECT cid, node FROM prolly_nodes WHERE cid IN (");
                {
                    let mut separated = builder.separated(", ");
                    for key in chunk {
                        separated.push_bind(*key);
                    }
                }
                builder.push(")");
                let rows = builder.build().fetch_all(&self.pool).await?;
                let mut by_cid = HashMap::with_capacity(rows.len());
                for row in rows {
                    by_cid.insert(
                        row.try_get::<Vec<u8>, _>("cid")?,
                        row.try_get::<Vec<u8>, _>("node")?,
                    );
                }
                values.extend(chunk.iter().map(|key| by_cid.get(*key).cloned()));
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
            let entries = deduplicate_entries(entries);
            let entries = entries.into_iter().collect::<Vec<_>>();
            let mut tx = self.pool.begin().await?;
            upsert_node_chunks(&mut tx, &entries, self.options.max_batch_items()).await?;
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
            let mut tx = self.pool.begin().await?;
            lock_root_names(&mut tx, &[name.to_vec()], self.options.max_batch_items()).await?;
            upsert_root_chunks(&mut tx, &[(name, manifest)], self.options.max_batch_items())
                .await?;
            tx.commit().await
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            let mut tx = self.pool.begin().await?;
            lock_root_names(&mut tx, &[name.to_vec()], self.options.max_batch_items()).await?;
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
            lock_root_names(&mut tx, &[name.to_vec()], self.options.max_batch_items()).await?;
            let current = sqlx::query("SELECT manifest FROM prolly_roots WHERE name = ?")
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
            if node_writes.is_empty() && root_conditions.is_empty() && root_writes.is_empty() {
                return Ok(RemoteTransactionUpdate::Applied);
            }
            let mut tx = self.pool.begin().await?;
            let root_names = root_names(root_conditions, root_writes);
            lock_root_names(&mut tx, &root_names, self.options.max_batch_items()).await?;
            let current_roots =
                read_root_manifests(&mut tx, &root_names, self.options.max_batch_items()).await?;

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

            let mut final_nodes = HashMap::<&[u8], Option<&[u8]>>::with_capacity(node_writes.len());
            for write in node_writes {
                match write {
                    RemoteBatchOp::Upsert { key, value } => {
                        final_nodes.insert(key, Some(value));
                    }
                    RemoteBatchOp::Delete { key } => {
                        final_nodes.insert(key, None);
                    }
                }
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

    const MYSQL_MAX_PARAMETERS: usize = 65_535;

    fn safe_batch_items(configured: usize, parameters_per_item: usize) -> usize {
        configured
            .min(MYSQL_MAX_PARAMETERS / parameters_per_item)
            .max(1)
    }

    fn deduplicate_entries<'a>(entries: &[(&'a [u8], &'a [u8])]) -> HashMap<&'a [u8], &'a [u8]> {
        entries.iter().copied().collect()
    }

    async fn upsert_node_chunks(
        connection: &mut MySqlConnection,
        entries: &[(&[u8], &[u8])],
        max_batch_items: usize,
    ) -> Result<(), sqlx::Error> {
        for chunk in entries.chunks(safe_batch_items(max_batch_items, 2)) {
            let mut builder = QueryBuilder::<MySql>::new("INSERT INTO prolly_nodes (cid, node) ");
            builder.push_values(chunk, |mut row, (key, value)| {
                row.push_bind(*key).push_bind(*value);
            });
            builder.push(" ON DUPLICATE KEY UPDATE node = VALUES(node)");
            builder.build().execute(&mut *connection).await?;
        }
        Ok(())
    }

    async fn delete_node_chunks(
        connection: &mut MySqlConnection,
        keys: &[&[u8]],
        max_batch_items: usize,
    ) -> Result<(), sqlx::Error> {
        for chunk in keys.chunks(safe_batch_items(max_batch_items, 1)) {
            let mut builder = QueryBuilder::<MySql>::new("DELETE FROM prolly_nodes WHERE cid IN (");
            {
                let mut separated = builder.separated(", ");
                for key in chunk {
                    separated.push_bind(*key);
                }
            }
            builder.push(")");
            builder.build().execute(&mut *connection).await?;
        }
        Ok(())
    }

    async fn upsert_root_chunks(
        connection: &mut MySqlConnection,
        entries: &[(&[u8], &[u8])],
        max_batch_items: usize,
    ) -> Result<(), sqlx::Error> {
        for chunk in entries.chunks(safe_batch_items(max_batch_items, 2)) {
            let mut builder =
                QueryBuilder::<MySql>::new("INSERT INTO prolly_roots (name, manifest) ");
            builder.push_values(chunk, |mut row, (name, manifest)| {
                row.push_bind(*name).push_bind(*manifest);
            });
            builder.push(" ON DUPLICATE KEY UPDATE manifest = VALUES(manifest)");
            builder.build().execute(&mut *connection).await?;
        }
        Ok(())
    }

    async fn delete_root_chunks(
        connection: &mut MySqlConnection,
        names: &[&[u8]],
        max_batch_items: usize,
    ) -> Result<(), sqlx::Error> {
        for chunk in names.chunks(safe_batch_items(max_batch_items, 1)) {
            let mut builder =
                QueryBuilder::<MySql>::new("DELETE FROM prolly_roots WHERE name IN (");
            {
                let mut separated = builder.separated(", ");
                for name in chunk {
                    separated.push_bind(*name);
                }
            }
            builder.push(")");
            builder.build().execute(&mut *connection).await?;
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
        connection: &mut MySqlConnection,
        names: &[Vec<u8>],
        max_batch_items: usize,
    ) -> Result<(), sqlx::Error> {
        if names.is_empty() {
            return Ok(());
        }
        for chunk in names.chunks(safe_batch_items(max_batch_items, 1)) {
            let mut builder = QueryBuilder::<MySql>::new("INSERT INTO prolly_root_locks (name) ");
            builder.push_values(chunk, |mut row, name| {
                row.push_bind(name);
            });
            builder.push(" ON DUPLICATE KEY UPDATE name = VALUES(name)");
            builder.build().execute(&mut *connection).await?;
        }
        for chunk in names.chunks(safe_batch_items(max_batch_items, 1)) {
            let mut builder =
                QueryBuilder::<MySql>::new("SELECT name FROM prolly_root_locks WHERE name IN (");
            {
                let mut separated = builder.separated(", ");
                for name in chunk {
                    separated.push_bind(name);
                }
            }
            builder.push(") ORDER BY name FOR UPDATE");
            builder.build().fetch_all(&mut *connection).await?;
        }
        Ok(())
    }

    async fn read_root_manifests(
        connection: &mut MySqlConnection,
        names: &[Vec<u8>],
        max_batch_items: usize,
    ) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>, sqlx::Error> {
        let mut manifests = names
            .iter()
            .cloned()
            .map(|name| (name, None))
            .collect::<BTreeMap<_, _>>();
        for chunk in names.chunks(safe_batch_items(max_batch_items, 1)) {
            let mut builder = QueryBuilder::<MySql>::new(
                "SELECT name, manifest FROM prolly_roots WHERE name IN (",
            );
            {
                let mut separated = builder.separated(", ");
                for name in chunk {
                    separated.push_bind(name);
                }
            }
            builder.push(")");
            let rows = builder.build().fetch_all(&mut *connection).await?;
            for row in rows {
                manifests.insert(
                    row.try_get::<Vec<u8>, _>("name")?,
                    Some(row.try_get::<Vec<u8>, _>("manifest")?),
                );
            }
        }
        Ok(manifests)
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
);
CREATE TABLE IF NOT EXISTS prolly_root_locks (
  name VARBINARY(255) PRIMARY KEY
);";
}

pub use mysql::*;
