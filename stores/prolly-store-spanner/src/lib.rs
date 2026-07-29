#![doc = include_str!("../README.md")]

pub use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteRootCondition,
    RemoteRootWrite, RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
};

/// Spanner adapter entry point.
pub mod spanner {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use google_cloud_gax::grpc::Code;
    use google_cloud_googleapis::spanner::v1::Mutation;
    use google_cloud_spanner::client::{Client, ClientConfig, Error};
    use google_cloud_spanner::key::Key;
    use google_cloud_spanner::mutation::{delete, insert_or_update};
    use google_cloud_spanner::statement::Statement;
    use google_cloud_spanner::transaction_rw::ReadWriteTransaction;

    use crate::{
        RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteRootCondition, RemoteRootWrite,
        RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
    };

    /// Store adapter for Spanner-backed prolly nodes and roots.
    pub type SpannerStore = crate::RemoteProllyStore<SpannerBackend>;

    /// Google Cloud Spanner-backed backend.
    #[derive(Clone)]
    pub struct SpannerBackend {
        client: Client,
        options: SpannerBackendOptions,
    }

    /// Performance controls for the Spanner backend.
    #[derive(Clone, Debug)]
    pub struct SpannerBackendOptions {
        /// Maximum in-flight reads used by prolly traversal paths.
        pub read_parallelism: usize,
        /// Maximum keys sent in one Spanner streaming-read request.
        pub batch_read_items: usize,
        /// Maintain rightmost-path hints for append-heavy maps.
        pub rightmost_path_hints: bool,
        /// Additional adapter retry attempts after the client exhausts an aborted transaction.
        pub max_transaction_retries: usize,
    }

    impl Default for SpannerBackendOptions {
        fn default() -> Self {
            Self {
                read_parallelism: DEFAULT_READ_PARALLELISM,
                batch_read_items: DEFAULT_BATCH_READ_ITEMS,
                rightmost_path_hints: true,
                max_transaction_retries: DEFAULT_TRANSACTION_RETRIES,
            }
        }
    }

    impl SpannerBackendOptions {
        /// Set maximum traversal read concurrency.
        pub fn with_read_parallelism(mut self, read_parallelism: usize) -> Self {
            self.read_parallelism = read_parallelism.max(1);
            self
        }

        /// Set the maximum number of keys in a native batch read.
        pub fn with_batch_read_items(mut self, batch_read_items: usize) -> Self {
            self.batch_read_items = batch_read_items.max(1);
            self
        }

        /// Enable or disable rightmost-path hint maintenance.
        pub fn with_rightmost_path_hints(mut self, enabled: bool) -> Self {
            self.rightmost_path_hints = enabled;
            self
        }

        /// Set additional retry attempts for transactions aborted by contention.
        pub fn with_max_transaction_retries(mut self, max_transaction_retries: usize) -> Self {
            self.max_transaction_retries = max_transaction_retries;
            self
        }
    }

    impl std::fmt::Debug for SpannerBackend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SpannerBackend")
                .field("options", &self.options)
                .finish_non_exhaustive()
        }
    }

    impl SpannerBackend {
        /// Create a backend from an existing Spanner client.
        pub fn new(client: Client) -> Self {
            Self::new_with_options(client, SpannerBackendOptions::default())
        }

        /// Create a backend from an existing client and explicit performance controls.
        pub fn new_with_options(client: Client, options: SpannerBackendOptions) -> Self {
            Self {
                client,
                options: SpannerBackendOptions {
                    read_parallelism: options.read_parallelism.max(1),
                    batch_read_items: options.batch_read_items.max(1),
                    rightmost_path_hints: options.rightmost_path_hints,
                    max_transaction_retries: options.max_transaction_retries,
                },
            }
        }

        /// Connect to a Spanner database resource name using a caller-provided config.
        pub async fn connect(database: &str, config: ClientConfig) -> Result<Self, Error> {
            Ok(Self::new(Client::new(database, config).await?))
        }

        /// Connect with explicit adapter performance controls.
        pub async fn connect_with_options(
            database: &str,
            config: ClientConfig,
            options: SpannerBackendOptions,
        ) -> Result<Self, Error> {
            Ok(Self::new_with_options(
                Client::new(database, config).await?,
                options,
            ))
        }

        /// Borrow the underlying Spanner client.
        pub fn client(&self) -> &Client {
            &self.client
        }

        /// Set the read parallelism advertised to async prolly traversals.
        pub fn with_read_parallelism(mut self, read_parallelism: usize) -> Self {
            self.options.read_parallelism = read_parallelism.max(1);
            self
        }

        async fn read_one_value(
            &self,
            table: &str,
            key: Key,
            column: &str,
        ) -> Result<Option<Vec<u8>>, Error> {
            let mut tx = self.client.single().await?;
            let row = tx
                .read_row(table, &[column], key)
                .await
                .map_err(Error::from)?;
            row.map(|row| row.column_by_name(column).map_err(Error::from))
                .transpose()
        }

        async fn query_bytes_column(
            &self,
            statement: Statement,
            column: &str,
        ) -> Result<Vec<Vec<u8>>, Error> {
            let mut tx = self.client.single().await?;
            let mut rows = tx.query(statement).await.map_err(Error::from)?;
            let mut values = Vec::new();
            while let Some(row) = rows.next().await.map_err(Error::from)? {
                values.push(row.column_by_name(column)?);
            }
            Ok(values)
        }
    }

    impl RemoteStoreBackend for SpannerBackend {
        type Error = Error;

        async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.read_one_value(NODES_TABLE, Key::new(&key.to_vec()), "Node")
                .await
        }

        async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.client
                .apply(vec![node_upsert(key, value)])
                .await
                .map(|_| ())
        }

        async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
            self.client.apply(vec![node_delete(key)]).await.map(|_| ())
        }

        async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
            let mutations = ops
                .iter()
                .map(|op| match op {
                    RemoteBatchOp::Upsert { key, value } => node_upsert(key, value),
                    RemoteBatchOp::Delete { key } => node_delete(key),
                })
                .collect::<Vec<_>>();
            if mutations.is_empty() {
                return Ok(());
            }
            self.client.apply(mutations).await.map(|_| ())
        }

        async fn batch_get_nodes_ordered(
            &self,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            if keys.is_empty() {
                return Ok(Vec::new());
            }

            let mut found = HashMap::with_capacity(keys.len());
            for chunk in keys.chunks(self.options.batch_read_items) {
                let key_set = chunk
                    .iter()
                    .map(|key| Key::new(&key.to_vec()))
                    .collect::<Vec<_>>();
                let mut tx = self.client.single().await?;
                let mut rows = tx
                    .read(NODES_TABLE, &["Cid", "Node"], key_set)
                    .await
                    .map_err(Error::from)?;
                while let Some(row) = rows.next().await.map_err(Error::from)? {
                    found.insert(
                        row.column_by_name::<Vec<u8>>("Cid")?,
                        row.column_by_name::<Vec<u8>>("Node")?,
                    );
                }
            }

            Ok(keys.iter().map(|key| found.get(*key).cloned()).collect())
        }

        async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            let mutations = entries
                .iter()
                .map(|(key, value)| node_upsert(key, value))
                .collect::<Vec<_>>();
            if mutations.is_empty() {
                return Ok(());
            }
            self.client.apply(mutations).await.map(|_| ())
        }

        async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
            self.query_bytes_column(
                Statement::new("SELECT Cid FROM ProllyNodes ORDER BY Cid"),
                "Cid",
            )
            .await
        }

        fn read_parallelism(&self) -> usize {
            self.options.read_parallelism
        }

        fn prefers_batch_reads(&self) -> bool {
            true
        }

        fn supports_hints(&self) -> bool {
            true
        }

        fn prefers_rightmost_path_hints(&self) -> bool {
            self.options.rightmost_path_hints
        }

        async fn get_hint(
            &self,
            namespace: &[u8],
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, Self::Error> {
            self.read_one_value(
                HINTS_TABLE,
                Key::composite(&[&namespace.to_vec(), &key.to_vec()]),
                "Value",
            )
            .await
        }

        async fn put_hint(
            &self,
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            self.client
                .apply(vec![hint_upsert(namespace, key, value)])
                .await
                .map(|_| ())
        }

        async fn batch_put_nodes_with_hint(
            &self,
            entries: &[(&[u8], &[u8])],
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            let mut mutations = entries
                .iter()
                .map(|(key, value)| node_upsert(key, value))
                .collect::<Vec<_>>();
            mutations.push(hint_upsert(namespace, key, value));
            self.client.apply(mutations).await.map(|_| ())
        }

        async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.read_one_value(ROOTS_TABLE, Key::new(&name.to_vec()), "Manifest")
                .await
        }

        async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
            self.client
                .apply(vec![root_upsert(name, manifest)])
                .await
                .map(|_| ())
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            self.client.apply(vec![root_delete(name)]).await.map(|_| ())
        }

        async fn compare_and_swap_root_manifest(
            &self,
            name: &[u8],
            expected: Option<&[u8]>,
            new: Option<&[u8]>,
        ) -> Result<RemoteManifestUpdate, Self::Error> {
            let name = name.to_vec();
            let expected = expected.map(<[u8]>::to_vec);
            let new = new.map(<[u8]>::to_vec);
            let mut outer_attempt = 0;
            loop {
                let result = self
                    .client
                    .read_write_transaction(|tx| {
                        let name = name.clone();
                        let expected = expected.clone();
                        let new = new.clone();
                        Box::pin(async move {
                            let current = read_root_in_transaction(tx, &name).await?;
                            if current.as_deref() != expected.as_deref() {
                                return Ok::<RemoteManifestUpdate, Error>(
                                    RemoteManifestUpdate::Conflict { current },
                                );
                            }

                            match new {
                                Some(manifest) => {
                                    tx.buffer_write(vec![root_upsert(&name, &manifest)])
                                }
                                None => tx.buffer_write(vec![root_delete(&name)]),
                            }
                            Ok::<RemoteManifestUpdate, Error>(RemoteManifestUpdate::Applied)
                        })
                    })
                    .await;
                match result {
                    Ok((_, update)) => return Ok(update),
                    Err(err)
                        if is_aborted(&err)
                            && outer_attempt < self.options.max_transaction_retries =>
                    {
                        tokio::time::sleep(transaction_retry_delay(outer_attempt)).await;
                        outer_attempt += 1;
                    }
                    Err(err) => return Err(err),
                }
            }
        }

        async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error> {
            let mut tx = self.client.single().await?;
            let mut rows = tx
                .query(Statement::new(
                    "SELECT Name, Manifest FROM ProllyRoots ORDER BY Name",
                ))
                .await
                .map_err(Error::from)?;
            let mut roots = Vec::new();
            while let Some(row) = rows.next().await.map_err(Error::from)? {
                roots.push(RemoteNamedRoot::new(
                    row.column_by_name("Name")?,
                    row.column_by_name("Manifest")?,
                ));
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
            let node_writes = Arc::new(
                node_writes
                    .iter()
                    .map(|write| match write {
                        RemoteBatchOp::Upsert { key, value } => {
                            (true, key.to_vec(), value.to_vec())
                        }
                        RemoteBatchOp::Delete { key } => (false, key.to_vec(), Vec::new()),
                    })
                    .collect::<Vec<_>>(),
            );
            let root_conditions = Arc::new(root_conditions.to_vec());
            let root_writes = Arc::new(root_writes.to_vec());

            let mut outer_attempt = 0;
            loop {
                let result = self
                    .client
                    .read_write_transaction(|tx| {
                        let node_writes = Arc::clone(&node_writes);
                        let root_conditions = Arc::clone(&root_conditions);
                        let root_writes = Arc::clone(&root_writes);
                        Box::pin(async move {
                            let current_roots =
                                read_roots_in_transaction(tx, &root_conditions).await?;
                            for condition in root_conditions.iter() {
                                let current = current_roots.get(&condition.name).cloned();
                                if current != condition.expected {
                                    return Ok::<RemoteTransactionUpdate, Error>(
                                        RemoteTransactionUpdate::Conflict(
                                            RemoteTransactionConflict::new(
                                                condition.name.clone(),
                                                condition.expected.clone(),
                                                current,
                                            ),
                                        ),
                                    );
                                }
                            }

                            let mut mutations = Vec::new();
                            for (is_upsert, key, value) in node_writes.iter() {
                                if *is_upsert {
                                    mutations.push(node_upsert(key, value));
                                } else {
                                    mutations.push(node_delete(key));
                                }
                            }
                            for write in root_writes.iter() {
                                match write {
                                    RemoteRootWrite::Put { name, manifest } => {
                                        mutations.push(root_upsert(name, manifest));
                                    }
                                    RemoteRootWrite::Delete { name } => {
                                        mutations.push(root_delete(name));
                                    }
                                }
                            }
                            if !mutations.is_empty() {
                                tx.buffer_write(mutations);
                            }
                            Ok::<RemoteTransactionUpdate, Error>(RemoteTransactionUpdate::Applied)
                        })
                    })
                    .await;
                match result {
                    Ok((_, update)) => return Ok(update),
                    Err(err)
                        if is_aborted(&err)
                            && outer_attempt < self.options.max_transaction_retries =>
                    {
                        tokio::time::sleep(transaction_retry_delay(outer_attempt)).await;
                        outer_attempt += 1;
                    }
                    Err(err) => return Err(err),
                }
            }
        }
    }

    fn is_aborted(error: &Error) -> bool {
        matches!(error, Error::GRPC(status) if status.code() == Code::Aborted)
    }

    fn transaction_retry_delay(attempt: usize) -> Duration {
        Duration::from_millis(10u64.saturating_mul(1u64 << attempt.min(5)))
    }

    async fn read_root_in_transaction(
        tx: &mut ReadWriteTransaction,
        name: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        let name = name.to_vec();
        let row = tx
            .read_row(ROOTS_TABLE, &["Manifest"], Key::new(&name))
            .await
            .map_err(Error::from)?;
        row.map(|row| row.column_by_name("Manifest").map_err(Error::from))
            .transpose()
    }

    async fn read_roots_in_transaction(
        tx: &mut ReadWriteTransaction,
        conditions: &[RemoteRootCondition],
    ) -> Result<HashMap<Vec<u8>, Vec<u8>>, Error> {
        if conditions.is_empty() {
            return Ok(HashMap::new());
        }
        let keys = conditions
            .iter()
            .map(|condition| Key::new(&condition.name))
            .collect::<Vec<_>>();
        let mut rows = tx
            .read(ROOTS_TABLE, &["Name", "Manifest"], keys)
            .await
            .map_err(Error::from)?;
        let mut roots = HashMap::with_capacity(conditions.len());
        while let Some(row) = rows.next().await.map_err(Error::from)? {
            roots.insert(
                row.column_by_name::<Vec<u8>>("Name")?,
                row.column_by_name::<Vec<u8>>("Manifest")?,
            );
        }
        Ok(roots)
    }

    fn node_upsert(key: &[u8], value: &[u8]) -> Mutation {
        let key = key.to_vec();
        let value = value.to_vec();
        insert_or_update(NODES_TABLE, &["Cid", "Node"], &[&key, &value])
    }

    fn node_delete(key: &[u8]) -> Mutation {
        let key = key.to_vec();
        delete(NODES_TABLE, Key::new(&key))
    }

    fn hint_upsert(namespace: &[u8], key: &[u8], value: &[u8]) -> Mutation {
        let namespace = namespace.to_vec();
        let key = key.to_vec();
        let value = value.to_vec();
        insert_or_update(
            HINTS_TABLE,
            &["Namespace", "HintKey", "Value"],
            &[&namespace, &key, &value],
        )
    }

    fn root_upsert(name: &[u8], manifest: &[u8]) -> Mutation {
        let name = name.to_vec();
        let manifest = manifest.to_vec();
        insert_or_update(ROOTS_TABLE, &["Name", "Manifest"], &[&name, &manifest])
    }

    fn root_delete(name: &[u8]) -> Mutation {
        let name = name.to_vec();
        delete(ROOTS_TABLE, Key::new(&name))
    }

    const DEFAULT_READ_PARALLELISM: usize = 16;
    const DEFAULT_BATCH_READ_ITEMS: usize = 5_000;
    const DEFAULT_TRANSACTION_RETRIES: usize = 16;
    const NODES_TABLE: &str = "ProllyNodes";
    const HINTS_TABLE: &str = "ProllyHints";
    const ROOTS_TABLE: &str = "ProllyRoots";

    /// Minimal GoogleSQL table layout for Spanner implementations.
    pub const SPANNER_SCHEMA: &str = "\
CREATE TABLE ProllyNodes (
  Cid BYTES(32) NOT NULL,
  Node BYTES(MAX) NOT NULL
) PRIMARY KEY (Cid);
CREATE TABLE ProllyHints (
  Namespace BYTES(MAX) NOT NULL,
  HintKey BYTES(MAX) NOT NULL,
  Value BYTES(MAX) NOT NULL
) PRIMARY KEY (Namespace, HintKey);
CREATE TABLE ProllyRoots (
  Name BYTES(MAX) NOT NULL,
  Manifest BYTES(MAX) NOT NULL
) PRIMARY KEY (Name);";

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn production_defaults_enable_native_batch_reads_and_hints() {
            let options = SpannerBackendOptions::default();
            assert_eq!(options.read_parallelism, 16);
            assert_eq!(options.batch_read_items, 5_000);
            assert_eq!(options.max_transaction_retries, 16);
            assert!(options.rightmost_path_hints);
        }

        #[test]
        fn checked_in_schema_matches_exported_schema() {
            let checked_in = include_str!("../schema.sql")
                .split_whitespace()
                .collect::<String>();
            let exported = SPANNER_SCHEMA.split_whitespace().collect::<String>();
            assert_eq!(checked_in, exported);
        }
    }
}

pub use spanner::*;
