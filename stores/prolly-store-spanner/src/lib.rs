//! Cloud Spanner store adapter for prolly-map.

pub use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteStoreBackend,
};

/// Spanner adapter entry point.
pub mod spanner {
    use google_cloud_googleapis::spanner::v1::Mutation;
    use google_cloud_spanner::client::{Client, ClientConfig, Error};
    use google_cloud_spanner::key::Key;
    use google_cloud_spanner::mutation::{delete, insert_or_update};
    use google_cloud_spanner::statement::Statement;
    use google_cloud_spanner::transaction_rw::ReadWriteTransaction;

    use crate::{RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteStoreBackend};

    /// Store adapter for Spanner-backed prolly nodes and roots.
    pub type SpannerStore = crate::RemoteProllyStore<SpannerBackend>;

    /// Google Cloud Spanner-backed backend.
    #[derive(Clone)]
    pub struct SpannerBackend {
        client: Client,
        read_parallelism: usize,
    }

    impl std::fmt::Debug for SpannerBackend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SpannerBackend")
                .field("read_parallelism", &self.read_parallelism)
                .finish_non_exhaustive()
        }
    }

    impl SpannerBackend {
        /// Create a backend from an existing Spanner client.
        pub fn new(client: Client) -> Self {
            Self {
                client,
                read_parallelism: DEFAULT_READ_PARALLELISM,
            }
        }

        /// Connect to a Spanner database resource name using a caller-provided config.
        pub async fn connect(database: &str, config: ClientConfig) -> Result<Self, Error> {
            Ok(Self::new(Client::new(database, config).await?))
        }

        /// Borrow the underlying Spanner client.
        pub fn client(&self) -> &Client {
            &self.client
        }

        /// Set the read parallelism advertised to async prolly traversals.
        pub fn with_read_parallelism(mut self, read_parallelism: usize) -> Self {
            self.read_parallelism = read_parallelism.max(1);
            self
        }

        async fn query_one_value(
            &self,
            statement: Statement,
            column: &str,
        ) -> Result<Option<Vec<u8>>, Error> {
            let mut tx = self.client.single().await?;
            let mut rows = tx.query(statement).await.map_err(Error::from)?;
            let row = rows.next().await.map_err(Error::from)?;
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
            let mut statement = Statement::new("SELECT Node FROM ProllyNodes WHERE Cid = @cid");
            statement.add_param("cid", &key.to_vec());
            self.query_one_value(statement, "Node").await
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
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values.push(self.get_node(key).await?);
            }
            Ok(values)
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
            self.read_parallelism
        }

        fn supports_hints(&self) -> bool {
            true
        }

        async fn get_hint(
            &self,
            namespace: &[u8],
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, Self::Error> {
            let mut statement = Statement::new(
                "SELECT Value FROM ProllyHints WHERE Namespace = @namespace AND HintKey = @key",
            );
            statement.add_param("namespace", &namespace.to_vec());
            statement.add_param("key", &key.to_vec());
            self.query_one_value(statement, "Value").await
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
            let mut statement =
                Statement::new("SELECT Manifest FROM ProllyRoots WHERE Name = @name");
            statement.add_param("name", &name.to_vec());
            self.query_one_value(statement, "Manifest").await
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
            let (_, update) = self
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
                            Some(manifest) => tx.buffer_write(vec![root_upsert(&name, &manifest)]),
                            None => tx.buffer_write(vec![root_delete(&name)]),
                        }
                        Ok::<RemoteManifestUpdate, Error>(RemoteManifestUpdate::Applied)
                    })
                })
                .await?;
            Ok(update)
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
}

pub use spanner::*;
