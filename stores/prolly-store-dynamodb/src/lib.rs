#![doc = include_str!("../README.md")]

pub use prolly::{
    BlockingRemoteBuildError, BlockingRemoteProllyStore, BlockingRemoteStoreError, RemoteBatchOp,
    RemoteManifestUpdate, RemoteNamedRoot, RemoteNamedRootPage, RemoteProllyStore,
    RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend, RemoteTransactionConflict,
    RemoteTransactionUpdate,
};

/// DynamoDB adapter entry point.
pub mod dynamodb {
    use std::collections::{HashMap, HashSet};
    use std::error::Error as StdError;
    use std::fmt;
    use std::time::{Duration, Instant};

    use aws_sdk_dynamodb::error::SdkError;
    use aws_sdk_dynamodb::operation::create_table::CreateTableError;
    use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
    use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
    use aws_sdk_dynamodb::operation::put_item::PutItemError;
    use aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError;
    use aws_sdk_dynamodb::primitives::Blob;
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, ConditionCheck, Delete as TransactDelete,
        DeleteRequest, KeySchemaElement, KeyType, KeysAndAttributes, Put as TransactPut,
        PutRequest, ReturnValuesOnConditionCheckFailure, ScalarAttributeType, TableDescription,
        TableStatus, TransactWriteItem, WriteRequest,
    };
    use futures_util::stream::{self, StreamExt, TryStreamExt};

    use crate::{
        RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteNamedRootPage,
        RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend, RemoteTransactionConflict,
        RemoteTransactionUpdate,
    };

    /// Store adapter for DynamoDB-backed prolly nodes and roots.
    pub type DynamoDbStore = crate::RemoteProllyStore<DynamoDbBackend>;

    /// Synchronous DynamoDB store supporting `Prolly::indexed_map`.
    pub type SyncDynamoDbStore = crate::BlockingRemoteProllyStore<DynamoDbBackend>;

    /// Maximum serialized Prolly node size used by [`dynamodb_safe_config`].
    ///
    /// This leaves substantial room below DynamoDB's item ceiling for the
    /// binary partition key and attribute encoding overhead.
    pub const DYNAMODB_SAFE_NODE_BYTES: u64 = 300 * 1024;

    /// Build a byte-measured tree configuration suitable for DynamoDB items.
    ///
    /// Values that can exceed this ceiling must be represented through a blob
    /// reference rather than stored inline in a leaf node.
    pub fn dynamodb_safe_config() -> prolly::Config {
        let mut format = prolly::TreeFormat::default();
        format.chunking.measure = prolly::ChunkMeasure::EncodedBytes;
        format.chunking.input = prolly::BoundaryInput::KeyValue;
        format.chunking.min = 32 * 1024;
        format.chunking.target = 128 * 1024;
        format.chunking.max = 256 * 1024;
        format.chunking.hard_max_node_bytes = DYNAMODB_SAFE_NODE_BYTES;
        prolly::Config::builder().format(format).build()
    }

    /// How strict engine transactions publish content-addressed nodes.
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub enum TransactionPublicationMode {
        /// Prepublish verified immutable node upserts, then atomically validate
        /// and move roots. Conflicts can leave unreachable nodes for normal GC.
        #[default]
        PrepublishImmutableNodes,
        /// Include node writes and root actions in one provider transaction.
        /// This is limited to DynamoDB's transaction-action ceiling.
        AtomicNodesAndRoots,
    }

    /// Transaction limits and behavior advertised by [`DynamoDbBackend`].
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct DynamoDbTransactionCapabilities {
        /// Maximum conditioned root actions in one transaction.
        pub root_action_limit: usize,
        /// Active node publication strategy.
        pub publication_mode: TransactionPublicationMode,
        /// Whether strict transactions accept staged content-addressed deletes.
        pub staged_node_deletes: bool,
    }

    /// Chunked content-addressed blob store sharing a DynamoDB backend namespace.
    ///
    /// Chunks are written before a compact manifest makes the blob visible.
    /// Failed writes can therefore leave only unreachable chunks; readers
    /// never accept a partial blob as complete.
    #[derive(Clone, Debug)]
    pub struct DynamoDbBlobStore {
        backend: DynamoDbBackend,
        chunk_size: usize,
    }

    /// One bounded DynamoDB Scan page of content-addressed node candidates.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct DynamoDbNodePage {
        pub cids: Vec<prolly::Cid>,
        /// Opaque namespace-bound cursor for the next provider page.
        pub next_cursor: Option<Vec<u8>>,
    }

    /// One bounded DynamoDB Scan page of visible blob manifests.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct DynamoDbBlobPage {
        pub references: Vec<prolly::BlobRef>,
        /// Opaque namespace-bound cursor for the next provider page.
        pub next_cursor: Option<Vec<u8>>,
    }

    impl DynamoDbBlobStore {
        /// Create a blob store using the conservative default chunk size.
        pub fn new(backend: DynamoDbBackend) -> Self {
            Self {
                backend,
                chunk_size: DYNAMODB_BLOB_CHUNK_BYTES,
            }
        }

        /// Configure the payload bytes stored in each chunk item.
        pub fn with_chunk_size(mut self, chunk_size: usize) -> Result<Self, DynamoDbBackendError> {
            if !(1..=DYNAMODB_BLOB_CHUNK_BYTES).contains(&chunk_size) {
                return Err(DynamoDbBackendError::InvalidBlobChunkSize {
                    requested: chunk_size,
                    maximum: DYNAMODB_BLOB_CHUNK_BYTES,
                });
            }
            self.chunk_size = chunk_size;
            Ok(self)
        }

        /// Borrow the backend used for blob operations.
        pub fn backend(&self) -> &DynamoDbBackend {
            &self.backend
        }

        /// Configured payload bytes per chunk.
        pub fn chunk_size(&self) -> usize {
            self.chunk_size
        }

        /// Enumerate one bounded provider page of visible blob references.
        pub async fn list_blob_refs_page(
            &self,
            cursor: Option<&[u8]>,
            evaluation_limit: usize,
        ) -> Result<DynamoDbBlobPage, DynamoDbBackendError> {
            let prefix = self.backend.family_prefix(BLOB_MANIFEST_FAMILY);
            let (items, next_cursor) = self
                .backend
                .scan_family_page(&prefix, cursor, evaluation_limit, true)
                .await?;
            let mut references = Vec::with_capacity(items.len());
            for (key, value) in items {
                let cid = key.strip_prefix(prefix.as_slice()).ok_or_else(|| {
                    DynamoDbBackendError::InvalidBlobManifest(
                        "blob scan returned a key outside its family".into(),
                    )
                })?;
                let cid: [u8; 32] = cid.try_into().map_err(|_| {
                    DynamoDbBackendError::InvalidBlobManifest(
                        "blob manifest key has an invalid CID length".into(),
                    )
                })?;
                let value = value.ok_or_else(|| {
                    DynamoDbBackendError::InvalidBlobManifest(
                        "blob scan omitted a manifest value".into(),
                    )
                })?;
                let (len, chunk_size, chunk_count) = decode_blob_manifest(&value)?;
                if blob_chunk_count(len, chunk_size)? != chunk_count {
                    return Err(DynamoDbBackendError::InvalidBlobManifest(
                        "blob manifest scan found an inconsistent chunk count".into(),
                    ));
                }
                references.push(prolly::BlobRef {
                    cid: prolly::Cid(cid),
                    len,
                });
            }
            references.sort_by(|left, right| left.cid.as_bytes().cmp(right.cid.as_bytes()));
            Ok(DynamoDbBlobPage {
                references,
                next_cursor,
            })
        }
    }

    /// AWS SDK-backed DynamoDB backend.
    ///
    /// The primary table must use a binary partition key named `pk`. The
    /// companion root registry table uses binary partition and sort keys named
    /// `pk` and `sk`. The adapter creates and validates both tables through
    /// [`DynamoDbBackend::initialize_schema`].
    #[derive(Clone, Debug)]
    pub struct DynamoDbBackend {
        client: aws_sdk_dynamodb::Client,
        table_name: String,
        root_table_name: String,
        key_prefix: Vec<u8>,
        read_parallelism: usize,
        batch_get_parallelism: usize,
        batch_write_parallelism: usize,
        scan_parallelism: usize,
        transaction_publication_mode: TransactionPublicationMode,
    }

    impl DynamoDbBackend {
        /// Create a backend from an existing AWS SDK DynamoDB client.
        pub fn new(client: aws_sdk_dynamodb::Client, table_name: impl Into<String>) -> Self {
            let table_name = table_name.into();
            let root_table_name = format!("{table_name}{ROOT_TABLE_SUFFIX}");
            Self {
                client,
                table_name,
                root_table_name,
                key_prefix: DEFAULT_KEY_PREFIX.to_vec(),
                read_parallelism: DEFAULT_READ_PARALLELISM,
                batch_get_parallelism: DEFAULT_BATCH_GET_PARALLELISM,
                batch_write_parallelism: DEFAULT_BATCH_WRITE_PARALLELISM,
                scan_parallelism: DEFAULT_SCAN_PARALLELISM,
                transaction_publication_mode: TransactionPublicationMode::default(),
            }
        }

        /// Borrow the underlying DynamoDB client.
        pub fn client(&self) -> &aws_sdk_dynamodb::Client {
            &self.client
        }

        /// Return the DynamoDB table name.
        pub fn table_name(&self) -> &str {
            &self.table_name
        }

        /// Return the companion root registry table name.
        pub fn root_table_name(&self) -> &str {
            &self.root_table_name
        }

        /// Override the companion root registry table name.
        ///
        /// All clients that share a primary table and key prefix must use the
        /// same root registry table.
        pub fn with_root_table_name(mut self, table_name: impl Into<String>) -> Self {
            self.root_table_name = table_name.into();
            self
        }

        /// Return the namespace prefix prepended to all item keys.
        pub fn key_prefix(&self) -> &[u8] {
            &self.key_prefix
        }

        /// Set the namespace prefix prepended to all item keys.
        pub fn with_key_prefix(mut self, key_prefix: impl Into<Vec<u8>>) -> Self {
            self.key_prefix = key_prefix.into();
            self
        }

        /// Set the read parallelism advertised to async prolly traversals.
        pub fn with_read_parallelism(mut self, read_parallelism: usize) -> Self {
            self.read_parallelism = read_parallelism.max(1);
            self
        }

        /// Return the configured async point/traversal read parallelism.
        pub fn read_parallelism(&self) -> usize {
            self.read_parallelism
        }

        /// Enumerate one bounded provider Scan page of node CIDs.
        pub async fn list_node_cids_page(
            &self,
            cursor: Option<&[u8]>,
            evaluation_limit: usize,
        ) -> Result<DynamoDbNodePage, DynamoDbBackendError> {
            let prefix = self.family_prefix(NODE_FAMILY);
            let (items, next_cursor) = self
                .scan_family_page(&prefix, cursor, evaluation_limit, false)
                .await?;
            let mut cids = Vec::with_capacity(items.len());
            for (key, _) in items {
                let suffix = key.strip_prefix(prefix.as_slice()).ok_or_else(|| {
                    DynamoDbBackendError::InvalidConfiguration(
                        "node scan returned a key outside its family".into(),
                    )
                })?;
                let cid: [u8; 32] = suffix.try_into().map_err(|_| {
                    DynamoDbBackendError::InvalidConfiguration(
                        "node key has an invalid CID length".into(),
                    )
                })?;
                cids.push(prolly::Cid(cid));
            }
            cids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
            Ok(DynamoDbNodePage { cids, next_cursor })
        }

        /// Set the maximum number of concurrent `BatchGetItem` requests.
        ///
        /// DynamoDB limits each request to 100 items. Values greater than one
        /// allow large reads to use multiple requests concurrently.
        pub fn with_batch_get_parallelism(mut self, parallelism: usize) -> Self {
            self.batch_get_parallelism = parallelism.max(1);
            self
        }

        /// Return the configured concurrent `BatchGetItem` request limit.
        pub fn batch_get_parallelism(&self) -> usize {
            self.batch_get_parallelism
        }

        /// Set the maximum number of concurrent `BatchWriteItem` requests.
        ///
        /// DynamoDB limits each request to 25 items. Keep this bounded to the
        /// write capacity available to the table.
        pub fn with_batch_write_parallelism(mut self, parallelism: usize) -> Self {
            self.batch_write_parallelism = parallelism.max(1);
            self
        }

        /// Return the configured concurrent `BatchWriteItem` request limit.
        pub fn batch_write_parallelism(&self) -> usize {
            self.batch_write_parallelism
        }

        /// Set the number of segments used for parallel table scans.
        ///
        /// Scans back root enumeration, node enumeration, and namespace
        /// cleanup because the adapter's binary partition key has no sortable
        /// family component. Parallel scans reduce wall-clock time but consume
        /// the same total read capacity more aggressively.
        pub fn with_scan_parallelism(mut self, parallelism: usize) -> Self {
            self.scan_parallelism = parallelism.clamp(1, DYNAMODB_SCAN_SEGMENT_LIMIT);
            self
        }

        /// Return the configured physical scan segment count.
        pub fn scan_parallelism(&self) -> usize {
            self.scan_parallelism
        }

        /// Select how strict transactions publish content-addressed nodes.
        pub fn with_transaction_publication_mode(
            mut self,
            mode: TransactionPublicationMode,
        ) -> Self {
            self.transaction_publication_mode = mode;
            self
        }

        /// Report provider transaction limits and active publication behavior.
        pub fn transaction_capabilities(&self) -> DynamoDbTransactionCapabilities {
            DynamoDbTransactionCapabilities {
                root_action_limit: DYNAMODB_TRANSACTION_WRITE_LIMIT,
                publication_mode: self.transaction_publication_mode,
                staged_node_deletes: matches!(
                    self.transaction_publication_mode,
                    TransactionPublicationMode::AtomicNodesAndRoots
                ),
            }
        }

        /// Create the required DynamoDB tables if they do not already exist.
        ///
        /// The primary table uses a binary partition key named `pk`. The root
        /// registry uses binary partition and sort keys named `pk` and `sk`.
        /// Version 0.4 uses the root table as the sole named-root store and
        /// does not read or migrate root entries from older schemas.
        pub async fn initialize_schema(&self) -> Result<(), DynamoDbBackendError> {
            self.initialize_primary_table().await?;
            self.initialize_root_table().await
        }

        /// Validate that both explicitly provisioned tables exist, are active,
        /// and use the required key schemas. This method never creates or
        /// modifies infrastructure.
        pub async fn validate_initialized_schema(&self) -> Result<(), DynamoDbBackendError> {
            let primary = self
                .client
                .describe_table()
                .table_name(&self.table_name)
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?
                .table
                .ok_or_else(|| {
                    DynamoDbBackendError::InvalidConfiguration(format!(
                        "DynamoDB table {} was described without table metadata",
                        self.table_name
                    ))
                })?;
            self.validate_primary_table_schema(&primary)?;
            self.require_active(&primary, &self.table_name)?;

            let roots = self
                .client
                .describe_table()
                .table_name(&self.root_table_name)
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?
                .table
                .ok_or_else(|| {
                    DynamoDbBackendError::InvalidConfiguration(format!(
                        "DynamoDB root registry table {} was described without table metadata",
                        self.root_table_name
                    ))
                })?;
            self.validate_root_table_schema(&roots)?;
            self.require_active(&roots, &self.root_table_name)
        }

        async fn initialize_primary_table(&self) -> Result<(), DynamoDbBackendError> {
            match self
                .client
                .describe_table()
                .table_name(&self.table_name)
                .send()
                .await
            {
                Ok(output) => {
                    let table = output.table().ok_or_else(|| {
                        DynamoDbBackendError::InvalidConfiguration(format!(
                            "DynamoDB table {} was described without table metadata",
                            self.table_name
                        ))
                    })?;
                    self.validate_primary_table_schema(table)?;
                    let active = self.wait_for_table_active(&self.table_name).await?;
                    return self.validate_primary_table_schema(&active);
                }
                Err(err) if describe_table_not_found(&err) => {}
                Err(err) => return Err(DynamoDbBackendError::sdk(err)),
            }

            self.client
                .create_table()
                .table_name(&self.table_name)
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name(PK_ATTR)
                        .attribute_type(ScalarAttributeType::B)
                        .build()
                        .map_err(DynamoDbBackendError::sdk)?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name(PK_ATTR)
                        .key_type(KeyType::Hash)
                        .build()
                        .map_err(DynamoDbBackendError::sdk)?,
                )
                .billing_mode(BillingMode::PayPerRequest)
                .send()
                .await
                .map(|_| ())
                .or_else(|err| {
                    if create_table_in_use(&err) {
                        Ok(())
                    } else {
                        Err(DynamoDbBackendError::sdk(err))
                    }
                })?;
            let active = self.wait_for_table_active(&self.table_name).await?;
            self.validate_primary_table_schema(&active)
        }

        async fn initialize_root_table(&self) -> Result<(), DynamoDbBackendError> {
            match self
                .client
                .describe_table()
                .table_name(&self.root_table_name)
                .send()
                .await
            {
                Ok(output) => {
                    let table = output.table().ok_or_else(|| {
                        DynamoDbBackendError::InvalidConfiguration(format!(
                            "DynamoDB root registry table {} was described without table metadata",
                            self.root_table_name
                        ))
                    })?;
                    self.validate_root_table_schema(table)?;
                    let active = self.wait_for_table_active(&self.root_table_name).await?;
                    return self.validate_root_table_schema(&active);
                }
                Err(err) if describe_table_not_found(&err) => {}
                Err(err) => return Err(DynamoDbBackendError::sdk(err)),
            }

            self.client
                .create_table()
                .table_name(&self.root_table_name)
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name(PK_ATTR)
                        .attribute_type(ScalarAttributeType::B)
                        .build()
                        .map_err(DynamoDbBackendError::sdk)?,
                )
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name(SK_ATTR)
                        .attribute_type(ScalarAttributeType::B)
                        .build()
                        .map_err(DynamoDbBackendError::sdk)?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name(PK_ATTR)
                        .key_type(KeyType::Hash)
                        .build()
                        .map_err(DynamoDbBackendError::sdk)?,
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name(SK_ATTR)
                        .key_type(KeyType::Range)
                        .build()
                        .map_err(DynamoDbBackendError::sdk)?,
                )
                .billing_mode(BillingMode::PayPerRequest)
                .send()
                .await
                .map(|_| ())
                .or_else(|err| {
                    if create_table_in_use(&err) {
                        Ok(())
                    } else {
                        Err(DynamoDbBackendError::sdk(err))
                    }
                })?;
            let active = self.wait_for_table_active(&self.root_table_name).await?;
            self.validate_root_table_schema(&active)
        }

        /// Delete every item under this backend's namespace prefix.
        ///
        /// This is primarily intended for isolated integration tests.
        pub async fn clear_namespace(&self) -> Result<(), DynamoDbBackendError> {
            if self.key_prefix.is_empty() {
                return Err(DynamoDbBackendError::InvalidConfiguration(
                    "refusing to clear an empty DynamoDB key prefix".to_string(),
                ));
            }

            let keys = self.scan_primary_keys_with_prefix(&self.key_prefix).await?;
            let requests = keys
                .into_iter()
                .map(|key| self.delete_write_request(key))
                .collect::<Result<Vec<_>, _>>()?;
            self.batch_write_requests(&requests).await?;

            let root_keys = self.root_keys().await?;
            let requests = root_keys
                .into_iter()
                .map(|key| self.delete_item_write_request(key))
                .collect::<Result<Vec<_>, _>>()?;
            self.batch_write_requests_for_table(&self.root_table_name, &requests)
                .await
        }

        fn node_key(&self, key: &[u8]) -> Vec<u8> {
            self.family_key(NODE_FAMILY, key)
        }

        fn blob_manifest_key(&self, cid: &prolly::Cid) -> Vec<u8> {
            self.family_key(BLOB_MANIFEST_FAMILY, cid.as_bytes())
        }

        fn blob_chunk_key(&self, cid: &prolly::Cid, chunk_size: u32, index: u32) -> Vec<u8> {
            let mut key = self.family_key(BLOB_CHUNK_FAMILY, cid.as_bytes());
            key.extend_from_slice(&chunk_size.to_be_bytes());
            key.extend_from_slice(&index.to_be_bytes());
            key
        }

        fn root_partition_key(&self) -> Vec<u8> {
            let mut key = Vec::with_capacity(ROOT_PARTITION_PREFIX.len() + self.key_prefix.len());
            key.extend_from_slice(ROOT_PARTITION_PREFIX);
            key.extend_from_slice(&self.key_prefix);
            key
        }

        fn root_sort_key(&self, name: &[u8]) -> Vec<u8> {
            let mut key = Vec::with_capacity(ROOT_ENTRY_PREFIX.len() + name.len());
            key.extend_from_slice(ROOT_ENTRY_PREFIX);
            key.extend_from_slice(name);
            key
        }

        fn hint_key(&self, namespace: &[u8], key: &[u8]) -> Vec<u8> {
            let mut dynamo_key = self.family_key(HINT_FAMILY, &[]);
            dynamo_key.extend_from_slice(&(namespace.len() as u64).to_be_bytes());
            dynamo_key.extend_from_slice(namespace);
            dynamo_key.extend_from_slice(key);
            dynamo_key
        }

        fn family_key(&self, family: &[u8], suffix: &[u8]) -> Vec<u8> {
            let mut key = Vec::with_capacity(self.key_prefix.len() + family.len() + suffix.len());
            key.extend_from_slice(&self.key_prefix);
            key.extend_from_slice(family);
            key.extend_from_slice(suffix);
            key
        }

        fn family_prefix(&self, family: &[u8]) -> Vec<u8> {
            self.family_key(family, &[])
        }

        fn item(&self, key: Vec<u8>, value: &[u8]) -> HashMap<String, AttributeValue> {
            HashMap::from([
                (PK_ATTR.to_string(), binary_attr(key)),
                (VALUE_ATTR.to_string(), binary_attr(value)),
            ])
        }

        fn key_item(&self, key: Vec<u8>) -> HashMap<String, AttributeValue> {
            HashMap::from([(PK_ATTR.to_string(), binary_attr(key))])
        }

        fn root_key_item(&self, name: &[u8]) -> HashMap<String, AttributeValue> {
            HashMap::from([
                (PK_ATTR.to_string(), binary_attr(self.root_partition_key())),
                (SK_ATTR.to_string(), binary_attr(self.root_sort_key(name))),
            ])
        }

        fn root_item(&self, name: &[u8], manifest: &[u8]) -> HashMap<String, AttributeValue> {
            let mut item = self.root_key_item(name);
            item.insert(VALUE_ATTR.to_string(), binary_attr(manifest));
            item
        }

        fn validate_primary_table_schema(
            &self,
            table: &TableDescription,
        ) -> Result<(), DynamoDbBackendError> {
            let key_schema = table.key_schema();
            if key_schema.len() != 1
                || key_schema[0].attribute_name() != PK_ATTR
                || key_schema[0].key_type() != &KeyType::Hash
            {
                return Err(DynamoDbBackendError::InvalidConfiguration(format!(
                    "DynamoDB table {} must use a single HASH partition key named {PK_ATTR}",
                    self.table_name
                )));
            }

            let has_binary_pk = table.attribute_definitions().iter().any(|attribute| {
                attribute.attribute_name() == PK_ATTR
                    && attribute.attribute_type() == &ScalarAttributeType::B
            });
            if !has_binary_pk {
                return Err(DynamoDbBackendError::InvalidConfiguration(format!(
                    "DynamoDB table {} partition key {PK_ATTR} must be binary",
                    self.table_name
                )));
            }

            Ok(())
        }

        fn validate_root_table_schema(
            &self,
            table: &TableDescription,
        ) -> Result<(), DynamoDbBackendError> {
            let key_schema = table.key_schema();
            let has_pk = key_schema
                .iter()
                .any(|key| key.attribute_name() == PK_ATTR && key.key_type() == &KeyType::Hash);
            let has_sk = key_schema
                .iter()
                .any(|key| key.attribute_name() == SK_ATTR && key.key_type() == &KeyType::Range);
            if key_schema.len() != 2 || !has_pk || !has_sk {
                return Err(DynamoDbBackendError::InvalidConfiguration(format!(
                    "DynamoDB root registry table {} must use HASH key {PK_ATTR} and RANGE key {SK_ATTR}",
                    self.root_table_name
                )));
            }

            for attribute_name in [PK_ATTR, SK_ATTR] {
                let has_binary_key = table.attribute_definitions().iter().any(|attribute| {
                    attribute.attribute_name() == attribute_name
                        && attribute.attribute_type() == &ScalarAttributeType::B
                });
                if !has_binary_key {
                    return Err(DynamoDbBackendError::InvalidConfiguration(format!(
                        "DynamoDB root registry table {} key {attribute_name} must be binary",
                        self.root_table_name
                    )));
                }
            }

            Ok(())
        }

        fn require_active(
            &self,
            table: &TableDescription,
            table_name: &str,
        ) -> Result<(), DynamoDbBackendError> {
            let status = table
                .table_status()
                .map(TableStatus::as_str)
                .unwrap_or("UNKNOWN");
            if status != TableStatus::Active.as_str() {
                return Err(DynamoDbBackendError::InvalidConfiguration(format!(
                    "DynamoDB table {table_name} must be ACTIVE (status {status})"
                )));
            }
            Ok(())
        }

        async fn wait_for_table_active(
            &self,
            table_name: &str,
        ) -> Result<TableDescription, DynamoDbBackendError> {
            let started = Instant::now();
            loop {
                match self
                    .client
                    .describe_table()
                    .table_name(table_name)
                    .send()
                    .await
                {
                    Ok(mut output)
                        if output
                            .table()
                            .and_then(TableDescription::table_status)
                            .is_some_and(|status| status == &TableStatus::Active) =>
                    {
                        return output.table.take().ok_or_else(|| {
                            DynamoDbBackendError::InvalidConfiguration(format!(
                                "DynamoDB table {table_name} became ACTIVE without table metadata"
                            ))
                        });
                    }
                    Ok(_) if started.elapsed() < DYNAMODB_TABLE_READY_TIMEOUT => {
                        tokio::time::sleep(DYNAMODB_TABLE_READY_POLL_INTERVAL).await;
                    }
                    Err(err)
                        if describe_table_not_found(&err)
                            && started.elapsed() < DYNAMODB_TABLE_READY_TIMEOUT =>
                    {
                        tokio::time::sleep(DYNAMODB_TABLE_READY_POLL_INTERVAL).await;
                    }
                    Ok(output) => {
                        let status = output
                            .table()
                            .and_then(TableDescription::table_status)
                            .map(TableStatus::as_str)
                            .unwrap_or("UNKNOWN");
                        return Err(DynamoDbBackendError::InvalidConfiguration(format!(
                            "DynamoDB table {table_name} did not become ACTIVE within {} seconds (status {status})",
                            DYNAMODB_TABLE_READY_TIMEOUT.as_secs()
                        )));
                    }
                    Err(err) => return Err(DynamoDbBackendError::sdk(err)),
                }
            }
        }

        async fn get_value_by_key(
            &self,
            key: Vec<u8>,
        ) -> Result<Option<Vec<u8>>, DynamoDbBackendError> {
            let output = self
                .client
                .get_item()
                .table_name(&self.table_name)
                .key(PK_ATTR, binary_attr(key))
                .consistent_read(true)
                .projection_expression("#value")
                .expression_attribute_names("#value", VALUE_ATTR)
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;

            output
                .item()
                .map(|item| binary_value_attr(item, VALUE_ATTR))
                .transpose()
        }

        async fn get_root_value(
            &self,
            name: &[u8],
        ) -> Result<Option<Vec<u8>>, DynamoDbBackendError> {
            let output = self
                .client
                .get_item()
                .table_name(&self.root_table_name)
                .set_key(Some(self.root_key_item(name)))
                .consistent_read(true)
                .projection_expression("#value")
                .expression_attribute_names("#value", VALUE_ATTR)
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;

            output
                .item()
                .map(|item| binary_value_attr(item, VALUE_ATTR))
                .transpose()
        }

        async fn batch_get_root_values_ordered(
            &self,
            names: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, DynamoDbBackendError> {
            let mut seen = HashSet::with_capacity(names.len());
            let unique_names = names
                .iter()
                .filter(|name| seen.insert(**name))
                .map(|name| (*name).to_vec())
                .collect::<Vec<_>>();
            let chunks = stream::iter(
                unique_names
                    .chunks(DYNAMODB_BATCH_GET_LIMIT)
                    .map(<[Vec<u8>]>::to_vec),
            )
            .map(|chunk| self.batch_get_root_chunk(chunk))
            .buffer_unordered(self.batch_get_parallelism)
            .try_collect::<Vec<_>>()
            .await?;
            let found = chunks.into_iter().flatten().collect::<HashMap<_, _>>();
            Ok(names
                .iter()
                .map(|name| found.get(&self.root_sort_key(name)).cloned())
                .collect())
        }

        async fn batch_get_root_chunk(
            &self,
            names: Vec<Vec<u8>>,
        ) -> Result<HashMap<Vec<u8>, Vec<u8>>, DynamoDbBackendError> {
            let mut pending = KeysAndAttributes::builder()
                .set_keys(Some(
                    names.iter().map(|name| self.root_key_item(name)).collect(),
                ))
                .consistent_read(true)
                .projection_expression("#sk, #value")
                .expression_attribute_names("#sk", SK_ATTR)
                .expression_attribute_names("#value", VALUE_ATTR)
                .build()
                .map_err(DynamoDbBackendError::sdk)?;
            let mut found = HashMap::new();
            let mut attempts = 0;
            loop {
                let mut output = self
                    .client
                    .batch_get_item()
                    .request_items(&self.root_table_name, pending)
                    .send()
                    .await
                    .map_err(DynamoDbBackendError::sdk)?;
                if let Some(items) = output
                    .responses
                    .take()
                    .and_then(|mut responses| responses.remove(&self.root_table_name))
                {
                    for mut item in items {
                        found.insert(
                            take_binary_value_attr(&mut item, SK_ATTR)?,
                            take_binary_value_attr(&mut item, VALUE_ATTR)?,
                        );
                    }
                }
                let unprocessed = output
                    .unprocessed_keys
                    .take()
                    .and_then(|mut items| items.remove(&self.root_table_name));
                match unprocessed {
                    Some(keys) if !keys.keys().is_empty() => pending = keys,
                    _ => return Ok(found),
                }
                attempts += 1;
                if attempts >= DYNAMODB_BATCH_RETRY_LIMIT {
                    return Err(DynamoDbBackendError::UnprocessedBatch {
                        operation: "batch_get_item",
                        remaining: pending.keys().len(),
                    });
                }
                retry_backoff(attempts).await;
            }
        }

        async fn scan_primary_keys_with_prefix(
            &self,
            prefix: &[u8],
        ) -> Result<Vec<Vec<u8>>, DynamoDbBackendError> {
            let partitions = stream::iter(0..self.scan_parallelism)
                .map(|segment| self.scan_primary_keys_segment(prefix, segment))
                .buffer_unordered(self.scan_parallelism)
                .try_collect::<Vec<_>>()
                .await?;
            Ok(partitions.into_iter().flatten().collect())
        }

        async fn scan_family_page(
            &self,
            prefix: &[u8],
            cursor: Option<&[u8]>,
            evaluation_limit: usize,
            include_value: bool,
        ) -> Result<(Vec<(Vec<u8>, Option<Vec<u8>>)>, Option<Vec<u8>>), DynamoDbBackendError>
        {
            if !(1..=DYNAMODB_SCAN_PAGE_LIMIT).contains(&evaluation_limit) {
                return Err(DynamoDbBackendError::InvalidConfiguration(format!(
                    "scan page evaluation limit must be 1..={DYNAMODB_SCAN_PAGE_LIMIT}"
                )));
            }
            let physical_cursor = cursor
                .map(|cursor| self.decode_scan_cursor(cursor))
                .transpose()?;
            let projection = if include_value { "#pk, #value" } else { "#pk" };
            let mut request = self
                .client
                .scan()
                .table_name(&self.table_name)
                .consistent_read(true)
                .projection_expression(projection)
                .filter_expression("begins_with(#pk, :prefix)")
                .expression_attribute_names("#pk", PK_ATTR)
                .expression_attribute_values(":prefix", binary_attr(prefix))
                .set_exclusive_start_key(
                    physical_cursor.map(|cursor| self.key_item(cursor.to_vec())),
                )
                .limit(i32::try_from(evaluation_limit).expect("bounded scan limit fits i32"));
            if include_value {
                request = request.expression_attribute_names("#value", VALUE_ATTR);
            }
            let output = request.send().await.map_err(DynamoDbBackendError::sdk)?;
            let mut items = Vec::with_capacity(output.items().len());
            for item in output.items() {
                let key = binary_value_attr(item, PK_ATTR)?;
                let value = if include_value {
                    Some(binary_value_attr(item, VALUE_ATTR)?)
                } else {
                    None
                };
                items.push((key, value));
            }
            let next_cursor = output
                .last_evaluated_key()
                .map(|key| binary_value_attr(key, PK_ATTR))
                .transpose()?
                .map(|cursor| self.encode_scan_cursor(&cursor));
            Ok((items, next_cursor))
        }

        fn scan_cursor_binding(&self) -> prolly::Cid {
            let table_name = self.table_name.as_bytes();
            let mut identity = Vec::with_capacity(8 + table_name.len() + self.key_prefix.len());
            identity.extend_from_slice(&(table_name.len() as u64).to_be_bytes());
            identity.extend_from_slice(table_name);
            identity.extend_from_slice(&self.key_prefix);
            prolly::Cid::from_bytes(&identity)
        }

        fn encode_scan_cursor(&self, physical_key: &[u8]) -> Vec<u8> {
            let mut cursor = Vec::with_capacity(
                DYNAMODB_SCAN_CURSOR_MAGIC.len()
                    + DYNAMODB_SCAN_CURSOR_BINDING_BYTES
                    + physical_key.len(),
            );
            cursor.extend_from_slice(DYNAMODB_SCAN_CURSOR_MAGIC);
            cursor.extend_from_slice(self.scan_cursor_binding().as_bytes());
            cursor.extend_from_slice(physical_key);
            cursor
        }

        fn decode_scan_cursor<'a>(
            &self,
            cursor: &'a [u8],
        ) -> Result<&'a [u8], DynamoDbBackendError> {
            let envelope_bytes =
                DYNAMODB_SCAN_CURSOR_MAGIC.len() + DYNAMODB_SCAN_CURSOR_BINDING_BYTES;
            if cursor.len() <= envelope_bytes
                || &cursor[..DYNAMODB_SCAN_CURSOR_MAGIC.len()] != DYNAMODB_SCAN_CURSOR_MAGIC
            {
                return Err(DynamoDbBackendError::InvalidConfiguration(
                    "scan page cursor has an invalid envelope".into(),
                ));
            }
            let binding = self.scan_cursor_binding();
            if &cursor[DYNAMODB_SCAN_CURSOR_MAGIC.len()..envelope_bytes] != binding.as_bytes() {
                return Err(DynamoDbBackendError::InvalidConfiguration(
                    "scan page cursor is outside this backend namespace".into(),
                ));
            }
            Ok(&cursor[envelope_bytes..])
        }

        async fn scan_primary_keys_segment(
            &self,
            prefix: &[u8],
            segment: usize,
        ) -> Result<Vec<Vec<u8>>, DynamoDbBackendError> {
            let mut start_key = None;
            let mut keys = Vec::new();

            loop {
                let output = self
                    .client
                    .scan()
                    .table_name(&self.table_name)
                    .consistent_read(true)
                    .projection_expression("#pk")
                    .filter_expression("begins_with(#pk, :prefix)")
                    .expression_attribute_names("#pk", PK_ATTR)
                    .expression_attribute_values(":prefix", binary_attr(prefix))
                    .total_segments(self.scan_parallelism as i32)
                    .segment(segment as i32)
                    .set_exclusive_start_key(start_key)
                    .send()
                    .await
                    .map_err(DynamoDbBackendError::sdk)?;

                for item in output.items() {
                    keys.push(binary_value_attr(item, PK_ATTR)?);
                }

                start_key = output.last_evaluated_key().cloned();
                if start_key.is_none() {
                    break;
                }
            }

            Ok(keys)
        }

        async fn root_keys(
            &self,
        ) -> Result<Vec<HashMap<String, AttributeValue>>, DynamoDbBackendError> {
            let mut start_key = None;
            let mut keys = Vec::new();

            loop {
                let output = self
                    .client
                    .query()
                    .table_name(&self.root_table_name)
                    .consistent_read(true)
                    .key_condition_expression("#pk = :pk")
                    .projection_expression("#sk")
                    .expression_attribute_names("#pk", PK_ATTR)
                    .expression_attribute_names("#sk", SK_ATTR)
                    .expression_attribute_values(":pk", binary_attr(self.root_partition_key()))
                    .set_exclusive_start_key(start_key)
                    .send()
                    .await
                    .map_err(DynamoDbBackendError::sdk)?;

                for item in output.items() {
                    let sort_key = binary_value_attr(item, SK_ATTR)?;
                    let name = sort_key
                        .strip_prefix(ROOT_ENTRY_PREFIX)
                        .ok_or(DynamoDbBackendError::UnexpectedAttribute(SK_ATTR))?;
                    keys.push(self.root_key_item(name));
                }
                start_key = output.last_evaluated_key().cloned();
                if start_key.is_none() {
                    break;
                }
            }

            Ok(keys)
        }

        async fn query_roots(&self) -> Result<Vec<RemoteNamedRoot>, DynamoDbBackendError> {
            let mut start_key = None;
            let mut roots = Vec::new();

            loop {
                let output = self
                    .client
                    .query()
                    .table_name(&self.root_table_name)
                    .consistent_read(true)
                    .key_condition_expression("#pk = :pk AND begins_with(#sk, :entry)")
                    .projection_expression("#sk, #value")
                    .expression_attribute_names("#pk", PK_ATTR)
                    .expression_attribute_names("#sk", SK_ATTR)
                    .expression_attribute_names("#value", VALUE_ATTR)
                    .expression_attribute_values(":pk", binary_attr(self.root_partition_key()))
                    .expression_attribute_values(":entry", binary_attr(ROOT_ENTRY_PREFIX))
                    .set_exclusive_start_key(start_key)
                    .send()
                    .await
                    .map_err(DynamoDbBackendError::sdk)?;

                for item in output.items() {
                    let sort_key = binary_value_attr(item, SK_ATTR)?;
                    let name = sort_key
                        .strip_prefix(ROOT_ENTRY_PREFIX)
                        .ok_or(DynamoDbBackendError::UnexpectedAttribute(SK_ATTR))?;
                    roots.push(RemoteNamedRoot::new(
                        name.to_vec(),
                        binary_value_attr(item, VALUE_ATTR)?,
                    ));
                }
                start_key = output.last_evaluated_key().cloned();
                if start_key.is_none() {
                    break;
                }
            }

            Ok(roots)
        }

        async fn query_roots_page(
            &self,
            prefix: &[u8],
            after: Option<&[u8]>,
            limit: usize,
        ) -> Result<RemoteNamedRootPage, DynamoDbBackendError> {
            if limit == 0 {
                return Ok(RemoteNamedRootPage::default());
            }
            if after.is_some_and(|after| !after.starts_with(prefix)) {
                return Err(DynamoDbBackendError::InvalidConfiguration(
                    "root page cursor is outside the requested prefix".into(),
                ));
            }
            let mut entry_prefix = Vec::with_capacity(ROOT_ENTRY_PREFIX.len() + prefix.len());
            entry_prefix.extend_from_slice(ROOT_ENTRY_PREFIX);
            entry_prefix.extend_from_slice(prefix);
            let request_limit = i32::try_from(limit.saturating_add(1)).unwrap_or(i32::MAX);
            let output = self
                .client
                .query()
                .table_name(&self.root_table_name)
                .consistent_read(true)
                .key_condition_expression("#pk = :pk AND begins_with(#sk, :entry)")
                .projection_expression("#sk, #value")
                .expression_attribute_names("#pk", PK_ATTR)
                .expression_attribute_names("#sk", SK_ATTR)
                .expression_attribute_names("#value", VALUE_ATTR)
                .expression_attribute_values(":pk", binary_attr(self.root_partition_key()))
                .expression_attribute_values(":entry", binary_attr(entry_prefix))
                .set_exclusive_start_key(after.map(|name| self.root_key_item(name)))
                .limit(request_limit)
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;

            let provider_has_more = output.last_evaluated_key().is_some();
            let mut roots = Vec::with_capacity(output.items().len().min(limit));
            for item in output.items() {
                let sort_key = binary_value_attr(item, SK_ATTR)?;
                let name = sort_key
                    .strip_prefix(ROOT_ENTRY_PREFIX)
                    .ok_or(DynamoDbBackendError::UnexpectedAttribute(SK_ATTR))?;
                roots.push(RemoteNamedRoot::new(
                    name.to_vec(),
                    binary_value_attr(item, VALUE_ATTR)?,
                ));
            }
            let over_limit = roots.len() > limit;
            if over_limit {
                roots.pop();
            }
            let has_more = over_limit || provider_has_more;
            let next_after = if has_more {
                Some(
                    roots
                        .last()
                        .ok_or_else(|| {
                            DynamoDbBackendError::InvalidConfiguration(
                                "DynamoDB returned an empty continued root page".into(),
                            )
                        })?
                        .name
                        .clone(),
                )
            } else {
                None
            };
            Ok(RemoteNamedRootPage { roots, next_after })
        }

        fn put_write_request(
            &self,
            key: Vec<u8>,
            value: &[u8],
        ) -> Result<WriteRequest, DynamoDbBackendError> {
            Ok(WriteRequest::builder()
                .put_request(
                    PutRequest::builder()
                        .set_item(Some(self.item(key, value)))
                        .build()
                        .map_err(DynamoDbBackendError::sdk)?,
                )
                .build())
        }

        fn delete_write_request(&self, key: Vec<u8>) -> Result<WriteRequest, DynamoDbBackendError> {
            self.delete_item_write_request(self.key_item(key))
        }

        fn delete_item_write_request(
            &self,
            key: HashMap<String, AttributeValue>,
        ) -> Result<WriteRequest, DynamoDbBackendError> {
            Ok(WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .set_key(Some(key))
                        .build()
                        .map_err(DynamoDbBackendError::sdk)?,
                )
                .build())
        }

        async fn batch_write_requests(
            &self,
            requests: &[WriteRequest],
        ) -> Result<(), DynamoDbBackendError> {
            self.batch_write_requests_for_table(&self.table_name, requests)
                .await
        }

        async fn batch_write_requests_for_table(
            &self,
            table_name: &str,
            requests: &[WriteRequest],
        ) -> Result<(), DynamoDbBackendError> {
            stream::iter(
                requests
                    .chunks(DYNAMODB_BATCH_WRITE_LIMIT)
                    .map(<[WriteRequest]>::to_vec),
            )
            .map(|pending| self.batch_write_chunk(table_name, pending))
            .buffer_unordered(self.batch_write_parallelism)
            .try_collect::<Vec<_>>()
            .await?;
            Ok(())
        }

        async fn batch_write_chunk(
            &self,
            table_name: &str,
            mut pending: Vec<WriteRequest>,
        ) -> Result<(), DynamoDbBackendError> {
            let mut attempts = 0;
            while !pending.is_empty() {
                let output = self
                    .client
                    .batch_write_item()
                    .request_items(table_name, pending)
                    .send()
                    .await
                    .map_err(DynamoDbBackendError::sdk)?;

                pending = output
                    .unprocessed_items()
                    .and_then(|items| items.get(table_name).cloned())
                    .unwrap_or_default();
                if pending.is_empty() {
                    return Ok(());
                }

                attempts += 1;
                if attempts >= DYNAMODB_BATCH_RETRY_LIMIT {
                    return Err(DynamoDbBackendError::UnprocessedBatch {
                        operation: "batch_write_item",
                        remaining: pending.len(),
                    });
                }
                retry_backoff(attempts).await;
            }
            Ok(())
        }

        async fn batch_get_values(
            &self,
            keys: &[Vec<u8>],
        ) -> Result<HashMap<Vec<u8>, Vec<u8>>, DynamoDbBackendError> {
            self.batch_get_values_from(&self.table_name, keys).await
        }

        async fn batch_get_values_from(
            &self,
            table_name: &str,
            keys: &[Vec<u8>],
        ) -> Result<HashMap<Vec<u8>, Vec<u8>>, DynamoDbBackendError> {
            let chunks = stream::iter(
                keys.chunks(DYNAMODB_BATCH_GET_LIMIT)
                    .map(<[Vec<u8>]>::to_vec),
            )
            .map(|chunk| self.batch_get_chunk_from(table_name, chunk))
            .buffer_unordered(self.batch_get_parallelism)
            .try_collect::<Vec<_>>()
            .await?;

            Ok(chunks.into_iter().flatten().collect())
        }

        async fn batch_get_chunk_from(
            &self,
            table_name: &str,
            keys: Vec<Vec<u8>>,
        ) -> Result<HashMap<Vec<u8>, Vec<u8>>, DynamoDbBackendError> {
            let mut pending = KeysAndAttributes::builder()
                .set_keys(Some(
                    keys.into_iter()
                        .map(|key| self.key_item(key))
                        .collect::<Vec<_>>(),
                ))
                .consistent_read(true)
                .projection_expression("#pk, #value")
                .expression_attribute_names("#pk", PK_ATTR)
                .expression_attribute_names("#value", VALUE_ATTR)
                .build()
                .map_err(DynamoDbBackendError::sdk)?;
            let mut found = HashMap::new();
            let mut attempts = 0;

            loop {
                let mut output = self
                    .client
                    .batch_get_item()
                    .request_items(table_name, pending)
                    .send()
                    .await
                    .map_err(DynamoDbBackendError::sdk)?;

                if let Some(items) = output
                    .responses
                    .take()
                    .and_then(|mut responses| responses.remove(table_name))
                {
                    for mut item in items {
                        found.insert(
                            take_binary_value_attr(&mut item, PK_ATTR)?,
                            take_binary_value_attr(&mut item, VALUE_ATTR)?,
                        );
                    }
                }

                let unprocessed = output
                    .unprocessed_keys
                    .take()
                    .and_then(|mut items| items.remove(table_name));
                match unprocessed {
                    Some(keys) if !keys.keys().is_empty() => pending = keys,
                    _ => return Ok(found),
                }

                attempts += 1;
                if attempts >= DYNAMODB_BATCH_RETRY_LIMIT {
                    return Err(DynamoDbBackendError::UnprocessedBatch {
                        operation: "batch_get_item",
                        remaining: pending.keys().len(),
                    });
                }
                retry_backoff(attempts).await;
            }
        }

        fn condition_check_item(
            &self,
            condition: &RemoteRootCondition,
        ) -> Result<TransactWriteItem, DynamoDbBackendError> {
            let check = self
                .apply_root_condition(
                    ConditionCheck::builder()
                        .table_name(&self.root_table_name)
                        .set_key(Some(self.root_key_item(&condition.name))),
                    condition.expected.as_deref(),
                )
                .build()
                .map_err(DynamoDbBackendError::sdk)?;
            Ok(TransactWriteItem::builder().condition_check(check).build())
        }

        fn root_put_item(
            &self,
            name: &[u8],
            manifest: &[u8],
            expected: Option<Option<&[u8]>>,
        ) -> Result<TransactWriteItem, DynamoDbBackendError> {
            let mut builder = TransactPut::builder()
                .table_name(&self.root_table_name)
                .set_item(Some(self.root_item(name, manifest)));
            if let Some(expected) = expected {
                builder = self.apply_root_condition(builder, expected);
            }
            let put = builder.build().map_err(DynamoDbBackendError::sdk)?;
            Ok(TransactWriteItem::builder().put(put).build())
        }

        fn root_delete_item(
            &self,
            name: &[u8],
            expected: Option<Option<&[u8]>>,
        ) -> Result<TransactWriteItem, DynamoDbBackendError> {
            let mut builder = TransactDelete::builder()
                .table_name(&self.root_table_name)
                .set_key(Some(self.root_key_item(name)));
            if let Some(expected) = expected {
                builder = self.apply_root_condition(builder, expected);
            }
            let delete = builder.build().map_err(DynamoDbBackendError::sdk)?;
            Ok(TransactWriteItem::builder().delete(delete).build())
        }

        fn node_put_item(
            &self,
            key: &[u8],
            value: &[u8],
        ) -> Result<TransactWriteItem, DynamoDbBackendError> {
            let put = TransactPut::builder()
                .table_name(&self.table_name)
                .set_item(Some(self.item(self.node_key(key), value)))
                .build()
                .map_err(DynamoDbBackendError::sdk)?;
            Ok(TransactWriteItem::builder().put(put).build())
        }

        fn node_delete_item(&self, key: &[u8]) -> Result<TransactWriteItem, DynamoDbBackendError> {
            let delete = TransactDelete::builder()
                .table_name(&self.table_name)
                .set_key(Some(self.key_item(self.node_key(key))))
                .build()
                .map_err(DynamoDbBackendError::sdk)?;
            Ok(TransactWriteItem::builder().delete(delete).build())
        }

        fn apply_root_condition<B>(&self, builder: B, expected: Option<&[u8]>) -> B
        where
            B: RootConditionBuilder,
        {
            match expected {
                Some(expected) => builder
                    .condition_expression("#value = :expected")
                    .expression_attribute_names("#value", VALUE_ATTR)
                    .expression_attribute_values(":expected", binary_attr(expected)),
                None => builder
                    .condition_expression("attribute_not_exists(#pk)")
                    .expression_attribute_names("#pk", PK_ATTR),
            }
            .return_values_on_condition_check_failure(ReturnValuesOnConditionCheckFailure::AllOld)
        }
    }

    impl prolly::AsyncBlobStore for DynamoDbBlobStore {
        type Error = DynamoDbBackendError;

        async fn get_blob(
            &self,
            reference: &prolly::BlobRef,
        ) -> Result<Option<Vec<u8>>, Self::Error> {
            let manifest_key = self.backend.blob_manifest_key(&reference.cid);
            let Some(manifest) = self.backend.get_value_by_key(manifest_key).await? else {
                return Ok(None);
            };
            let (len, chunk_size, chunk_count) = decode_blob_manifest(&manifest)?;
            if len != reference.len {
                return Err(DynamoDbBackendError::InvalidBlobManifest(format!(
                    "manifest length {len} does not match reference length {} for {:?}",
                    reference.len, reference.cid
                )));
            }
            let expected_count = blob_chunk_count(len, chunk_size)?;
            if chunk_count != expected_count {
                return Err(DynamoDbBackendError::InvalidBlobManifest(format!(
                    "manifest chunk count {chunk_count} does not match expected {expected_count} for {:?}",
                    reference.cid
                )));
            }

            let keys = (0..chunk_count)
                .map(|index| {
                    self.backend
                        .blob_chunk_key(&reference.cid, chunk_size, index)
                })
                .collect::<Vec<_>>();
            let found = self.backend.batch_get_values(&keys).await?;
            let capacity = usize::try_from(len).map_err(|_| {
                DynamoDbBackendError::InvalidBlobManifest(
                    "blob length exceeds this platform's address space".to_string(),
                )
            })?;
            let mut bytes = Vec::with_capacity(capacity);
            for (index, key) in keys.iter().enumerate() {
                let chunk =
                    found
                        .get(key)
                        .ok_or_else(|| DynamoDbBackendError::MissingBlobChunk {
                            cid: reference.cid.clone(),
                            index: index as u32,
                        })?;
                let is_last = index + 1 == keys.len();
                if (!is_last && chunk.len() != chunk_size as usize)
                    || (is_last && chunk.len() > chunk_size as usize)
                {
                    return Err(DynamoDbBackendError::InvalidBlobManifest(format!(
                        "invalid chunk length {} at index {index} for {:?}",
                        chunk.len(),
                        reference.cid
                    )));
                }
                bytes.extend_from_slice(chunk);
            }
            if bytes.len() as u64 != len {
                return Err(DynamoDbBackendError::InvalidBlobManifest(format!(
                    "assembled length {} does not match manifest length {len} for {:?}",
                    bytes.len(),
                    reference.cid
                )));
            }
            reference
                .validate_bytes(&bytes)
                .map_err(|error| DynamoDbBackendError::InvalidBlobManifest(error.to_string()))?;
            Ok(Some(bytes))
        }

        async fn put_blob(&self, bytes: &[u8]) -> Result<prolly::BlobRef, Self::Error> {
            let reference = prolly::BlobRef::from_bytes(bytes);
            let chunk_count =
                u32::try_from(bytes.len().div_ceil(self.chunk_size)).map_err(|_| {
                    DynamoDbBackendError::InvalidBlobManifest(
                        "blob requires more than u32::MAX chunks".to_string(),
                    )
                })?;
            let requests = bytes
                .chunks(self.chunk_size)
                .enumerate()
                .map(|(index, chunk)| {
                    let index = u32::try_from(index).map_err(|_| {
                        DynamoDbBackendError::InvalidBlobManifest(
                            "blob chunk index exceeds u32::MAX".to_string(),
                        )
                    })?;
                    self.backend.put_write_request(
                        self.backend
                            .blob_chunk_key(&reference.cid, self.chunk_size as u32, index),
                        chunk,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.backend.batch_write_requests(&requests).await?;

            let manifest = encode_blob_manifest(reference.len, self.chunk_size as u32, chunk_count);
            self.backend
                .client
                .put_item()
                .table_name(&self.backend.table_name)
                .set_item(Some(
                    self.backend
                        .item(self.backend.blob_manifest_key(&reference.cid), &manifest),
                ))
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;
            Ok(reference)
        }

        async fn delete_blob(&self, reference: &prolly::BlobRef) -> Result<(), Self::Error> {
            let manifest_key = self.backend.blob_manifest_key(&reference.cid);
            let Some(manifest) = self.backend.get_value_by_key(manifest_key.clone()).await? else {
                return Ok(());
            };
            let (len, chunk_size, chunk_count) = decode_blob_manifest(&manifest)?;
            if len != reference.len || blob_chunk_count(len, chunk_size)? != chunk_count {
                return Err(DynamoDbBackendError::InvalidBlobManifest(format!(
                    "blob manifest does not match reference {:?}",
                    reference.cid
                )));
            }

            // Remove the visibility marker first. A failed cleanup leaves only
            // unreachable chunks and can be safely retried by namespace GC.
            self.backend
                .client
                .delete_item()
                .table_name(&self.backend.table_name)
                .set_key(Some(self.backend.key_item(manifest_key)))
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;
            let requests = (0..chunk_count)
                .map(|index| {
                    self.backend
                        .delete_write_request(self.backend.blob_chunk_key(
                            &reference.cid,
                            chunk_size,
                            index,
                        ))
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.backend.batch_write_requests(&requests).await
        }

        fn read_parallelism(&self) -> usize {
            self.backend.read_parallelism
        }
    }

    impl RemoteStoreBackend for DynamoDbBackend {
        type Error = DynamoDbBackendError;

        async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.get_value_by_key(self.node_key(key)).await
        }

        async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.client
                .put_item()
                .table_name(&self.table_name)
                .set_item(Some(self.item(self.node_key(key), value)))
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;
            Ok(())
        }

        async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
            self.client
                .delete_item()
                .table_name(&self.table_name)
                .set_key(Some(self.key_item(self.node_key(key))))
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;
            Ok(())
        }

        async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
            let mut latest = HashMap::<Vec<u8>, Option<&[u8]>>::with_capacity(ops.len());
            for op in ops {
                match op {
                    RemoteBatchOp::Upsert { key, value } => {
                        latest.insert(self.node_key(key), Some(value));
                    }
                    RemoteBatchOp::Delete { key } => {
                        latest.insert(self.node_key(key), None);
                    }
                }
            }

            let requests = latest
                .into_iter()
                .map(|(key, value)| match value {
                    Some(value) => self.put_write_request(key, value),
                    None => self.delete_write_request(key),
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.batch_write_requests(&requests).await
        }

        async fn batch_get_nodes_ordered(
            &self,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            let dynamo_keys = keys
                .iter()
                .map(|key| self.node_key(key))
                .collect::<Vec<_>>();
            let mut seen = HashSet::with_capacity(dynamo_keys.len());
            let mut unique_keys = Vec::with_capacity(dynamo_keys.len());
            for dynamo_key in &dynamo_keys {
                if seen.insert(dynamo_key.clone()) {
                    unique_keys.push(dynamo_key.clone());
                }
            }

            let found = self.batch_get_values(&unique_keys).await?;

            Ok(dynamo_keys
                .iter()
                .map(|key| found.get(key).cloned())
                .collect())
        }

        async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            let mut latest = HashMap::<Vec<u8>, &[u8]>::with_capacity(entries.len());
            for (key, value) in entries {
                latest.insert(self.node_key(key), value);
            }
            let requests = latest
                .into_iter()
                .map(|(key, value)| self.put_write_request(key, value))
                .collect::<Result<Vec<_>, _>>()?;
            self.batch_write_requests(&requests).await
        }

        async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
            let prefix = self.family_prefix(NODE_FAMILY);
            let mut cids = self
                .scan_primary_keys_with_prefix(&prefix)
                .await?
                .into_iter()
                .filter_map(|key| {
                    key.strip_prefix(prefix.as_slice())
                        .filter(|cid| cid.len() == 32)
                        .map(<[u8]>::to_vec)
                })
                .collect::<Vec<_>>();
            cids.sort();
            Ok(cids)
        }

        fn read_parallelism(&self) -> usize {
            self.read_parallelism
        }

        fn prefers_batch_reads(&self) -> bool {
            true
        }

        fn guarantees_durable_publication(&self) -> bool {
            // DynamoDB acknowledges successful writes only after durable
            // persistence. The batch path retries exactly the returned
            // UnprocessedItems and fails unless that set becomes empty.
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
            self.get_value_by_key(self.hint_key(namespace, key)).await
        }

        async fn put_hint(
            &self,
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            self.client
                .put_item()
                .table_name(&self.table_name)
                .set_item(Some(self.item(self.hint_key(namespace, key), value)))
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;
            Ok(())
        }

        async fn batch_put_nodes_with_hint(
            &self,
            entries: &[(&[u8], &[u8])],
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            let mut latest = HashMap::<Vec<u8>, &[u8]>::with_capacity(entries.len() + 1);
            for (key, value) in entries {
                latest.insert(self.node_key(key), value);
            }
            latest.insert(self.hint_key(namespace, key), value);
            let requests = latest
                .into_iter()
                .map(|(key, value)| self.put_write_request(key, value))
                .collect::<Result<Vec<_>, _>>()?;
            self.batch_write_requests(&requests).await
        }

        async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.get_root_value(name).await
        }

        async fn get_root_manifests_ordered(
            &self,
            names: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            self.batch_get_root_values_ordered(names).await
        }

        async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
            self.client
                .put_item()
                .table_name(&self.root_table_name)
                .set_item(Some(self.root_item(name, manifest)))
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;
            Ok(())
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            self.client
                .delete_item()
                .table_name(&self.root_table_name)
                .set_key(Some(self.root_key_item(name)))
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;
            Ok(())
        }

        async fn compare_and_swap_root_manifest(
            &self,
            name: &[u8],
            expected: Option<&[u8]>,
            new: Option<&[u8]>,
        ) -> Result<RemoteManifestUpdate, Self::Error> {
            let result = match new {
                Some(manifest) => {
                    let mut request = self
                        .client
                        .put_item()
                        .table_name(&self.root_table_name)
                        .set_item(Some(self.root_item(name, manifest)))
                        .return_values_on_condition_check_failure(
                            ReturnValuesOnConditionCheckFailure::AllOld,
                        );
                    request = match expected {
                        Some(expected) => request
                            .condition_expression("#value = :expected")
                            .expression_attribute_names("#value", VALUE_ATTR)
                            .expression_attribute_values(":expected", binary_attr(expected)),
                        None => request
                            .condition_expression("attribute_not_exists(#pk)")
                            .expression_attribute_names("#pk", PK_ATTR),
                    };
                    request
                        .send()
                        .await
                        .map(|_| ())
                        .map_err(DynamoDbCasError::Put)
                }
                None => {
                    let mut request = self
                        .client
                        .delete_item()
                        .table_name(&self.root_table_name)
                        .set_key(Some(self.root_key_item(name)))
                        .return_values_on_condition_check_failure(
                            ReturnValuesOnConditionCheckFailure::AllOld,
                        );
                    request = match expected {
                        Some(expected) => request
                            .condition_expression("#value = :expected")
                            .expression_attribute_names("#value", VALUE_ATTR)
                            .expression_attribute_values(":expected", binary_attr(expected)),
                        None => request
                            .condition_expression("attribute_not_exists(#pk)")
                            .expression_attribute_names("#pk", PK_ATTR),
                    };
                    request
                        .send()
                        .await
                        .map(|_| ())
                        .map_err(DynamoDbCasError::Delete)
                }
            };

            match result {
                Ok(()) => Ok(RemoteManifestUpdate::Applied),
                Err(err) if err.is_condition_failed() => {
                    let current = err.condition_failure_value()?;
                    Ok(RemoteManifestUpdate::Conflict { current })
                }
                Err(err) => Err(DynamoDbBackendError::sdk(err)),
            }
        }

        async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error> {
            let mut roots = self.query_roots().await?;
            roots.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(roots)
        }

        async fn list_root_manifests_page(
            &self,
            prefix: &[u8],
            after: Option<&[u8]>,
            limit: usize,
        ) -> Result<RemoteNamedRootPage, Self::Error> {
            self.query_roots_page(prefix, after, limit).await
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
            let mut items = Vec::new();
            let conditions_by_name = root_conditions
                .iter()
                .map(|condition| (condition.name.as_slice(), condition))
                .collect::<HashMap<_, _>>();
            let written_roots = root_writes
                .iter()
                .map(|write| match write {
                    RemoteRootWrite::Put { name, .. } | RemoteRootWrite::Delete { name } => {
                        name.as_slice()
                    }
                })
                .collect::<HashSet<_>>();

            for condition in root_conditions {
                if !written_roots.contains(condition.name.as_slice()) {
                    items.push(self.condition_check_item(condition)?);
                }
            }
            for write in root_writes {
                match write {
                    RemoteRootWrite::Put { name, manifest } => {
                        let expected = conditions_by_name
                            .get(name.as_slice())
                            .map(|condition| condition.expected.as_deref());
                        items.push(self.root_put_item(name, manifest, expected)?);
                    }
                    RemoteRootWrite::Delete { name } => {
                        let expected = conditions_by_name
                            .get(name.as_slice())
                            .map(|condition| condition.expected.as_deref());
                        items.push(self.root_delete_item(name, expected)?);
                    }
                }
            }
            if items.len() > DYNAMODB_TRANSACTION_WRITE_LIMIT {
                return Err(DynamoDbBackendError::TransactionTooLarge {
                    items: items.len(),
                    limit: DYNAMODB_TRANSACTION_WRITE_LIMIT,
                });
            }
            match self.transaction_publication_mode {
                TransactionPublicationMode::PrepublishImmutableNodes => {
                    if node_writes
                        .iter()
                        .any(|write| matches!(write, RemoteBatchOp::Delete { .. }))
                    {
                        return Err(DynamoDbBackendError::StagedNodeDeleteUnsupported);
                    }
                    // RemoteProllyStore verifies every upsert's CID before this
                    // call. Publishing immutable content first is safe: a root
                    // conflict can create only unreachable nodes, reclaimed by
                    // the normal retention-aware GC flow.
                    self.batch_nodes(node_writes).await?;
                }
                TransactionPublicationMode::AtomicNodesAndRoots => {
                    for write in node_writes {
                        match write {
                            RemoteBatchOp::Upsert { key, value } => {
                                items.push(self.node_put_item(key, value)?);
                            }
                            RemoteBatchOp::Delete { key } => {
                                items.push(self.node_delete_item(key)?);
                            }
                        }
                    }
                }
            }

            if items.len() > DYNAMODB_TRANSACTION_WRITE_LIMIT {
                return Err(DynamoDbBackendError::TransactionTooLarge {
                    items: items.len(),
                    limit: DYNAMODB_TRANSACTION_WRITE_LIMIT,
                });
            }
            if items.is_empty() {
                return Ok(RemoteTransactionUpdate::Applied);
            }

            let transaction_token = transaction_token(
                &self.table_name,
                &self.root_table_name,
                &self.key_prefix,
                node_writes,
                root_conditions,
                root_writes,
                self.transaction_publication_mode,
            );
            let mut ambiguous_attempts = 0;
            let result = loop {
                let result = self
                    .client
                    .transact_write_items()
                    .set_transact_items(Some(items.clone()))
                    .client_request_token(&transaction_token)
                    .send()
                    .await;
                match &result {
                    Err(error)
                        if transaction_failure_kind(error) != TransactionFailureKind::Terminal
                            && ambiguous_attempts + 1 < DYNAMODB_AMBIGUOUS_RETRY_LIMIT =>
                    {
                        ambiguous_attempts += 1;
                        retry_backoff(ambiguous_attempts).await;
                    }
                    _ => break result,
                }
            };

            match result {
                Ok(_) => Ok(RemoteTransactionUpdate::Applied),
                Err(err)
                    if err
                        .as_service_error()
                        .is_some_and(|err| err.is_transaction_canceled_exception()) =>
                {
                    let conditions = root_conditions.to_vec();
                    let current_roots = stream::iter(conditions)
                        .map(|condition| async move {
                            Ok::<_, DynamoDbBackendError>((
                                condition.name.clone(),
                                self.get_root_value(&condition.name).await?,
                            ))
                        })
                        .buffer_unordered(self.batch_get_parallelism)
                        .try_collect::<HashMap<_, _>>()
                        .await?;
                    for condition in root_conditions {
                        let current = current_roots
                            .get(condition.name.as_slice())
                            .cloned()
                            .flatten();
                        if current != condition.expected {
                            return Ok(RemoteTransactionUpdate::Conflict(
                                RemoteTransactionConflict::new(
                                    condition.name.clone(),
                                    condition.expected.clone(),
                                    current,
                                ),
                            ));
                        }
                    }
                    Err(DynamoDbBackendError::sdk(err))
                }
                Err(err) if transaction_failure_kind(&err) == TransactionFailureKind::Ambiguous => {
                    Err(DynamoDbBackendError::OutcomeUnknown {
                        token: transaction_token,
                        source: err.to_string(),
                    })
                }
                Err(err) if transaction_failure_kind(&err) == TransactionFailureKind::Retryable => {
                    Err(DynamoDbBackendError::RetryableTransaction {
                        token: transaction_token,
                        source: err.to_string(),
                    })
                }
                Err(err) => Err(DynamoDbBackendError::sdk(err)),
            }
        }
    }

    /// Error returned by the DynamoDB backend.
    #[derive(Debug)]
    pub enum DynamoDbBackendError {
        /// DynamoDB SDK call failed.
        Sdk(String),
        /// A required item attribute was missing.
        MissingAttribute(&'static str),
        /// An item attribute had an unexpected type.
        UnexpectedAttribute(&'static str),
        /// Backend configuration is unsafe or invalid.
        InvalidConfiguration(String),
        /// DynamoDB returned unprocessed batch entries after bounded retries.
        UnprocessedBatch {
            /// DynamoDB operation name.
            operation: &'static str,
            /// Number of keys or write requests that remained unprocessed.
            remaining: usize,
        },
        /// A single DynamoDB transaction would exceed the service item limit.
        TransactionTooLarge {
            /// Number of transaction items requested.
            items: usize,
            /// Maximum transaction items allowed by DynamoDB.
            limit: usize,
        },
        /// Immutable prepublication cannot preserve atomic node deletion.
        StagedNodeDeleteUnsupported,
        /// Requested blob chunks would not fit the provider safety envelope.
        InvalidBlobChunkSize {
            /// Requested chunk payload bytes.
            requested: usize,
            /// Maximum accepted chunk payload bytes.
            maximum: usize,
        },
        /// A durable blob manifest was malformed or inconsistent.
        InvalidBlobManifest(String),
        /// A visible blob manifest referenced an absent chunk.
        MissingBlobChunk {
            /// Content identifier of the incomplete blob.
            cid: prolly::Cid,
            /// Zero-based missing chunk index.
            index: u32,
        },
        /// Provider response did not establish whether an idempotent root
        /// transaction committed after bounded same-token reconciliation.
        OutcomeUnknown {
            /// Stable DynamoDB client request token for operator reconciliation.
            token: String,
            /// Final SDK error text.
            source: String,
        },
        /// DynamoDB explicitly rejected an idempotent root transaction before
        /// applying it, after bounded retries.
        RetryableTransaction {
            /// Stable token that a retry of the identical operation will reuse.
            token: String,
            /// Final provider error text.
            source: String,
        },
    }

    impl DynamoDbBackendError {
        fn sdk(err: impl fmt::Display) -> Self {
            Self::Sdk(err.to_string())
        }

        /// Classify whether a failed logical write may be retried without
        /// first reconciling an uncertain root publication.
        pub fn write_disposition(&self) -> WriteFailureDisposition {
            match self {
                Self::UnprocessedBatch { .. } | Self::RetryableTransaction { .. } => {
                    WriteFailureDisposition::RetryableNotApplied
                }
                Self::OutcomeUnknown { .. } => WriteFailureDisposition::OutcomeUnknown,
                _ => WriteFailureDisposition::Terminal,
            }
        }

        /// Return the deterministic transaction token when reconciliation or
        /// exact replay is available.
        pub fn transaction_token(&self) -> Option<&str> {
            match self {
                Self::OutcomeUnknown { token, .. } | Self::RetryableTransaction { token, .. } => {
                    Some(token)
                }
                _ => None,
            }
        }
    }

    impl fmt::Display for DynamoDbBackendError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Sdk(err) => write!(f, "DynamoDB SDK error: {err}"),
                Self::MissingAttribute(attribute) => {
                    write!(f, "DynamoDB item missing {attribute} attribute")
                }
                Self::UnexpectedAttribute(attribute) => {
                    write!(f, "DynamoDB item has non-binary {attribute} attribute")
                }
                Self::InvalidConfiguration(message) => f.write_str(message),
                Self::UnprocessedBatch {
                    operation,
                    remaining,
                } => write!(
                    f,
                    "DynamoDB {operation} left {remaining} entries unprocessed"
                ),
                Self::TransactionTooLarge { items, limit } => write!(
                    f,
                    "DynamoDB transaction has {items} items, exceeding the {limit} item limit"
                ),
                Self::StagedNodeDeleteUnsupported => f.write_str(
                    "DynamoDB immutable-node prepublication does not support staged node deletes",
                ),
                Self::InvalidBlobChunkSize { requested, maximum } => write!(
                    f,
                    "DynamoDB blob chunk size {requested} is outside 1..={maximum} bytes"
                ),
                Self::InvalidBlobManifest(message) => {
                    write!(f, "invalid DynamoDB blob manifest: {message}")
                }
                Self::MissingBlobChunk { cid, index } => {
                    write!(f, "DynamoDB blob {cid:?} is missing chunk {index}")
                }
                Self::OutcomeUnknown { token, source } => write!(
                    f,
                    "DynamoDB transaction outcome is ambiguous after idempotent retries (token {token}): {source}"
                ),
                Self::RetryableTransaction { token, source } => write!(
                    f,
                    "DynamoDB transaction was not applied after bounded retries (token {token}): {source}"
                ),
            }
        }
    }

    impl StdError for DynamoDbBackendError {}

    /// Safety classification for errors produced while publishing a logical
    /// write's root transaction.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum WriteFailureDisposition {
        /// The provider explicitly did not apply the transaction; exact replay
        /// is safe.
        RetryableNotApplied,
        /// The root may have advanced. Reconcile by deterministic token or
        /// observed state before retrying.
        OutcomeUnknown,
        /// Configuration, validation, corruption, or another non-retryable
        /// failure.
        Terminal,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum TransactionFailureKind {
        Retryable,
        Ambiguous,
        Terminal,
    }

    fn transaction_failure_kind(
        error: &SdkError<TransactWriteItemsError>,
    ) -> TransactionFailureKind {
        match error {
            SdkError::TimeoutError(_)
            | SdkError::DispatchFailure(_)
            | SdkError::ResponseError(_)
            | SdkError::ConstructionFailure(_) => TransactionFailureKind::Ambiguous,
            SdkError::ServiceError(context) => {
                let error = context.err();
                if error.is_internal_server_error() || error.is_transaction_in_progress_exception()
                {
                    TransactionFailureKind::Ambiguous
                } else if error.is_provisioned_throughput_exceeded_exception()
                    || error.is_request_limit_exceeded()
                {
                    TransactionFailureKind::Retryable
                } else {
                    TransactionFailureKind::Terminal
                }
            }
            _ => TransactionFailureKind::Terminal,
        }
    }

    fn describe_table_not_found(err: &SdkError<DescribeTableError>) -> bool {
        err.as_service_error()
            .is_some_and(DescribeTableError::is_resource_not_found_exception)
    }

    fn create_table_in_use(err: &SdkError<CreateTableError>) -> bool {
        err.as_service_error()
            .is_some_and(CreateTableError::is_resource_in_use_exception)
    }

    enum DynamoDbCasError {
        Put(SdkError<PutItemError>),
        Delete(SdkError<DeleteItemError>),
    }

    impl DynamoDbCasError {
        fn is_condition_failed(&self) -> bool {
            match self {
                Self::Put(err) => err
                    .as_service_error()
                    .is_some_and(PutItemError::is_conditional_check_failed_exception),
                Self::Delete(err) => err
                    .as_service_error()
                    .is_some_and(DeleteItemError::is_conditional_check_failed_exception),
            }
        }

        fn condition_failure_value(&self) -> Result<Option<Vec<u8>>, DynamoDbBackendError> {
            let item = match self {
                Self::Put(err) => match err.as_service_error() {
                    Some(PutItemError::ConditionalCheckFailedException(failure)) => failure.item(),
                    _ => None,
                },
                Self::Delete(err) => match err.as_service_error() {
                    Some(DeleteItemError::ConditionalCheckFailedException(failure)) => {
                        failure.item()
                    }
                    _ => None,
                },
            };
            item.map(|item| binary_value_attr(item, VALUE_ATTR))
                .transpose()
        }
    }

    impl fmt::Display for DynamoDbCasError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Put(err) => write!(f, "{err}"),
                Self::Delete(err) => write!(f, "{err}"),
            }
        }
    }

    fn binary_attr(bytes: impl Into<Vec<u8>>) -> AttributeValue {
        AttributeValue::B(Blob::new(bytes))
    }

    trait RootConditionBuilder: Sized {
        fn condition_expression(self, input: impl Into<String>) -> Self;
        fn expression_attribute_names(
            self,
            key: impl Into<String>,
            value: impl Into<String>,
        ) -> Self;
        fn expression_attribute_values(self, key: impl Into<String>, value: AttributeValue)
            -> Self;
        fn return_values_on_condition_check_failure(
            self,
            input: ReturnValuesOnConditionCheckFailure,
        ) -> Self;
    }

    impl RootConditionBuilder for aws_sdk_dynamodb::types::builders::ConditionCheckBuilder {
        fn condition_expression(self, input: impl Into<String>) -> Self {
            aws_sdk_dynamodb::types::builders::ConditionCheckBuilder::condition_expression(
                self, input,
            )
        }

        fn expression_attribute_names(
            self,
            key: impl Into<String>,
            value: impl Into<String>,
        ) -> Self {
            aws_sdk_dynamodb::types::builders::ConditionCheckBuilder::expression_attribute_names(
                self, key, value,
            )
        }

        fn expression_attribute_values(
            self,
            key: impl Into<String>,
            value: AttributeValue,
        ) -> Self {
            aws_sdk_dynamodb::types::builders::ConditionCheckBuilder::expression_attribute_values(
                self, key, value,
            )
        }

        fn return_values_on_condition_check_failure(
            self,
            input: ReturnValuesOnConditionCheckFailure,
        ) -> Self {
            aws_sdk_dynamodb::types::builders::ConditionCheckBuilder::return_values_on_condition_check_failure(self, input)
        }
    }

    impl RootConditionBuilder for aws_sdk_dynamodb::types::builders::PutBuilder {
        fn condition_expression(self, input: impl Into<String>) -> Self {
            aws_sdk_dynamodb::types::builders::PutBuilder::condition_expression(self, input)
        }

        fn expression_attribute_names(
            self,
            key: impl Into<String>,
            value: impl Into<String>,
        ) -> Self {
            aws_sdk_dynamodb::types::builders::PutBuilder::expression_attribute_names(
                self, key, value,
            )
        }

        fn expression_attribute_values(
            self,
            key: impl Into<String>,
            value: AttributeValue,
        ) -> Self {
            aws_sdk_dynamodb::types::builders::PutBuilder::expression_attribute_values(
                self, key, value,
            )
        }

        fn return_values_on_condition_check_failure(
            self,
            input: ReturnValuesOnConditionCheckFailure,
        ) -> Self {
            aws_sdk_dynamodb::types::builders::PutBuilder::return_values_on_condition_check_failure(
                self, input,
            )
        }
    }

    impl RootConditionBuilder for aws_sdk_dynamodb::types::builders::DeleteBuilder {
        fn condition_expression(self, input: impl Into<String>) -> Self {
            aws_sdk_dynamodb::types::builders::DeleteBuilder::condition_expression(self, input)
        }

        fn expression_attribute_names(
            self,
            key: impl Into<String>,
            value: impl Into<String>,
        ) -> Self {
            aws_sdk_dynamodb::types::builders::DeleteBuilder::expression_attribute_names(
                self, key, value,
            )
        }

        fn expression_attribute_values(
            self,
            key: impl Into<String>,
            value: AttributeValue,
        ) -> Self {
            aws_sdk_dynamodb::types::builders::DeleteBuilder::expression_attribute_values(
                self, key, value,
            )
        }

        fn return_values_on_condition_check_failure(
            self,
            input: ReturnValuesOnConditionCheckFailure,
        ) -> Self {
            aws_sdk_dynamodb::types::builders::DeleteBuilder::return_values_on_condition_check_failure(
                self, input,
            )
        }
    }

    fn binary_value_attr(
        item: &HashMap<String, AttributeValue>,
        attribute: &'static str,
    ) -> Result<Vec<u8>, DynamoDbBackendError> {
        let value = item
            .get(attribute)
            .ok_or(DynamoDbBackendError::MissingAttribute(attribute))?;
        let blob = value
            .as_b()
            .map_err(|_| DynamoDbBackendError::UnexpectedAttribute(attribute))?;
        Ok(blob.as_ref().to_vec())
    }

    fn take_binary_value_attr(
        item: &mut HashMap<String, AttributeValue>,
        attribute: &'static str,
    ) -> Result<Vec<u8>, DynamoDbBackendError> {
        let value = item
            .remove(attribute)
            .ok_or(DynamoDbBackendError::MissingAttribute(attribute))?;
        match value {
            AttributeValue::B(blob) => Ok(blob.into_inner()),
            _ => Err(DynamoDbBackendError::UnexpectedAttribute(attribute)),
        }
    }

    async fn retry_backoff(attempt: usize) {
        let exponent = attempt.saturating_sub(1).min(7) as u32;
        tokio::time::sleep(Duration::from_millis(
            DYNAMODB_BATCH_RETRY_BASE_DELAY_MS.saturating_mul(1_u64 << exponent),
        ))
        .await;
    }

    fn encode_blob_manifest(len: u64, chunk_size: u32, chunk_count: u32) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(BLOB_MANIFEST_BYTES);
        bytes.extend_from_slice(BLOB_MANIFEST_MAGIC);
        bytes.push(BLOB_MANIFEST_VERSION);
        bytes.extend_from_slice(&len.to_be_bytes());
        bytes.extend_from_slice(&chunk_size.to_be_bytes());
        bytes.extend_from_slice(&chunk_count.to_be_bytes());
        bytes
    }

    fn decode_blob_manifest(bytes: &[u8]) -> Result<(u64, u32, u32), DynamoDbBackendError> {
        if bytes.len() != BLOB_MANIFEST_BYTES
            || &bytes[..BLOB_MANIFEST_MAGIC.len()] != BLOB_MANIFEST_MAGIC
            || bytes[BLOB_MANIFEST_MAGIC.len()] != BLOB_MANIFEST_VERSION
        {
            return Err(DynamoDbBackendError::InvalidBlobManifest(
                "unsupported or malformed envelope".to_string(),
            ));
        }
        let mut offset = BLOB_MANIFEST_MAGIC.len() + 1;
        let len = u64::from_be_bytes(
            bytes
                .get(offset..offset + 8)
                .and_then(|value| value.try_into().ok())
                .ok_or_else(|| {
                    DynamoDbBackendError::InvalidBlobManifest("truncated length field".to_string())
                })?,
        );
        offset += 8;
        let chunk_size = u32::from_be_bytes(
            bytes
                .get(offset..offset + 4)
                .and_then(|value| value.try_into().ok())
                .ok_or_else(|| {
                    DynamoDbBackendError::InvalidBlobManifest(
                        "truncated chunk-size field".to_string(),
                    )
                })?,
        );
        offset += 4;
        let chunk_count = u32::from_be_bytes(
            bytes
                .get(offset..offset + 4)
                .and_then(|value| value.try_into().ok())
                .ok_or_else(|| {
                    DynamoDbBackendError::InvalidBlobManifest(
                        "truncated chunk-count field".to_string(),
                    )
                })?,
        );
        if chunk_size == 0 || chunk_size as usize > DYNAMODB_BLOB_CHUNK_BYTES {
            return Err(DynamoDbBackendError::InvalidBlobManifest(format!(
                "chunk size {chunk_size} is outside the supported range"
            )));
        }
        Ok((len, chunk_size, chunk_count))
    }

    fn blob_chunk_count(len: u64, chunk_size: u32) -> Result<u32, DynamoDbBackendError> {
        let count = len.div_ceil(u64::from(chunk_size));
        u32::try_from(count).map_err(|_| {
            DynamoDbBackendError::InvalidBlobManifest(
                "blob requires more than u32::MAX chunks".to_string(),
            )
        })
    }

    fn transaction_token(
        table_name: &str,
        root_table_name: &str,
        key_prefix: &[u8],
        node_writes: &[RemoteBatchOp<'_>],
        root_conditions: &[RemoteRootCondition],
        root_writes: &[RemoteRootWrite],
        mode: TransactionPublicationMode,
    ) -> String {
        let mut bytes = b"prolly-ddb-tx-v2".to_vec();
        append_token_part(&mut bytes, table_name.as_bytes());
        append_token_part(&mut bytes, root_table_name.as_bytes());
        append_token_part(&mut bytes, key_prefix);
        bytes.push(match mode {
            TransactionPublicationMode::PrepublishImmutableNodes => 0,
            TransactionPublicationMode::AtomicNodesAndRoots => 1,
        });
        for write in node_writes {
            match write {
                RemoteBatchOp::Upsert { key, value } => {
                    bytes.push(0);
                    append_token_part(&mut bytes, key);
                    append_token_part(&mut bytes, value);
                }
                RemoteBatchOp::Delete { key } => {
                    bytes.push(1);
                    append_token_part(&mut bytes, key);
                }
            }
        }
        for condition in root_conditions {
            bytes.push(2);
            append_token_part(&mut bytes, &condition.name);
            append_optional_token_part(&mut bytes, condition.expected.as_deref());
        }
        for write in root_writes {
            match write {
                RemoteRootWrite::Put { name, manifest } => {
                    bytes.push(3);
                    append_token_part(&mut bytes, name);
                    append_token_part(&mut bytes, manifest);
                }
                RemoteRootWrite::Delete { name } => {
                    bytes.push(4);
                    append_token_part(&mut bytes, name);
                }
            }
        }
        let cid = prolly::Cid::from_bytes(&bytes);
        cid.as_bytes()[..16]
            .iter()
            .fold(String::with_capacity(32), |mut token, byte| {
                use std::fmt::Write as _;
                write!(&mut token, "{byte:02x}").expect("writing to String cannot fail");
                token
            })
    }

    fn append_token_part(bytes: &mut Vec<u8>, part: &[u8]) {
        bytes.extend_from_slice(&(part.len() as u64).to_be_bytes());
        bytes.extend_from_slice(part);
    }

    fn append_optional_token_part(bytes: &mut Vec<u8>, part: Option<&[u8]>) {
        match part {
            Some(part) => {
                bytes.push(1);
                append_token_part(bytes, part);
            }
            None => bytes.push(0),
        }
    }

    const DEFAULT_KEY_PREFIX: &[u8] = b"prolly:";
    const DEFAULT_READ_PARALLELISM: usize = 16;
    const DEFAULT_BATCH_GET_PARALLELISM: usize = 16;
    const DEFAULT_BATCH_WRITE_PARALLELISM: usize = 16;
    const DEFAULT_SCAN_PARALLELISM: usize = 8;
    const DYNAMODB_BATCH_GET_LIMIT: usize = 100;
    const DYNAMODB_BATCH_WRITE_LIMIT: usize = 25;
    const DYNAMODB_TRANSACTION_WRITE_LIMIT: usize = 100;
    const DYNAMODB_BATCH_RETRY_LIMIT: usize = 8;
    const DYNAMODB_AMBIGUOUS_RETRY_LIMIT: usize = 3;
    const DYNAMODB_BATCH_RETRY_BASE_DELAY_MS: u64 = 5;
    const DYNAMODB_SCAN_SEGMENT_LIMIT: usize = 1_000_000;
    const DYNAMODB_SCAN_CURSOR_MAGIC: &[u8] = b"PDSCAN\x01";
    const DYNAMODB_SCAN_CURSOR_BINDING_BYTES: usize = 32;
    /// Maximum evaluated physical items in one bounded maintenance Scan call.
    pub const DYNAMODB_SCAN_PAGE_LIMIT: usize = 1_000;
    const DYNAMODB_TABLE_READY_TIMEOUT: Duration = Duration::from_secs(120);
    const DYNAMODB_TABLE_READY_POLL_INTERVAL: Duration = Duration::from_millis(250);
    const PK_ATTR: &str = "pk";
    const SK_ATTR: &str = "sk";
    const VALUE_ATTR: &str = "value";
    const ROOT_TABLE_SUFFIX: &str = "-roots";
    const ROOT_PARTITION_PREFIX: &[u8] = b"roots:";
    const ROOT_ENTRY_PREFIX: &[u8] = b"\x01";

    const NODE_FAMILY: &[u8] = b"node:";
    const HINT_FAMILY: &[u8] = b"hint:";
    const BLOB_MANIFEST_FAMILY: &[u8] = b"blob-manifest:";
    const BLOB_CHUNK_FAMILY: &[u8] = b"blob-chunk:";
    const BLOB_MANIFEST_MAGIC: &[u8; 4] = b"PDBL";
    const BLOB_MANIFEST_VERSION: u8 = 1;
    const BLOB_MANIFEST_BYTES: usize = 4 + 1 + 8 + 4 + 4;
    const DYNAMODB_BLOB_CHUNK_BYTES: usize = 300 * 1024;

    /// Recommended partition key prefix for immutable node items.
    pub const NODE_PK_PREFIX: &str = "node#";
    /// Recommended partition key prefix for hint items.
    pub const HINT_PK_PREFIX: &str = "hint#";

    #[cfg(test)]
    mod tests {
        use super::*;
        use aws_credential_types::Credentials;
        use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
        use aws_smithy_types::body::SdkBody;
        use aws_types::region::Region;
        use prolly::{NodeStoreScan, Prolly, Store};
        use std::sync::Arc;

        fn replay_request() -> http::Request<SdkBody> {
            http::Request::builder()
                .uri("https://dynamodb.us-east-1.amazonaws.com/")
                .body(SdkBody::empty())
                .expect("static replay request is valid")
        }

        fn replay_response(status: u16, body: &'static str) -> http::Response<SdkBody> {
            http::Response::builder()
                .status(status)
                .header("content-type", "application/x-amz-json-1.0")
                .body(SdkBody::from(body))
                .expect("static replay response is valid")
        }

        fn replay_backend(replay: &StaticReplayClient) -> DynamoDbBackend {
            let config = aws_sdk_dynamodb::Config::builder()
                .behavior_version_latest()
                .credentials_provider(Credentials::for_tests())
                .region(Region::new("us-east-1"))
                .http_client(replay.clone())
                .build();
            DynamoDbBackend::new(aws_sdk_dynamodb::Client::from_conf(config), "physical")
        }

        #[tokio::test]
        async fn initialization_revalidates_resource_in_use_race_winner() {
            let replay = StaticReplayClient::new(vec![
                ReplayEvent::new(
                    replay_request(),
                    replay_response(
                        400,
                        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"not found"}"#,
                    ),
                ),
                ReplayEvent::new(
                    replay_request(),
                    replay_response(
                        400,
                        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceInUseException","message":"concurrent creator won"}"#,
                    ),
                ),
                ReplayEvent::new(
                    replay_request(),
                    replay_response(
                        200,
                        r#"{"Table":{"AttributeDefinitions":[{"AttributeName":"wrong","AttributeType":"S"}],"KeySchema":[{"AttributeName":"wrong","KeyType":"HASH"}],"TableName":"physical","TableStatus":"ACTIVE"}}"#,
                    ),
                ),
            ]);
            let backend = replay_backend(&replay);

            let error = backend.initialize_schema().await.unwrap_err();
            assert!(matches!(
                error,
                DynamoDbBackendError::InvalidConfiguration(message)
                    if message.contains("single HASH partition key named pk")
            ));
            assert_eq!(replay.actual_requests().count(), 3);
        }

        #[tokio::test]
        async fn root_initialization_revalidates_resource_in_use_race_winner() {
            const VALID_PRIMARY: &str = r#"{"Table":{"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"B"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"TableName":"physical","TableStatus":"ACTIVE"}}"#;
            let replay = StaticReplayClient::new(vec![
                ReplayEvent::new(replay_request(), replay_response(200, VALID_PRIMARY)),
                ReplayEvent::new(replay_request(), replay_response(200, VALID_PRIMARY)),
                ReplayEvent::new(
                    replay_request(),
                    replay_response(
                        400,
                        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceNotFoundException","message":"not found"}"#,
                    ),
                ),
                ReplayEvent::new(
                    replay_request(),
                    replay_response(
                        400,
                        r#"{"__type":"com.amazonaws.dynamodb.v20120810#ResourceInUseException","message":"concurrent creator won"}"#,
                    ),
                ),
                ReplayEvent::new(
                    replay_request(),
                    replay_response(
                        200,
                        r#"{"Table":{"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"B"}],"KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"TableName":"physical-roots","TableStatus":"ACTIVE"}}"#,
                    ),
                ),
            ]);
            let backend = replay_backend(&replay);

            let error = backend.initialize_schema().await.unwrap_err();
            assert!(matches!(
                error,
                DynamoDbBackendError::InvalidConfiguration(message)
                    if message.contains("root registry table physical-roots must use")
            ));
            assert_eq!(replay.actual_requests().count(), 5);
        }

        #[test]
        fn safe_config_bounds_every_serialized_node_below_the_item_ceiling() {
            let config = dynamodb_safe_config();
            assert_eq!(
                config.format.chunking.measure,
                prolly::ChunkMeasure::EncodedBytes
            );
            assert_eq!(
                config.format.chunking.hard_max_node_bytes,
                DYNAMODB_SAFE_NODE_BYTES
            );

            let store = Arc::new(prolly::MemStore::new());
            let engine = Prolly::new(store.clone(), config);
            let entries = (0_u32..24)
                .map(|index| {
                    (
                        index.to_be_bytes().to_vec(),
                        vec![(index % 251) as u8; 24 * 1024],
                    )
                })
                .collect::<Vec<_>>();
            engine.build_from_entries(entries).unwrap();

            let cids = store.list_node_cids().unwrap();
            assert!(!cids.is_empty());
            for cid in cids {
                let encoded = store.get(cid.as_bytes()).unwrap().unwrap();
                assert!(
                    encoded.len() <= DYNAMODB_SAFE_NODE_BYTES as usize,
                    "serialized node used {} bytes",
                    encoded.len()
                );
            }
        }

        #[test]
        fn immutable_prepublication_is_the_safe_default() {
            assert_eq!(
                TransactionPublicationMode::default(),
                TransactionPublicationMode::PrepublishImmutableNodes
            );
        }

        #[test]
        fn root_transaction_tokens_are_deterministic_and_content_bound() {
            let conditions = vec![RemoteRootCondition::new(
                b"head".to_vec(),
                Some(b"old".to_vec()),
            )];
            let writes = vec![RemoteRootWrite::Put {
                name: b"head".to_vec(),
                manifest: b"new".to_vec(),
            }];
            let first = transaction_token(
                "physical",
                "physical-roots",
                b"tenant-a:",
                &[],
                &conditions,
                &writes,
                TransactionPublicationMode::PrepublishImmutableNodes,
            );
            let repeated = transaction_token(
                "physical",
                "physical-roots",
                b"tenant-a:",
                &[],
                &conditions,
                &writes,
                TransactionPublicationMode::PrepublishImmutableNodes,
            );
            let changed = transaction_token(
                "physical",
                "physical-roots",
                b"tenant-a:",
                &[],
                &conditions,
                &[RemoteRootWrite::Put {
                    name: b"head".to_vec(),
                    manifest: b"other".to_vec(),
                }],
                TransactionPublicationMode::PrepublishImmutableNodes,
            );
            assert_eq!(first.len(), 32);
            assert_eq!(first, repeated);
            assert_ne!(first, changed);
            assert_ne!(
                first,
                transaction_token(
                    "physical",
                    "physical-roots",
                    b"tenant-b:",
                    &[],
                    &conditions,
                    &writes,
                    TransactionPublicationMode::PrepublishImmutableNodes,
                )
            );
        }

        #[test]
        fn write_failure_disposition_never_conflates_unknown_with_retryable() {
            let retryable = DynamoDbBackendError::RetryableTransaction {
                token: "retry-token".into(),
                source: "throttled".into(),
            };
            assert_eq!(
                retryable.write_disposition(),
                WriteFailureDisposition::RetryableNotApplied
            );
            assert_eq!(retryable.transaction_token(), Some("retry-token"));

            let unknown = DynamoDbBackendError::OutcomeUnknown {
                token: "unknown-token".into(),
                source: "connection closed".into(),
            };
            assert_eq!(
                unknown.write_disposition(),
                WriteFailureDisposition::OutcomeUnknown
            );
            assert_eq!(unknown.transaction_token(), Some("unknown-token"));

            let terminal = DynamoDbBackendError::InvalidConfiguration("bad namespace".into());
            assert_eq!(
                terminal.write_disposition(),
                WriteFailureDisposition::Terminal
            );
            assert_eq!(terminal.transaction_token(), None);
        }

        #[test]
        fn blob_manifest_decoder_is_fail_closed_for_every_truncation() {
            let manifest = encode_blob_manifest(600_001, 300_000, 3);
            assert_eq!(
                decode_blob_manifest(&manifest).unwrap(),
                (600_001, 300_000, 3)
            );
            for length in 0..manifest.len() {
                assert!(matches!(
                    decode_blob_manifest(&manifest[..length]),
                    Err(DynamoDbBackendError::InvalidBlobManifest(_))
                ));
            }

            let mut trailing = manifest.clone();
            trailing.push(0);
            assert!(matches!(
                decode_blob_manifest(&trailing),
                Err(DynamoDbBackendError::InvalidBlobManifest(_))
            ));
        }
    }
}

pub use dynamodb::*;
