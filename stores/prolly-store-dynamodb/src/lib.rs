#![doc = include_str!("../README.md")]

pub use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteRootCondition,
    RemoteRootWrite, RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
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
    use aws_sdk_dynamodb::primitives::Blob;
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, ConditionCheck, Delete as TransactDelete,
        DeleteRequest, KeySchemaElement, KeyType, KeysAndAttributes, Put as TransactPut,
        PutRequest, ReturnValuesOnConditionCheckFailure, ScalarAttributeType, TableDescription,
        TableStatus, TransactWriteItem, WriteRequest,
    };
    use futures_util::stream::{self, StreamExt, TryStreamExt};

    use crate::{
        RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteRootCondition, RemoteRootWrite,
        RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
    };

    /// Store adapter for DynamoDB-backed prolly nodes and roots.
    pub type DynamoDbStore = crate::RemoteProllyStore<DynamoDbBackend>;

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

        /// Set the maximum number of concurrent `BatchGetItem` requests.
        ///
        /// DynamoDB limits each request to 100 items. Values greater than one
        /// allow large reads to use multiple requests concurrently.
        pub fn with_batch_get_parallelism(mut self, parallelism: usize) -> Self {
            self.batch_get_parallelism = parallelism.max(1);
            self
        }

        /// Set the maximum number of concurrent `BatchWriteItem` requests.
        ///
        /// DynamoDB limits each request to 25 items. Keep this bounded to the
        /// write capacity available to the table.
        pub fn with_batch_write_parallelism(mut self, parallelism: usize) -> Self {
            self.batch_write_parallelism = parallelism.max(1);
            self
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
                    return self.wait_for_table_active(&self.table_name).await;
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
            self.wait_for_table_active(&self.table_name).await
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
                    return self.wait_for_table_active(&self.root_table_name).await;
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
            self.wait_for_table_active(&self.root_table_name).await
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

        async fn wait_for_table_active(
            &self,
            table_name: &str,
        ) -> Result<(), DynamoDbBackendError> {
            let started = Instant::now();
            loop {
                match self
                    .client
                    .describe_table()
                    .table_name(table_name)
                    .send()
                    .await
                {
                    Ok(output)
                        if output
                            .table()
                            .and_then(TableDescription::table_status)
                            .is_some_and(|status| status == &TableStatus::Active) =>
                    {
                        return Ok(());
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
            let chunks = stream::iter(
                keys.chunks(DYNAMODB_BATCH_GET_LIMIT)
                    .map(<[Vec<u8>]>::to_vec),
            )
            .map(|chunk| self.batch_get_chunk(chunk))
            .buffer_unordered(self.batch_get_parallelism)
            .try_collect::<Vec<_>>()
            .await?;

            Ok(chunks.into_iter().flatten().collect())
        }

        async fn batch_get_chunk(
            &self,
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
                    .request_items(&self.table_name, pending)
                    .send()
                    .await
                    .map_err(DynamoDbBackendError::sdk)?;

                if let Some(items) = output
                    .responses
                    .take()
                    .and_then(|mut responses| responses.remove(&self.table_name))
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
                    .and_then(|mut items| items.remove(&self.table_name));
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

            if items.len() > DYNAMODB_TRANSACTION_WRITE_LIMIT {
                return Err(DynamoDbBackendError::TransactionTooLarge {
                    items: items.len(),
                    limit: DYNAMODB_TRANSACTION_WRITE_LIMIT,
                });
            }
            if items.is_empty() {
                return Ok(RemoteTransactionUpdate::Applied);
            }

            match self
                .client
                .transact_write_items()
                .set_transact_items(Some(items))
                .send()
                .await
            {
                Ok(_) => Ok(RemoteTransactionUpdate::Applied),
                Err(err)
                    if err
                        .as_service_error()
                        .is_some_and(|err| err.is_transaction_canceled_exception()) =>
                {
                    let current_roots = stream::iter(root_conditions)
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
    }

    impl DynamoDbBackendError {
        fn sdk(err: impl fmt::Display) -> Self {
            Self::Sdk(err.to_string())
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
            }
        }
    }

    impl StdError for DynamoDbBackendError {}

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

    const DEFAULT_KEY_PREFIX: &[u8] = b"prolly:";
    const DEFAULT_READ_PARALLELISM: usize = 16;
    const DEFAULT_BATCH_GET_PARALLELISM: usize = 16;
    const DEFAULT_BATCH_WRITE_PARALLELISM: usize = 16;
    const DEFAULT_SCAN_PARALLELISM: usize = 8;
    const DYNAMODB_BATCH_GET_LIMIT: usize = 100;
    const DYNAMODB_BATCH_WRITE_LIMIT: usize = 25;
    const DYNAMODB_TRANSACTION_WRITE_LIMIT: usize = 100;
    const DYNAMODB_BATCH_RETRY_LIMIT: usize = 8;
    const DYNAMODB_BATCH_RETRY_BASE_DELAY_MS: u64 = 5;
    const DYNAMODB_SCAN_SEGMENT_LIMIT: usize = 1_000_000;
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

    /// Recommended partition key prefix for immutable node items.
    pub const NODE_PK_PREFIX: &str = "node#";
    /// Recommended partition key prefix for hint items.
    pub const HINT_PK_PREFIX: &str = "hint#";
}

pub use dynamodb::*;
