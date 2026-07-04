//! DynamoDB store adapter for prolly-map.

pub use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteStoreBackend,
};

/// DynamoDB adapter entry point.
pub mod dynamodb {
    use std::collections::{HashMap, HashSet};
    use std::error::Error as StdError;
    use std::fmt;

    use aws_sdk_dynamodb::error::SdkError;
    use aws_sdk_dynamodb::operation::create_table::CreateTableError;
    use aws_sdk_dynamodb::operation::delete_item::DeleteItemError;
    use aws_sdk_dynamodb::operation::describe_table::DescribeTableError;
    use aws_sdk_dynamodb::operation::put_item::PutItemError;
    use aws_sdk_dynamodb::primitives::Blob;
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, DeleteRequest, KeySchemaElement, KeyType,
        KeysAndAttributes, PutRequest, ReturnValuesOnConditionCheckFailure, ScalarAttributeType,
        TableDescription, WriteRequest,
    };

    use crate::{RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteStoreBackend};

    /// Store adapter for DynamoDB-backed prolly nodes and roots.
    pub type DynamoDbStore = crate::RemoteProllyStore<DynamoDbBackend>;

    /// AWS SDK-backed DynamoDB backend.
    ///
    /// The table must use a binary partition key named `pk`. The adapter stores
    /// binary payloads in a `value` attribute and separates nodes, roots, and
    /// hints by prefixing `pk`.
    #[derive(Clone, Debug)]
    pub struct DynamoDbBackend {
        client: aws_sdk_dynamodb::Client,
        table_name: String,
        key_prefix: Vec<u8>,
        read_parallelism: usize,
    }

    impl DynamoDbBackend {
        /// Create a backend from an existing AWS SDK DynamoDB client.
        pub fn new(client: aws_sdk_dynamodb::Client, table_name: impl Into<String>) -> Self {
            Self {
                client,
                table_name: table_name.into(),
                key_prefix: DEFAULT_KEY_PREFIX.to_vec(),
                read_parallelism: DEFAULT_READ_PARALLELISM,
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

        /// Create the required DynamoDB table if it does not already exist.
        ///
        /// Existing tables must have a binary partition key named `pk`.
        pub async fn initialize_schema(&self) -> Result<(), DynamoDbBackendError> {
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
                    self.validate_table_schema(table)?;
                    return Ok(());
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
                })
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
            self.batch_write_requests(&requests).await
        }

        fn node_key(&self, key: &[u8]) -> Vec<u8> {
            self.family_key(NODE_FAMILY, key)
        }

        fn root_key(&self, name: &[u8]) -> Vec<u8> {
            self.family_key(ROOT_FAMILY, name)
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

        fn validate_table_schema(
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

        async fn scan_primary_keys_with_prefix(
            &self,
            prefix: &[u8],
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
            Ok(WriteRequest::builder()
                .delete_request(
                    DeleteRequest::builder()
                        .set_key(Some(self.key_item(key)))
                        .build()
                        .map_err(DynamoDbBackendError::sdk)?,
                )
                .build())
        }

        async fn batch_write_requests(
            &self,
            requests: &[WriteRequest],
        ) -> Result<(), DynamoDbBackendError> {
            for chunk in requests.chunks(DYNAMODB_BATCH_WRITE_LIMIT) {
                let mut pending = chunk.to_vec();
                let mut attempts = 0;

                while !pending.is_empty() {
                    let output = self
                        .client
                        .batch_write_item()
                        .request_items(&self.table_name, pending)
                        .send()
                        .await
                        .map_err(DynamoDbBackendError::sdk)?;

                    pending = output
                        .unprocessed_items()
                        .and_then(|items| items.get(&self.table_name).cloned())
                        .unwrap_or_default();
                    if pending.is_empty() {
                        break;
                    }

                    attempts += 1;
                    if attempts >= DYNAMODB_BATCH_RETRY_LIMIT {
                        return Err(DynamoDbBackendError::UnprocessedBatch {
                            operation: "batch_write_item",
                            remaining: pending.len(),
                        });
                    }
                }
            }

            Ok(())
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
            let mut latest = HashMap::<Vec<u8>, Option<Vec<u8>>>::new();
            for op in ops {
                match op {
                    RemoteBatchOp::Upsert { key, value } => {
                        latest.insert(self.node_key(key), Some(value.to_vec()));
                    }
                    RemoteBatchOp::Delete { key } => {
                        latest.insert(self.node_key(key), None);
                    }
                }
            }

            let requests = latest
                .into_iter()
                .map(|(key, value)| match value {
                    Some(value) => self.put_write_request(key, &value),
                    None => self.delete_write_request(key),
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.batch_write_requests(&requests).await
        }

        async fn batch_get_nodes_ordered(
            &self,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            let mut seen = HashSet::new();
            let mut unique_keys = Vec::new();
            for key in keys {
                let dynamo_key = self.node_key(key);
                if seen.insert(dynamo_key.clone()) {
                    unique_keys.push(dynamo_key);
                }
            }

            let mut found = HashMap::<Vec<u8>, Vec<u8>>::new();
            for chunk in unique_keys.chunks(DYNAMODB_BATCH_GET_LIMIT) {
                let mut pending = KeysAndAttributes::builder()
                    .set_keys(Some(
                        chunk
                            .iter()
                            .map(|key| self.key_item(key.clone()))
                            .collect::<Vec<_>>(),
                    ))
                    .consistent_read(true)
                    .projection_expression("#pk, #value")
                    .expression_attribute_names("#pk", PK_ATTR)
                    .expression_attribute_names("#value", VALUE_ATTR)
                    .build()
                    .map_err(DynamoDbBackendError::sdk)?;
                let mut attempts = 0;

                loop {
                    let output = self
                        .client
                        .batch_get_item()
                        .request_items(&self.table_name, pending)
                        .send()
                        .await
                        .map_err(DynamoDbBackendError::sdk)?;

                    if let Some(items) = output
                        .responses()
                        .and_then(|responses| responses.get(&self.table_name))
                    {
                        for item in items {
                            let key = binary_value_attr(item, PK_ATTR)?;
                            let value = binary_value_attr(item, VALUE_ATTR)?;
                            found.insert(key, value);
                        }
                    }

                    let unprocessed = output
                        .unprocessed_keys()
                        .and_then(|items| items.get(&self.table_name).cloned());
                    match unprocessed {
                        Some(keys) if !keys.keys().is_empty() => pending = keys,
                        _ => break,
                    }

                    attempts += 1;
                    if attempts >= DYNAMODB_BATCH_RETRY_LIMIT {
                        return Err(DynamoDbBackendError::UnprocessedBatch {
                            operation: "batch_get_item",
                            remaining: pending.keys().len(),
                        });
                    }
                }
            }

            Ok(keys
                .iter()
                .map(|key| found.get(&self.node_key(key)).cloned())
                .collect())
        }

        async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            let mut latest = HashMap::<Vec<u8>, Vec<u8>>::new();
            for (key, value) in entries {
                latest.insert(self.node_key(key), value.to_vec());
            }
            let requests = latest
                .into_iter()
                .map(|(key, value)| self.put_write_request(key, &value))
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
            self.batch_put_nodes(entries).await?;
            self.put_hint(namespace, key, value).await
        }

        async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.get_value_by_key(self.root_key(name)).await
        }

        async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
            self.client
                .put_item()
                .table_name(&self.table_name)
                .set_item(Some(self.item(self.root_key(name), manifest)))
                .send()
                .await
                .map_err(DynamoDbBackendError::sdk)?;
            Ok(())
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            self.client
                .delete_item()
                .table_name(&self.table_name)
                .set_key(Some(self.key_item(self.root_key(name))))
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
            let root_key = self.root_key(name);
            let result = match new {
                Some(manifest) => {
                    let mut request = self
                        .client
                        .put_item()
                        .table_name(&self.table_name)
                        .set_item(Some(self.item(root_key.clone(), manifest)))
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
                        .table_name(&self.table_name)
                        .set_key(Some(self.key_item(root_key.clone())))
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
                    let current = self.get_value_by_key(root_key).await?;
                    Ok(RemoteManifestUpdate::Conflict { current })
                }
                Err(err) => Err(DynamoDbBackendError::sdk(err)),
            }
        }

        async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error> {
            let prefix = self.family_prefix(ROOT_FAMILY);
            let mut names = self
                .scan_primary_keys_with_prefix(&prefix)
                .await?
                .into_iter()
                .filter_map(|key| key.strip_prefix(prefix.as_slice()).map(<[u8]>::to_vec))
                .collect::<Vec<_>>();
            names.sort();

            let mut roots = Vec::with_capacity(names.len());
            for name in names {
                if let Some(manifest) = self.get_root_manifest(&name).await? {
                    roots.push(RemoteNamedRoot::new(name, manifest));
                }
            }
            Ok(roots)
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

    const DEFAULT_KEY_PREFIX: &[u8] = b"prolly:";
    const DEFAULT_READ_PARALLELISM: usize = 16;
    const DYNAMODB_BATCH_GET_LIMIT: usize = 100;
    const DYNAMODB_BATCH_WRITE_LIMIT: usize = 25;
    const DYNAMODB_BATCH_RETRY_LIMIT: usize = 8;
    const PK_ATTR: &str = "pk";
    const VALUE_ATTR: &str = "value";

    const NODE_FAMILY: &[u8] = b"node:";
    const ROOT_FAMILY: &[u8] = b"root:";
    const HINT_FAMILY: &[u8] = b"hint:";

    /// Recommended partition key prefix for immutable node items.
    pub const NODE_PK_PREFIX: &str = "node#";
    /// Recommended partition key prefix for named root manifest items.
    pub const ROOT_PK_PREFIX: &str = "root#";
    /// Recommended partition key prefix for hint items.
    pub const HINT_PK_PREFIX: &str = "hint#";
}

pub use dynamodb::*;
