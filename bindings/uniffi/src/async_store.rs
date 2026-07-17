use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteRootCondition, RemoteRootWrite,
    RemoteStoreBackend, RemoteTransactionUpdate,
};

const STORE_PROTOCOL_MAJOR: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StoreCapabilitiesRecord {
    pub native_batch_reads: bool,
    pub atomic_batch_writes: bool,
    pub node_scan: bool,
    pub hints: bool,
    pub atomic_nodes_and_hint: bool,
    pub root_scan: bool,
    pub root_compare_and_swap: bool,
    pub transactions: bool,
    pub read_parallelism: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StoreLimitsRecord {
    pub max_batch_read_items: Option<u32>,
    pub max_batch_write_items: Option<u32>,
    pub max_transaction_operations: Option<u32>,
    pub max_node_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StoreDescriptorRecord {
    pub protocol_major: u32,
    pub adapter_name: String,
    pub provider: String,
    pub schema_version: u32,
    pub capabilities: StoreCapabilitiesRecord,
    pub limits: StoreLimitsRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StoreErrorRecord {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub provider_code: Option<String>,
}

impl StoreErrorRecord {
    fn invalid_descriptor(message: impl Into<String>) -> Self {
        Self {
            code: "invalid_descriptor".to_string(),
            message: message.into(),
            retryable: false,
            provider_code: None,
        }
    }

    fn unsupported(operation: &str) -> Self {
        Self {
            code: "unsupported".to_string(),
            message: format!("store operation {operation} is unsupported"),
            retryable: false,
            provider_code: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StoreDescriptorResultRecord {
    pub value: Option<StoreDescriptorRecord>,
    pub error: Option<StoreErrorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct OptionalBytesRecord {
    pub present: bool,
    pub value: Vec<u8>,
}

impl OptionalBytesRecord {
    fn from_option(value: Option<Vec<u8>>) -> Self {
        match value {
            Some(value) => Self {
                present: true,
                value,
            },
            None => Self {
                present: false,
                value: Vec::new(),
            },
        }
    }

    fn into_option(self) -> Result<Option<Vec<u8>>, ForeignStoreError> {
        if self.present {
            Ok(Some(self.value))
        } else if self.value.is_empty() {
            Ok(None)
        } else {
            Err(ForeignStoreError(StoreErrorRecord {
                code: "invalid_result".to_string(),
                message: "absent optional bytes must have an empty value".to_string(),
                retryable: false,
                provider_code: None,
            }))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct OptionalBytesListResultRecord {
    pub values: Vec<OptionalBytesRecord>,
    pub error: Option<StoreErrorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct UnitResultRecord {
    pub error: Option<StoreErrorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct OptionalBytesResultRecord {
    pub value: OptionalBytesRecord,
    pub error: Option<StoreErrorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct BytesListResultRecord {
    pub values: Vec<Vec<u8>>,
    pub error: Option<StoreErrorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NodeMutationRecord {
    pub key: Vec<u8>,
    pub value: OptionalBytesRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NodeEntryRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NamedBytesRecord {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NamedBytesListResultRecord {
    pub values: Vec<NamedBytesRecord>,
    pub error: Option<StoreErrorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RootCasResultRecord {
    pub applied: bool,
    pub current: OptionalBytesRecord,
    pub error: Option<StoreErrorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RootConditionRecord {
    pub name: Vec<u8>,
    pub expected: OptionalBytesRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RootWriteRecord {
    pub name: Vec<u8>,
    pub replacement: OptionalBytesRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StoreTransactionConflictRecord {
    pub name: Vec<u8>,
    pub expected: OptionalBytesRecord,
    pub current: OptionalBytesRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct TransactionResultRecord {
    pub applied: bool,
    pub conflict: Option<StoreTransactionConflictRecord>,
    pub error: Option<StoreErrorRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignStoreError(StoreErrorRecord);

impl ForeignStoreError {
    fn unsupported(operation: &str) -> Self {
        Self(StoreErrorRecord::unsupported(operation))
    }
}

impl fmt::Display for ForeignStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.0.code, self.0.message)
    }
}

impl StdError for ForeignStoreError {}

#[uniffi::export(with_foreign)]
#[async_trait::async_trait]
pub trait ForeignRemoteStore: Send + Sync {
    async fn descriptor(&self) -> StoreDescriptorResultRecord;

    async fn get_node(&self, cid: Vec<u8>) -> OptionalBytesResultRecord;

    async fn put_node(&self, cid: Vec<u8>, value: Vec<u8>) -> UnitResultRecord;

    async fn delete_node(&self, cid: Vec<u8>) -> UnitResultRecord;

    async fn batch_nodes(&self, ops: Vec<NodeMutationRecord>) -> UnitResultRecord;

    async fn batch_get_nodes_ordered(
        &self,
        cids: Vec<Vec<u8>>,
    ) -> OptionalBytesListResultRecord;

    async fn list_node_cids(&self) -> BytesListResultRecord;

    async fn get_hint(
        &self,
        namespace: Vec<u8>,
        key: Vec<u8>,
    ) -> OptionalBytesResultRecord;

    async fn put_hint(
        &self,
        namespace: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> UnitResultRecord;

    async fn batch_put_nodes_with_hint(
        &self,
        nodes: Vec<NodeEntryRecord>,
        namespace: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> UnitResultRecord;

    async fn get_root_manifest(&self, name: Vec<u8>) -> OptionalBytesResultRecord;

    async fn put_root_manifest(
        &self,
        name: Vec<u8>,
        manifest: Vec<u8>,
    ) -> UnitResultRecord;

    async fn delete_root_manifest(&self, name: Vec<u8>) -> UnitResultRecord;

    async fn compare_and_swap_root_manifest(
        &self,
        name: Vec<u8>,
        expected: OptionalBytesRecord,
        new: OptionalBytesRecord,
    ) -> RootCasResultRecord;

    async fn list_root_manifests(&self) -> NamedBytesListResultRecord;

    async fn commit_transaction(
        &self,
        nodes: Vec<NodeMutationRecord>,
        conditions: Vec<RootConditionRecord>,
        roots: Vec<RootWriteRecord>,
    ) -> TransactionResultRecord;
}

#[derive(Clone)]
pub struct ForeignRemoteBackend {
    callback: Arc<dyn ForeignRemoteStore>,
    descriptor: StoreDescriptorRecord,
}

impl ForeignRemoteBackend {
    pub async fn new(
        callback: Arc<dyn ForeignRemoteStore>,
    ) -> Result<Self, ForeignStoreError> {
        let result = callback.descriptor().await;
        if let Some(error) = result.error {
            return Err(ForeignStoreError(error));
        }
        let descriptor = result.value.ok_or_else(|| {
            ForeignStoreError(StoreErrorRecord::invalid_descriptor(
                "descriptor callback returned neither a value nor an error",
            ))
        })?;
        let descriptor = validate_descriptor(descriptor).map_err(ForeignStoreError)?;
        Ok(Self {
            callback,
            descriptor,
        })
    }

    pub fn descriptor(&self) -> &StoreDescriptorRecord {
        &self.descriptor
    }

    fn check_node_size(&self, size: usize) -> Result<(), ForeignStoreError> {
        check_limit(
            "node bytes",
            size as u64,
            self.descriptor.limits.max_node_bytes,
        )
    }

    fn check_batch_write_count(&self, count: usize) -> Result<(), ForeignStoreError> {
        check_limit(
            "batch write items",
            count as u64,
            self.descriptor
                .limits
                .max_batch_write_items
                .map(u64::from),
        )
    }
}

impl RemoteStoreBackend for ForeignRemoteBackend {
    type Error = ForeignStoreError;

    async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        finish_optional_bytes(self.callback.get_node(key.to_vec()).await)
    }

    async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.check_node_size(value.len())?;
        finish_unit(self.callback.put_node(key.to_vec(), value.to_vec()).await)
    }

    async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
        finish_unit(self.callback.delete_node(key.to_vec()).await)
    }

    async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
        self.check_batch_write_count(ops.len())?;
        let mut records = Vec::with_capacity(ops.len());
        for op in ops {
            let record = match op {
                RemoteBatchOp::Upsert { key, value } => {
                    self.check_node_size(value.len())?;
                    NodeMutationRecord {
                        key: key.to_vec(),
                        value: OptionalBytesRecord::from_option(Some(value.to_vec())),
                    }
                }
                RemoteBatchOp::Delete { key } => NodeMutationRecord {
                    key: key.to_vec(),
                    value: OptionalBytesRecord::from_option(None),
                },
            };
            records.push(record);
        }
        finish_unit(self.callback.batch_nodes(records).await)
    }

    async fn batch_get_nodes_ordered(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        check_limit(
            "batch read items",
            keys.len() as u64,
            self.descriptor
                .limits
                .max_batch_read_items
                .map(u64::from),
        )?;
        let result = self
            .callback
            .batch_get_nodes_ordered(keys.iter().map(|key| key.to_vec()).collect())
            .await;
        if let Some(error) = result.error {
            return Err(ForeignStoreError(error));
        }
        if result.values.len() != keys.len() {
            return Err(ForeignStoreError(StoreErrorRecord {
                code: "invalid_result".to_string(),
                message: format!(
                    "ordered batch returned {} values for {} keys",
                    result.values.len(),
                    keys.len()
                ),
                retryable: false,
                provider_code: None,
            }));
        }
        result
            .values
            .into_iter()
            .map(OptionalBytesRecord::into_option)
            .collect()
    }

    async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
        if !self.descriptor.capabilities.node_scan {
            return Err(ForeignStoreError::unsupported("list_node_cids"));
        }
        let result = self.callback.list_node_cids().await;
        let mut values = finish_bytes_list(result)?;
        values.sort();
        Ok(values)
    }

    fn prefers_batch_reads(&self) -> bool {
        self.descriptor.capabilities.native_batch_reads
    }

    fn read_parallelism(&self) -> usize {
        self.descriptor.capabilities.read_parallelism as usize
    }

    fn supports_hints(&self) -> bool {
        self.descriptor.capabilities.hints
    }

    async fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        if !self.descriptor.capabilities.hints {
            return Err(ForeignStoreError::unsupported("get_hint"));
        }
        finish_optional_bytes(
            self.callback
                .get_hint(namespace.to_vec(), key.to_vec())
                .await,
        )
    }

    async fn put_hint(
        &self,
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        if !self.descriptor.capabilities.hints {
            return Err(ForeignStoreError::unsupported("put_hint"));
        }
        finish_unit(
            self.callback
                .put_hint(namespace.to_vec(), key.to_vec(), value.to_vec())
                .await,
        )
    }

    async fn batch_put_nodes_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        if !self.descriptor.capabilities.hints {
            return Err(ForeignStoreError::unsupported(
                "batch_put_nodes_with_hint",
            ));
        }
        self.check_batch_write_count(entries.len())?;
        if !self.descriptor.capabilities.atomic_nodes_and_hint {
            let ops = entries
                .iter()
                .map(|(key, value)| RemoteBatchOp::Upsert { key, value })
                .collect::<Vec<_>>();
            self.batch_nodes(&ops).await?;
            return self.put_hint(namespace, key, value).await;
        }
        let mut nodes = Vec::with_capacity(entries.len());
        for (entry_key, entry_value) in entries {
            self.check_node_size(entry_value.len())?;
            nodes.push(NodeEntryRecord {
                key: entry_key.to_vec(),
                value: entry_value.to_vec(),
            });
        }
        finish_unit(
            self.callback
                .batch_put_nodes_with_hint(
                    nodes,
                    namespace.to_vec(),
                    key.to_vec(),
                    value.to_vec(),
                )
                .await,
        )
    }

    async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        finish_optional_bytes(self.callback.get_root_manifest(name.to_vec()).await)
    }

    async fn put_root_manifest(
        &self,
        name: &[u8],
        manifest: &[u8],
    ) -> Result<(), Self::Error> {
        finish_unit(
            self.callback
                .put_root_manifest(name.to_vec(), manifest.to_vec())
                .await,
        )
    }

    async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
        finish_unit(self.callback.delete_root_manifest(name.to_vec()).await)
    }

    async fn compare_and_swap_root_manifest(
        &self,
        name: &[u8],
        expected: Option<&[u8]>,
        new: Option<&[u8]>,
    ) -> Result<RemoteManifestUpdate, Self::Error> {
        if !self.descriptor.capabilities.root_compare_and_swap {
            return Err(ForeignStoreError::unsupported(
                "compare_and_swap_root_manifest",
            ));
        }
        let result = self
            .callback
            .compare_and_swap_root_manifest(
                name.to_vec(),
                OptionalBytesRecord::from_option(expected.map(<[u8]>::to_vec)),
                OptionalBytesRecord::from_option(new.map(<[u8]>::to_vec)),
            )
            .await;
        if let Some(error) = result.error {
            return Err(ForeignStoreError(error));
        }
        if result.applied {
            Ok(RemoteManifestUpdate::Applied)
        } else {
            Ok(RemoteManifestUpdate::Conflict {
                current: result.current.into_option()?,
            })
        }
    }

    async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error> {
        if !self.descriptor.capabilities.root_scan {
            return Err(ForeignStoreError::unsupported("list_root_manifests"));
        }
        let result = self.callback.list_root_manifests().await;
        if let Some(error) = result.error {
            return Err(ForeignStoreError(error));
        }
        let mut values = result
            .values
            .into_iter()
            .map(|record| RemoteNamedRoot::new(record.name, record.value))
            .collect::<Vec<_>>();
        values.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(values)
    }

    fn supports_transactions(&self) -> bool {
        self.descriptor.capabilities.transactions
    }

    async fn commit_transaction(
        &self,
        node_writes: &[RemoteBatchOp<'_>],
        root_conditions: &[RemoteRootCondition],
        root_writes: &[RemoteRootWrite],
    ) -> Result<RemoteTransactionUpdate, Self::Error> {
        if !self.descriptor.capabilities.transactions {
            return Err(ForeignStoreError::unsupported("commit_transaction"));
        }
        let operation_count = node_writes.len() + root_conditions.len() + root_writes.len();
        check_limit(
            "transaction operations",
            operation_count as u64,
            self.descriptor
                .limits
                .max_transaction_operations
                .map(u64::from),
        )?;
        let mut nodes = Vec::with_capacity(node_writes.len());
        for write in node_writes {
            nodes.push(match write {
                RemoteBatchOp::Upsert { key, value } => {
                    self.check_node_size(value.len())?;
                    NodeMutationRecord {
                        key: key.to_vec(),
                        value: OptionalBytesRecord::from_option(Some(value.to_vec())),
                    }
                }
                RemoteBatchOp::Delete { key } => NodeMutationRecord {
                    key: key.to_vec(),
                    value: OptionalBytesRecord::from_option(None),
                },
            });
        }
        let conditions = root_conditions
            .iter()
            .map(|condition| RootConditionRecord {
                name: condition.name.clone(),
                expected: OptionalBytesRecord::from_option(condition.expected.clone()),
            })
            .collect();
        let roots = root_writes
            .iter()
            .map(|write| match write {
                RemoteRootWrite::Put { name, manifest } => RootWriteRecord {
                    name: name.clone(),
                    replacement: OptionalBytesRecord::from_option(Some(manifest.clone())),
                },
                RemoteRootWrite::Delete { name } => RootWriteRecord {
                    name: name.clone(),
                    replacement: OptionalBytesRecord::from_option(None),
                },
            })
            .collect();
        let result = self
            .callback
            .commit_transaction(nodes, conditions, roots)
            .await;
        if let Some(error) = result.error {
            return Err(ForeignStoreError(error));
        }
        match (result.applied, result.conflict) {
            (true, None) => Ok(RemoteTransactionUpdate::Applied),
            (false, Some(conflict)) => Ok(RemoteTransactionUpdate::Conflict(
                prolly::RemoteTransactionConflict::new(
                    conflict.name,
                    conflict.expected.into_option()?,
                    conflict.current.into_option()?,
                ),
            )),
            _ => Err(ForeignStoreError(StoreErrorRecord {
                code: "invalid_result".to_string(),
                message: "transaction result must be either applied or one conflict".to_string(),
                retryable: false,
                provider_code: None,
            })),
        }
    }
}

fn finish_unit(result: UnitResultRecord) -> Result<(), ForeignStoreError> {
    match result.error {
        Some(error) => Err(ForeignStoreError(error)),
        None => Ok(()),
    }
}

fn finish_optional_bytes(
    result: OptionalBytesResultRecord,
) -> Result<Option<Vec<u8>>, ForeignStoreError> {
    if let Some(error) = result.error {
        return Err(ForeignStoreError(error));
    }
    result.value.into_option()
}

fn finish_bytes_list(result: BytesListResultRecord) -> Result<Vec<Vec<u8>>, ForeignStoreError> {
    match result.error {
        Some(error) => Err(ForeignStoreError(error)),
        None => Ok(result.values),
    }
}

fn check_limit(name: &str, actual: u64, maximum: Option<u64>) -> Result<(), ForeignStoreError> {
    if let Some(maximum) = maximum {
        if actual > maximum {
            return Err(ForeignStoreError(StoreErrorRecord {
                code: "limit_exceeded".to_string(),
                message: format!("{name} is {actual}, maximum is {maximum}"),
                retryable: false,
                provider_code: None,
            }));
        }
    }
    Ok(())
}

fn validate_descriptor(
    descriptor: StoreDescriptorRecord,
) -> Result<StoreDescriptorRecord, StoreErrorRecord> {
    if descriptor.protocol_major != STORE_PROTOCOL_MAJOR {
        return Err(StoreErrorRecord::invalid_descriptor(format!(
            "protocol major must be {STORE_PROTOCOL_MAJOR}, got {}",
            descriptor.protocol_major
        )));
    }
    if descriptor.adapter_name.trim().is_empty() {
        return Err(StoreErrorRecord::invalid_descriptor(
            "adapter_name must not be empty",
        ));
    }
    if descriptor.provider.trim().is_empty() {
        return Err(StoreErrorRecord::invalid_descriptor(
            "provider must not be empty",
        ));
    }
    if descriptor.schema_version == 0 {
        return Err(StoreErrorRecord::invalid_descriptor(
            "schema_version must be at least 1",
        ));
    }
    if descriptor.capabilities.read_parallelism == 0 {
        return Err(StoreErrorRecord::invalid_descriptor(
            "read_parallelism must be at least 1",
        ));
    }
    if descriptor.capabilities.atomic_nodes_and_hint && !descriptor.capabilities.hints {
        return Err(StoreErrorRecord::invalid_descriptor(
            "atomic_nodes_and_hint requires hints support",
        ));
    }
    validate_optional_limit(
        "max_batch_read_items",
        descriptor.limits.max_batch_read_items.map(u64::from),
    )?;
    validate_optional_limit(
        "max_batch_write_items",
        descriptor.limits.max_batch_write_items.map(u64::from),
    )?;
    validate_optional_limit(
        "max_transaction_operations",
        descriptor
            .limits
            .max_transaction_operations
            .map(u64::from),
    )?;
    validate_optional_limit("max_node_bytes", descriptor.limits.max_node_bytes)?;
    Ok(descriptor)
}

fn validate_optional_limit(name: &str, value: Option<u64>) -> Result<(), StoreErrorRecord> {
    if value == Some(0) {
        return Err(StoreErrorRecord::invalid_descriptor(format!(
            "{name} must be at least 1 when present"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use prolly::RemoteStoreBackend;
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll, Wake, Waker};

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = Waker::from(Arc::new(NoopWake));
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn descriptor(protocol_major: u32, read_parallelism: u32) -> StoreDescriptorRecord {
        StoreDescriptorRecord {
            protocol_major,
            adapter_name: "test-adapter".to_string(),
            provider: "test".to_string(),
            schema_version: 1,
            capabilities: StoreCapabilitiesRecord {
                native_batch_reads: true,
                atomic_batch_writes: true,
                node_scan: true,
                hints: true,
                atomic_nodes_and_hint: true,
                root_scan: true,
                root_compare_and_swap: true,
                transactions: true,
                read_parallelism,
            },
            limits: StoreLimitsRecord {
                max_batch_read_items: None,
                max_batch_write_items: None,
                max_transaction_operations: None,
                max_node_bytes: None,
            },
        }
    }

    #[test]
    fn descriptor_rejects_wrong_protocol_and_zero_parallelism() {
        let wrong_protocol = validate_descriptor(descriptor(2, 4)).unwrap_err();
        assert_eq!(wrong_protocol.code, "invalid_descriptor");
        assert!(wrong_protocol.message.contains("protocol major"));

        let zero_parallelism = validate_descriptor(descriptor(1, 0)).unwrap_err();
        assert_eq!(zero_parallelism.code, "invalid_descriptor");
        assert!(zero_parallelism.message.contains("read_parallelism"));
    }

    #[test]
    fn descriptor_rejects_zero_limits() {
        let mut value = descriptor(1, 4);
        value.limits.max_batch_read_items = Some(0);
        let error = validate_descriptor(value).unwrap_err();
        assert_eq!(error.code, "invalid_descriptor");
        assert!(error.message.contains("max_batch_read_items"));
    }

    #[test]
    fn descriptor_rejects_atomic_hint_without_hint_support() {
        let mut value = descriptor(1, 4);
        value.capabilities.hints = false;
        let error = validate_descriptor(value).unwrap_err();
        assert_eq!(error.code, "invalid_descriptor");
        assert!(error.message.contains("atomic_nodes_and_hint"));
    }

    struct OrderedBatchStore;

    #[async_trait::async_trait]
    impl ForeignRemoteStore for OrderedBatchStore {
        async fn descriptor(&self) -> StoreDescriptorResultRecord {
            StoreDescriptorResultRecord {
                value: Some(descriptor(1, 4)),
                error: None,
            }
        }

        async fn batch_get_nodes_ordered(
            &self,
            _cids: Vec<Vec<u8>>,
        ) -> OptionalBytesListResultRecord {
            OptionalBytesListResultRecord {
                values: vec![
                    OptionalBytesRecord {
                        present: true,
                        value: b"second".to_vec(),
                    },
                    OptionalBytesRecord {
                        present: false,
                        value: Vec::new(),
                    },
                ],
                error: None,
            }
        }

        async fn get_node(&self, cid: Vec<u8>) -> OptionalBytesResultRecord {
            if cid == b"fail" {
                return OptionalBytesResultRecord {
                    value: OptionalBytesRecord {
                        present: false,
                        value: Vec::new(),
                    },
                    error: Some(StoreErrorRecord {
                        code: "throttled".to_string(),
                        message: "slow down".to_string(),
                        retryable: true,
                        provider_code: Some("429".to_string()),
                    }),
                };
            }
            OptionalBytesResultRecord {
                value: OptionalBytesRecord {
                    present: true,
                    value: cid,
                },
                error: None,
            }
        }

        async fn put_node(&self, _cid: Vec<u8>, _value: Vec<u8>) -> UnitResultRecord {
            UnitResultRecord { error: None }
        }

        async fn delete_node(&self, _cid: Vec<u8>) -> UnitResultRecord {
            UnitResultRecord { error: None }
        }

        async fn batch_nodes(&self, _ops: Vec<NodeMutationRecord>) -> UnitResultRecord {
            UnitResultRecord { error: None }
        }

        async fn list_node_cids(&self) -> BytesListResultRecord {
            BytesListResultRecord {
                values: vec![b"b".to_vec(), b"a".to_vec()],
                error: None,
            }
        }

        async fn get_hint(
            &self,
            namespace: Vec<u8>,
            key: Vec<u8>,
        ) -> OptionalBytesResultRecord {
            OptionalBytesResultRecord {
                value: OptionalBytesRecord {
                    present: true,
                    value: [namespace, key].concat(),
                },
                error: None,
            }
        }

        async fn put_hint(
            &self,
            _namespace: Vec<u8>,
            _key: Vec<u8>,
            _value: Vec<u8>,
        ) -> UnitResultRecord {
            UnitResultRecord { error: None }
        }

        async fn batch_put_nodes_with_hint(
            &self,
            _nodes: Vec<NodeEntryRecord>,
            _namespace: Vec<u8>,
            _key: Vec<u8>,
            _value: Vec<u8>,
        ) -> UnitResultRecord {
            UnitResultRecord { error: None }
        }

        async fn get_root_manifest(&self, name: Vec<u8>) -> OptionalBytesResultRecord {
            OptionalBytesResultRecord {
                value: OptionalBytesRecord {
                    present: true,
                    value: name,
                },
                error: None,
            }
        }

        async fn put_root_manifest(
            &self,
            _name: Vec<u8>,
            _manifest: Vec<u8>,
        ) -> UnitResultRecord {
            UnitResultRecord { error: None }
        }

        async fn delete_root_manifest(&self, _name: Vec<u8>) -> UnitResultRecord {
            UnitResultRecord { error: None }
        }

        async fn compare_and_swap_root_manifest(
            &self,
            _name: Vec<u8>,
            _expected: OptionalBytesRecord,
            _new: OptionalBytesRecord,
        ) -> RootCasResultRecord {
            RootCasResultRecord {
                applied: true,
                current: OptionalBytesRecord {
                    present: false,
                    value: Vec::new(),
                },
                error: None,
            }
        }

        async fn list_root_manifests(&self) -> NamedBytesListResultRecord {
            NamedBytesListResultRecord {
                values: vec![NamedBytesRecord {
                    name: b"main".to_vec(),
                    value: b"manifest".to_vec(),
                }],
                error: None,
            }
        }

        async fn commit_transaction(
            &self,
            _nodes: Vec<NodeMutationRecord>,
            _conditions: Vec<RootConditionRecord>,
            _roots: Vec<RootWriteRecord>,
        ) -> TransactionResultRecord {
            TransactionResultRecord {
                applied: true,
                conflict: None,
                error: None,
            }
        }
    }

    #[test]
    fn foreign_backend_preserves_batch_order() {
        block_on(async {
            let backend = ForeignRemoteBackend::new(Arc::new(OrderedBatchStore))
                .await
                .unwrap();
            let values = backend
                .batch_get_nodes_ordered(&[b"second", b"missing"])
                .await
                .unwrap();
            assert_eq!(values, vec![Some(b"second".to_vec()), None]);
        });
    }

    #[test]
    fn foreign_backend_maps_complete_protocol_and_structured_errors() {
        block_on(async {
            let backend = ForeignRemoteBackend::new(Arc::new(OrderedBatchStore))
                .await
                .unwrap();
            assert_eq!(backend.get_node(b"node").await.unwrap(), Some(b"node".to_vec()));
            backend.put_node(b"node", b"value").await.unwrap();
            backend.delete_node(b"node").await.unwrap();
            backend
                .batch_nodes(&[RemoteBatchOp::Upsert {
                    key: b"node",
                    value: b"value",
                }])
                .await
                .unwrap();
            assert_eq!(backend.list_node_cids().await.unwrap(), vec![b"a".to_vec(), b"b".to_vec()]);
            assert_eq!(backend.get_hint(b"ns", b"key").await.unwrap(), Some(b"nskey".to_vec()));
            backend.put_hint(b"ns", b"key", b"value").await.unwrap();
            backend
                .batch_put_nodes_with_hint(&[(b"node", b"value")], b"ns", b"key", b"hint")
                .await
                .unwrap();
            assert_eq!(backend.get_root_manifest(b"main").await.unwrap(), Some(b"main".to_vec()));
            backend.put_root_manifest(b"main", b"manifest").await.unwrap();
            backend.delete_root_manifest(b"main").await.unwrap();
            assert_eq!(
                backend
                    .compare_and_swap_root_manifest(b"main", None, Some(b"manifest"))
                    .await
                    .unwrap(),
                RemoteManifestUpdate::Applied
            );
            assert_eq!(
                backend.list_root_manifests().await.unwrap(),
                vec![RemoteNamedRoot::new(b"main".to_vec(), b"manifest".to_vec())]
            );
            assert_eq!(
                backend.commit_transaction(&[], &[], &[]).await.unwrap(),
                RemoteTransactionUpdate::Applied
            );

            let error = backend.get_node(b"fail").await.unwrap_err();
            assert_eq!(error.0.code, "throttled");
            assert!(error.0.retryable);
            assert_eq!(error.0.provider_code.as_deref(), Some("429"));
        });
    }
}
