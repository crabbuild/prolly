use super::{
    to_napi_error, NodeConfigRecord, NodeEntryRecord, NodeMutationRecord,
    NodeNamedRootUpdateRecord, NodeTransactionUpdateRecord, NodeTreeRecord,
};
use napi::bindgen_prelude::{Buffer, Error, Promise, Result, Status};
use napi::threadsafe_function::{ErrorStrategy, ThreadsafeFunction};
use napi_derive::napi;
use prolly_bindings::{
    default_config, AsyncProllyEngine as BindingRemoteEngine,
    AsyncProllyTransaction as BindingRemoteTransaction, BytesListResultRecord, ForeignRemoteStore,
    NamedBytesListResultRecord, NamedBytesRecord, NodeEntryRecord as StoreNodeEntryRecord,
    NodeMutationRecord as StoreNodeMutationRecord, OptionalBytesListResultRecord,
    OptionalBytesRecord, OptionalBytesResultRecord, RootCasResultRecord, RootConditionRecord,
    RootWriteRecord, StoreCapabilitiesRecord, StoreDescriptorRecord, StoreDescriptorResultRecord,
    StoreErrorRecord, StoreTransactionConflictRecord, TransactionResultRecord, TreeRecord,
    UnitResultRecord,
};
use std::sync::{Arc, Mutex};

tokio::task_local! {
    static ACTIVE_REQUEST_ID: String;
}

#[napi(object)]
pub struct NodeRemoteStoreCapabilitiesRecord {
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

#[napi(object)]
pub struct NodeRemoteStoreLimitsRecord {
    pub max_batch_read_items: Option<u32>,
    pub max_batch_write_items: Option<u32>,
    pub max_transaction_operations: Option<u32>,
    pub max_node_bytes: Option<String>,
}

#[napi(object)]
pub struct NodeRemoteStoreDescriptorRecord {
    pub protocol_major: u32,
    pub adapter_name: String,
    pub provider: String,
    pub schema_version: u32,
    pub capabilities: NodeRemoteStoreCapabilitiesRecord,
    pub limits: NodeRemoteStoreLimitsRecord,
}

#[napi(object)]
pub struct NodeRemoteStoreErrorRecord {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub provider_code: Option<String>,
}

#[napi(object)]
pub struct NodeRemoteOptionalBytesRecord {
    pub present: bool,
    pub value: Buffer,
}

#[napi(object)]
pub struct NodeRemoteMutationRecord {
    pub cid: Buffer,
    pub value: NodeRemoteOptionalBytesRecord,
}

#[napi(object)]
pub struct NodeRemoteEntryRecord {
    pub cid: Buffer,
    pub node: Buffer,
}

#[napi(object)]
pub struct NodeRemoteNamedRootRecord {
    pub name: Buffer,
    pub manifest: Buffer,
}

#[napi(object)]
pub struct NodeRemoteRootConditionRecord {
    pub name: Buffer,
    pub expected: NodeRemoteOptionalBytesRecord,
}

#[napi(object)]
pub struct NodeRemoteRootWriteRecord {
    pub name: Buffer,
    pub replacement: NodeRemoteOptionalBytesRecord,
}

#[napi(object)]
pub struct NodeRemoteRootCasRecord {
    pub applied: bool,
    pub current: NodeRemoteOptionalBytesRecord,
}

#[napi(object)]
pub struct NodeRemoteTransactionConflictRecord {
    pub name: Buffer,
    pub expected: NodeRemoteOptionalBytesRecord,
    pub current: NodeRemoteOptionalBytesRecord,
}

#[napi(object)]
pub struct NodeRemoteTransactionRecord {
    pub applied: bool,
    pub conflict: Option<NodeRemoteTransactionConflictRecord>,
}

#[napi(object)]
pub struct NodeRemoteStoreRequest {
    pub operation: String,
    pub request_id: String,
    pub bytes: Option<Vec<Buffer>>,
    pub optional_bytes: Option<Vec<NodeRemoteOptionalBytesRecord>>,
    pub mutations: Option<Vec<NodeRemoteMutationRecord>>,
    pub entries: Option<Vec<NodeRemoteEntryRecord>>,
    pub conditions: Option<Vec<NodeRemoteRootConditionRecord>>,
    pub roots: Option<Vec<NodeRemoteRootWriteRecord>>,
}

#[napi(object)]
pub struct NodeRemoteStoreResponse {
    pub descriptor: Option<NodeRemoteStoreDescriptorRecord>,
    pub optional_bytes: Option<NodeRemoteOptionalBytesRecord>,
    pub optional_values: Option<Vec<NodeRemoteOptionalBytesRecord>>,
    pub bytes_values: Option<Vec<Buffer>>,
    pub named_roots: Option<Vec<NodeRemoteNamedRootRecord>>,
    pub root_cas: Option<NodeRemoteRootCasRecord>,
    pub transaction: Option<NodeRemoteTransactionRecord>,
    pub error: Option<NodeRemoteStoreErrorRecord>,
}

type StoreDispatcher = ThreadsafeFunction<NodeRemoteStoreRequest, ErrorStrategy::Fatal>;

struct NodePromiseStore {
    dispatcher: StoreDispatcher,
}

impl NodePromiseStore {
    fn request(operation: &str) -> NodeRemoteStoreRequest {
        NodeRemoteStoreRequest {
            operation: operation.to_string(),
            request_id: ACTIVE_REQUEST_ID
                .try_with(Clone::clone)
                .unwrap_or_else(|_| "unscoped".to_string()),
            bytes: None,
            optional_bytes: None,
            mutations: None,
            entries: None,
            conditions: None,
            roots: None,
        }
    }

    async fn dispatch(
        &self,
        request: NodeRemoteStoreRequest,
    ) -> std::result::Result<NodeRemoteStoreResponse, StoreErrorRecord> {
        let promise: Promise<NodeRemoteStoreResponse> = self
            .dispatcher
            .call_async(request)
            .await
            .map_err(callback_error)?;
        promise.await.map_err(callback_error)
    }
}

#[async_trait::async_trait]
impl ForeignRemoteStore for NodePromiseStore {
    async fn descriptor(&self) -> StoreDescriptorResultRecord {
        match self.dispatch(Self::request("descriptor")).await {
            Ok(response) => StoreDescriptorResultRecord {
                value: response.descriptor.map(Into::into),
                error: response.error.map(Into::into),
            },
            Err(error) => StoreDescriptorResultRecord {
                value: None,
                error: Some(error),
            },
        }
    }

    async fn get_node(&self, cid: Vec<u8>) -> OptionalBytesResultRecord {
        let mut request = Self::request("getNode");
        request.bytes = Some(vec![Buffer::from(cid)]);
        optional_response(self.dispatch(request).await)
    }

    async fn put_node(&self, cid: Vec<u8>, value: Vec<u8>) -> UnitResultRecord {
        let mut request = Self::request("putNode");
        request.bytes = Some(vec![Buffer::from(cid), Buffer::from(value)]);
        unit_response(self.dispatch(request).await)
    }

    async fn delete_node(&self, cid: Vec<u8>) -> UnitResultRecord {
        let mut request = Self::request("deleteNode");
        request.bytes = Some(vec![Buffer::from(cid)]);
        unit_response(self.dispatch(request).await)
    }

    async fn batch_nodes(&self, ops: Vec<StoreNodeMutationRecord>) -> UnitResultRecord {
        let mut request = Self::request("batchNodes");
        request.mutations = Some(ops.into_iter().map(Into::into).collect());
        unit_response(self.dispatch(request).await)
    }

    async fn batch_get_nodes_ordered(&self, cids: Vec<Vec<u8>>) -> OptionalBytesListResultRecord {
        let mut request = Self::request("batchGetNodesOrdered");
        request.bytes = Some(cids.into_iter().map(Buffer::from).collect());
        match self.dispatch(request).await {
            Ok(response) => OptionalBytesListResultRecord {
                values: response
                    .optional_values
                    .unwrap_or_default()
                    .into_iter()
                    .map(Into::into)
                    .collect(),
                error: response.error.map(Into::into),
            },
            Err(error) => OptionalBytesListResultRecord {
                values: Vec::new(),
                error: Some(error),
            },
        }
    }

    async fn list_node_cids(&self) -> BytesListResultRecord {
        bytes_list_response(self.dispatch(Self::request("listNodeCids")).await)
    }

    async fn get_hint(&self, namespace: Vec<u8>, key: Vec<u8>) -> OptionalBytesResultRecord {
        let mut request = Self::request("getHint");
        request.bytes = Some(vec![Buffer::from(namespace), Buffer::from(key)]);
        optional_response(self.dispatch(request).await)
    }

    async fn put_hint(&self, namespace: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> UnitResultRecord {
        let mut request = Self::request("putHint");
        request.bytes = Some(vec![
            Buffer::from(namespace),
            Buffer::from(key),
            Buffer::from(value),
        ]);
        unit_response(self.dispatch(request).await)
    }

    async fn batch_put_nodes_with_hint(
        &self,
        nodes: Vec<StoreNodeEntryRecord>,
        namespace: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> UnitResultRecord {
        let mut request = Self::request("batchPutNodesWithHint");
        request.entries = Some(nodes.into_iter().map(Into::into).collect());
        request.bytes = Some(vec![
            Buffer::from(namespace),
            Buffer::from(key),
            Buffer::from(value),
        ]);
        unit_response(self.dispatch(request).await)
    }

    async fn get_root_manifest(&self, name: Vec<u8>) -> OptionalBytesResultRecord {
        let mut request = Self::request("getRootManifest");
        request.bytes = Some(vec![Buffer::from(name)]);
        optional_response(self.dispatch(request).await)
    }

    async fn put_root_manifest(&self, name: Vec<u8>, manifest: Vec<u8>) -> UnitResultRecord {
        let mut request = Self::request("putRootManifest");
        request.bytes = Some(vec![Buffer::from(name), Buffer::from(manifest)]);
        unit_response(self.dispatch(request).await)
    }

    async fn delete_root_manifest(&self, name: Vec<u8>) -> UnitResultRecord {
        let mut request = Self::request("deleteRootManifest");
        request.bytes = Some(vec![Buffer::from(name)]);
        unit_response(self.dispatch(request).await)
    }

    async fn compare_and_swap_root_manifest(
        &self,
        name: Vec<u8>,
        expected: OptionalBytesRecord,
        new: OptionalBytesRecord,
    ) -> RootCasResultRecord {
        let mut request = Self::request("compareAndSwapRootManifest");
        request.bytes = Some(vec![Buffer::from(name)]);
        request.optional_bytes = Some(vec![expected.into(), new.into()]);
        match self.dispatch(request).await {
            Ok(response) => match response.root_cas {
                Some(value) => RootCasResultRecord {
                    applied: value.applied,
                    current: value.current.into(),
                    error: response.error.map(Into::into),
                },
                None => RootCasResultRecord {
                    applied: false,
                    current: missing_optional(),
                    error: response
                        .error
                        .map(Into::into)
                        .or_else(|| Some(missing_field("rootCas"))),
                },
            },
            Err(error) => RootCasResultRecord {
                applied: false,
                current: missing_optional(),
                error: Some(error),
            },
        }
    }

    async fn list_root_manifests(&self) -> NamedBytesListResultRecord {
        match self.dispatch(Self::request("listRootManifests")).await {
            Ok(response) => NamedBytesListResultRecord {
                values: response
                    .named_roots
                    .unwrap_or_default()
                    .into_iter()
                    .map(|root| NamedBytesRecord {
                        name: root.name.to_vec(),
                        value: root.manifest.to_vec(),
                    })
                    .collect(),
                error: response.error.map(Into::into),
            },
            Err(error) => NamedBytesListResultRecord {
                values: Vec::new(),
                error: Some(error),
            },
        }
    }

    async fn commit_transaction(
        &self,
        nodes: Vec<StoreNodeMutationRecord>,
        conditions: Vec<RootConditionRecord>,
        roots: Vec<RootWriteRecord>,
    ) -> TransactionResultRecord {
        let mut request = Self::request("commitTransaction");
        request.mutations = Some(nodes.into_iter().map(Into::into).collect());
        request.conditions = Some(conditions.into_iter().map(Into::into).collect());
        request.roots = Some(roots.into_iter().map(Into::into).collect());
        match self.dispatch(request).await {
            Ok(response) => match response.transaction {
                Some(value) => TransactionResultRecord {
                    applied: value.applied,
                    conflict: value.conflict.map(Into::into),
                    error: response.error.map(Into::into),
                },
                None => TransactionResultRecord {
                    applied: false,
                    conflict: None,
                    error: response
                        .error
                        .map(Into::into)
                        .or_else(|| Some(missing_field("transaction"))),
                },
            },
            Err(error) => TransactionResultRecord {
                applied: false,
                conflict: None,
                error: Some(error),
            },
        }
    }
}

#[napi]
pub struct NativeRemoteProllyEngine {
    inner: Mutex<Option<Arc<BindingRemoteEngine>>>,
    config: prolly_bindings::ConfigRecord,
}

#[napi]
pub struct NativeRemoteProllyTransaction {
    inner: Mutex<Option<Arc<BindingRemoteTransaction>>>,
    config: prolly_bindings::ConfigRecord,
}

impl NativeRemoteProllyTransaction {
    fn transaction(&self) -> Result<Arc<BindingRemoteTransaction>> {
        self.inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "remote transaction lock poisoned"))?
            .clone()
            .ok_or_else(|| Error::new(Status::InvalidArg, "remote prolly transaction is closed"))
    }

    fn tree(&self, tree: NodeTreeRecord) -> TreeRecord {
        tree.into_tree(self.config.clone())
    }
}

#[napi]
impl NativeRemoteProllyTransaction {
    #[napi]
    pub async fn create(&self, request_id: String) -> Result<NodeTreeRecord> {
        let transaction = self.transaction()?;
        ACTIVE_REQUEST_ID
            .scope(request_id, transaction.create())
            .await
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub async fn get(
        &self,
        tree: NodeTreeRecord,
        key: Buffer,
        request_id: String,
    ) -> Result<Option<Buffer>> {
        let transaction = self.transaction()?;
        ACTIVE_REQUEST_ID
            .scope(request_id, transaction.get(self.tree(tree), key.to_vec()))
            .await
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi]
    pub async fn put(
        &self,
        tree: NodeTreeRecord,
        key: Buffer,
        value: Buffer,
        request_id: String,
    ) -> Result<NodeTreeRecord> {
        let transaction = self.transaction()?;
        ACTIVE_REQUEST_ID
            .scope(
                request_id,
                transaction.put(self.tree(tree), key.to_vec(), value.to_vec()),
            )
            .await
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "publishNamedRoot")]
    pub async fn publish_named_root(
        &self,
        name: Buffer,
        tree: NodeTreeRecord,
        request_id: String,
    ) -> Result<()> {
        let transaction = self.transaction()?;
        ACTIVE_REQUEST_ID
            .scope(
                request_id,
                transaction.publish_named_root(name.to_vec(), self.tree(tree)),
            )
            .await
            .map_err(to_napi_error)
    }

    #[napi]
    pub async fn commit(&self, request_id: String) -> Result<NodeTransactionUpdateRecord> {
        let transaction = self.transaction()?;
        ACTIVE_REQUEST_ID
            .scope(request_id, transaction.commit())
            .await
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub async fn rollback(&self, request_id: String) -> Result<()> {
        let transaction = self.transaction()?;
        ACTIVE_REQUEST_ID
            .scope(request_id, transaction.rollback())
            .await
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn close(&self) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "remote transaction lock poisoned"))?
            .take();
        Ok(())
    }
}

impl NativeRemoteProllyEngine {
    fn engine(&self) -> Result<Arc<BindingRemoteEngine>> {
        self.inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "remote engine lock poisoned"))?
            .clone()
            .ok_or_else(|| Error::new(Status::InvalidArg, "remote prolly engine is closed"))
    }

    fn tree(&self, tree: NodeTreeRecord) -> TreeRecord {
        tree.into_tree(self.config.clone())
    }
}

#[napi]
impl NativeRemoteProllyEngine {
    #[napi(
        factory,
        ts_args_type = "dispatcher: (request: NodeRemoteStoreRequest) => Promise<NodeRemoteStoreResponse>, config: NodeConfigRecord | null | undefined, requestId: string"
    )]
    pub async fn open(
        dispatcher: StoreDispatcher,
        config: Option<NodeConfigRecord>,
        request_id: String,
    ) -> Result<Self> {
        let config = config
            .map(TryInto::try_into)
            .transpose()?
            .unwrap_or_else(default_config);
        let store: Arc<dyn ForeignRemoteStore> = Arc::new(NodePromiseStore { dispatcher });
        let engine = ACTIVE_REQUEST_ID
            .scope(request_id, BindingRemoteEngine::new(store, config.clone()))
            .await
            .map_err(to_napi_error)?;
        Ok(Self {
            inner: Mutex::new(Some(Arc::new(engine))),
            config,
        })
    }

    #[napi]
    pub fn create(&self) -> Result<NodeTreeRecord> {
        Ok(self.engine()?.create().into())
    }

    #[napi]
    pub async fn get(
        &self,
        tree: NodeTreeRecord,
        key: Buffer,
        request_id: String,
    ) -> Result<Option<Buffer>> {
        let engine = self.engine()?;
        ACTIVE_REQUEST_ID
            .scope(request_id, engine.get(self.tree(tree), key.to_vec()))
            .await
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi]
    pub async fn put(
        &self,
        tree: NodeTreeRecord,
        key: Buffer,
        value: Buffer,
        request_id: String,
    ) -> Result<NodeTreeRecord> {
        let engine = self.engine()?;
        ACTIVE_REQUEST_ID
            .scope(
                request_id,
                engine.put(self.tree(tree), key.to_vec(), value.to_vec()),
            )
            .await
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "getMany")]
    pub async fn get_many(
        &self,
        tree: NodeTreeRecord,
        keys: Vec<Buffer>,
        request_id: String,
    ) -> Result<Vec<Option<Buffer>>> {
        let engine = self.engine()?;
        ACTIVE_REQUEST_ID
            .scope(
                request_id,
                engine.get_many(
                    self.tree(tree),
                    keys.into_iter().map(|key| key.to_vec()).collect(),
                ),
            )
            .await
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| value.map(Buffer::from))
                    .collect()
            })
            .map_err(to_napi_error)
    }

    #[napi]
    pub async fn delete(
        &self,
        tree: NodeTreeRecord,
        key: Buffer,
        request_id: String,
    ) -> Result<NodeTreeRecord> {
        let engine = self.engine()?;
        ACTIVE_REQUEST_ID
            .scope(request_id, engine.delete(self.tree(tree), key.to_vec()))
            .await
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub async fn batch(
        &self,
        tree: NodeTreeRecord,
        mutations: Vec<NodeMutationRecord>,
        request_id: String,
    ) -> Result<NodeTreeRecord> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        let engine = self.engine()?;
        ACTIVE_REQUEST_ID
            .scope(request_id, engine.batch(self.tree(tree), mutations))
            .await
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub async fn range(
        &self,
        tree: NodeTreeRecord,
        start: Buffer,
        end: Option<Buffer>,
        request_id: String,
    ) -> Result<Vec<NodeEntryRecord>> {
        let engine = self.engine()?;
        ACTIVE_REQUEST_ID
            .scope(
                request_id,
                engine.range(
                    self.tree(tree),
                    start.to_vec(),
                    end.map(|value| value.to_vec()),
                ),
            )
            .await
            .map(|entries| entries.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "loadNamedRoot")]
    pub async fn load_named_root(
        &self,
        name: Buffer,
        request_id: String,
    ) -> Result<Option<NodeTreeRecord>> {
        let engine = self.engine()?;
        ACTIVE_REQUEST_ID
            .scope(request_id, engine.load_named_root(name.to_vec()))
            .await
            .map(|tree| tree.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "publishNamedRoot")]
    pub async fn publish_named_root(
        &self,
        name: Buffer,
        tree: NodeTreeRecord,
        request_id: String,
    ) -> Result<()> {
        let engine = self.engine()?;
        ACTIVE_REQUEST_ID
            .scope(
                request_id,
                engine.publish_named_root(name.to_vec(), self.tree(tree)),
            )
            .await
            .map_err(to_napi_error)
    }

    #[napi(js_name = "compareAndSwapNamedRoot")]
    pub async fn compare_and_swap_named_root(
        &self,
        name: Buffer,
        expected: Option<NodeTreeRecord>,
        replacement: Option<NodeTreeRecord>,
        request_id: String,
    ) -> Result<NodeNamedRootUpdateRecord> {
        let expected = expected.map(|tree| self.tree(tree));
        let replacement = replacement.map(|tree| self.tree(tree));
        let engine = self.engine()?;
        ACTIVE_REQUEST_ID
            .scope(
                request_id,
                engine.compare_and_swap_named_root(name.to_vec(), expected, replacement),
            )
            .await
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "beginTransaction")]
    pub async fn begin_transaction(
        &self,
        request_id: String,
    ) -> Result<NativeRemoteProllyTransaction> {
        let engine = self.engine()?;
        let transaction = ACTIVE_REQUEST_ID
            .scope(request_id, engine.begin_transaction())
            .await
            .map_err(to_napi_error)?;
        Ok(NativeRemoteProllyTransaction {
            inner: Mutex::new(Some(transaction)),
            config: self.config.clone(),
        })
    }

    #[napi]
    pub fn close(&self) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| Error::new(Status::GenericFailure, "remote engine lock poisoned"))?
            .take();
        Ok(())
    }
}

fn optional_response(
    response: std::result::Result<NodeRemoteStoreResponse, StoreErrorRecord>,
) -> OptionalBytesResultRecord {
    match response {
        Ok(response) => OptionalBytesResultRecord {
            value: response
                .optional_bytes
                .map(Into::into)
                .unwrap_or_else(missing_optional),
            error: response.error.map(Into::into),
        },
        Err(error) => OptionalBytesResultRecord {
            value: missing_optional(),
            error: Some(error),
        },
    }
}

fn unit_response(
    response: std::result::Result<NodeRemoteStoreResponse, StoreErrorRecord>,
) -> UnitResultRecord {
    UnitResultRecord {
        error: match response {
            Ok(response) => response.error.map(Into::into),
            Err(error) => Some(error),
        },
    }
}

fn bytes_list_response(
    response: std::result::Result<NodeRemoteStoreResponse, StoreErrorRecord>,
) -> BytesListResultRecord {
    match response {
        Ok(response) => BytesListResultRecord {
            values: response
                .bytes_values
                .unwrap_or_default()
                .into_iter()
                .map(|value| value.to_vec())
                .collect(),
            error: response.error.map(Into::into),
        },
        Err(error) => BytesListResultRecord {
            values: Vec::new(),
            error: Some(error),
        },
    }
}

fn callback_error(_: Error) -> StoreErrorRecord {
    StoreErrorRecord {
        code: "internal".to_string(),
        message: "remote store callback rejected or returned an invalid value".to_string(),
        retryable: false,
        provider_code: None,
    }
}

fn missing_field(field: &str) -> StoreErrorRecord {
    StoreErrorRecord {
        code: "invalid_data".to_string(),
        message: format!("remote store response omitted {field}"),
        retryable: false,
        provider_code: None,
    }
}

fn missing_optional() -> OptionalBytesRecord {
    OptionalBytesRecord {
        present: false,
        value: Vec::new(),
    }
}

impl From<NodeRemoteStoreCapabilitiesRecord> for StoreCapabilitiesRecord {
    fn from(value: NodeRemoteStoreCapabilitiesRecord) -> Self {
        Self {
            native_batch_reads: value.native_batch_reads,
            atomic_batch_writes: value.atomic_batch_writes,
            node_scan: value.node_scan,
            hints: value.hints,
            atomic_nodes_and_hint: value.atomic_nodes_and_hint,
            root_scan: value.root_scan,
            root_compare_and_swap: value.root_compare_and_swap,
            transactions: value.transactions,
            read_parallelism: value.read_parallelism,
        }
    }
}

impl From<NodeRemoteStoreDescriptorRecord> for StoreDescriptorRecord {
    fn from(value: NodeRemoteStoreDescriptorRecord) -> Self {
        Self {
            protocol_major: value.protocol_major,
            adapter_name: value.adapter_name,
            provider: value.provider,
            schema_version: value.schema_version,
            capabilities: value.capabilities.into(),
            limits: prolly_bindings::StoreLimitsRecord {
                max_batch_read_items: value.limits.max_batch_read_items,
                max_batch_write_items: value.limits.max_batch_write_items,
                max_transaction_operations: value.limits.max_transaction_operations,
                max_node_bytes: value
                    .limits
                    .max_node_bytes
                    .and_then(|item| item.parse().ok()),
            },
        }
    }
}

impl From<NodeRemoteStoreErrorRecord> for StoreErrorRecord {
    fn from(value: NodeRemoteStoreErrorRecord) -> Self {
        Self {
            code: value.code,
            message: value.message,
            retryable: value.retryable,
            provider_code: value.provider_code,
        }
    }
}

impl From<OptionalBytesRecord> for NodeRemoteOptionalBytesRecord {
    fn from(value: OptionalBytesRecord) -> Self {
        Self {
            present: value.present,
            value: Buffer::from(value.value),
        }
    }
}

impl From<NodeRemoteOptionalBytesRecord> for OptionalBytesRecord {
    fn from(value: NodeRemoteOptionalBytesRecord) -> Self {
        Self {
            present: value.present,
            value: value.value.to_vec(),
        }
    }
}

impl From<StoreNodeMutationRecord> for NodeRemoteMutationRecord {
    fn from(value: StoreNodeMutationRecord) -> Self {
        Self {
            cid: Buffer::from(value.key),
            value: value.value.into(),
        }
    }
}

impl From<StoreNodeEntryRecord> for NodeRemoteEntryRecord {
    fn from(value: StoreNodeEntryRecord) -> Self {
        Self {
            cid: Buffer::from(value.key),
            node: Buffer::from(value.value),
        }
    }
}

impl From<RootConditionRecord> for NodeRemoteRootConditionRecord {
    fn from(value: RootConditionRecord) -> Self {
        Self {
            name: Buffer::from(value.name),
            expected: value.expected.into(),
        }
    }
}

impl From<RootWriteRecord> for NodeRemoteRootWriteRecord {
    fn from(value: RootWriteRecord) -> Self {
        Self {
            name: Buffer::from(value.name),
            replacement: value.replacement.into(),
        }
    }
}

impl From<NodeRemoteTransactionConflictRecord> for StoreTransactionConflictRecord {
    fn from(value: NodeRemoteTransactionConflictRecord) -> Self {
        Self {
            name: value.name.to_vec(),
            expected: value.expected.into(),
            current: value.current.into(),
        }
    }
}
