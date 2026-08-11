use std::collections::HashMap;
use std::sync::Arc;

use prolly::{ProllyCacheUsage, RemoteProllyStore, RemoteStoreConfig};
use prolly_dynamodb_core::{
    BulkImportOptions, BulkImportResult, Clock, Database, IdGenerator, ImportAuditRecord,
    ImportPlanId, KeyAttribute, LargeValueConfig,
    MaintenanceContext, MaintenanceLease, MaintenanceLeaseId, MaintenanceLeaseRelease,
    StoragePublicationMode,
};
use prolly_store_dynamodb::{
    dynamodb_safe_config, DynamoDbBackend, DynamoDbStore, TransactionPublicationMode,
};

use crate::blob::dynamo_blob_storage;
use crate::operation::{
    BatchGetItem, BatchWriteItem, CreateTable, DeleteItem, DeleteTable, DescribeTable, GetItem,
    ListTables, PutItem, Query, Scan, TransactGetItems, TransactWriteItems, UpdateItem,
};
use crate::table::{Import, Table};
use crate::worker::Workers;
use crate::{CapabilityReport, WriteSession};

type CoreDatabase = Database<DynamoDbStore>;

/// Default retained serialized-node weight for one client process.
///
/// This is deliberately lower than the general Prolly engine default because
/// a DynamoDB client also retains AWS SDK buffers and decoded logical records.
/// It is a cache weight, not a hard process-RSS limit.
pub const DEFAULT_NODE_CACHE_MAX_BYTES: usize = 64 * 1024 * 1024;

/// In-process versioned DynamoDB client backed by a caller-configured provider.
#[derive(Clone)]
pub struct Client {
    pub(crate) database: Arc<CoreDatabase>,
    pub(crate) capabilities: Arc<CapabilityReport>,
}

/// Construction inputs for a versioned DynamoDB client.
///
/// Supply either an already-configured backend or an already-configured remote
/// store. A store owns its `RemoteStoreConfig`; a backend may be paired with an
/// explicit adapter configuration here.
#[derive(Clone, Default)]
pub struct ClientBuilder {
    backend: Option<DynamoDbBackend>,
    store: Option<DynamoDbStore>,
    remote_store_config: Option<RemoteStoreConfig>,
    id_generator: Option<Arc<dyn IdGenerator>>,
    clock: Option<Arc<dyn Clock>>,
    logical_retry_limit: Option<usize>,
    node_cache_max_nodes: Option<usize>,
    node_cache_max_bytes: Option<usize>,
}

impl ClientBuilder {
    pub fn backend(mut self, backend: DynamoDbBackend) -> Self {
        self.backend = Some(backend);
        self
    }

    pub fn set_backend(mut self, backend: Option<DynamoDbBackend>) -> Self {
        self.backend = backend;
        self
    }

    pub fn store(mut self, store: DynamoDbStore) -> Self {
        self.store = Some(store);
        self
    }

    pub fn set_store(mut self, store: Option<DynamoDbStore>) -> Self {
        self.store = store;
        self
    }

    pub fn remote_store_config(mut self, config: RemoteStoreConfig) -> Self {
        self.remote_store_config = Some(config);
        self
    }

    pub fn set_remote_store_config(mut self, config: Option<RemoteStoreConfig>) -> Self {
        self.remote_store_config = config;
        self
    }

    /// Override the collision-resistant system ID source.
    ///
    /// This is primarily intended for deterministic conformance and replay
    /// tests. Production implementations must preserve uniqueness across all
    /// writers that share a physical namespace.
    pub fn id_generator(mut self, id_generator: Arc<dyn IdGenerator>) -> Self {
        self.id_generator = Some(id_generator);
        self
    }

    pub fn set_id_generator(mut self, id_generator: Option<Arc<dyn IdGenerator>>) -> Self {
        self.id_generator = id_generator;
        self
    }

    /// Override the wall-clock source used for durable metadata timestamps.
    ///
    /// This is primarily intended for deterministic conformance and replay
    /// tests. Production clocks must be monotonic enough for the documented
    /// lease, token-expiry, and retention assumptions.
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    pub fn set_clock(mut self, clock: Option<Arc<dyn Clock>>) -> Self {
        self.clock = clock;
        self
    }

    /// Configure retries after the first optimistic logical attempt.
    ///
    /// Zero disables retries. The value is runtime-only and does not alter the
    /// durable database format.
    pub fn logical_retry_limit(mut self, retries: usize) -> Self {
        self.logical_retry_limit = Some(retries);
        self
    }

    pub fn set_logical_retry_limit(mut self, retries: Option<usize>) -> Self {
        self.logical_retry_limit = retries;
        self
    }

    /// Bound decoded-node cache entries for this client process. Zero disables
    /// node caching. Pinned correctness-optional hints can temporarily exceed
    /// the configured limit until explicitly unpinned.
    pub fn node_cache_max_nodes(mut self, max_nodes: usize) -> Self {
        self.node_cache_max_nodes = Some(max_nodes);
        self
    }

    pub fn set_node_cache_max_nodes(mut self, max_nodes: Option<usize>) -> Self {
        self.node_cache_max_nodes = max_nodes;
        self
    }

    /// Bound retained serialized-node weight for this client process. Zero
    /// disables node caching. The client default is 64 MiB of retained
    /// serialized-node weight; this is not a hard process-RSS limit.
    pub fn node_cache_max_bytes(mut self, max_bytes: usize) -> Self {
        self.node_cache_max_bytes = Some(max_bytes);
        self
    }

    pub fn set_node_cache_max_bytes(mut self, max_bytes: Option<usize>) -> Self {
        self.node_cache_max_bytes = max_bytes;
        self
    }

    /// Validate construction inputs before performing any provider request.
    #[tracing::instrument(
        name = "prolly_dynamodb.ClientOpen",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "ClientOpen"),
        err
    )]
    pub async fn open(self) -> crate::Result<Client> {
        if self.id_generator.is_some() != self.clock.is_some() {
            return Err(crate::Error::InvalidRequest(
                "ClientBuilder id_generator and clock must be configured together".into(),
            ));
        }
        if self
            .logical_retry_limit
            .is_some_and(|value| value > prolly_dynamodb_core::MAX_LOGICAL_RETRY_LIMIT)
        {
            return Err(crate::Error::InvalidRequest(format!(
                "logical retry limit must be <= {}",
                prolly_dynamodb_core::MAX_LOGICAL_RETRY_LIMIT
            )));
        }
        let id_generator = self.id_generator.clone();
        let clock = self.clock.clone();
        let logical_retry_limit = self.logical_retry_limit;
        let node_cache_max_nodes = self.node_cache_max_nodes;
        let node_cache_max_bytes = self.node_cache_max_bytes;
        Client::open_store_with_sources(
            self.into_store()?,
            id_generator,
            clock,
            logical_retry_limit,
            node_cache_max_nodes,
            node_cache_max_bytes,
        )
        .await
    }

    fn into_store(self) -> crate::Result<DynamoDbStore> {
        match (self.backend, self.store, self.remote_store_config) {
            (Some(backend), None, config) => Ok(RemoteProllyStore::with_config(
                backend,
                config.unwrap_or_default(),
            )),
            (None, Some(store), None) => Ok(store),
            (None, None, _) => Err(crate::Error::InvalidRequest(
                "ClientBuilder requires exactly one of backend or store".into(),
            )),
            (Some(_), Some(_), _) => Err(crate::Error::InvalidRequest(
                "ClientBuilder backend and store inputs are mutually exclusive".into(),
            )),
            (None, Some(_), Some(_)) => Err(crate::Error::InvalidRequest(
                "ClientBuilder remote_store_config cannot override an existing store".into(),
            )),
        }
    }
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    /// Open from an existing backend without changing its AWS client or namespace.
    pub async fn open(backend: DynamoDbBackend) -> crate::Result<Self> {
        Self::open_store(RemoteProllyStore::new(backend)).await
    }

    /// Open from an existing configured remote store.
    #[tracing::instrument(
        name = "prolly_dynamodb.ClientOpenStore",
        level = "debug",
        skip_all,
        fields(db_system = "dynamodb", db_operation = "ClientOpenStore"),
        err
    )]
    pub async fn open_store(store: DynamoDbStore) -> crate::Result<Self> {
        Self::open_store_with_sources(store, None, None, None, None, None).await
    }

    async fn open_store_with_sources(
        store: DynamoDbStore,
        id_generator: Option<Arc<dyn IdGenerator>>,
        clock: Option<Arc<dyn Clock>>,
        logical_retry_limit: Option<usize>,
        node_cache_max_nodes: Option<usize>,
        node_cache_max_bytes: Option<usize>,
    ) -> crate::Result<Self> {
        store.backend().validate_initialized_schema().await?;
        let transaction_capabilities = store.backend().transaction_capabilities();
        let blob_storage = dynamo_blob_storage(&store);
        let publication_mode = match store.backend().transaction_capabilities().publication_mode {
            TransactionPublicationMode::PrepublishImmutableNodes => {
                StoragePublicationMode::PrepublishImmutableNodes
            }
            TransactionPublicationMode::AtomicNodesAndRoots => {
                StoragePublicationMode::AtomicNodesAndRoots
            }
        };
        let mut engine_config = dynamodb_safe_config();
        engine_config.runtime.node_cache_max_bytes = Some(DEFAULT_NODE_CACHE_MAX_BYTES);
        if let Some(max_nodes) = node_cache_max_nodes {
            engine_config.runtime.node_cache_max_nodes = Some(max_nodes);
        }
        if let Some(max_bytes) = node_cache_max_bytes {
            engine_config.runtime.node_cache_max_bytes = Some(max_bytes);
        }
        let database = match (id_generator, clock) {
            (None, None) => {
                Database::open_with_blob_storage_and_mode(
                    store,
                    engine_config,
                    blob_storage,
                    LargeValueConfig::default(),
                    publication_mode,
                )
                .await?
            }
            (Some(id_generator), Some(clock)) => {
                Database::open_with_blob_storage_and_mode_and_sources(
                    store,
                    engine_config,
                    blob_storage,
                    LargeValueConfig::default(),
                    publication_mode,
                    id_generator,
                    clock,
                )
                .await?
            }
            _ => {
                return Err(crate::Error::InvalidRequest(
                    "ClientBuilder id_generator and clock must be configured together".into(),
                ));
            }
        }
        .with_logical_retry_limit(
            logical_retry_limit.unwrap_or(prolly_dynamodb_core::DEFAULT_LOGICAL_RETRY_LIMIT),
        )?;
        let capabilities = Arc::new(CapabilityReport::new(
            transaction_capabilities,
            &database.format_record()?,
            &database.engine().config().runtime,
            database.logical_retry_limit(),
        ));
        Ok(Self {
            database: Arc::new(database),
            capabilities,
        })
    }

    /// Frozen support and provider-limit contract negotiated at open.
    pub fn capabilities(&self) -> &CapabilityReport {
        &self.capabilities
    }

    /// Effective retries after the first optimistic logical attempt.
    pub fn logical_retry_limit(&self) -> usize {
        self.database.logical_retry_limit()
    }

    /// Inspect the exact caller-configured physical backend retained at open.
    pub fn backend(&self) -> &DynamoDbBackend {
        self.database.engine().store().backend()
    }

    /// Inspect the exact remote-adapter policy retained at open.
    pub fn remote_store_config(&self) -> &RemoteStoreConfig {
        self.database.engine().store().config()
    }

    /// Return one internally consistent snapshot of retained node-cache usage.
    ///
    /// Serialized bytes are the weight governed by the configured cache byte
    /// ceiling, not process RSS. Pinned hint entries may temporarily exceed
    /// that ceiling until they are unpinned.
    pub fn cache_usage(&self) -> ProllyCacheUsage {
        self.database.engine().cache_usage()
    }

    pub fn get_item(&self) -> GetItem {
        GetItem::new(self.clone(), None)
    }

    pub fn create_table(&self) -> CreateTable {
        CreateTable::new(self.clone())
    }

    pub fn describe_table(&self) -> DescribeTable {
        DescribeTable::new(self.clone())
    }

    pub fn list_tables(&self) -> ListTables {
        ListTables::new(self.clone())
    }

    pub fn delete_table(&self) -> DeleteTable {
        DeleteTable::new(self.clone())
    }

    pub fn put_item(&self) -> PutItem {
        PutItem::new(self.clone())
    }

    pub fn delete_item(&self) -> DeleteItem {
        DeleteItem::new(self.clone())
    }

    pub fn update_item(&self) -> UpdateItem {
        UpdateItem::new(self.clone())
    }

    pub fn query(&self) -> Query {
        Query::new(self.clone(), None)
    }

    pub fn scan(&self) -> Scan {
        Scan::new(self.clone(), None)
    }

    pub fn batch_get_item(&self) -> BatchGetItem {
        BatchGetItem::new(self.clone())
    }

    pub fn batch_write_item(&self) -> BatchWriteItem {
        BatchWriteItem::new(self.clone())
    }

    pub fn transact_get_items(&self) -> TransactGetItems {
        TransactGetItems::new(self.clone())
    }

    pub fn transact_write_items(&self) -> TransactWriteItems {
        TransactWriteItems::new(self.clone())
    }

    pub fn table(&self, name: impl Into<String>) -> Table {
        Table::new(self.clone(), name.into())
    }

    /// Open an explicit large write session for one logical table.
    pub fn write_session(&self, table_name: impl Into<String>) -> WriteSession {
        WriteSession::new(self.clone(), table_name.into())
    }

    /// Create a table from strictly primary-key-sorted items as one initial
    /// version. This administrative path deliberately does not emulate an AWS
    /// operation builder and never changes `PutItem` or `BatchWriteItem`
    /// commit semantics.
    pub async fn bulk_import_sorted<I>(
        &self,
        table_name: impl Into<String>,
        partition_key: KeyAttribute,
        sort_key: Option<KeyAttribute>,
        items: I,
        options: BulkImportOptions,
    ) -> crate::Result<BulkImportResult>
    where
        I: IntoIterator<Item = HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>,
    {
        Ok(self
            .core()
            .bulk_import_sorted(
                table_name,
                partition_key,
                sort_key,
                items.into_iter().map(|item| {
                    crate::conversion::item_from_aws(item)
                        .map_err(|error| prolly_dynamodb_core::Error::Validation(error.to_string()))
                }),
                options,
            )
            .await?)
    }

    /// Construct explicit leased background workers. Merely opening a client
    /// never creates, acquires, or runs a worker.
    pub fn workers(&self) -> Workers {
        Workers::new(self.clone())
    }

    /// Prepare an explicit dry-run/apply workflow for a verified table archive.
    pub fn import(
        &self,
        archive: prolly_dynamodb_core::TableArchive,
        target_table_name: impl Into<String>,
        limits: prolly_dynamodb_core::TableArchiveLimits,
    ) -> Import {
        Import::new(self.clone(), archive, target_table_name.into(), limits)
    }

    /// Resolve durable import evidence by content-addressed plan identity.
    pub async fn import_audit(
        &self,
        id: &ImportPlanId,
    ) -> crate::Result<Option<ImportAuditRecord>> {
        Ok(self.core().import_audit(id).await?)
    }

    /// Inspect the global fail-closed maintenance writer fence.
    pub async fn maintenance_lease(&self) -> crate::Result<Option<MaintenanceLease>> {
        Ok(self.core().maintenance_lease().await?)
    }

    /// Acquire the global writer fence for destructive physical maintenance.
    pub async fn acquire_maintenance_lease(
        &self,
        context: MaintenanceContext,
        duration_millis: u64,
    ) -> crate::Result<MaintenanceLease> {
        Ok(self
            .core()
            .acquire_maintenance_lease(context, duration_millis)
            .await?)
    }

    /// Release a held writer fence and durably record operator attribution.
    pub async fn release_maintenance_lease(
        &self,
        id: &MaintenanceLeaseId,
        context: MaintenanceContext,
    ) -> crate::Result<MaintenanceLeaseRelease> {
        Ok(self.core().release_maintenance_lease(id, context).await?)
    }

    /// Force-break a crashed holder's fence only after durable expiry.
    pub async fn break_expired_maintenance_lease(
        &self,
        id: &MaintenanceLeaseId,
        context: MaintenanceContext,
    ) -> crate::Result<MaintenanceLeaseRelease> {
        Ok(self
            .core()
            .break_expired_maintenance_lease(id, context)
            .await?)
    }

    pub(crate) fn core(&self) -> &CoreDatabase {
        &self.database
    }

    pub async fn execute_create_table(
        &self,
        input: aws_sdk_dynamodb::operation::create_table::CreateTableInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::create_table::CreateTableOutput> {
        if input.billing_mode.is_some()
            || input.provisioned_throughput.is_some()
            || input.stream_specification.is_some()
            || input.sse_specification.is_some()
            || input.tags.is_some()
            || input.table_class.is_some()
            || input.deletion_protection_enabled.is_some()
            || input.warm_throughput.is_some()
            || input.resource_policy.is_some()
            || input.on_demand_throughput.is_some()
        {
            return Err(crate::Error::Unsupported(
                "CreateTable input contains unsupported capacity, stream, encryption, tag, class, protection, or policy configuration"
                    .into(),
            ));
        }
        self.create_table()
            .table_name(input.table_name.ok_or_else(|| {
                crate::Error::InvalidRequest("CreateTable.table_name is required".into())
            })?)
            .set_attribute_definitions(input.attribute_definitions)
            .set_key_schema(input.key_schema)
            .set_local_secondary_indexes(input.local_secondary_indexes)
            .set_global_secondary_indexes(input.global_secondary_indexes)
            .send()
            .await
    }

    pub async fn execute_describe_table(
        &self,
        input: aws_sdk_dynamodb::operation::describe_table::DescribeTableInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::describe_table::DescribeTableOutput> {
        self.describe_table()
            .table_name(input.table_name.ok_or_else(|| {
                crate::Error::InvalidRequest("DescribeTable.table_name is required".into())
            })?)
            .send()
            .await
    }

    pub async fn execute_list_tables(
        &self,
        input: aws_sdk_dynamodb::operation::list_tables::ListTablesInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::list_tables::ListTablesOutput> {
        self.list_tables()
            .set_exclusive_start_table_name(input.exclusive_start_table_name)
            .set_limit(input.limit)
            .send()
            .await
    }

    pub async fn execute_delete_table(
        &self,
        input: aws_sdk_dynamodb::operation::delete_table::DeleteTableInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::delete_table::DeleteTableOutput> {
        self.delete_table()
            .table_name(input.table_name.ok_or_else(|| {
                crate::Error::InvalidRequest("DeleteTable.table_name is required".into())
            })?)
            .send()
            .await
    }

    /// Execute an official AWS SDK input for the supported GetItem subset.
    pub async fn execute_get_item(
        &self,
        input: aws_sdk_dynamodb::operation::get_item::GetItemInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::get_item::GetItemOutput> {
        if input.attributes_to_get.is_some() || input.return_consumed_capacity.is_some() {
            return Err(crate::Error::Unsupported(
                "GetItem legacy attributes_to_get and capacity fields are not implemented".into(),
            ));
        }
        self.get_item()
            .table_name(input.table_name.ok_or_else(|| {
                crate::Error::InvalidRequest("GetItem.table_name is required".into())
            })?)
            .set_key(input.key)
            .set_projection_expression(input.projection_expression)
            .set_expression_attribute_names(input.expression_attribute_names)
            .send()
            .await
    }

    /// Execute an official AWS SDK input for the supported PutItem subset.
    pub async fn execute_put_item(
        &self,
        input: aws_sdk_dynamodb::operation::put_item::PutItemInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::put_item::PutItemOutput> {
        if input.expected.is_some()
            || input.return_consumed_capacity.is_some()
            || input.return_item_collection_metrics.is_some()
            || input.conditional_operator.is_some()
        {
            return Err(crate::Error::Unsupported(
                "legacy expected, return-value, and capacity PutItem fields are not implemented"
                    .into(),
            ));
        }
        self.put_item()
            .table_name(input.table_name.ok_or_else(|| {
                crate::Error::InvalidRequest("PutItem.table_name is required".into())
            })?)
            .set_item(input.item)
            .set_condition_expression(input.condition_expression)
            .set_expression_attribute_names(input.expression_attribute_names)
            .set_expression_attribute_values(input.expression_attribute_values)
            .set_return_values(input.return_values)
            .set_return_values_on_condition_check_failure(
                input.return_values_on_condition_check_failure,
            )
            .send()
            .await
    }

    /// Execute an official AWS SDK input for the supported DeleteItem subset.
    pub async fn execute_delete_item(
        &self,
        input: aws_sdk_dynamodb::operation::delete_item::DeleteItemInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::delete_item::DeleteItemOutput> {
        if input.expected.is_some()
            || input.conditional_operator.is_some()
            || input.return_consumed_capacity.is_some()
            || input.return_item_collection_metrics.is_some()
        {
            return Err(crate::Error::Unsupported(
                "legacy expected, return-value, and capacity DeleteItem fields are not implemented"
                    .into(),
            ));
        }
        self.delete_item()
            .table_name(input.table_name.ok_or_else(|| {
                crate::Error::InvalidRequest("DeleteItem.table_name is required".into())
            })?)
            .set_key(input.key)
            .set_condition_expression(input.condition_expression)
            .set_expression_attribute_names(input.expression_attribute_names)
            .set_expression_attribute_values(input.expression_attribute_values)
            .set_return_values(input.return_values)
            .set_return_values_on_condition_check_failure(
                input.return_values_on_condition_check_failure,
            )
            .send()
            .await
    }

    /// Execute an official AWS SDK input for the supported UpdateItem subset.
    pub async fn execute_update_item(
        &self,
        input: aws_sdk_dynamodb::operation::update_item::UpdateItemInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::update_item::UpdateItemOutput> {
        if input.attribute_updates.is_some()
            || input.expected.is_some()
            || input.conditional_operator.is_some()
            || input.return_consumed_capacity.is_some()
            || input.return_item_collection_metrics.is_some()
        {
            return Err(crate::Error::Unsupported(
                "legacy updates/expected, capacity, collection metrics, and condition-failure returns are not implemented for UpdateItem"
                    .into(),
            ));
        }
        self.update_item()
            .table_name(input.table_name.ok_or_else(|| {
                crate::Error::InvalidRequest("UpdateItem.table_name is required".into())
            })?)
            .set_key(input.key)
            .set_update_expression(input.update_expression)
            .set_condition_expression(input.condition_expression)
            .set_expression_attribute_names(input.expression_attribute_names)
            .set_expression_attribute_values(input.expression_attribute_values)
            .set_return_values(input.return_values)
            .set_return_values_on_condition_check_failure(
                input.return_values_on_condition_check_failure,
            )
            .send()
            .await
    }

    pub async fn execute_query(
        &self,
        input: aws_sdk_dynamodb::operation::query::QueryInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::query::QueryOutput> {
        if input.attributes_to_get.is_some()
            || input.key_conditions.is_some()
            || input.query_filter.is_some()
            || input.conditional_operator.is_some()
            || input
                .return_consumed_capacity
                .as_ref()
                .is_some_and(|value| {
                    value != &aws_sdk_dynamodb::types::ReturnConsumedCapacity::None
                })
        {
            return Err(crate::Error::Unsupported(
                "Query input contains unsupported legacy condition/filter or capacity fields"
                    .into(),
            ));
        }
        self.query()
            .table_name(input.table_name.ok_or_else(|| {
                crate::Error::InvalidRequest("Query.table_name is required".into())
            })?)
            .set_index_name(input.index_name)
            .set_key_condition_expression(input.key_condition_expression)
            .set_filter_expression(input.filter_expression)
            .set_projection_expression(input.projection_expression)
            .set_expression_attribute_names(input.expression_attribute_names)
            .set_expression_attribute_values(input.expression_attribute_values)
            .set_exclusive_start_key(input.exclusive_start_key)
            .set_limit(input.limit)
            .set_scan_index_forward(input.scan_index_forward)
            .set_select(input.select)
            .set_consistent_read(input.consistent_read)
            .send()
            .await
    }

    pub async fn execute_scan(
        &self,
        input: aws_sdk_dynamodb::operation::scan::ScanInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::scan::ScanOutput> {
        if input.attributes_to_get.is_some()
            || input.scan_filter.is_some()
            || input.conditional_operator.is_some()
            || input
                .return_consumed_capacity
                .as_ref()
                .is_some_and(|value| {
                    value != &aws_sdk_dynamodb::types::ReturnConsumedCapacity::None
                })
            || input.total_segments.is_some()
            || input.segment.is_some()
        {
            return Err(crate::Error::Unsupported(
                "Scan input contains unsupported legacy filter, capacity, or parallel-segment fields"
                    .into(),
            ));
        }
        self.scan()
            .table_name(input.table_name.ok_or_else(|| {
                crate::Error::InvalidRequest("Scan.table_name is required".into())
            })?)
            .set_index_name(input.index_name)
            .set_exclusive_start_key(input.exclusive_start_key)
            .set_filter_expression(input.filter_expression)
            .set_projection_expression(input.projection_expression)
            .set_expression_attribute_names(input.expression_attribute_names)
            .set_expression_attribute_values(input.expression_attribute_values)
            .set_limit(input.limit)
            .set_select(input.select)
            .set_consistent_read(input.consistent_read)
            .send()
            .await
    }

    pub async fn execute_batch_get_item(
        &self,
        input: aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemOutput> {
        self.batch_get_item()
            .set_request_items(input.request_items)
            .set_return_consumed_capacity(input.return_consumed_capacity)
            .send()
            .await
    }

    pub async fn execute_batch_write_item(
        &self,
        input: aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput> {
        self.batch_write_item()
            .set_request_items(input.request_items)
            .set_return_consumed_capacity(input.return_consumed_capacity)
            .set_return_item_collection_metrics(input.return_item_collection_metrics)
            .send()
            .await
    }

    pub async fn execute_transact_get_items(
        &self,
        input: aws_sdk_dynamodb::operation::transact_get_items::TransactGetItemsInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::transact_get_items::TransactGetItemsOutput>
    {
        self.transact_get_items()
            .set_transact_items(input.transact_items)
            .set_return_consumed_capacity(input.return_consumed_capacity)
            .send()
            .await
    }

    pub async fn execute_transact_write_items(
        &self,
        input: aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsInput,
    ) -> crate::Result<aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsOutput>
    {
        self.transact_write_items()
            .set_transact_items(input.transact_items)
            .set_client_request_token(input.client_request_token)
            .set_return_consumed_capacity(input.return_consumed_capacity)
            .set_return_item_collection_metrics(input.return_item_collection_metrics)
            .send()
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
    use std::sync::Mutex;
    use tracing::metadata::LevelFilter;
    use tracing::span::{Attributes, Id, Record};
    use tracing::subscriber::Interest;
    use tracing::{Event, Metadata, Subscriber};

    struct SpanRecorder(Arc<Mutex<Vec<&'static str>>>);

    impl Subscriber for SpanRecorder {
        fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
            // Unit tests execute in parallel while tracing maintains one global
            // callsite cache. Dynamic interest prevents another test thread's
            // dispatcher lifecycle from caching this callsite as disabled.
            Interest::sometimes()
        }

        fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
            true
        }

        fn max_level_hint(&self) -> Option<LevelFilter> {
            Some(LevelFilter::TRACE)
        }

        fn new_span(&self, attributes: &Attributes<'_>) -> Id {
            let mut spans = self.0.lock().unwrap();
            spans.push(attributes.metadata().name());
            Id::from_u64(u64::try_from(spans.len()).unwrap())
        }

        fn record(&self, _span: &Id, _values: &Record<'_>) {}

        fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

        fn event(&self, _event: &Event<'_>) {}

        fn enter(&self, _span: &Id) {}

        fn exit(&self, _span: &Id) {}
    }

    fn backend() -> DynamoDbBackend {
        let config = aws_sdk_dynamodb::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "unit-test"))
            .build();
        DynamoDbBackend::new(
            aws_sdk_dynamodb::Client::from_conf(config),
            "physical-nodes",
        )
        .with_root_table_name("physical-roots")
        .with_key_prefix(b"tenant-a:".to_vec())
        .with_read_parallelism(3)
        .with_batch_get_parallelism(5)
        .with_batch_write_parallelism(7)
        .with_scan_parallelism(11)
    }

    #[test]
    fn builder_preserves_backend_namespace_and_remote_policy() {
        let store = Client::builder()
            .backend(backend())
            .remote_store_config(RemoteStoreConfig {
                verify_node_cids: false,
            })
            .into_store()
            .unwrap();
        assert_eq!(store.backend().table_name(), "physical-nodes");
        assert_eq!(store.backend().root_table_name(), "physical-roots");
        assert_eq!(store.backend().key_prefix(), b"tenant-a:");
        assert_eq!(store.backend().read_parallelism(), 3);
        assert_eq!(store.backend().batch_get_parallelism(), 5);
        assert_eq!(store.backend().batch_write_parallelism(), 7);
        assert_eq!(store.backend().scan_parallelism(), 11);
        assert!(!store.config().verify_node_cids);
    }

    #[test]
    fn builder_rejects_ambiguous_inputs_before_provider_access() {
        let existing = RemoteProllyStore::with_config(
            backend(),
            RemoteStoreConfig {
                verify_node_cids: false,
            },
        );
        assert!(matches!(
            Client::builder()
                .backend(backend())
                .store(existing.clone())
                .into_store(),
            Err(crate::Error::InvalidRequest(message)) if message.contains("mutually exclusive")
        ));
        assert!(matches!(
            Client::builder()
                .store(existing)
                .remote_store_config(RemoteStoreConfig::default())
                .into_store(),
            Err(crate::Error::InvalidRequest(message)) if message.contains("cannot override")
        ));
        assert!(matches!(
            Client::builder().into_store(),
            Err(crate::Error::InvalidRequest(message)) if message.contains("exactly one")
        ));
    }

    #[tokio::test]
    async fn builder_rejects_partial_deterministic_sources_before_provider_access() {
        assert!(matches!(
            Client::builder()
                .clock(Arc::new(prolly_dynamodb_core::SystemClock))
                .open()
                .await,
            Err(crate::Error::InvalidRequest(message)) if message.contains("configured together")
        ));
        assert!(matches!(
            Client::builder()
                .id_generator(Arc::new(prolly_dynamodb_core::SystemIdGenerator))
                .open()
                .await,
            Err(crate::Error::InvalidRequest(message)) if message.contains("configured together")
        ));
        assert!(matches!(
            Client::builder()
                .logical_retry_limit(prolly_dynamodb_core::MAX_LOGICAL_RETRY_LIMIT + 1)
                .open()
                .await,
            Err(crate::Error::InvalidRequest(message)) if message.contains("logical retry limit")
        ));
    }

    #[tokio::test]
    async fn async_open_emits_a_stable_data_safe_span_before_provider_access() {
        const CHILD: &str = "PROLLY_DYNAMODB_TRACE_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .arg("--exact")
                .arg("client::tests::async_open_emits_a_stable_data_safe_span_before_provider_access")
                .arg("--test-threads=1")
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success(), "isolated tracing assertion failed");
            return;
        }
        let spans = Arc::new(Mutex::new(Vec::new()));
        let dispatch = tracing::Dispatch::new(SpanRecorder(Arc::clone(&spans)));
        // Tokio's default test runtime is current-thread. Keep one dispatcher
        // guard active for both future construction and polling; nesting a
        // second per-future dispatcher creates avoidable global callsite-cache
        // churn when the suite runs in parallel.
        let _guard = tracing::dispatcher::set_default(&dispatch);
        let result = Client::builder().open().await;
        assert!(matches!(result, Err(crate::Error::InvalidRequest(_))));
        assert_eq!(
            spans.lock().unwrap().as_slice(),
            ["prolly_dynamodb.ClientOpen"]
        );
    }
}
