use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, Get, GlobalSecondaryIndex, KeySchemaElement, KeyType,
    KeysAndAttributes, Projection, ProjectionType, Put as TransactPut, PutRequest,
    ScalarAttributeType, TransactGetItem, TransactWriteItem, WriteRequest,
};
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeDeserializationInterceptorContextRef, BeforeSerializationInterceptorContextRef,
    BeforeTransmitInterceptorContextRef,
};
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_types::config_bag::ConfigBag;
use prolly_dynamodb_client::{
    Client, GcApplyOptions, GcPlanLimits, KeyAttribute, KeyKind, MaintenanceContext,
    RetentionPolicy, SecondaryIndexDefinition, SecondaryIndexKind, SecondaryIndexProjection,
    WithMetadata, MIN_MAINTENANCE_LEASE_MILLIS,
};
use prolly_store_dynamodb::DynamoDbBackend;
use serde::Serialize;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

const SCHEMA: &str = "versioned-dynamodb-client-samples-v2";
const LOGICAL_TABLE: &str = "BenchmarkItems";
const RESTORE_TABLE: &str = "BenchmarkRestore";
const GROUP_INDEX: &str = "ByGroup";
const REGION_INDEX: &str = "ByRegion";
const FIXED_OPERATIONS_PER_SAMPLE: usize = 32;
const LOGICAL_TRANSACTION_BYTES: usize = 4 * 1024 * 1024;
const LOGICAL_PAGE_BYTES: usize = 1024 * 1024;
const EXPLICIT_BLOB_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug)]
struct Args {
    endpoint: String,
    table: String,
    root_table: String,
    output: PathBuf,
    samples: usize,
    records: usize,
    value_bytes: usize,
    read_batch_items: usize,
    history_depth: usize,
    workload: Workload,
    transaction_shapes: Vec<usize>,
    concurrency_writers: Vec<usize>,
    concurrency_operations_per_writer: usize,
    concurrency_retry_limit: usize,
    node_cache_max_bytes: usize,
    revision: String,
    dirty: bool,
    cleanup: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    Full,
    History,
}

#[derive(Clone, Debug, Default)]
struct PhysicalSnapshot {
    executions: u64,
    attempts: u64,
    request_bytes: u64,
    response_bytes: u64,
    response_bytes_unknown: u64,
    transaction_actions: u64,
    api_attempts: BTreeMap<String, u64>,
}

impl PhysicalSnapshot {
    fn delta(&self, before: &Self) -> Result<Self, String> {
        let mut api_attempts = BTreeMap::new();
        for (api, after) in &self.api_attempts {
            let prior = before.api_attempts.get(api).copied().unwrap_or_default();
            if *after > prior {
                api_attempts.insert(api.clone(), after - prior);
            }
        }
        Ok(Self {
            executions: self
                .executions
                .checked_sub(before.executions)
                .ok_or("execution counter regressed")?,
            attempts: self
                .attempts
                .checked_sub(before.attempts)
                .ok_or("attempt counter regressed")?,
            request_bytes: self
                .request_bytes
                .checked_sub(before.request_bytes)
                .ok_or("request byte counter regressed")?,
            response_bytes: self
                .response_bytes
                .checked_sub(before.response_bytes)
                .ok_or("response byte counter regressed")?,
            response_bytes_unknown: self
                .response_bytes_unknown
                .checked_sub(before.response_bytes_unknown)
                .ok_or("unknown response byte counter regressed")?,
            transaction_actions: self
                .transaction_actions
                .checked_sub(before.transaction_actions)
                .ok_or("transaction action counter regressed")?,
            api_attempts,
        })
    }
}

#[derive(Clone, Debug, Default)]
struct PhysicalMetrics(Arc<Mutex<PhysicalSnapshot>>);

impl PhysicalMetrics {
    fn snapshot(&self) -> PhysicalSnapshot {
        self.0.lock().expect("metrics mutex poisoned").clone()
    }
}

impl Intercept for PhysicalMetrics {
    fn name(&self) -> &'static str {
        "VersionedDynamoDbBenchmarkMetrics"
    }

    fn read_before_execution(
        &self,
        context: &BeforeSerializationInterceptorContextRef<'_>,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let mut metrics = self.0.lock().expect("metrics mutex poisoned");
        metrics.executions += 1;
        if let Some(input) = context.inner().input().and_then(|input| {
            input.downcast_ref::<aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsInput>()
        }) {
            metrics.transaction_actions += input.transact_items().len() as u64;
        }
        Ok(())
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _runtime: &aws_sdk_dynamodb::config::RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let request = context.request();
        let api = request
            .headers()
            .get("x-amz-target")
            .and_then(|target| target.rsplit('.').next())
            .unwrap_or("Unknown")
            .to_owned();
        let mut metrics = self.0.lock().expect("metrics mutex poisoned");
        metrics.attempts += 1;
        metrics.request_bytes += request.body().bytes().map_or(0, |body| body.len() as u64);
        *metrics.api_attempts.entry(api).or_default() += 1;
        Ok(())
    }

    fn read_after_transmit(
        &self,
        context: &BeforeDeserializationInterceptorContextRef<'_>,
        _runtime: &aws_sdk_dynamodb::config::RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let response = context.response();
        let observed = response
            .body()
            .bytes()
            .map(|body| body.len() as u64)
            .or_else(|| {
                response
                    .headers()
                    .get("content-length")
                    .and_then(|value| value.parse().ok())
            });
        let mut metrics = self.0.lock().expect("metrics mutex poisoned");
        match observed {
            Some(bytes) => metrics.response_bytes += bytes,
            None => metrics.response_bytes_unknown += 1,
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct Sample {
    schema: &'static str,
    revision: String,
    dirty: bool,
    environment: &'static str,
    operation: String,
    cache_mode: &'static str,
    sample: usize,
    records: usize,
    configured_value_bytes: usize,
    logical_input_item_bytes: usize,
    logical_output_item_bytes: usize,
    logical_item_bytes_complete: bool,
    observed_items: usize,
    versions_created: usize,
    latency_ns: u128,
    sdk_executions: u64,
    http_attempts: u64,
    sdk_retries: u64,
    physical_request_bytes: u64,
    physical_response_bytes: u64,
    physical_response_bytes_complete: bool,
    transaction_actions: u64,
    api_attempts_json: String,
    validated: bool,
}

#[derive(Debug, Serialize)]
struct GcReachabilitySample<'a> {
    schema: &'static str,
    revision: &'a str,
    sample: usize,
    max_protected_trees: usize,
    retained_roots: usize,
    protected_trees: usize,
    live_nodes: usize,
    live_node_bytes: usize,
    scanned_blob_nodes: usize,
    scanned_values: usize,
    live_blobs: usize,
    live_blob_bytes: u64,
    examined_node_candidates: usize,
    examined_blob_candidates: usize,
}

#[derive(Debug, Serialize)]
struct CacheUsageSample<'a> {
    schema: &'static str,
    revision: &'a str,
    sample: usize,
    client_role: &'static str,
    configured_max_bytes: usize,
    entries: usize,
    serialized_bytes: usize,
    pinned_entries: usize,
    pinned_serialized_bytes: usize,
}

fn record_cache_usage(
    writer: &mut csv::Writer<std::fs::File>,
    args: &Args,
    sample: usize,
    client: &Client,
) -> Result<(), String> {
    let usage = client.cache_usage();
    writer
        .serialize(CacheUsageSample {
            schema: "versioned-dynamodb-client-cache-usage-v1",
            revision: &args.revision,
            sample,
            client_role: "primary",
            configured_max_bytes: args.node_cache_max_bytes,
            entries: usage.entries,
            serialized_bytes: usage.serialized_bytes,
            pinned_entries: usage.pinned_entries,
            pinned_serialized_bytes: usage.pinned_serialized_bytes,
        })
        .map_err(error)?;
    writer.flush().map_err(error)?;
    writer.get_ref().sync_data().map_err(error)
}

fn benchmark_gc_limits(args: &Args) -> Result<GcPlanLimits, String> {
    // One retained table version currently contributes a named version root,
    // its detached snapshot-manifest tree, and its source/index trees. Keep a
    // fourth slot plus a fixed allowance for schema, catalog, registry,
    // restore-table, and benchmark setup roots. This is an evidence-harness
    // allocation bound, not a relaxation of the client's fail-closed GC walk.
    let history_tree_allowance = args
        .history_depth
        .checked_mul(4)
        .ok_or("history depth overflows the GC protected-tree allowance")?;
    let max_protected_trees = history_tree_allowance
        .checked_add(10_000)
        .ok_or("GC protected-tree allowance overflowed")?
        .max(10_000);
    Ok(GcPlanLimits::new(
        max_protected_trees,
        100_000usize.max(max_protected_trees.saturating_mul(4)),
        1024 * 1024 * 1024,
        1_000_000usize.max(max_protected_trees.saturating_mul(4)),
        100_000,
        4 * 1024 * 1024 * 1024,
        40,
    ))
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(parse_args()).await {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Result<Args, String>) -> Result<(), String> {
    let args = args?;
    validate_args(&args)?;
    std::fs::create_dir_all(&args.output).map_err(error)?;
    let raw_path = args.output.join("raw-samples.csv");
    if raw_path.exists() {
        return Err(format!(
            "refusing to append ambiguous samples to existing {}",
            raw_path.display()
        ));
    }
    let metrics = PhysicalMetrics::default();
    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(&args.endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .interceptor(metrics.clone())
        .build();
    let prefix = format!(
        "client-bench-{}-{}:",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(error)?
            .as_nanos()
    )
    .into_bytes();
    let backend = DynamoDbBackend::new(aws_sdk_dynamodb::Client::from_conf(config), &args.table)
        .with_root_table_name(&args.root_table)
        .with_key_prefix(prefix);
    backend.initialize_schema().await.map_err(error)?;
    if args.workload == Workload::History {
        return run_history_workload(&args, &metrics, &backend, &raw_path).await;
    }
    let client = Client::builder()
        .backend(backend.clone())
        .node_cache_max_bytes(args.node_cache_max_bytes)
        .open()
        .await
        .map_err(error)?;
    let concurrency_client = Client::builder()
        .backend(backend.clone())
        .logical_retry_limit(args.concurrency_retry_limit)
        .node_cache_max_bytes(args.node_cache_max_bytes)
        .open()
        .await
        .map_err(error)?;
    if concurrency_client.logical_retry_limit() != args.concurrency_retry_limit {
        return Err("concurrency client did not retain its configured retry limit".into());
    }
    client
        .create_table()
        .table_name(LOGICAL_TABLE)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .map_err(error)?,
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("group")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .map_err(error)?,
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .map_err(error)?,
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name(GROUP_INDEX)
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("group")
                        .key_type(KeyType::Hash)
                        .build()
                        .map_err(error)?,
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .map_err(error)?,
        )
        .request_token("benchmark-create")
        .send()
        .await
        .map_err(error)?;
    let fixture_batch_size = fixture_batch_size(args.value_bytes);
    for start in (0..args.records).step_by(fixture_batch_size) {
        let end = (start + fixture_batch_size).min(args.records);
        let actions = (start..end)
            .map(|index| {
                let put = TransactPut::builder()
                    .table_name(LOGICAL_TABLE)
                    .item("id", AttributeValue::S(format!("fixture-{index:020}")))
                    .item(
                        "payload",
                        AttributeValue::B(value(index, 0, args.value_bytes).into()),
                    )
                    .build()
                    .map_err(error)?;
                Ok(TransactWriteItem::builder().put(put).build())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let result = client
            .transact_write_items()
            .set_transact_items(Some(actions))
            .client_request_token(format!("fixture-{start}"))
            .send_with_metadata()
            .await
            .map_err(error)?;
        validate_versions(&result, 1)?;
    }
    let fixture_version = client.table(LOGICAL_TABLE).head().await.map_err(error)?.id;

    client
        .create_table()
        .table_name(RESTORE_TABLE)
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .map_err(error)?,
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .map_err(error)?,
        )
        .request_token("benchmark-restore-create")
        .send()
        .await
        .map_err(error)?;
    client
        .put_item()
        .table_name(RESTORE_TABLE)
        .item("id", AttributeValue::S("state".into()))
        .item("value", AttributeValue::S("A".into()))
        .request_token("benchmark-restore-a")
        .send()
        .await
        .map_err(error)?;
    let restore_a = client.table(RESTORE_TABLE).head().await.map_err(error)?.id;
    client
        .put_item()
        .table_name(RESTORE_TABLE)
        .item("id", AttributeValue::S("state".into()))
        .item("value", AttributeValue::S("B".into()))
        .request_token("benchmark-restore-b")
        .send()
        .await
        .map_err(error)?;
    let restore_b = client.table(RESTORE_TABLE).head().await.map_err(error)?.id;
    let mut restore_current = restore_b.clone();

    let mut writer = csv::Writer::from_path(&raw_path).map_err(error)?;
    let gc_path = args.output.join("gc-reachability.csv");
    let mut gc_writer = csv::Writer::from_path(&gc_path).map_err(error)?;
    let cache_path = args.output.join("cache-usage.csv");
    let mut cache_writer = csv::Writer::from_path(&cache_path).map_err(error)?;
    for sample in 0..args.samples {
        let id = format!("fixture-{sample:020}");
        let key_bytes = logical_key_bytes(&id);
        let item_bytes = logical_item_bytes(&id, args.value_bytes);
        let mut activated_generation = None;
        measure(
            &args,
            &metrics,
            &mut writer,
            "GetItem",
            "warm",
            sample,
            key_bytes,
            item_bytes,
            true,
            0,
            async {
                let output = client
                    .get_item()
                    .table_name(LOGICAL_TABLE)
                    .key("id", AttributeValue::S(id.clone()))
                    .send()
                    .await
                    .map_err(error)?;
                Ok(usize::from(output.item().is_some()))
            },
        )
        .await?;

        let indexed_id = format!("indexed-{sample:020}");
        let indexed_group = format!("group-{sample:020}");
        let indexed_item_bytes = logical_item_bytes(&indexed_id, args.value_bytes)
            + logical_attribute_bytes("group", &indexed_group);
        measure(
            &args,
            &metrics,
            &mut writer,
            "PutItemIndexed",
            "warm",
            sample,
            indexed_item_bytes,
            0,
            true,
            1,
            async {
                let result = client
                    .put_item()
                    .table_name(LOGICAL_TABLE)
                    .item("id", AttributeValue::S(indexed_id.clone()))
                    .item("group", AttributeValue::S(indexed_group.clone()))
                    .item(
                        "payload",
                        AttributeValue::B(value(sample, 4, args.value_bytes).into()),
                    )
                    .request_token(format!("bench-indexed-put-{sample}"))
                    .send_with_metadata()
                    .await
                    .map_err(error)?;
                validate_versions(&result, 1)?;
                Ok(1)
            },
        )
        .await?;

        let index_table = format!("BenchmarkIndex{sample}");
        client
            .create_table()
            .table_name(&index_table)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(error)?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(error)?,
            )
            .request_token(format!("benchmark-index-create-{sample}"))
            .send()
            .await
            .map_err(error)?;
        client
            .put_item()
            .table_name(&index_table)
            .item("id", AttributeValue::S("indexed".into()))
            .item("group", AttributeValue::S("admin".into()))
            .item("region", AttributeValue::S("west".into()))
            .item("payload", AttributeValue::B(value(sample, 6, 128).into()))
            .send()
            .await
            .map_err(error)?;
        let desired_indexes = vec![
            SecondaryIndexDefinition {
                name: GROUP_INDEX.into(),
                kind: SecondaryIndexKind::Global,
                partition_key: KeyAttribute {
                    name: "group".into(),
                    kind: KeyKind::String,
                },
                sort_key: None,
                projection: SecondaryIndexProjection::All,
            },
            SecondaryIndexDefinition {
                name: REGION_INDEX.into(),
                kind: SecondaryIndexKind::Global,
                partition_key: KeyAttribute {
                    name: "region".into(),
                    kind: KeyKind::String,
                },
                sort_key: None,
                projection: SecondaryIndexProjection::KeysOnly,
            },
        ];
        let mut index_plan = None;
        measure(
            &args,
            &metrics,
            &mut writer,
            "IndexPlan",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let plan = client
                    .table(&index_table)
                    .indexes(desired_indexes)
                    .plan()
                    .await
                    .map_err(error)?;
                let observed = plan.after.secondary_indexes.len();
                index_plan = Some(plan);
                Ok(observed)
            },
        )
        .await?;
        let index_plan = index_plan.ok_or("IndexPlan did not return a reviewed plan")?;
        let index_context = MaintenanceContext::new(
            "benchmark-index-admin",
            format!("measure index activation sample {sample}"),
        );
        measure(
            &args,
            &metrics,
            &mut writer,
            "IndexApply",
            "warm",
            sample,
            0,
            0,
            false,
            1,
            async {
                let result = client
                    .table(&index_table)
                    .apply_indexes(&index_plan, index_context)
                    .await
                    .map_err(error)?;
                if result.replayed || result.description.secondary_indexes.len() != 2 {
                    return Err("IndexApply did not activate two fresh index generations".into());
                }
                activated_generation = Some(
                    result
                        .description
                        .secondary_indexes
                        .iter()
                        .find(|index| index.name == GROUP_INDEX)
                        .ok_or("IndexApply omitted ByGroup")?
                        .generation,
                );
                Ok(2)
            },
        )
        .await?;
        let activated_generation = activated_generation
            .ok_or("IndexApply did not return the activated index generation")?;
        let indexed = client
            .query()
            .table_name(&index_table)
            .index_name(GROUP_INDEX)
            .key_condition_expression("#group = :group")
            .expression_attribute_names("#group", "group")
            .expression_attribute_values(":group", AttributeValue::S("admin".into()))
            .send()
            .await
            .map_err(error)?;
        if indexed.count != 1 {
            return Err("activated benchmark index did not return its source item".into());
        }
        let regional = client
            .query()
            .table_name(&index_table)
            .index_name(REGION_INDEX)
            .key_condition_expression("#region = :region")
            .expression_attribute_names("#region", "region")
            .expression_attribute_values(":region", AttributeValue::S("west".into()))
            .send()
            .await
            .map_err(error)?;
        if regional.count != 1
            || regional
                .items()
                .first()
                .is_none_or(|item| item.contains_key("payload"))
        {
            return Err("activated ByRegion keys-only index returned an invalid result".into());
        }

        let replacement_indexes = vec![
            SecondaryIndexDefinition {
                name: GROUP_INDEX.into(),
                kind: SecondaryIndexKind::Global,
                partition_key: KeyAttribute {
                    name: "group".into(),
                    kind: KeyKind::String,
                },
                sort_key: None,
                projection: SecondaryIndexProjection::KeysOnly,
            },
            SecondaryIndexDefinition {
                name: REGION_INDEX.into(),
                kind: SecondaryIndexKind::Global,
                partition_key: KeyAttribute {
                    name: "region".into(),
                    kind: KeyKind::String,
                },
                sort_key: None,
                projection: SecondaryIndexProjection::All,
            },
        ];
        let mut replacement_plan = None;
        measure(
            &args,
            &metrics,
            &mut writer,
            "IndexReplacePlan",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let plan = client
                    .table(&index_table)
                    .indexes(replacement_indexes)
                    .plan()
                    .await
                    .map_err(error)?;
                let replacement = plan
                    .after
                    .secondary_indexes
                    .iter()
                    .find(|index| index.name == GROUP_INDEX)
                    .ok_or("IndexReplacePlan omitted the replacement index")?;
                if replacement.projection != SecondaryIndexProjection::KeysOnly
                    || replacement.generation <= activated_generation
                    || plan.after.secondary_indexes.len() != 2
                {
                    return Err(
                        "IndexReplacePlan did not create a newer keys-only generation".into(),
                    );
                }
                replacement_plan = Some(plan);
                Ok(2)
            },
        )
        .await?;
        let replacement_plan =
            replacement_plan.ok_or("IndexReplacePlan did not return a reviewed plan")?;
        let replacement_generation = replacement_plan
            .after
            .secondary_indexes
            .iter()
            .find(|index| index.name == GROUP_INDEX)
            .ok_or("IndexReplacePlan omitted ByGroup")?
            .generation;
        measure(
            &args,
            &metrics,
            &mut writer,
            "IndexReplaceApply",
            "warm",
            sample,
            0,
            0,
            false,
            1,
            async {
                let result = client
                    .table(&index_table)
                    .apply_indexes(
                        &replacement_plan,
                        MaintenanceContext::new(
                            "benchmark-index-admin",
                            format!("measure index replacement sample {sample}"),
                        ),
                    )
                    .await
                    .map_err(error)?;
                let replacement = result
                    .description
                    .secondary_indexes
                    .iter()
                    .find(|index| index.name == GROUP_INDEX)
                    .ok_or("IndexReplaceApply omitted the replacement index")?;
                if result.replayed
                    || replacement.generation != replacement_generation
                    || replacement.projection != SecondaryIndexProjection::KeysOnly
                    || result.description.secondary_indexes.len() != 2
                {
                    return Err("IndexReplaceApply activated the wrong generation".into());
                }
                Ok(2)
            },
        )
        .await?;
        let replaced = client
            .query()
            .table_name(&index_table)
            .index_name(GROUP_INDEX)
            .key_condition_expression("#group = :group")
            .expression_attribute_names("#group", "group")
            .expression_attribute_values(":group", AttributeValue::S("admin".into()))
            .send()
            .await
            .map_err(error)?;
        if replaced.count != 1
            || replaced
                .items()
                .first()
                .is_none_or(|item| item.contains_key("payload"))
        {
            return Err("replacement keys-only index returned an invalid projection".into());
        }

        let mut removal_plan = None;
        measure(
            &args,
            &metrics,
            &mut writer,
            "IndexRemovePlan",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let plan = client
                    .table(&index_table)
                    .indexes(Vec::new())
                    .plan()
                    .await
                    .map_err(error)?;
                if plan.before.secondary_indexes.len() != 2
                    || !plan.after.secondary_indexes.is_empty()
                {
                    return Err("IndexRemovePlan did not select the active index".into());
                }
                removal_plan = Some(plan);
                Ok(2)
            },
        )
        .await?;
        let removal_plan = removal_plan.ok_or("IndexRemovePlan did not return a reviewed plan")?;
        measure(
            &args,
            &metrics,
            &mut writer,
            "IndexRemoveApply",
            "warm",
            sample,
            0,
            0,
            false,
            1,
            async {
                let result = client
                    .table(&index_table)
                    .apply_indexes(
                        &removal_plan,
                        MaintenanceContext::new(
                            "benchmark-index-admin",
                            format!("measure index removal sample {sample}"),
                        ),
                    )
                    .await
                    .map_err(error)?;
                if result.replayed || !result.description.secondary_indexes.is_empty() {
                    return Err("IndexRemoveApply left an active index".into());
                }
                Ok(2)
            },
        )
        .await?;
        let after_removal = client
            .describe_table()
            .table_name(&index_table)
            .send()
            .await
            .map_err(error)?;
        if after_removal
            .table()
            .is_none_or(|table| !table.global_secondary_indexes().is_empty())
        {
            return Err("post-removal index catalog verification failed".into());
        }

        let retention_table = format!("BenchmarkRetention{sample}");
        client
            .create_table()
            .table_name(&retention_table)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(error)?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(error)?,
            )
            .send()
            .await
            .map_err(error)?;
        let mut first_history_version = None;
        for generation in 0..args.history_depth as u64 {
            let result = client
                .put_item()
                .table_name(&retention_table)
                .item("id", AttributeValue::S("state".into()))
                .item(
                    "payload",
                    AttributeValue::B(value(sample, 10 + generation, 128).into()),
                )
                .send_with_metadata()
                .await
                .map_err(error)?;
            validate_versions(&result, 1)?;
            if generation == 0 {
                first_history_version = Some(
                    client
                        .table(&retention_table)
                        .head()
                        .await
                        .map_err(error)?
                        .id,
                );
            }
        }
        let first_history_version = first_history_version
            .ok_or("history workload did not create its first immutable version")?;
        let final_history_version = client
            .table(&retention_table)
            .head()
            .await
            .map_err(error)?
            .id;
        let history_key_bytes = logical_key_bytes("state");
        let history_item_bytes = logical_item_bytes("state", 128);

        measure(
            &args,
            &metrics,
            &mut writer,
            "HistoryVersionsAll",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let mut paginator = client.table(&retention_table).versions().page_size(1000);
                let mut seen = HashSet::new();
                while let Some(page) = paginator.next_page().await.map_err(error)? {
                    for version in page.versions {
                        if !seen.insert(version.id) {
                            return Err("history paginator emitted a duplicate version".into());
                        }
                    }
                }
                let expected = args
                    .history_depth
                    .checked_add(1)
                    .ok_or("history depth overflowed")?;
                if seen.len() != expected {
                    return Err(format!(
                        "history paginator emitted {} versions; expected {expected}",
                        seen.len()
                    ));
                }
                Ok(seen.len())
            },
        )
        .await?;

        let first_history_payload = value(sample, 10, 128);
        measure(
            &args,
            &metrics,
            &mut writer,
            "HistoryGetOldest",
            "warm",
            sample,
            history_key_bytes,
            history_item_bytes,
            true,
            0,
            async {
                let output = client
                    .table(&retention_table)
                    .at(first_history_version.clone())
                    .get_item()
                    .key("id", AttributeValue::S("state".into()))
                    .send()
                    .await
                    .map_err(error)?;
                let payload = output
                    .item()
                    .and_then(|item| item.get("payload"))
                    .and_then(|payload| payload.as_b().ok())
                    .ok_or("oldest historical read omitted its payload")?;
                if payload.as_ref() != first_history_payload.as_slice() {
                    return Err("oldest historical read returned different bytes".into());
                }
                Ok(1)
            },
        )
        .await?;

        measure(
            &args,
            &metrics,
            &mut writer,
            "HistoryDiffOldestHead",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let mut paginator = client
                    .table(&retention_table)
                    .diff(first_history_version, final_history_version)
                    .page_size(1000);
                let mut changes = 0usize;
                while let Some(page) = paginator.next_page().await.map_err(error)? {
                    changes = changes
                        .checked_add(page.diffs.len())
                        .ok_or("history diff count overflowed")?;
                }
                if changes != 1 {
                    return Err(format!(
                        "oldest-to-head history diff returned {changes} changes; expected 1"
                    ));
                }
                Ok(changes)
            },
        )
        .await?;
        let mut retention_plan = None;
        measure(
            &args,
            &metrics,
            &mut writer,
            "QueryGsi",
            "warm",
            sample,
            logical_attribute_bytes("group", &indexed_group),
            indexed_item_bytes,
            true,
            0,
            async {
                let output = client
                    .query()
                    .table_name(LOGICAL_TABLE)
                    .index_name(GROUP_INDEX)
                    .key_condition_expression("#group = :group")
                    .expression_attribute_names("#group", "group")
                    .expression_attribute_values(":group", AttributeValue::S(indexed_group.clone()))
                    .limit(1)
                    .send()
                    .await
                    .map_err(error)?;
                if output.count != 1 || output.scanned_count != 1 {
                    return Err("GSI Query did not return exactly one indexed item".into());
                }
                Ok(1)
            },
        )
        .await?;

        let blob_id = format!("blob-{sample:020}");
        let blob_payload = value(sample, 5, EXPLICIT_BLOB_BYTES);
        let blob_item_bytes = logical_item_bytes(&blob_id, EXPLICIT_BLOB_BYTES);
        measure(
            &args,
            &metrics,
            &mut writer,
            "PutItemBlob128KiB",
            "warm",
            sample,
            blob_item_bytes,
            0,
            true,
            1,
            async {
                let result = client
                    .put_item()
                    .table_name(LOGICAL_TABLE)
                    .item("id", AttributeValue::S(blob_id.clone()))
                    .item("payload", AttributeValue::B(blob_payload.clone().into()))
                    .request_token(format!("bench-blob-put-{sample}"))
                    .send_with_metadata()
                    .await
                    .map_err(error)?;
                validate_versions(&result, 1)?;
                Ok(1)
            },
        )
        .await?;

        measure(
            &args,
            &metrics,
            &mut writer,
            "GetItemBlob128KiB",
            "warm",
            sample,
            logical_key_bytes(&blob_id),
            blob_item_bytes,
            true,
            0,
            async {
                let output = client
                    .get_item()
                    .table_name(LOGICAL_TABLE)
                    .key("id", AttributeValue::S(blob_id.clone()))
                    .send()
                    .await
                    .map_err(error)?;
                let payload_bytes = output
                    .item()
                    .and_then(|item| item.get("payload"))
                    .and_then(|payload| payload.as_b().ok())
                    .map(|payload| payload.as_ref())
                    .ok_or("blob GetItem omitted the binary payload")?;
                if payload_bytes != blob_payload.as_slice() {
                    return Err("blob GetItem returned different payload bytes".into());
                }
                Ok(1)
            },
        )
        .await?;

        let restore_target = if restore_current == restore_a {
            restore_b.clone()
        } else {
            restore_a.clone()
        };
        let expected_restore_head = restore_current.clone();
        let measured_restore_target = restore_target.clone();
        measure(
            &args,
            &metrics,
            &mut writer,
            "Restore",
            "warm",
            sample,
            64,
            0,
            true,
            0,
            async {
                let update = client
                    .table(RESTORE_TABLE)
                    .restore(measured_restore_target)
                    .expected_head(expected_restore_head)
                    .request_token(format!("bench-restore-{sample}"))
                    .send()
                    .await
                    .map_err(error)?;
                if !update.is_applied() {
                    return Err("Restore did not move the isolated table head".into());
                }
                Ok(1)
            },
        )
        .await?;
        restore_current = restore_target;

        measure(
            &args,
            &metrics,
            &mut writer,
            "RetentionPlan",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let plan = client
                    .table(&retention_table)
                    .retention(RetentionPolicy::keep_last(1))
                    .plan()
                    .await
                    .map_err(error)?;
                validate_retention_shape(args.history_depth, &plan)?;
                let observed = usize::try_from(plan.examined_versions).map_err(error)?;
                retention_plan = Some(plan);
                Ok(observed)
            },
        )
        .await?;
        let retention_plan =
            retention_plan.ok_or("RetentionPlan did not return a reviewed plan")?;
        let retention_context = MaintenanceContext::new(
            "benchmark-records-admin",
            format!("measure retention sample {sample}"),
        );
        measure(
            &args,
            &metrics,
            &mut writer,
            "RetentionApply",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let result = client
                    .table(&retention_table)
                    .apply_retention(&retention_plan, retention_context)
                    .await
                    .map_err(error)?;
                if result.replayed || result.removed != retention_plan.remove {
                    return Err("RetentionApply did not durably remove the reviewed set".into());
                }
                Ok(result.removed.len())
            },
        )
        .await?;

        let gc_context = MaintenanceContext::new(
            "benchmark-storage-admin",
            format!("measure fenced GC sample {sample}"),
        );
        let lease = client
            .acquire_maintenance_lease(gc_context.clone(), MIN_MAINTENANCE_LEASE_MILLIS)
            .await
            .map_err(error)?;
        let gc_limits = benchmark_gc_limits(&args)?;
        let mut gc_plan = None;
        measure(
            &args,
            &metrics,
            &mut writer,
            "GcPlan",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let plan = client
                    .plan_gc(&lease.id, None, gc_limits)
                    .await
                    .map_err(error)?;
                let observed = plan
                    .examined_node_candidates
                    .checked_add(plan.examined_blob_candidates)
                    .ok_or("GcPlan examined-candidate count overflowed")?;
                gc_plan = Some(plan);
                Ok(observed)
            },
        )
        .await?;
        let gc_plan = gc_plan.ok_or("GcPlan did not return a reviewed plan")?;
        gc_writer
            .serialize(GcReachabilitySample {
                schema: "versioned-dynamodb-client-gc-reachability-v2",
                revision: &args.revision,
                sample,
                max_protected_trees: gc_plan.limits.max_roots,
                retained_roots: gc_plan.retained_roots,
                protected_trees: gc_plan.protected_trees,
                live_nodes: gc_plan.live_nodes,
                live_node_bytes: gc_plan.live_node_bytes,
                scanned_blob_nodes: gc_plan.scanned_blob_nodes,
                scanned_values: gc_plan.scanned_values,
                live_blobs: gc_plan.live_blobs,
                live_blob_bytes: gc_plan.live_blob_bytes,
                examined_node_candidates: gc_plan.examined_node_candidates,
                examined_blob_candidates: gc_plan.examined_blob_candidates,
            })
            .map_err(error)?;
        measure(
            &args,
            &metrics,
            &mut writer,
            "GcApply",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let result = client
                    .apply_gc(&gc_plan, gc_context.clone(), GcApplyOptions::default())
                    .await
                    .map_err(error)?;
                if result.replayed || result.plan_id != gc_plan.id {
                    return Err("GcApply did not complete the reviewed fresh plan".into());
                }
                Ok(1 + result.node_deletes + result.blob_deletes)
            },
        )
        .await?;
        client
            .plan_gc(&lease.id, None, gc_limits)
            .await
            .map_err(|source| format!("post-GcApply reachability verification failed: {source}"))?;
        client
            .release_maintenance_lease(&lease.id, gc_context)
            .await
            .map_err(error)?;

        measure(
            &args,
            &metrics,
            &mut writer,
            "GetItemAt",
            "warm",
            sample,
            key_bytes,
            item_bytes,
            true,
            0,
            async {
                let output = client
                    .table(LOGICAL_TABLE)
                    .at(fixture_version.clone())
                    .get_item()
                    .key("id", AttributeValue::S(id.clone()))
                    .send()
                    .await
                    .map_err(error)?;
                Ok(usize::from(output.item().is_some()))
            },
        )
        .await?;

        measure(
            &args,
            &metrics,
            &mut writer,
            "Query",
            "warm",
            sample,
            key_bytes,
            item_bytes,
            true,
            0,
            async {
                let output = client
                    .query()
                    .table_name(LOGICAL_TABLE)
                    .key_condition_expression("#id = :id")
                    .expression_attribute_names("#id", "id")
                    .expression_attribute_values(":id", AttributeValue::S(id.clone()))
                    .limit(1)
                    .send()
                    .await
                    .map_err(error)?;
                if output.count != 1 || output.scanned_count != 1 {
                    return Err("Query did not return exactly one fixture".into());
                }
                Ok(1)
            },
        )
        .await?;

        let page_items = 10usize;
        let scan_items = (LOGICAL_PAGE_BYTES / item_bytes).clamp(1, page_items);
        measure(
            &args,
            &metrics,
            &mut writer,
            "Scan",
            "warm",
            sample,
            0,
            scan_items * item_bytes,
            true,
            0,
            async {
                let output = client
                    .scan()
                    .table_name(LOGICAL_TABLE)
                    .limit(scan_items as i32)
                    .send()
                    .await
                    .map_err(error)?;
                if output.count != scan_items as i32 || output.scanned_count != scan_items as i32 {
                    return Err("Scan did not return the requested full fixture page".into());
                }
                Ok(scan_items)
            },
        )
        .await?;

        let batch_keys = (0..page_items)
            .map(|index| {
                HashMap::from([(
                    "id".to_owned(),
                    AttributeValue::S(format!("fixture-{index:020}")),
                )])
            })
            .collect::<Vec<_>>();
        let batch_input_bytes = batch_keys
            .iter()
            .map(|key| match key.get("id") {
                Some(AttributeValue::S(id)) => logical_key_bytes(id),
                _ => 0,
            })
            .sum();
        measure(
            &args,
            &metrics,
            &mut writer,
            "BatchGetItem10",
            "warm",
            sample,
            batch_input_bytes,
            page_items * item_bytes,
            true,
            0,
            async {
                let request = KeysAndAttributes::builder()
                    .set_keys(Some(batch_keys))
                    .build()
                    .map_err(error)?;
                let output = client
                    .batch_get_item()
                    .request_items(LOGICAL_TABLE, request)
                    .send()
                    .await
                    .map_err(error)?;
                let observed = output
                    .responses()
                    .and_then(|responses| responses.get(LOGICAL_TABLE))
                    .map(Vec::len)
                    .unwrap_or_default();
                if observed != page_items
                    || output
                        .unprocessed_keys()
                        .is_some_and(|unprocessed| !unprocessed.is_empty())
                {
                    return Err("BatchGetItem did not return all ten fixture items".into());
                }
                Ok(observed)
            },
        )
        .await?;

        let extended_batch_input_bytes = (0..args.read_batch_items)
            .map(|index| logical_key_bytes(&format!("fixture-{index:020}")))
            .sum();
        if args.read_batch_items > page_items {
            let batch_100_keys = (0..args.read_batch_items)
                .map(|index| {
                    HashMap::from([(
                        "id".to_owned(),
                        AttributeValue::S(format!("fixture-{index:020}")),
                    )])
                })
                .collect::<Vec<_>>();
            measure(
                &args,
                &metrics,
                &mut writer,
                &format!("BatchGetItem{}", args.read_batch_items),
                "warm",
                sample,
                extended_batch_input_bytes,
                args.read_batch_items * item_bytes,
                true,
                0,
                async {
                    let request = KeysAndAttributes::builder()
                        .set_keys(Some(batch_100_keys))
                        .build()
                        .map_err(error)?;
                    let output = client
                        .batch_get_item()
                        .request_items(LOGICAL_TABLE, request)
                        .send()
                        .await
                        .map_err(error)?;
                    let observed = output
                        .responses()
                        .and_then(|responses| responses.get(LOGICAL_TABLE))
                        .map(Vec::len)
                        .unwrap_or_default();
                    if observed != args.read_batch_items
                        || output
                            .unprocessed_keys()
                            .is_some_and(|unprocessed| !unprocessed.is_empty())
                    {
                        return Err("BatchGetItem did not return the extended fixture shape".into());
                    }
                    Ok(observed)
                },
            )
            .await?;
        }

        let transact_gets = (0..page_items)
            .map(|index| {
                let get = Get::builder()
                    .table_name(LOGICAL_TABLE)
                    .key("id", AttributeValue::S(format!("fixture-{index:020}")))
                    .build()
                    .map_err(error)?;
                Ok(TransactGetItem::builder().get(get).build())
            })
            .collect::<Result<Vec<_>, String>>()?;
        measure(
            &args,
            &metrics,
            &mut writer,
            "TransactGetItems10",
            "warm",
            sample,
            batch_input_bytes,
            page_items * item_bytes,
            true,
            0,
            async {
                let output = client
                    .transact_get_items()
                    .set_transact_items(Some(transact_gets))
                    .send()
                    .await
                    .map_err(error)?;
                if output.responses().len() != page_items
                    || output
                        .responses()
                        .iter()
                        .any(|response| response.item.is_none())
                {
                    return Err("TransactGetItems did not return all ten fixture items".into());
                }
                Ok(page_items)
            },
        )
        .await?;

        if args.read_batch_items > page_items {
            let transact_100_gets = (0..args.read_batch_items)
                .map(|index| {
                    let get = Get::builder()
                        .table_name(LOGICAL_TABLE)
                        .key("id", AttributeValue::S(format!("fixture-{index:020}")))
                        .build()
                        .map_err(error)?;
                    Ok(TransactGetItem::builder().get(get).build())
                })
                .collect::<Result<Vec<_>, String>>()?;
            measure(
                &args,
                &metrics,
                &mut writer,
                &format!("TransactGetItems{}", args.read_batch_items),
                "warm",
                sample,
                extended_batch_input_bytes,
                args.read_batch_items * item_bytes,
                true,
                0,
                async {
                    let output = client
                        .transact_get_items()
                        .set_transact_items(Some(transact_100_gets))
                        .send()
                        .await
                        .map_err(error)?;
                    if output.responses().len() != args.read_batch_items
                        || output
                            .responses()
                            .iter()
                            .any(|response| response.item.is_none())
                    {
                        return Err(
                            "TransactGetItems did not return the extended fixture shape".into()
                        );
                    }
                    Ok(args.read_batch_items)
                },
            )
            .await?;
        }

        let cold = Client::builder()
            .backend(backend.clone())
            .node_cache_max_bytes(args.node_cache_max_bytes)
            .open()
            .await
            .map_err(error)?;
        measure(
            &args,
            &metrics,
            &mut writer,
            "GetItem",
            "cold",
            sample,
            key_bytes,
            item_bytes,
            true,
            0,
            async {
                let output = cold
                    .get_item()
                    .table_name(LOGICAL_TABLE)
                    .key("id", AttributeValue::S(id))
                    .send()
                    .await
                    .map_err(error)?;
                Ok(usize::from(output.item().is_some()))
            },
        )
        .await?;

        let batch_writes = (0..page_items)
            .map(|action| {
                let id = format!("batch-{sample:020}-{action:02}");
                let put = PutRequest::builder()
                    .item("id", AttributeValue::S(id))
                    .item(
                        "payload",
                        AttributeValue::B(
                            value(sample * page_items + action, 3, args.value_bytes).into(),
                        ),
                    )
                    .build()
                    .map_err(error)?;
                Ok(WriteRequest::builder().put_request(put).build())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let batch_write_bytes = (0..page_items)
            .map(|action| {
                logical_item_bytes(&format!("batch-{sample:020}-{action:02}"), args.value_bytes)
            })
            .sum();
        measure(
            &args,
            &metrics,
            &mut writer,
            "BatchWriteItem10",
            "warm",
            sample,
            batch_write_bytes,
            0,
            true,
            page_items,
            async {
                let result = client
                    .batch_write_item()
                    .request_items(LOGICAL_TABLE, batch_writes)
                    .send_with_metadata()
                    .await
                    .map_err(|source| {
                        source.batch_write_failure().map_or_else(
                            || source.to_string(),
                            |failure| format!("{source}: {}", failure.cause()),
                        )
                    })?;
                validate_versions(&result, page_items)?;
                if result
                    .output
                    .unprocessed_items()
                    .is_some_and(|unprocessed| !unprocessed.is_empty())
                {
                    return Err("BatchWriteItem returned unprocessed writes".into());
                }
                Ok(page_items)
            },
        )
        .await?;

        let batch_25_writes = (0..25)
            .map(|action| {
                let id = format!("batch25-{sample:020}-{action:02}");
                let put = PutRequest::builder()
                    .item("id", AttributeValue::S(id))
                    .item(
                        "payload",
                        AttributeValue::B(value(sample * 25 + action, 7, args.value_bytes).into()),
                    )
                    .build()
                    .map_err(error)?;
                Ok(WriteRequest::builder().put_request(put).build())
            })
            .collect::<Result<Vec<_>, String>>()?;
        let batch_25_write_bytes = (0..25)
            .map(|action| {
                logical_item_bytes(
                    &format!("batch25-{sample:020}-{action:02}"),
                    args.value_bytes,
                )
            })
            .sum();
        measure(
            &args,
            &metrics,
            &mut writer,
            "BatchWriteItem25",
            "warm",
            sample,
            batch_25_write_bytes,
            0,
            true,
            25,
            async {
                let result = client
                    .batch_write_item()
                    .request_items(LOGICAL_TABLE, batch_25_writes)
                    .send_with_metadata()
                    .await
                    .map_err(|source| {
                        source.batch_write_failure().map_or_else(
                            || source.to_string(),
                            |failure| format!("{source}: {}", failure.cause()),
                        )
                    })?;
                validate_versions(&result, 25)?;
                if result
                    .output
                    .unprocessed_items()
                    .is_some_and(|unprocessed| !unprocessed.is_empty())
                {
                    return Err("BatchWriteItem returned unprocessed writes".into());
                }
                Ok(25)
            },
        )
        .await?;

        for &transaction_items in &args.transaction_shapes {
            let transact_writes = (0..transaction_items)
                .map(|action| {
                    let id = format!("transact-{transaction_items:03}-{sample:020}-{action:03}");
                    let put = TransactPut::builder()
                        .table_name(LOGICAL_TABLE)
                        .item("id", AttributeValue::S(id))
                        .item(
                            "payload",
                            AttributeValue::B(
                                value(
                                    sample * transaction_items + action,
                                    transaction_items as u64,
                                    args.value_bytes,
                                )
                                .into(),
                            ),
                        )
                        .build()
                        .map_err(error)?;
                    Ok(TransactWriteItem::builder().put(put).build())
                })
                .collect::<Result<Vec<_>, String>>()?;
            let transact_write_bytes = (0..transaction_items)
                .map(|action| {
                    logical_item_bytes(
                        &format!("transact-{transaction_items:03}-{sample:020}-{action:03}"),
                        args.value_bytes,
                    )
                })
                .sum();
            measure(
                &args,
                &metrics,
                &mut writer,
                &format!("TransactWriteItems{transaction_items}"),
                "warm",
                sample,
                transact_write_bytes,
                0,
                true,
                1,
                async {
                    let result = client
                        .transact_write_items()
                        .set_transact_items(Some(transact_writes))
                        .client_request_token(format!("bench-tx-{transaction_items}-{sample}"))
                        .send_with_metadata()
                        .await
                        .map_err(error)?;
                    validate_versions(&result, 1)?;
                    Ok(transaction_items)
                },
            )
            .await?;
        }

        for &writers in &args.concurrency_writers {
            let operations_per_writer = args.concurrency_operations_per_writer;
            let total_operations = writers
                .checked_mul(operations_per_writer)
                .ok_or("concurrency operation count overflowed")?;
            let logical_input_bytes = (0..writers)
                .flat_map(|writer| {
                    (0..operations_per_writer).map(move |operation| {
                        logical_item_bytes(
                            &format!(
                                "concurrent-{writers:03}-{sample:020}-{writer:03}-{operation:03}"
                            ),
                            args.value_bytes,
                        )
                    })
                })
                .sum();
            let barrier = Arc::new(Barrier::new(writers));
            measure(
                &args,
                &metrics,
                &mut writer,
                &format!("ConcurrentPutItemW{writers}O{operations_per_writer}"),
                "warm",
                sample,
                logical_input_bytes,
                0,
                true,
                total_operations,
                async {
                    let mut tasks = JoinSet::new();
                    for writer_index in 0..writers {
                        let worker_client = concurrency_client.clone();
                        let worker_barrier = barrier.clone();
                        let value_bytes = args.value_bytes;
                        tasks.spawn(async move {
                            worker_barrier.wait().await;
                            for operation in 0..operations_per_writer {
                                let id = format!(
                                    "concurrent-{writers:03}-{sample:020}-{writer_index:03}-{operation:03}"
                                );
                                let result = worker_client
                                    .put_item()
                                    .table_name(LOGICAL_TABLE)
                                    .item("id", AttributeValue::S(id))
                                    .item(
                                        "payload",
                                        AttributeValue::B(
                                            value(
                                                sample * total_operations
                                                    + writer_index * operations_per_writer
                                                    + operation,
                                                writers as u64,
                                                value_bytes,
                                            )
                                            .into(),
                                        ),
                                    )
                                    .request_token(format!(
                                        "bench-concurrent-{writers}-{sample}-{writer_index}-{operation}"
                                    ))
                                    .send_with_metadata()
                                    .await
                                    .map_err(error)?;
                                validate_versions(&result, 1)?;
                            }
                            Ok::<usize, String>(operations_per_writer)
                        });
                    }
                    let mut completed = 0usize;
                    while let Some(joined) = tasks.join_next().await {
                        completed = completed
                            .checked_add(joined.map_err(error)??)
                            .ok_or("completed concurrency count overflowed")?;
                    }
                    if completed != total_operations {
                        return Err(format!(
                            "concurrent workload completed {completed} writes; expected {total_operations}"
                        ));
                    }
                    Ok(completed)
                },
            )
            .await?;
        }

        measure(
            &args,
            &metrics,
            &mut writer,
            "PutItem",
            "warm",
            sample,
            logical_item_bytes(&format!("put-{sample:020}"), args.value_bytes),
            0,
            true,
            1,
            async {
                let result = client
                    .put_item()
                    .table_name(LOGICAL_TABLE)
                    .item("id", AttributeValue::S(format!("put-{sample:020}")))
                    .item(
                        "payload",
                        AttributeValue::B(value(sample, 1, args.value_bytes).into()),
                    )
                    .request_token(format!("bench-put-{sample}"))
                    .send_with_metadata()
                    .await
                    .map_err(error)?;
                validate_versions(&result, 1)?;
                Ok(1)
            },
        )
        .await?;

        let diff_target = client.table(LOGICAL_TABLE).head().await.map_err(error)?.id;
        measure(
            &args,
            &metrics,
            &mut writer,
            "Diff",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let page = client
                    .table(LOGICAL_TABLE)
                    .diff(fixture_version.clone(), diff_target)
                    .page_size(1000)
                    .next_page()
                    .await
                    .map_err(error)?
                    .ok_or("diff unexpectedly finished")?;
                if page.diffs.is_empty() {
                    return Err(
                        "diff from the immutable fixture unexpectedly returned empty".into(),
                    );
                }
                Ok(page.diffs.len())
            },
        )
        .await?;

        measure(
            &args,
            &metrics,
            &mut writer,
            "UpdateItem",
            "warm",
            sample,
            logical_item_bytes(
                &format!("fixture-{:020}", args.samples + sample),
                args.value_bytes,
            ),
            0,
            true,
            1,
            async {
                let result = client
                    .update_item()
                    .table_name(LOGICAL_TABLE)
                    .key(
                        "id",
                        AttributeValue::S(format!("fixture-{:020}", args.samples + sample)),
                    )
                    .update_expression("SET #payload = :payload")
                    .expression_attribute_names("#payload", "payload")
                    .expression_attribute_values(
                        ":payload",
                        AttributeValue::B(value(sample, 2, args.value_bytes).into()),
                    )
                    .request_token(format!("bench-update-{sample}"))
                    .send_with_metadata()
                    .await
                    .map_err(error)?;
                validate_versions(&result, 1)?;
                Ok(1)
            },
        )
        .await?;

        measure(
            &args,
            &metrics,
            &mut writer,
            "DeleteItem",
            "warm",
            sample,
            logical_key_bytes(&format!("fixture-{:020}", 10 + sample)),
            0,
            true,
            1,
            async {
                let result = client
                    .delete_item()
                    .table_name(LOGICAL_TABLE)
                    .key(
                        "id",
                        AttributeValue::S(format!("fixture-{:020}", 10 + sample)),
                    )
                    .request_token(format!("bench-delete-{sample}"))
                    .send_with_metadata()
                    .await
                    .map_err(error)?;
                validate_versions(&result, 1)?;
                Ok(1)
            },
        )
        .await?;

        measure(
            &args,
            &metrics,
            &mut writer,
            "Versions",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let page = client
                    .table(LOGICAL_TABLE)
                    .versions()
                    .page_size(1000)
                    .next_page()
                    .await
                    .map_err(error)?
                    .ok_or("versions unexpectedly finished")?;
                Ok(page.versions.len())
            },
        )
        .await?;
        record_cache_usage(&mut cache_writer, &args, sample, &client)?;
    }
    writer.flush().map_err(error)?;
    writer.get_ref().sync_all().map_err(error)?;
    gc_writer.flush().map_err(error)?;
    gc_writer.get_ref().sync_all().map_err(error)?;
    cache_writer.flush().map_err(error)?;
    cache_writer.get_ref().sync_all().map_err(error)?;
    if args.cleanup {
        backend.clear_namespace().await.map_err(error)?;
    }
    println!(
        "wrote {} validated logical samples to {}",
        args.samples
            * (FIXED_OPERATIONS_PER_SAMPLE
                + args.transaction_shapes.len()
                + args.concurrency_writers.len()
                + usize::from(args.read_batch_items > 10) * 2),
        raw_path.display()
    );
    Ok(())
}

async fn run_history_workload(
    args: &Args,
    metrics: &PhysicalMetrics,
    backend: &DynamoDbBackend,
    raw_path: &std::path::Path,
) -> Result<(), String> {
    let mut writer = csv::Writer::from_path(raw_path).map_err(error)?;
    let cache_path = args.output.join("cache-usage.csv");
    let mut cache_writer = csv::Writer::from_path(&cache_path).map_err(error)?;
    for sample in 0..args.samples {
        let mut sample_prefix = backend.key_prefix().to_vec();
        sample_prefix.extend_from_slice(format!("history-sample-{sample}:").as_bytes());
        let sample_backend = backend.clone().with_key_prefix(sample_prefix);
        let client = Client::builder()
            .backend(sample_backend)
            .node_cache_max_bytes(args.node_cache_max_bytes)
            .open()
            .await
            .map_err(error)?;
        let table = format!("BenchmarkHistory{sample}");
        client
            .create_table()
            .table_name(&table)
            .attribute_definitions(
                AttributeDefinition::builder()
                    .attribute_name("id")
                    .attribute_type(ScalarAttributeType::S)
                    .build()
                    .map_err(error)?,
            )
            .key_schema(
                KeySchemaElement::builder()
                    .attribute_name("id")
                    .key_type(KeyType::Hash)
                    .build()
                    .map_err(error)?,
            )
            .send()
            .await
            .map_err(error)?;

        let history_key_bytes = logical_key_bytes("state");
        let history_item_bytes = logical_item_bytes("state", 128);
        let total_input_bytes = history_item_bytes
            .checked_mul(args.history_depth)
            .ok_or("history logical input byte count overflowed")?;
        let mut first_history_version = None;
        let mut final_history_version = None;
        measure(
            args,
            metrics,
            &mut writer,
            "HistoryAppendAll",
            "warm",
            sample,
            total_input_bytes,
            0,
            true,
            args.history_depth,
            async {
                for generation in 0..args.history_depth as u64 {
                    let result = client
                        .put_item()
                        .table_name(&table)
                        .item("id", AttributeValue::S("state".into()))
                        .item(
                            "payload",
                            AttributeValue::B(value(sample, 10 + generation, 128).into()),
                        )
                        .send_with_metadata()
                        .await
                        .map_err(error)?;
                    validate_versions(&result, 1)?;
                    if generation == 0 {
                        first_history_version =
                            Some(client.table(&table).head().await.map_err(error)?.id);
                    }
                }
                final_history_version = Some(client.table(&table).head().await.map_err(error)?.id);
                Ok(args.history_depth)
            },
        )
        .await?;
        let first_history_version = first_history_version
            .ok_or("history workload did not create its first immutable version")?;
        let final_history_version = final_history_version
            .ok_or("history workload did not capture its final immutable version")?;

        measure(
            args,
            metrics,
            &mut writer,
            "HistoryVersionsAll",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let mut paginator = client.table(&table).versions().page_size(1000);
                let mut seen = HashSet::new();
                while let Some(page) = paginator.next_page().await.map_err(error)? {
                    for version in page.versions {
                        if !seen.insert(version.id) {
                            return Err("history paginator emitted a duplicate version".into());
                        }
                    }
                }
                let expected = args
                    .history_depth
                    .checked_add(1)
                    .ok_or("history depth overflowed")?;
                if seen.len() != expected {
                    return Err(format!(
                        "history paginator emitted {} versions; expected {expected}",
                        seen.len()
                    ));
                }
                Ok(seen.len())
            },
        )
        .await?;

        let first_history_payload = value(sample, 10, 128);
        measure(
            args,
            metrics,
            &mut writer,
            "HistoryGetOldest",
            "warm",
            sample,
            history_key_bytes,
            history_item_bytes,
            true,
            0,
            async {
                let output = client
                    .table(&table)
                    .at(first_history_version.clone())
                    .get_item()
                    .key("id", AttributeValue::S("state".into()))
                    .send()
                    .await
                    .map_err(error)?;
                let payload = output
                    .item()
                    .and_then(|item| item.get("payload"))
                    .and_then(|payload| payload.as_b().ok())
                    .ok_or("oldest historical read omitted its payload")?;
                if payload.as_ref() != first_history_payload.as_slice() {
                    return Err("oldest historical read returned different bytes".into());
                }
                Ok(1)
            },
        )
        .await?;

        measure(
            args,
            metrics,
            &mut writer,
            "HistoryDiffOldestHead",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let mut paginator = client
                    .table(&table)
                    .diff(first_history_version, final_history_version)
                    .page_size(1000);
                let mut changes = 0usize;
                while let Some(page) = paginator.next_page().await.map_err(error)? {
                    changes = changes
                        .checked_add(page.diffs.len())
                        .ok_or("history diff count overflowed")?;
                }
                if changes != 1 {
                    return Err(format!(
                        "oldest-to-head history diff returned {changes} changes; expected 1"
                    ));
                }
                Ok(changes)
            },
        )
        .await?;

        let mut retention_plan = None;
        measure(
            args,
            metrics,
            &mut writer,
            "RetentionPlan",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let plan = client
                    .table(&table)
                    .retention(RetentionPolicy::keep_last(1))
                    .plan()
                    .await
                    .map_err(error)?;
                validate_retention_shape(args.history_depth, &plan)?;
                let observed = usize::try_from(plan.examined_versions).map_err(error)?;
                retention_plan = Some(plan);
                Ok(observed)
            },
        )
        .await?;
        let retention_plan = retention_plan.ok_or("retention planner returned no plan")?;
        measure(
            args,
            metrics,
            &mut writer,
            "RetentionApply",
            "warm",
            sample,
            0,
            0,
            false,
            0,
            async {
                let result = client
                    .table(&table)
                    .apply_retention(
                        &retention_plan,
                        MaintenanceContext::new(
                            "benchmark-history-admin",
                            format!("measure history retention sample {sample}"),
                        ),
                    )
                    .await
                    .map_err(error)?;
                if result.replayed || result.removed != retention_plan.remove {
                    return Err("RetentionApply did not durably remove the reviewed set".into());
                }
                Ok(result.removed.len())
            },
        )
        .await?;
        record_cache_usage(&mut cache_writer, args, sample, &client)?;
    }
    writer.flush().map_err(error)?;
    writer.get_ref().sync_all().map_err(error)?;
    cache_writer.flush().map_err(error)?;
    cache_writer.get_ref().sync_all().map_err(error)?;
    if args.cleanup {
        backend.clear_namespace().await.map_err(error)?;
    }
    println!(
        "wrote {} validated history samples to {}",
        args.samples * 6,
        raw_path.display()
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn measure<F>(
    args: &Args,
    metrics: &PhysicalMetrics,
    writer: &mut csv::Writer<std::fs::File>,
    operation: &str,
    cache_mode: &'static str,
    sample: usize,
    logical_input_item_bytes: usize,
    logical_output_item_bytes: usize,
    logical_item_bytes_complete: bool,
    versions_created: usize,
    future: F,
) -> Result<(), String>
where
    F: std::future::Future<Output = Result<usize, String>>,
{
    let before = metrics.snapshot();
    let started = Instant::now();
    let observed_items = future
        .await
        .map_err(|cause| format!("{operation}/{cache_mode}/{sample}: {cause}"))?;
    let latency_ns = started.elapsed().as_nanos();
    let physical = metrics.snapshot().delta(&before)?;
    if observed_items == 0
        || latency_ns == 0
        || physical.executions == 0
        || physical.attempts < physical.executions
    {
        return Err(format!("invalid sample {operation}/{cache_mode}/{sample}"));
    }
    let row = Sample {
        schema: SCHEMA,
        revision: args.revision.clone(),
        dirty: args.dirty,
        environment: "dynamodb-local",
        operation: operation.to_owned(),
        cache_mode,
        sample,
        records: args.records,
        configured_value_bytes: args.value_bytes,
        logical_input_item_bytes,
        logical_output_item_bytes,
        logical_item_bytes_complete,
        observed_items,
        versions_created,
        latency_ns,
        sdk_executions: physical.executions,
        http_attempts: physical.attempts,
        sdk_retries: physical.attempts - physical.executions,
        physical_request_bytes: physical.request_bytes,
        physical_response_bytes: physical.response_bytes,
        physical_response_bytes_complete: physical.response_bytes_unknown == 0,
        transaction_actions: physical.transaction_actions,
        api_attempts_json: serde_json::to_string(&physical.api_attempts).map_err(error)?,
        validated: true,
    };
    writer.serialize(row).map_err(error)?;
    writer.flush().map_err(error)?;
    writer.get_ref().sync_data().map_err(error)?;
    Ok(())
}

fn value(index: usize, generation: u64, bytes: usize) -> Vec<u8> {
    let mut seed = generation.to_le_bytes().to_vec();
    seed.extend_from_slice(&(index as u64).to_le_bytes());
    seed.iter().copied().cycle().take(bytes).collect()
}

fn logical_key_bytes(id: &str) -> usize {
    logical_attribute_bytes("id", id)
}

fn logical_attribute_bytes(name: &str, value: &str) -> usize {
    name.len() + value.len()
}

fn logical_item_bytes(id: &str, payload_bytes: usize) -> usize {
    logical_key_bytes(id) + "payload".len() + payload_bytes
}

fn fixture_batch_size(value_bytes: usize) -> usize {
    let item_bytes = logical_item_bytes("fixture-00000000000000000000", value_bytes);
    (LOGICAL_TRANSACTION_BYTES / item_bytes).clamp(1, 100)
}

fn validate_versions<T>(result: &WithMetadata<T>, expected: usize) -> Result<(), String> {
    let applied = result
        .transitions
        .iter()
        .filter(|transition| transition.applied)
        .count();
    if applied != expected || result.transitions.len() != expected {
        return Err(format!(
            "operation reported {} transitions with {applied} applied; expected {expected}",
            result.transitions.len()
        ));
    }
    Ok(())
}

fn validate_retention_shape(
    history_depth: usize,
    plan: &prolly_dynamodb_client::RetentionPlan,
) -> Result<(), String> {
    if plan.remove.is_empty() {
        return Err("RetentionPlan selected no isolated historical versions".into());
    }
    let expected_removals = history_depth.min(prolly_dynamodb_client::MAX_RETENTION_REMOVALS);
    let expected_more = history_depth > expected_removals;
    if plan.remove.len() != expected_removals
        || plan.more_removable != expected_more
        || usize::try_from(plan.examined_versions).ok() != history_depth.checked_add(1)
    {
        return Err(format!(
            "RetentionPlan did not match the exact history shape: removed={} expected_removed={} more_removable={} expected_more_removable={} examined={} expected_examined={}",
            plan.remove.len(),
            expected_removals,
            plan.more_removable,
            expected_more,
            plan.examined_versions,
            history_depth.saturating_add(1)
        ));
    }
    Ok(())
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        endpoint: "http://127.0.0.1:8000".into(),
        table: "prolly-versioned-client-bench".into(),
        root_table: String::new(),
        output: PathBuf::from("performance-results/dynamodb-client"),
        samples: 25,
        records: 100,
        value_bytes: 1024,
        read_batch_items: 100,
        history_depth: 100,
        workload: Workload::Full,
        transaction_shapes: vec![1, 10, 100],
        concurrency_writers: vec![1, 4, 8],
        concurrency_operations_per_writer: 5,
        concurrency_retry_limit: prolly_dynamodb_client::DEFAULT_LOGICAL_RETRY_LIMIT,
        node_cache_max_bytes: prolly_dynamodb_client::DEFAULT_NODE_CACHE_MAX_BYTES,
        revision: "unknown".into(),
        dirty: true,
        cleanup: true,
    };
    let values = std::env::args().collect::<Vec<_>>();
    let mut index = 1;
    while index < values.len() {
        let flag = values[index].as_str();
        match flag {
            "--endpoint" => args.endpoint = take(&values, &mut index, flag)?,
            "--table" => args.table = take(&values, &mut index, flag)?,
            "--root-table" => args.root_table = take(&values, &mut index, flag)?,
            "--output" => args.output = PathBuf::from(take(&values, &mut index, flag)?),
            "--samples" => args.samples = number(&take(&values, &mut index, flag)?, flag)?,
            "--records" => args.records = number(&take(&values, &mut index, flag)?, flag)?,
            "--value-bytes" => args.value_bytes = number(&take(&values, &mut index, flag)?, flag)?,
            "--read-batch-items" => {
                args.read_batch_items = number(&take(&values, &mut index, flag)?, flag)?
            }
            "--history-depth" => {
                args.history_depth = number(&take(&values, &mut index, flag)?, flag)?
            }
            "--workload" => {
                args.workload = match take(&values, &mut index, flag)?.as_str() {
                    "full" => Workload::Full,
                    "history" => Workload::History,
                    value => return Err(format!("invalid --workload: {value}")),
                }
            }
            "--transaction-shapes" => {
                args.transaction_shapes = transaction_shapes(&take(&values, &mut index, flag)?)?
            }
            "--concurrency-writers" => {
                args.concurrency_writers =
                    positive_shapes(&take(&values, &mut index, flag)?, flag, 64)?
            }
            "--concurrency-operations-per-writer" => {
                args.concurrency_operations_per_writer =
                    number(&take(&values, &mut index, flag)?, flag)?
            }
            "--concurrency-retry-limit" => {
                args.concurrency_retry_limit = number(&take(&values, &mut index, flag)?, flag)?
            }
            "--node-cache-max-bytes" => {
                args.node_cache_max_bytes = number(&take(&values, &mut index, flag)?, flag)?
            }
            "--revision" => args.revision = take(&values, &mut index, flag)?,
            "--dirty" => args.dirty = true,
            "--clean" => args.dirty = false,
            "--skip-cleanup" => args.cleanup = false,
            _ => return Err(format!("unknown argument {flag}")),
        }
        index += 1;
    }
    if args.root_table.is_empty() {
        args.root_table = format!("{}-roots", args.table);
    }
    Ok(args)
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.endpoint.is_empty()
        || args.table.is_empty()
        || args.root_table.is_empty()
        || args.revision.is_empty()
        || args.samples == 0
        || args.records < args.samples * 3
        || args.records < 100
        || args.records < 10 + args.samples
        || args.value_bytes == 0
        || !(10..=100).contains(&args.read_batch_items)
        || logical_item_bytes("fixture-00000000000000000000", args.value_bytes)
            .saturating_mul(args.read_batch_items)
            > LOGICAL_TRANSACTION_BYTES
        || args.history_depth < 10
        || args.transaction_shapes.is_empty()
        || args.concurrency_writers.is_empty()
        || args.concurrency_writers.first() != Some(&1)
        || args.concurrency_operations_per_writer == 0
        || args.concurrency_operations_per_writer > 1_000
        || args.concurrency_retry_limit > prolly_dynamodb_client::MAX_LOGICAL_RETRY_LIMIT
        || args.transaction_shapes.iter().any(|shape| {
            *shape == 0
                || *shape > 100
                || logical_item_bytes("transact-100-00000000000000000000-099", args.value_bytes)
                    .saturating_mul(*shape)
                    > LOGICAL_TRANSACTION_BYTES
        })
    {
        return Err(
            "invalid benchmark configuration: endpoint/table/revision must be non-empty; records >= 100, >= 10 + samples, and >= samples * 3; read batch items must be 10..=100 within the 4-MiB transaction-read response envelope; history depth must be at least 10; transaction shapes must be 1..=100 and remain within the 4-MiB logical aggregate; concurrency writers must be 1..=64, operations per writer 1..=1000, and retry limit within the advertised client maximum"
                .into(),
        );
    }
    Ok(())
}

fn transaction_shapes(value: &str) -> Result<Vec<usize>, String> {
    positive_shapes(value, "--transaction-shapes", 100)
}

fn positive_shapes(value: &str, flag: &str, maximum: usize) -> Result<Vec<usize>, String> {
    let mut shapes = value
        .split(',')
        .map(|shape| number(shape, flag))
        .collect::<Result<Vec<_>, _>>()?;
    shapes.sort_unstable();
    shapes.dedup();
    if shapes.is_empty() || shapes.iter().any(|shape| *shape == 0 || *shape > maximum) {
        return Err(format!("{flag} must contain values in 1..={maximum}"));
    }
    Ok(shapes)
}

fn take(values: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    values
        .get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn number<T: std::str::FromStr>(value: &str, flag: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|cause| format!("invalid {flag}: {cause}"))
}

fn error(value: impl std::fmt::Display) -> String {
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_delta_is_checked_and_api_scoped() {
        let before = PhysicalSnapshot {
            executions: 2,
            attempts: 3,
            request_bytes: 10,
            response_bytes: 20,
            response_bytes_unknown: 0,
            transaction_actions: 1,
            api_attempts: BTreeMap::from([("GetItem".into(), 3)]),
        };
        let after = PhysicalSnapshot {
            executions: 4,
            attempts: 6,
            request_bytes: 40,
            response_bytes: 70,
            response_bytes_unknown: 1,
            transaction_actions: 5,
            api_attempts: BTreeMap::from([("GetItem".into(), 4), ("TransactWriteItems".into(), 2)]),
        };
        let delta = after.delta(&before).unwrap();
        assert_eq!(delta.executions, 2);
        assert_eq!(delta.attempts, 3);
        assert_eq!(delta.api_attempts.get("GetItem"), Some(&1));
        assert_eq!(delta.api_attempts.get("TransactWriteItems"), Some(&2));
        assert_eq!(delta.transaction_actions, 4);
    }

    #[test]
    fn generated_values_have_exact_size_and_change() {
        assert_eq!(value(1, 0, 1024).len(), 1024);
        assert_ne!(value(1, 0, 16), value(1, 1, 16));
    }

    #[test]
    fn transaction_shapes_are_canonical_and_size_bounded() {
        assert_eq!(transaction_shapes("100,1,10,10").unwrap(), [1, 10, 100]);
        assert!(transaction_shapes("").is_err());

        let mut args = parse_args_from_defaults_for_test();
        args.value_bytes = 399_000;
        args.read_batch_items = 10;
        args.transaction_shapes = vec![1, 10];
        assert!(validate_args(&args).is_ok());
        args.transaction_shapes.push(100);
        assert!(validate_args(&args).is_err());
    }

    #[test]
    fn extended_read_shape_respects_transaction_response_limit() {
        let mut args = parse_args_from_defaults_for_test();
        args.read_batch_items = 100;
        assert!(validate_args(&args).is_ok());
        args.value_bytes = 399_000;
        assert!(validate_args(&args).is_err());
        args.read_batch_items = 10;
        args.transaction_shapes = vec![1, 10];
        assert!(validate_args(&args).is_ok());
    }

    #[test]
    fn fixture_batches_respect_action_and_logical_byte_limits() {
        assert_eq!(fixture_batch_size(1024), 100);
        let large = fixture_batch_size(399_000);
        assert_eq!(large, 10);
        assert!(
            logical_item_bytes("fixture-00000000000000000000", 399_000) * large
                <= LOGICAL_TRANSACTION_BYTES
        );
    }

    #[test]
    fn history_depth_includes_shallow_and_bounded_retention_paths() {
        let mut args = parse_args_from_defaults_for_test();
        args.history_depth = 10;
        assert!(validate_args(&args).is_ok());
        args.history_depth = 9;
        assert!(validate_args(&args).is_err());
        args.history_depth = prolly_dynamodb_client::MAX_RETENTION_REMOVALS;
        assert!(validate_args(&args).is_ok());
        args.history_depth = prolly_dynamodb_client::MAX_RETENTION_REMOVALS + 1;
        assert!(validate_args(&args).is_ok());
    }

    #[test]
    fn gc_tree_limit_scales_with_history_and_checks_overflow() {
        let mut args = parse_args_from_defaults_for_test();
        assert_eq!(benchmark_gc_limits(&args).unwrap().max_roots, 10_400);
        args.history_depth = 10_000;
        let limits = benchmark_gc_limits(&args).unwrap();
        assert_eq!(limits.max_roots, 50_000);
        assert_eq!(limits.max_live_nodes, 200_000);
        args.history_depth = usize::MAX;
        assert!(benchmark_gc_limits(&args).is_err());
    }

    fn parse_args_from_defaults_for_test() -> Args {
        Args {
            endpoint: "http://127.0.0.1:8000".into(),
            table: "table".into(),
            root_table: "roots".into(),
            output: PathBuf::from("out"),
            samples: 1,
            records: 100,
            value_bytes: 1024,
            read_batch_items: 100,
            history_depth: 100,
            workload: Workload::Full,
            transaction_shapes: vec![1, 10, 100],
            concurrency_writers: vec![1, 4, 8],
            concurrency_operations_per_writer: 5,
            concurrency_retry_limit: prolly_dynamodb_client::DEFAULT_LOGICAL_RETRY_LIMIT,
            node_cache_max_bytes: prolly_dynamodb_client::DEFAULT_NODE_CACHE_MAX_BYTES,
            revision: "test".into(),
            dirty: false,
            cleanup: true,
        }
    }
}
