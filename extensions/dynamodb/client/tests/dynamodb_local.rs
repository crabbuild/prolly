use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{path::Path, process::Command};

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, GlobalSecondaryIndex, KeySchemaElement, KeyType,
    Projection, ProjectionType, ScalarAttributeType,
};
use prolly::{AsyncBlobStore, Node, RemoteProllyStore, RemoteStoreBackend, RemoteStoreConfig};
use prolly_dynamodb_client::{
    Client, Error, GcApplyOptions, GcCursor, GcPlanLimits, MaintenanceContext, RetentionPolicy,
    StreamWorkerOptions, TtlWorkerOptions, Worker,
};
use prolly_dynamodb_core::{
    AttributeValue as CoreAttributeValue, BlobFuture, BlobStorage, Clock, Database, IdGenerator,
    Item, KeyAttribute, KeyKind, LargeValueConfig, StoragePublicationMode, TableId,
};
use prolly_store_dynamodb::{dynamodb_safe_config, DynamoDbBackend, DynamoDbBlobStore};

#[test]
fn configured_store_and_versioned_crud_round_trip_through_dynamodb_local() {
    // The deliberately broad debug-build contract future includes AWS SDK,
    // history, indexes, workers, and GC. Give only this test a deterministic
    // stack instead of requiring a process-wide RUST_MIN_STACK override.
    std::thread::Builder::new()
        .name("dynamodb-local-contract".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(run_dynamodb_local_contract());
        })
        .unwrap()
        .join()
        .unwrap();
}

#[derive(Default)]
struct DeterministicIds(Mutex<u8>);

impl IdGenerator for DeterministicIds {
    fn generate(&self) -> prolly_dynamodb_core::Result<TableId> {
        let mut value = self.0.lock().unwrap();
        *value = value.checked_add(1).expect("test ID sequence exhausted");
        Ok(TableId([*value; 32]))
    }
}

struct FixedClock;

impl Clock for FixedClock {
    fn now_millis(&self) -> u64 {
        1_700_000_000_000
    }
}

#[test]
fn fluent_input_and_core_traces_have_identical_canonical_roots() {
    std::thread::Builder::new()
        .name("dynamodb-local-canonical-parity".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(run_canonical_parity_contract());
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn multi_process_soak_preserves_items_versions_and_commits() {
    if std::env::var("PROLLY_DYNAMODB_RUN_SOAK").as_deref() != Ok("1") {
        eprintln!("skipping: PROLLY_DYNAMODB_RUN_SOAK is not set to 1");
        return;
    }
    std::thread::Builder::new()
        .name("dynamodb-local-multi-process-soak".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(run_multi_process_soak());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run_multi_process_soak() {
    let endpoint = std::env::var("PROLLY_STORE_DYNAMODB_ENDPOINT")
        .expect("soak requires PROLLY_STORE_DYNAMODB_ENDPOINT");
    let physical_table = std::env::var("PROLLY_DYNAMODB_CLIENT_TEST_TABLE")
        .unwrap_or_else(|_| "prolly-versioned-client-test".into());
    let root_table = format!("{physical_table}-soak-roots");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("client-soak-{}-{nonce}:", std::process::id()).into_bytes();
    let workers = soak_usize("PROLLY_DYNAMODB_SOAK_WORKERS", 4, 2, 16);
    let iterations = soak_usize("PROLLY_DYNAMODB_SOAK_ITERATIONS", 50, 8, 2_000);
    let pause_at = 5.min(iterations - 1);

    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(&endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .build();
    let backend =
        DynamoDbBackend::new(aws_sdk_dynamodb::Client::from_conf(config), &physical_table)
            .with_root_table_name(&root_table)
            .with_key_prefix(prefix.clone());
    backend.initialize_schema().await.unwrap();
    let client = Client::open(backend.clone()).await.unwrap();
    client
        .create_table()
        .table_name("Soak")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .request_token(format!("soak-create-{nonce}"))
        .send()
        .await
        .unwrap();

    let progress_path = |writer: usize, generation: &str| {
        std::env::temp_dir().join(format!(
            "prolly-dynamodb-soak-{}-{nonce}-{writer}-{generation}.json",
            std::process::id()
        ))
    };
    let crashed_path = progress_path(0, "crashed");
    let mut crashed = soak_writer_command(
        &endpoint,
        &physical_table,
        &root_table,
        &prefix,
        0,
        iterations,
        &crashed_path,
        Some(pause_at),
    )
    .spawn()
    .unwrap();
    let mut normal_children = Vec::new();
    let mut normal_paths = Vec::new();
    for writer in 1..workers {
        let path = progress_path(writer, "normal");
        normal_children.push(
            soak_writer_command(
                &endpoint,
                &physical_table,
                &root_table,
                &prefix,
                writer,
                iterations,
                &path,
                None,
            )
            .spawn()
            .unwrap(),
        );
        normal_paths.push(path);
    }

    wait_for_soak_started(&crashed_path, pause_at, Duration::from_secs(30));
    crashed.kill().unwrap();
    let crashed_status = crashed.wait().unwrap();
    assert!(
        !crashed_status.success(),
        "the selected writer must be killed"
    );

    let restarted_path = progress_path(0, "restarted");
    let restarted = soak_writer_command(
        &endpoint,
        &physical_table,
        &root_table,
        &prefix,
        0,
        iterations,
        &restarted_path,
        None,
    )
    .output()
    .unwrap();
    assert_child_succeeded("restarted soak writer", &restarted);
    for (writer, child) in normal_children.into_iter().enumerate() {
        let output = child.wait_with_output().unwrap();
        assert_child_succeeded(&format!("soak writer {}", writer + 1), &output);
    }

    for path in normal_paths.iter().chain([&restarted_path]) {
        let progress: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(progress["acked"], iterations);
        std::fs::remove_file(path).unwrap();
    }
    let crashed_progress: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&crashed_path).unwrap()).unwrap();
    assert_eq!(crashed_progress["started"], pause_at);
    assert!(crashed_progress["acked"].as_u64().unwrap() <= pause_at as u64);
    std::fs::remove_file(&crashed_path).unwrap();

    let expected_writes = workers * iterations;
    let scan = client
        .scan()
        .table_name("Soak")
        .limit(i32::try_from(expected_writes + 1).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(scan.count as usize, expected_writes);
    assert!(scan.last_evaluated_key().is_none());
    let versions = client.table("Soak").collect_versions().await.unwrap();
    assert_eq!(versions.len(), expected_writes + 1);
    let commits = client
        .table("Soak")
        .commits(None, expected_writes + 1)
        .await
        .unwrap();
    assert_eq!(commits.commits.len(), expected_writes + 1);
    assert_eq!(
        commits
            .commits
            .iter()
            .map(|commit| commit.sequence)
            .collect::<Vec<_>>(),
        (1..=u64::try_from(expected_writes + 1).unwrap()).collect::<Vec<_>>()
    );
    let unique_commits = commits
        .commits
        .iter()
        .map(|commit| commit.commit_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(unique_commits.len(), expected_writes + 1);
    backend.clear_namespace().await.unwrap();
}

async fn run_canonical_parity_contract() {
    let Some(endpoint) = std::env::var("PROLLY_STORE_DYNAMODB_ENDPOINT").ok() else {
        eprintln!("skipping: PROLLY_STORE_DYNAMODB_ENDPOINT is not set");
        return;
    };
    let physical_table = std::env::var("PROLLY_DYNAMODB_CLIENT_TEST_TABLE")
        .unwrap_or_else(|_| "prolly-versioned-client-test".into());
    let root_table = format!("{physical_table}-canonical-parity-roots");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let aws_config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .build();
    let aws_client = aws_sdk_dynamodb::Client::from_conf(aws_config);
    let backend = |mode: &str| {
        DynamoDbBackend::new(aws_client.clone(), &physical_table)
            .with_root_table_name(&root_table)
            .with_key_prefix(
                format!("canonical-parity-{}-{nonce}:{mode}:", std::process::id()).into_bytes(),
            )
    };
    let fluent_backend = backend("fluent");
    let input_backend = backend("input");
    let core_backend = backend("core");
    for candidate in [&fluent_backend, &input_backend, &core_backend] {
        candidate.initialize_schema().await.unwrap();
    }

    let fluent = Client::builder()
        .backend(fluent_backend.clone())
        .id_generator(Arc::new(DeterministicIds::default()))
        .clock(Arc::new(FixedClock))
        .open()
        .await
        .unwrap();
    let input = Client::builder()
        .backend(input_backend.clone())
        .id_generator(Arc::new(DeterministicIds::default()))
        .clock(Arc::new(FixedClock))
        .open()
        .await
        .unwrap();
    let core_store = RemoteProllyStore::with_config(
        core_backend.clone(),
        RemoteStoreConfig {
            verify_node_cids: true,
        },
    );
    let mut core_config = dynamodb_safe_config();
    core_config.runtime.node_cache_max_bytes =
        Some(prolly_dynamodb_client::DEFAULT_NODE_CACHE_MAX_BYTES);
    let core = Database::open_with_blob_storage_and_mode_and_sources(
        core_store,
        core_config,
        Arc::new(TestBlobStorage(DynamoDbBlobStore::new(
            core_backend.clone(),
        ))),
        LargeValueConfig::default(),
        StoragePublicationMode::PrepublishImmutableNodes,
        Arc::new(DeterministicIds::default()),
        Arc::new(FixedClock),
    )
    .await
    .unwrap();

    fluent
        .create_table()
        .table_name("Orders")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("account")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("account")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    input
        .execute_create_table(
            aws_sdk_dynamodb::operation::create_table::CreateTableInput::builder()
                .table_name("Orders")
                .attribute_definitions(
                    AttributeDefinition::builder()
                        .attribute_name("account")
                        .attribute_type(ScalarAttributeType::S)
                        .build()
                        .unwrap(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("account")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    core.create_table(
        "Orders",
        KeyAttribute {
            name: "account".into(),
            kind: KeyKind::String,
        },
        None,
    )
    .await
    .unwrap();

    let aws_item = std::collections::HashMap::from([
        ("account".into(), AttributeValue::S("acct-1".into())),
        ("amount".into(), AttributeValue::N("123.4500".into())),
        (
            "evidence".into(),
            AttributeValue::B(aws_sdk_dynamodb::primitives::Blob::new([0, 1, 2, 255])),
        ),
    ]);
    fluent
        .put_item()
        .table_name("Orders")
        .set_item(Some(aws_item.clone()))
        .send()
        .await
        .unwrap();
    input
        .execute_put_item(
            aws_sdk_dynamodb::operation::put_item::PutItemInput::builder()
                .table_name("Orders")
                .set_item(Some(aws_item))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    core.put_item(
        "Orders",
        Item::from([
            ("account".into(), CoreAttributeValue::S("acct-1".into())),
            (
                "amount".into(),
                CoreAttributeValue::N(
                    prolly_dynamodb_core::DynamoNumber::parse("123.4500").unwrap(),
                ),
            ),
            ("evidence".into(), CoreAttributeValue::B(vec![0, 1, 2, 255])),
        ]),
        None,
    )
    .await
    .unwrap();

    let fluent_roots = canonical_root_manifests(&fluent_backend).await;
    let input_roots = canonical_root_manifests(&input_backend).await;
    let core_roots = canonical_root_manifests(&core_backend).await;
    assert!(!fluent_roots.is_empty());
    assert_canonical_roots_equal("input", &input_roots, "fluent", &fluent_roots);
    assert_canonical_roots_equal("core", &core_roots, "fluent", &fluent_roots);

    fluent_backend.clear_namespace().await.unwrap();
    input_backend.clear_namespace().await.unwrap();
    core_backend.clear_namespace().await.unwrap();
}

async fn canonical_root_manifests(
    backend: &DynamoDbBackend,
) -> std::collections::BTreeMap<Vec<u8>, Vec<u8>> {
    backend
        .list_root_manifests()
        .await
        .unwrap()
        .into_iter()
        .map(|root| (root.name, root.manifest))
        .collect()
}

fn assert_canonical_roots_equal(
    left_name: &str,
    left: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
    right_name: &str,
    right: &std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
) {
    let left_only = left
        .keys()
        .filter(|key| !right.contains_key(*key))
        .map(|key| root_name_for_diagnostic(key))
        .collect::<Vec<_>>();
    let right_only = right
        .keys()
        .filter(|key| !left.contains_key(*key))
        .map(|key| root_name_for_diagnostic(key))
        .collect::<Vec<_>>();
    let changed = left
        .iter()
        .filter(|(key, value)| right.get(*key).is_some_and(|other| other != *value))
        .map(|(key, _)| root_name_for_diagnostic(key))
        .collect::<Vec<_>>();
    assert!(
        left_only.is_empty() && right_only.is_empty() && changed.is_empty(),
        "canonical roots differ: {left_name}_only={left_only:?}, {right_name}_only={right_only:?}, changed={changed:?}"
    );
}

fn root_name_for_diagnostic(name: &[u8]) -> String {
    name.iter()
        .map(|byte| {
            if byte.is_ascii_graphic() || *byte == b' ' {
                char::from(*byte).to_string()
            } else {
                format!("\\x{byte:02x}")
            }
        })
        .collect()
}

async fn run_dynamodb_local_contract() {
    let Some(endpoint) = std::env::var("PROLLY_STORE_DYNAMODB_ENDPOINT").ok() else {
        eprintln!("skipping: PROLLY_STORE_DYNAMODB_ENDPOINT is not set");
        return;
    };
    let physical_table = std::env::var("PROLLY_DYNAMODB_CLIENT_TEST_TABLE")
        .unwrap_or_else(|_| "prolly-versioned-client-test".into());
    let root_table = format!("{physical_table}-custom-roots");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("client-contract-{}-{nonce}:", std::process::id()).into_bytes();
    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint.clone())
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .build();
    let backend =
        DynamoDbBackend::new(aws_sdk_dynamodb::Client::from_conf(config), &physical_table)
            .with_root_table_name(&root_table)
            .with_key_prefix(prefix.clone())
            .with_read_parallelism(3)
            .with_batch_get_parallelism(5)
            .with_batch_write_parallelism(7)
            .with_scan_parallelism(11);
    backend.initialize_schema().await.unwrap();
    let cleanup = backend.clone();
    let existing_store = RemoteProllyStore::with_config(
        backend.clone(),
        RemoteStoreConfig {
            verify_node_cids: true,
        },
    );
    let core_store = existing_store.clone();

    let client = Client::builder()
        .backend(backend)
        .remote_store_config(RemoteStoreConfig {
            verify_node_cids: true,
        })
        .logical_retry_limit(3)
        .node_cache_max_nodes(17)
        .node_cache_max_bytes(1024 * 1024)
        .open()
        .await
        .unwrap();
    assert_eq!(client.backend().table_name(), physical_table);
    assert_eq!(client.backend().root_table_name(), root_table);
    assert_eq!(client.backend().key_prefix(), prefix);
    assert_eq!(client.backend().read_parallelism(), 3);
    assert_eq!(client.backend().batch_get_parallelism(), 5);
    assert_eq!(client.backend().batch_write_parallelism(), 7);
    assert_eq!(client.backend().scan_parallelism(), 11);
    assert!(client.remote_store_config().verify_node_cids);
    assert_eq!(client.logical_retry_limit(), 3);
    assert_eq!(client.capabilities().logical_retry_limit, 3);
    assert!(client.capabilities().process_local_write_admission);
    assert_eq!(client.capabilities().node_cache_max_nodes, Some(17));
    assert_eq!(
        client.capabilities().node_cache_max_bytes,
        Some(1024 * 1024)
    );
    let open_cache = client.cache_usage();
    assert!(open_cache.entries > 0);
    assert!(open_cache.serialized_bytes > 0);
    assert_eq!(open_cache.pinned_entries, 0);
    assert_eq!(open_cache.pinned_serialized_bytes, 0);
    assert!(open_cache.serialized_bytes <= 1024 * 1024);

    let from_existing_store = Client::open_store(existing_store).await.unwrap();
    assert_eq!(from_existing_store.backend().table_name(), physical_table);
    assert_eq!(from_existing_store.backend().root_table_name(), root_table);
    assert_eq!(from_existing_store.backend().key_prefix(), prefix);
    assert!(from_existing_store.remote_store_config().verify_node_cids);
    assert_eq!(
        from_existing_store.logical_retry_limit(),
        prolly_dynamodb_core::DEFAULT_LOGICAL_RETRY_LIMIT
    );
    assert_eq!(
        from_existing_store.capabilities().node_cache_max_bytes,
        Some(prolly_dynamodb_client::DEFAULT_NODE_CACHE_MAX_BYTES)
    );
    let core = Database::open_with_blob_storage_and_mode(
        core_store,
        dynamodb_safe_config(),
        Arc::new(TestBlobStorage(DynamoDbBlobStore::new(
            from_existing_store.backend().clone(),
        ))),
        LargeValueConfig::default(),
        StoragePublicationMode::PrepublishImmutableNodes,
    )
    .await
    .unwrap();

    client
        .create_table()
        .table_name("Orders")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("account")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("status")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("account")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("ByStatus")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("status")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let parity_item = std::collections::HashMap::from([
        ("account".into(), AttributeValue::S("parity".into())),
        ("status".into(), AttributeValue::S("STABLE".into())),
    ]);
    client
        .put_item()
        .table_name("Orders")
        .set_item(Some(parity_item.clone()))
        .send()
        .await
        .unwrap();
    let fluent_head = client.table("Orders").head().await.unwrap().id;
    client
        .execute_put_item(
            aws_sdk_dynamodb::operation::put_item::PutItemInput::builder()
                .table_name("Orders")
                .set_item(Some(parity_item.clone()))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    let input_head = client.table("Orders").head().await.unwrap().id;
    assert_eq!(input_head, fluent_head);

    let core_item = Item::from([
        ("account".into(), CoreAttributeValue::S("parity".into())),
        ("status".into(), CoreAttributeValue::S("STABLE".into())),
    ]);
    core.put_item("Orders", core_item.clone(), None)
        .await
        .unwrap();
    assert_eq!(core.head("Orders").await.unwrap().id, fluent_head);

    let parity_key =
        std::collections::HashMap::from([("account".into(), AttributeValue::S("parity".into()))]);
    let fluent_read = client
        .get_item()
        .table_name("Orders")
        .set_key(Some(parity_key.clone()))
        .send()
        .await
        .unwrap();
    let input_read = client
        .execute_get_item(
            aws_sdk_dynamodb::operation::get_item::GetItemInput::builder()
                .table_name("Orders")
                .set_key(Some(parity_key))
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(fluent_read.item(), input_read.item());
    let core_key = Item::from([("account".into(), CoreAttributeValue::S("parity".into()))]);
    assert_eq!(
        core.get_item("Orders", &core_key).await.unwrap(),
        Some(core_item)
    );
    let first = client
        .put_item()
        .table_name("Orders")
        .item("account", AttributeValue::S("acct-1".into()))
        .item("status", AttributeValue::S("OPEN".into()))
        .send_with_metadata()
        .await
        .unwrap();
    let first_version = first.version_id.unwrap();
    client
        .put_item()
        .table_name("Orders")
        .item("account", AttributeValue::S("acct-1".into()))
        .item("status", AttributeValue::S("CLOSED".into()))
        .send()
        .await
        .unwrap();

    let historical = client
        .table("Orders")
        .at(first_version.clone())
        .get_item()
        .key("account", AttributeValue::S("acct-1".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        historical.item().unwrap()["status"],
        AttributeValue::S("OPEN".into())
    );
    let current_index = client
        .query()
        .table_name("Orders")
        .index_name("ByStatus")
        .key_condition_expression("#status = :status")
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":status", AttributeValue::S("CLOSED".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(current_index.count, 1);
    let historical_index = client
        .table("Orders")
        .at(first_version.clone())
        .query()
        .index_name("ByStatus")
        .key_condition_expression("#status = :status")
        .expression_attribute_names("#status", "status")
        .expression_attribute_values(":status", AttributeValue::S("OPEN".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(historical_index.count, 1);
    let stale = client
        .table("Orders")
        .if_head(first_version.clone())
        .put_item()
        .item("account", AttributeValue::S("acct-2".into()))
        .send()
        .await;
    assert!(matches!(stale, Err(Error::HeadConflict { .. })));

    let race_head = client.table("Orders").head().await.unwrap().id;
    run_point_write_race(
        &endpoint,
        &physical_table,
        &root_table,
        &prefix,
        &race_head,
        nonce,
    );

    // Explicit stream workers are single-owner and resume strictly after the
    // durable sequence checkpoint when reconstructed.
    let mut stream = client
        .workers()
        .stream(StreamWorkerOptions::new(
            "Orders",
            "dynamodb-local-audit",
            "worker-a",
        ))
        .await
        .unwrap();
    let competing = client
        .workers()
        .stream(StreamWorkerOptions::new(
            "Orders",
            "dynamodb-local-audit",
            "worker-b",
        ))
        .await;
    assert!(matches!(
        competing,
        Err(Error::Core(
            prolly_dynamodb_core::Error::WorkerLeaseHeld { .. }
        ))
    ));
    run_worker_probe(
        "expect-held",
        &endpoint,
        &physical_table,
        &root_table,
        &prefix,
        None,
    );
    let rejected_id = Arc::new(Mutex::new(None));
    let rejected_sink = Arc::clone(&rejected_id);
    let rejected = stream
        .run_once(&mut move |commit| {
            let rejected_sink = Arc::clone(&rejected_sink);
            async move {
                *rejected_sink.lock().unwrap() = Some(commit.commit_id);
                Err::<(), _>(std::io::Error::other("injected sink rejection"))
            }
        })
        .await;
    assert!(matches!(rejected, Err(Error::WorkerSink { .. })));
    assert!(stream.checkpoint().is_none());
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let sink_records = Arc::clone(&delivered);
    let first_stream_page = stream
        .run_once(&mut move |commit| {
            let sink_records = Arc::clone(&sink_records);
            async move {
                sink_records.lock().unwrap().push(commit);
                Ok::<_, std::io::Error>(())
            }
        })
        .await
        .unwrap();
    assert!(first_stream_page.delivered >= 3);
    assert_eq!(
        delivered
            .lock()
            .unwrap()
            .first()
            .map(|commit| &commit.commit_id),
        rejected_id.lock().unwrap().as_ref(),
    );
    assert_eq!(
        stream
            .checkpoint()
            .and_then(|checkpoint| match checkpoint.progress {
                prolly_dynamodb_client::WorkerProgress::Stream {
                    delivered_through_sequence,
                    ..
                } => Some(delivered_through_sequence),
                _ => None,
            }),
        first_stream_page.delivered_through_sequence
    );
    stream.shutdown().await.unwrap();

    let retention = client
        .table("Orders")
        .retention(RetentionPolicy::keep_last(0).protect(first_version.clone()))
        .plan()
        .await
        .unwrap();
    assert!(!retention.remove.contains(&first_version));
    client
        .table("Orders")
        .apply_retention(
            &retention,
            MaintenanceContext::new("worker-test", "checkpoint retention compatibility"),
        )
        .await
        .unwrap();
    let protected_history = client
        .table("Orders")
        .at(first_version)
        .get_item()
        .key("account", AttributeValue::S("acct-1".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        protected_history.item().unwrap()["status"],
        AttributeValue::S("OPEN".into())
    );

    client
        .put_item()
        .table_name("Orders")
        .item("account", AttributeValue::S("acct-stream-resume".into()))
        .item("status", AttributeValue::S("OPEN".into()))
        .send()
        .await
        .unwrap();
    let probe_path = std::env::temp_dir().join(format!(
        "prolly-dynamodb-worker-probe-{}-{nonce}.json",
        std::process::id()
    ));
    let probe = run_worker_probe(
        "resume",
        &endpoint,
        &physical_table,
        &root_table,
        &prefix,
        Some(&probe_path),
    )
    .unwrap();
    assert_eq!(probe["delivered"], 1);
    assert_eq!(probe["commit_ids"].as_array().unwrap().len(), 1);

    // The parent process sees the checkpoint written by the child and has no
    // duplicate source event after acquiring the next fencing generation.
    let mut after_child = client
        .workers()
        .stream(StreamWorkerOptions::new(
            "Orders",
            "dynamodb-local-audit",
            "worker-parent-after-child",
        ))
        .await
        .unwrap();
    let after_child_page = after_child
        .run_once(&mut |_commit| async { Ok::<_, std::io::Error>(()) })
        .await
        .unwrap();
    assert_eq!(after_child_page.delivered, 0);
    after_child.shutdown().await.unwrap();

    // TTL applies DynamoDB's epoch-number eligibility window and deletes only
    // while the observed TTL value is still equal.
    let now = 2_000_000_000_u64;
    for (account, expiry) in [
        ("ttl-expired", AttributeValue::N((now - 1).to_string())),
        ("ttl-future", AttributeValue::N((now + 1).to_string())),
        (
            "ttl-fractional",
            AttributeValue::N(format!("{}.5", now - 1)),
        ),
        ("ttl-string", AttributeValue::S((now - 1).to_string())),
    ] {
        client
            .put_item()
            .table_name("Orders")
            .item("account", AttributeValue::S(account.into()))
            .item("status", AttributeValue::S("OPEN".into()))
            .item("expiresAt", expiry)
            .send()
            .await
            .unwrap();
    }
    let mut ttl = client
        .workers()
        .ttl(TtlWorkerOptions::new("Orders", "expiresAt", "ttl-worker-a"))
        .await
        .unwrap();
    let ttl_page = ttl.run_once_at(now).await.unwrap();
    assert_eq!(ttl_page.deleted, 1);
    ttl.shutdown().await.unwrap();
    for (account, exists) in [
        ("ttl-expired", false),
        ("ttl-future", true),
        ("ttl-fractional", true),
        ("ttl-string", true),
    ] {
        let output = client
            .get_item()
            .table_name("Orders")
            .key("account", AttributeValue::S(account.into()))
            .send()
            .await
            .unwrap();
        assert_eq!(output.item.is_some(), exists, "TTL item {account}");
    }

    let mut orphan_node = Node::new_leaf();
    orphan_node.keys.push(b"orphan".to_vec());
    orphan_node.vals.push(b"unreachable".to_vec());
    let orphan_node_bytes = orphan_node.to_bytes();
    let orphan_node_cid = orphan_node.cid();
    RemoteStoreBackend::put_node(
        client.backend(),
        orphan_node_cid.as_bytes(),
        &orphan_node_bytes,
    )
    .await
    .unwrap();
    let blob_store = DynamoDbBlobStore::new(client.backend().clone());
    let orphan_blob = blob_store.put_blob(b"unreachable blob").await.unwrap();
    let lease = client
        .acquire_maintenance_lease(
            MaintenanceContext::new("gc-test", "bounded dry-run"),
            60_000,
        )
        .await
        .unwrap();
    let limits = GcPlanLimits::new(
        10_000,
        100_000,
        64 * 1024 * 1024,
        1_000_000,
        100_000,
        64 * 1024 * 1024,
        1_000,
    );
    let mut cursor: Option<GcCursor> = None;
    let mut found_node = false;
    let mut found_blob = false;
    for _ in 0..100 {
        let gc = client
            .plan_gc(&lease.id, cursor.as_ref(), limits)
            .await
            .unwrap();
        let page_has_node = gc.reclaimable_nodes.contains(&orphan_node_cid);
        let page_has_blob = gc
            .reclaimable_blobs
            .iter()
            .any(|candidate| candidate.cid == orphan_blob.cid && candidate.len == orphan_blob.len);
        found_node |= page_has_node;
        found_blob |= page_has_blob;
        // Planning is strictly read-only.
        if page_has_node {
            assert!(
                RemoteStoreBackend::get_node(client.backend(), orphan_node_cid.as_bytes())
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        if page_has_node
            || page_has_blob
            || !gc.reclaimable_nodes.is_empty()
            || !gc.reclaimable_blobs.is_empty()
        {
            let context = MaintenanceContext::new("gc-test", "apply reviewed bounded page")
                .change_ticket("TEST-GC-1");
            let applied = client
                .apply_gc(&gc, context.clone(), GcApplyOptions::default())
                .await
                .unwrap();
            assert!(!applied.replayed);
            let replay = client
                .apply_gc(&gc, context, GcApplyOptions::default())
                .await
                .unwrap();
            assert!(replay.replayed);
        }
        cursor = gc.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(found_node);
    assert!(found_blob);
    assert!(
        RemoteStoreBackend::get_node(client.backend(), orphan_node_cid.as_bytes())
            .await
            .unwrap()
            .is_none()
    );
    assert!(blob_store.get_blob(&orphan_blob).await.unwrap().is_none());
    client
        .release_maintenance_lease(
            &lease.id,
            MaintenanceContext::new("gc-test", "dry-run complete"),
        )
        .await
        .unwrap();

    let mut maintenance = client
        .workers()
        .maintenance(
            MaintenanceContext::new("gc-test", "idempotent worker shutdown"),
            60_000,
        )
        .await
        .unwrap();
    let first_release = maintenance
        .shutdown(MaintenanceContext::new(
            "gc-test",
            "idempotent worker shutdown complete",
        ))
        .await
        .unwrap();
    assert!(!first_release.replayed);
    let replayed_release = maintenance
        .shutdown(MaintenanceContext::new(
            "ignored-on-replay",
            "the original durable release remains authoritative",
        ))
        .await
        .unwrap();
    assert!(replayed_release.replayed);
    assert_eq!(replayed_release.lease, first_release.lease);
    assert_eq!(replayed_release.context, first_release.context);

    cleanup.clear_namespace().await.unwrap();
}

struct TestBlobStorage(DynamoDbBlobStore);

impl BlobStorage for TestBlobStorage {
    fn get_blob<'a>(&'a self, reference: &'a prolly::BlobRef) -> BlobFuture<'a, Option<Vec<u8>>> {
        Box::pin(async move {
            self.0
                .get_blob(reference)
                .await
                .map_err(|error| prolly_dynamodb_core::Error::Blob(error.to_string()))
        })
    }

    fn put_blob<'a>(&'a self, bytes: &'a [u8]) -> BlobFuture<'a, prolly::BlobRef> {
        Box::pin(async move {
            self.0
                .put_blob(bytes)
                .await
                .map_err(|error| prolly_dynamodb_core::Error::Blob(error.to_string()))
        })
    }
}

fn soak_usize(name: &str, default: usize, minimum: usize, maximum: usize) -> usize {
    let value = std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("soak setting must be an integer")
        })
        .unwrap_or(default);
    assert!(
        (minimum..=maximum).contains(&value),
        "{name} must be in {minimum}..={maximum}"
    );
    value
}

#[allow(clippy::too_many_arguments)]
fn soak_writer_command(
    endpoint: &str,
    physical_table: &str,
    root_table: &str,
    prefix: &[u8],
    writer: usize,
    iterations: usize,
    progress_path: &Path,
    pause_at: Option<usize>,
) -> Command {
    let mut command = worker_probe_command(endpoint, physical_table, root_table, prefix);
    command
        .env("PROLLY_DYNAMODB_WORKER_PROBE_MODE", "soak-writer")
        .env("PROLLY_DYNAMODB_SOAK_WRITER", writer.to_string())
        .env("PROLLY_DYNAMODB_SOAK_ITERATIONS", iterations.to_string())
        .env("PROLLY_DYNAMODB_WORKER_OUTPUT", progress_path);
    if let Some(pause_at) = pause_at {
        command.env("PROLLY_DYNAMODB_SOAK_PAUSE_AT", pause_at.to_string());
    }
    command
}

fn wait_for_soak_started(path: &Path, expected: usize, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(bytes) = std::fs::read(path) {
            if serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|value| value["started"].as_u64())
                == Some(u64::try_from(expected).unwrap())
            {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for soak writer to start iteration {expected}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn assert_child_succeeded(label: &str, output: &std::process::Output) {
    assert!(
        output.status.success(),
        "{label} failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

async fn execute_soak_put(
    client: Client,
    id: String,
    writer: usize,
    iteration: usize,
    token: String,
) -> Option<String> {
    const MAX_SOAK_RETRIES: usize = 256;
    for attempt in 0..MAX_SOAK_RETRIES {
        match client
            .put_item()
            .table_name("Soak")
            .item("id", AttributeValue::S(id.clone()))
            .item("writer", AttributeValue::N(writer.to_string()))
            .item("iteration", AttributeValue::N(iteration.to_string()))
            .request_token(token.clone())
            .send_with_metadata()
            .await
        {
            Ok(result) => return result.commit_id.map(|commit| commit.to_string()),
            Err(error) if is_retryable_soak_conflict(&error) => {
                let jitter = ((writer + iteration + attempt) % 7) as u64;
                tokio::time::sleep(Duration::from_millis(1 + jitter)).await;
            }
            Err(error) => panic!(
                "soak writer {writer} iteration {iteration} failed with non-retryable error: {error}"
            ),
        }
    }
    panic!("soak writer {writer} iteration {iteration} exhausted {MAX_SOAK_RETRIES} retries")
}

fn is_retryable_soak_conflict(error: &Error) -> bool {
    match error {
        Error::Core(prolly_dynamodb_core::Error::ConflictExhausted) => true,
        Error::Core(prolly_dynamodb_core::Error::TransactionCanceled { reasons }) => {
            reasons.iter().all(|reason| {
                reason.code
                    == Some(prolly_dynamodb_core::TransactionCancellationCode::TransactionConflict)
            })
        }
        _ => false,
    }
}

fn run_worker_probe(
    mode: &str,
    endpoint: &str,
    physical_table: &str,
    root_table: &str,
    prefix: &[u8],
    output_path: Option<&Path>,
) -> Option<serde_json::Value> {
    let mut command = worker_probe_command(endpoint, physical_table, root_table, prefix);
    command
        .env("PROLLY_DYNAMODB_WORKER_PROBE_MODE", mode)
        .env("PROLLY_DYNAMODB_WORKER_OWNER", "worker-child");
    if let Some(path) = output_path {
        command.env("PROLLY_DYNAMODB_WORKER_OUTPUT", path);
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "worker probe failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    output_path.map(|path| {
        let bytes = std::fs::read(path).unwrap();
        std::fs::remove_file(path).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    })
}

fn run_point_write_race(
    endpoint: &str,
    physical_table: &str,
    root_table: &str,
    prefix: &[u8],
    expected: &prolly::MapVersionId,
    nonce: u128,
) {
    let expected = encode_hex(expected.as_cid().as_bytes());
    let mut paths = Vec::new();
    let mut children = Vec::new();
    for contender in ["a", "b"] {
        let path = std::env::temp_dir().join(format!(
            "prolly-dynamodb-point-race-{}-{nonce}-{contender}.json",
            std::process::id()
        ));
        let mut command = worker_probe_command(endpoint, physical_table, root_table, prefix);
        command
            .env("PROLLY_DYNAMODB_WORKER_PROBE_MODE", "point-write")
            .env("PROLLY_DYNAMODB_WORKER_EXPECTED", &expected)
            .env(
                "PROLLY_DYNAMODB_WORKER_ACCOUNT",
                format!("acct-point-race-{contender}"),
            )
            .env("PROLLY_DYNAMODB_WORKER_OUTPUT", &path);
        paths.push(path);
        children.push(command.spawn().unwrap());
    }
    let outputs = children
        .into_iter()
        .map(|child| child.wait_with_output().unwrap())
        .collect::<Vec<_>>();
    for output in &outputs {
        assert!(
            output.status.success(),
            "point-write probe failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
    let mut statuses = paths
        .into_iter()
        .map(|path| {
            let value: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
            std::fs::remove_file(path).unwrap();
            value["status"].as_str().unwrap().to_owned()
        })
        .collect::<Vec<_>>();
    statuses.sort();
    assert_eq!(statuses, vec!["accepted", "head-conflict"]);
}

fn worker_probe_command(
    endpoint: &str,
    physical_table: &str,
    root_table: &str,
    prefix: &[u8],
) -> Command {
    let prefix = String::from_utf8(prefix.to_vec()).expect("test prefix is UTF-8");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .arg("--exact")
        .arg("worker_process_probe")
        .arg("--nocapture")
        .env("PROLLY_DYNAMODB_WORKER_PROBE", "1")
        .env("PROLLY_STORE_DYNAMODB_ENDPOINT", endpoint)
        .env("PROLLY_DYNAMODB_CLIENT_TEST_TABLE", physical_table)
        .env("PROLLY_DYNAMODB_WORKER_ROOT_TABLE", root_table)
        .env("PROLLY_DYNAMODB_WORKER_PREFIX", prefix);
    command
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let text = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(text, 16).unwrap()
        })
        .collect()
}

#[test]
fn worker_process_probe() {
    if std::env::var("PROLLY_DYNAMODB_WORKER_PROBE").as_deref() != Ok("1") {
        return;
    }
    std::thread::Builder::new()
        .name("dynamodb-worker-probe".into())
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            runtime.block_on(run_worker_process_probe());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn run_worker_process_probe() {
    let endpoint = std::env::var("PROLLY_STORE_DYNAMODB_ENDPOINT").unwrap();
    let physical_table = std::env::var("PROLLY_DYNAMODB_CLIENT_TEST_TABLE").unwrap();
    let root_table = std::env::var("PROLLY_DYNAMODB_WORKER_ROOT_TABLE").unwrap();
    let prefix = std::env::var("PROLLY_DYNAMODB_WORKER_PREFIX")
        .unwrap()
        .into_bytes();
    let mode = std::env::var("PROLLY_DYNAMODB_WORKER_PROBE_MODE").unwrap();
    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .build();
    let backend = DynamoDbBackend::new(aws_sdk_dynamodb::Client::from_conf(config), physical_table)
        .with_root_table_name(root_table)
        .with_key_prefix(prefix);
    let client = Client::open(backend).await.unwrap();

    match mode.as_str() {
        "expect-held" => {
            let result = client
                .workers()
                .stream(StreamWorkerOptions::new(
                    "Orders",
                    "dynamodb-local-audit",
                    "worker-child-held",
                ))
                .await;
            assert!(matches!(
                result,
                Err(Error::Core(
                    prolly_dynamodb_core::Error::WorkerLeaseHeld { .. }
                ))
            ));
        }
        "resume" => {
            let mut worker = client
                .workers()
                .stream(StreamWorkerOptions::new(
                    "Orders",
                    "dynamodb-local-audit",
                    "worker-child-resume",
                ))
                .await
                .unwrap();
            let ids = Arc::new(Mutex::new(Vec::new()));
            let sink_ids = Arc::clone(&ids);
            let page = worker
                .run_once(&mut move |commit| {
                    let sink_ids = Arc::clone(&sink_ids);
                    async move {
                        sink_ids.lock().unwrap().push(commit.commit_id.to_string());
                        Ok::<_, std::io::Error>(())
                    }
                })
                .await
                .unwrap();
            let fence = worker.lease().fence;
            worker.shutdown().await.unwrap();
            let output = serde_json::json!({
                "delivered": page.delivered,
                "commit_ids": ids.lock().unwrap().clone(),
                "fence": fence,
            });
            let path = std::env::var("PROLLY_DYNAMODB_WORKER_OUTPUT").unwrap();
            std::fs::write(path, serde_json::to_vec(&output).unwrap()).unwrap();
        }
        "point-write" => {
            let bytes = decode_hex(&std::env::var("PROLLY_DYNAMODB_WORKER_EXPECTED").unwrap());
            let expected = prolly::MapVersionId::from_bytes(&bytes).unwrap();
            let account = std::env::var("PROLLY_DYNAMODB_WORKER_ACCOUNT").unwrap();
            let status = match client
                .table("Orders")
                .if_head(expected)
                .put_item()
                .item("account", AttributeValue::S(account))
                .item("status", AttributeValue::S("OPEN".into()))
                .send()
                .await
            {
                Ok(_) => "accepted",
                Err(Error::HeadConflict { .. }) => "head-conflict",
                Err(error) => panic!("unexpected point-write result: {error}"),
            };
            let path = std::env::var("PROLLY_DYNAMODB_WORKER_OUTPUT").unwrap();
            std::fs::write(
                path,
                serde_json::to_vec(&serde_json::json!({ "status": status })).unwrap(),
            )
            .unwrap();
        }
        "soak-writer" => {
            let writer = std::env::var("PROLLY_DYNAMODB_SOAK_WRITER")
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let iterations = std::env::var("PROLLY_DYNAMODB_SOAK_ITERATIONS")
                .unwrap()
                .parse::<usize>()
                .unwrap();
            let pause_at = std::env::var("PROLLY_DYNAMODB_SOAK_PAUSE_AT")
                .ok()
                .map(|value| value.parse::<usize>().unwrap());
            let path = std::env::var("PROLLY_DYNAMODB_WORKER_OUTPUT").unwrap();
            let mut acked = 0;
            for iteration in 0..iterations {
                let id = format!("writer-{writer:04}-item-{iteration:08}");
                let token = format!("soak-writer-{writer:04}-item-{iteration:08}");
                let operation_client = client.clone();
                let operation_id = id.clone();
                let operation_token = token.clone();
                let operation = tokio::spawn(async move {
                    execute_soak_put(
                        operation_client,
                        operation_id,
                        writer,
                        iteration,
                        operation_token,
                    )
                    .await
                });
                std::fs::write(
                    &path,
                    serde_json::to_vec(&serde_json::json!({
                        "writer": writer,
                        "started": iteration,
                        "acked": acked,
                    }))
                    .unwrap(),
                )
                .unwrap();

                if pause_at == Some(iteration) {
                    // The parent kills this process while the exact-token write
                    // is in flight or has committed without local acknowledgement.
                    tokio::time::sleep(Duration::from_secs(30)).await;
                }

                let commit_id = if iteration % 7 == 3 {
                    tokio::task::yield_now().await;
                    operation.abort();
                    match operation.await {
                        Ok(commit_id) => commit_id,
                        Err(error) if error.is_cancelled() => {
                            execute_soak_put(client.clone(), id, writer, iteration, token).await
                        }
                        Err(error) => panic!("soak operation task failed: {error}"),
                    }
                } else {
                    operation.await.unwrap()
                };
                acked = iteration + 1;
                std::fs::write(
                    &path,
                    serde_json::to_vec(&serde_json::json!({
                        "writer": writer,
                        "started": iteration,
                        "acked": acked,
                        "commit_id": commit_id,
                    }))
                    .unwrap(),
                )
                .unwrap();
            }
        }
        other => panic!("unknown worker probe mode {other:?}"),
    }
}
