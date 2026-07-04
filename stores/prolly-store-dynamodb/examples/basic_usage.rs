use std::error::Error;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use prolly::{AsyncProlly, Config, Error as ProllyError, Mutation, RemoteProllyStore};
use prolly::{Resolution, Resolver};
use prolly_store_dynamodb::DynamoDbBackend;

fn main() -> Result<(), Box<dyn Error>> {
    runtime().block_on(run())
}

async fn run() -> Result<(), Box<dyn Error>> {
    let table = std::env::var("PROLLY_STORE_DYNAMODB_TABLE")
        .unwrap_or_else(|_| "prolly_store_example".to_string());
    let backend = DynamoDbBackend::new(dynamodb_client().await, table)
        .with_key_prefix(unique_prefix("dynamodb"));
    backend.initialize_schema().await?;
    wait_for_table(&backend).await?;
    backend.clear_namespace().await?;

    let prolly = AsyncProlly::new(RemoteProllyStore::new(backend.clone()), Config::default());
    let base = seed_tree(&prolly).await?;
    let left = prolly
        .batch(
            &base,
            vec![upsert("task/001", "in-review"), upsert("task/003", "open")],
        )
        .await?;
    let right = prolly
        .batch(&base, vec![upsert("task/002", "done")])
        .await?;

    let diffs = prolly.diff(&base, &left).await?;
    assert_eq!(diffs.len(), 2);

    let merged = prolly.merge(&base, &left, &right, None).await?;
    assert_value(&prolly, &merged, "task/001", "in-review").await?;
    assert_value(&prolly, &merged, "task/002", "done").await?;
    assert_value(&prolly, &merged, "task/003", "open").await?;

    let root_name = b"examples/dynamodb/main";
    prolly.publish_named_root(root_name, &merged).await?;
    let loaded = prolly
        .load_named_root(root_name)
        .await?
        .expect("named root");
    assert_value(&prolly, &loaded, "task/002", "done").await?;

    let conflict_left = prolly
        .batch(&base, vec![upsert("task/001", "left")])
        .await?;
    let conflict_right = prolly
        .batch(&base, vec![upsert("task/001", "right")])
        .await?;
    assert!(matches!(
        prolly
            .merge(&base, &conflict_left, &conflict_right, None)
            .await,
        Err(ProllyError::Conflict(_))
    ));

    let resolver: Resolver = Box::new(|conflict| {
        let mut value = conflict.left.clone().unwrap_or_default();
        value.extend_from_slice(b"+");
        value.extend_from_slice(
            conflict
                .right
                .as_ref()
                .map(Vec::as_slice)
                .unwrap_or_default(),
        );
        Resolution::value(value)
    });
    let resolved = prolly
        .merge(&base, &conflict_left, &conflict_right, Some(resolver))
        .await?;
    assert_value(&prolly, &resolved, "task/001", "left+right").await?;

    let roots = prolly.list_named_roots().await?;
    println!("dynamodb example ok; named_roots={}", roots.len());

    backend.clear_namespace().await?;
    Ok(())
}

async fn dynamodb_client() -> aws_sdk_dynamodb::Client {
    if let Ok(endpoint) = std::env::var("PROLLY_STORE_DYNAMODB_ENDPOINT") {
        let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-west-2".to_string());
        let config = aws_sdk_dynamodb::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(region))
            .endpoint_url(endpoint)
            .credentials_provider(Credentials::new("test", "test", None, None, "local"))
            .build();
        aws_sdk_dynamodb::Client::from_conf(config)
    } else {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        aws_sdk_dynamodb::Client::new(&config)
    }
}

async fn wait_for_table(backend: &DynamoDbBackend) -> Result<(), Box<dyn Error>> {
    for _ in 0..30 {
        let output = backend
            .client()
            .describe_table()
            .table_name(backend.table_name())
            .send()
            .await?;
        let active = output
            .table()
            .and_then(|table| table.table_status())
            .is_some_and(|status| status.as_str() == "ACTIVE");
        if active {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err(format!("DynamoDB table {} was not ACTIVE", backend.table_name()).into())
}

async fn seed_tree<S>(prolly: &AsyncProlly<S>) -> Result<prolly::Tree, prolly::Error>
where
    S: prolly::AsyncStore,
    S::Error: Send + Sync,
{
    prolly
        .batch(
            &prolly.create(),
            vec![upsert("task/001", "open"), upsert("task/002", "open")],
        )
        .await
}

async fn assert_value<S>(
    prolly: &AsyncProlly<S>,
    tree: &prolly::Tree,
    key: &str,
    expected: &str,
) -> Result<(), prolly::Error>
where
    S: prolly::AsyncStore,
    S::Error: Send + Sync,
{
    assert_eq!(
        prolly.get(tree, key.as_bytes()).await?,
        Some(expected.as_bytes().to_vec())
    );
    Ok(())
}

fn upsert(key: &str, value: &str) -> Mutation {
    Mutation::Upsert {
        key: key.as_bytes().to_vec(),
        val: value.as_bytes().to_vec(),
    }
}

fn unique_prefix(provider: &str) -> Vec<u8> {
    format!(
        "prolly:example:{provider}:{}:",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
    .into_bytes()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
