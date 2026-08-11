use std::time::{SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, KeySchemaElement, KeyType, ScalarAttributeType,
};
use prolly_dynamodb_client::Client;
use prolly_store_dynamodb::DynamoDbBackend;

#[test]
fn current_thread_runtime_executes_complete_client_lifecycle() {
    run_on_large_stack(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(runtime_smoke("current-thread"));
    });
}

#[test]
fn multi_thread_runtime_executes_complete_client_lifecycle() {
    run_on_large_stack(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
            .block_on(runtime_smoke("multi-thread"));
    });
}

fn run_on_large_stack(run: impl FnOnce() + Send + 'static) {
    std::thread::Builder::new()
        .name("dynamodb-client-runtime-smoke".into())
        .stack_size(32 * 1024 * 1024)
        .spawn(run)
        .unwrap()
        .join()
        .unwrap();
}

async fn runtime_smoke(runtime: &str) {
    let Ok(endpoint) = std::env::var("PROLLY_STORE_DYNAMODB_ENDPOINT") else {
        eprintln!("skipping: PROLLY_STORE_DYNAMODB_ENDPOINT is not set");
        return;
    };
    let physical_table = std::env::var("PROLLY_DYNAMODB_CLIENT_TEST_TABLE")
        .unwrap_or_else(|_| "prolly-versioned-client-test".into());
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .build();
    let backend = DynamoDbBackend::new(aws_sdk_dynamodb::Client::from_conf(config), physical_table)
        .with_key_prefix(
            format!("runtime-smoke-{}-{nonce}:{runtime}:", std::process::id()).into_bytes(),
        );
    backend.initialize_schema().await.unwrap();
    let cleanup = backend.clone();
    let client = Client::open(backend).await.unwrap();

    client
        .create_table()
        .table_name("RuntimeEvidence")
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
        .send()
        .await
        .unwrap();
    client
        .put_item()
        .table_name("RuntimeEvidence")
        .item("id", AttributeValue::S(runtime.into()))
        .item("verified", AttributeValue::Bool(true))
        .send()
        .await
        .unwrap();
    let output = client
        .get_item()
        .table_name("RuntimeEvidence")
        .key("id", AttributeValue::S(runtime.into()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        output.item().and_then(|item| item.get("verified")),
        Some(&AttributeValue::Bool(true))
    );
    let mut versions = client.table("RuntimeEvidence").versions().page_size(8);
    assert_eq!(
        versions.next_page().await.unwrap().unwrap().versions.len(),
        2
    );

    drop(client);
    cleanup.clear_namespace().await.unwrap();
}
