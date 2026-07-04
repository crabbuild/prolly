use std::time::{SystemTime, UNIX_EPOCH};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn unique_prefix(provider: &str) -> Vec<u8> {
    format!(
        "prolly:test:{provider}:{}:",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
    .into_bytes()
}

fn env_var(primary: &str, legacy: &str) -> Option<String> {
    std::env::var(primary)
        .or_else(|_| std::env::var(legacy))
        .ok()
}

#[test]
fn dynamodb_backend_satisfies_remote_backend_contract_when_table_is_set() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };

    runtime().block_on(async {
        use prolly::remote_conformance::assert_remote_backend_contract;
        use prolly_store_dynamodb::DynamoDbBackend;

        let client = dynamodb_client().await;
        let backend =
            DynamoDbBackend::new(client, table_name).with_key_prefix(unique_prefix("dynamodb"));

        backend.initialize_schema().await.unwrap();
        backend.clear_namespace().await.unwrap();
        assert_remote_backend_contract(&backend).await;
        backend.clear_namespace().await.unwrap();
    });
}

async fn dynamodb_client() -> aws_sdk_dynamodb::Client {
    if let Some(endpoint) = env_var(
        "PROLLY_STORE_DYNAMODB_ENDPOINT",
        "PROLLY_ADAPTERS_DYNAMODB_ENDPOINT",
    ) {
        use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};

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
