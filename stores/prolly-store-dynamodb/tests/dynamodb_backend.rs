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
        use prolly::remote_conformance::{
            assert_remote_backend_contract, assert_remote_backend_indexed_map_contract,
            assert_remote_backend_transaction_contract,
        };
        use prolly_store_dynamodb::DynamoDbBackend;

        let client = dynamodb_client().await;
        let backend =
            DynamoDbBackend::new(client, table_name).with_key_prefix(unique_prefix("dynamodb"));

        backend.initialize_schema().await.unwrap();
        backend.clear_namespace().await.unwrap();
        assert_remote_backend_contract(&backend).await;
        assert_remote_backend_transaction_contract(&backend).await;
        backend.clear_namespace().await.unwrap();
        assert_remote_backend_indexed_map_contract(backend.clone());
        backend.clear_namespace().await.unwrap();
    });
}

#[test]
fn dynamodb_backend_hard_cutover_ignores_legacy_primary_table_roots() {
    let Some(table_name) = env_var(
        "PROLLY_STORE_DYNAMODB_TABLE",
        "PROLLY_ADAPTERS_DYNAMODB_TABLE",
    ) else {
        return;
    };

    runtime().block_on(async {
        use aws_sdk_dynamodb::primitives::Blob;
        use aws_sdk_dynamodb::types::AttributeValue;
        use prolly::RemoteStoreBackend;
        use prolly_store_dynamodb::DynamoDbBackend;

        let client = dynamodb_client().await;
        let bootstrap = DynamoDbBackend::new(client.clone(), &table_name)
            .with_key_prefix(unique_prefix("bootstrap"));
        bootstrap.initialize_schema().await.unwrap();

        let prefix = unique_prefix("legacy-roots");
        let backend =
            DynamoDbBackend::new(client.clone(), &table_name).with_key_prefix(prefix.clone());
        let name = b"branches/main";
        let legacy_manifest = b"legacy-manifest";
        let mut legacy_key = prefix;
        legacy_key.extend_from_slice(b"root:");
        legacy_key.extend_from_slice(name);
        client
            .put_item()
            .table_name(&table_name)
            .item("pk", AttributeValue::B(Blob::new(legacy_key)))
            .item("value", AttributeValue::B(Blob::new(legacy_manifest)))
            .send()
            .await
            .unwrap();

        backend.initialize_schema().await.unwrap();
        assert_eq!(backend.get_root_manifest(name).await.unwrap(), None);
        assert!(backend.list_root_manifests().await.unwrap().is_empty());

        backend
            .put_root_manifest(name, b"current-manifest")
            .await
            .unwrap();
        assert_eq!(
            backend.get_root_manifest(name).await.unwrap(),
            Some(b"current-manifest".to_vec())
        );
        assert_eq!(
            backend.list_root_manifests().await.unwrap()[0].manifest,
            b"current-manifest"
        );

        backend.delete_root_manifest(name).await.unwrap();
        assert!(backend.list_root_manifests().await.unwrap().is_empty());
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
