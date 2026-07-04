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
fn cosmosdb_backend_satisfies_remote_backend_contract_when_env_is_set() {
    let (Some(endpoint), Some(account_key), Some(database), Some(container)) = (
        env_var(
            "PROLLY_STORE_COSMOS_ENDPOINT",
            "PROLLY_ADAPTERS_COSMOS_ENDPOINT",
        ),
        env_var("PROLLY_STORE_COSMOS_KEY", "PROLLY_ADAPTERS_COSMOS_KEY"),
        env_var(
            "PROLLY_STORE_COSMOS_DATABASE",
            "PROLLY_ADAPTERS_COSMOS_DATABASE",
        ),
        env_var(
            "PROLLY_STORE_COSMOS_CONTAINER",
            "PROLLY_ADAPTERS_COSMOS_CONTAINER",
        ),
    ) else {
        return;
    };

    runtime().block_on(async {
        use prolly::remote_conformance::assert_remote_backend_contract;
        use prolly_store_cosmosdb::CosmosDbBackend;

        let backend = CosmosDbBackend::with_key(endpoint, &account_key, database, container)
            .unwrap()
            .with_key_prefix(unique_prefix("cosmosdb"));

        backend.clear_namespace().await.unwrap();
        assert_remote_backend_contract(&backend).await;
        backend.clear_namespace().await.unwrap();
    });
}
