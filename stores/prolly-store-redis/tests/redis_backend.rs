use std::time::{SystemTime, UNIX_EPOCH};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

fn env_var(primary: &str, legacy: &str) -> Option<String> {
    std::env::var(primary)
        .or_else(|_| std::env::var(legacy))
        .ok()
}

#[test]
fn redis_backend_satisfies_remote_backend_contract_when_url_is_set() {
    let Some(redis_url) = env_var("PROLLY_STORE_REDIS_URL", "PROLLY_ADAPTERS_REDIS_URL") else {
        return;
    };

    runtime().block_on(async {
        use prolly::remote_conformance::assert_remote_backend_contract;
        use prolly_store_redis::RedisBackend;

        let prefix = format!(
            "prolly:test:{}:",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let backend = RedisBackend::connect(&redis_url)
            .await
            .unwrap()
            .with_key_prefix(prefix.into_bytes());

        backend.clear_namespace().await.unwrap();
        assert_remote_backend_contract(&backend).await;
        backend.clear_namespace().await.unwrap();
    });
}
