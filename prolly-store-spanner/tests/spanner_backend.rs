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
fn spanner_backend_satisfies_remote_backend_contract_when_database_is_set() {
    let Some(database) = env_var(
        "PROLLY_STORE_SPANNER_DATABASE",
        "PROLLY_ADAPTERS_SPANNER_DATABASE",
    ) else {
        return;
    };

    runtime().block_on(async {
        use google_cloud_spanner::client::ClientConfig;
        use prolly::remote_conformance::assert_remote_backend_contract;
        use prolly_store_spanner::SpannerBackend;

        let mut config = ClientConfig::default();
        if env_var("PROLLY_STORE_SPANNER_AUTH", "PROLLY_ADAPTERS_SPANNER_AUTH").is_some() {
            config = config.with_auth().await.unwrap();
        }

        let backend = SpannerBackend::connect(&database, config).await.unwrap();
        clear_spanner(backend.client()).await.unwrap();
        assert_remote_backend_contract(&backend).await;
        clear_spanner(backend.client()).await.unwrap();
        backend.client().clone().close().await;
    });
}

async fn clear_spanner(
    client: &google_cloud_spanner::client::Client,
) -> Result<(), google_cloud_spanner::client::Error> {
    use google_cloud_spanner::key::all_keys;
    use google_cloud_spanner::mutation::delete;

    client
        .apply(vec![
            delete("ProllyHints", all_keys()),
            delete("ProllyRoots", all_keys()),
            delete("ProllyNodes", all_keys()),
        ])
        .await?;
    Ok(())
}
