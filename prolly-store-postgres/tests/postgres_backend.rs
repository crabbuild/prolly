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
fn postgres_backend_satisfies_remote_backend_contract_when_url_is_set() {
    let Some(database_url) = env_var("PROLLY_STORE_POSTGRES_URL", "PROLLY_ADAPTERS_POSTGRES_URL")
    else {
        return;
    };

    runtime().block_on(async {
        use prolly::remote_conformance::assert_remote_backend_contract;
        use prolly_store_postgres::PostgresBackend;

        let backend = PostgresBackend::connect(&database_url).await.unwrap();
        backend.initialize_schema().await.unwrap();
        clear_postgres(backend.pool()).await.unwrap();
        assert_remote_backend_contract(&backend).await;
        clear_postgres(backend.pool()).await.unwrap();
    });
}

async fn clear_postgres(pool: &sqlx::PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM prolly_hints")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM prolly_roots")
        .execute(pool)
        .await?;
    sqlx::query("DELETE FROM prolly_nodes")
        .execute(pool)
        .await?;
    Ok(())
}
