use std::num::NonZeroUsize;

#[cfg(feature = "dynamodb")]
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use prolly::RemoteProllyStore;
use prolly_backend_workload_contract::Workload;
#[cfg(feature = "dynamodb")]
use prolly_store_dynamodb::DynamoDbBackend;
use prolly_store_mysql::{MySqlBackend, MySqlBackendOptions};
use prolly_store_postgres::{PostgresBackend, PostgresBackendOptions};
#[cfg(feature = "spanner")]
use prolly_store_spanner::{SpannerBackend, SpannerBackendOptions};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;

#[cfg(feature = "dynamodb")]
use crate::DynamoDbConnection;
use crate::{run_service_workload, run_workload, EvidenceRow, RunConfig, ServiceEvidenceRow};

pub async fn run_postgres(config: &RunConfig, url: &str) -> Result<Vec<EvidenceRow>, String> {
    let pool = PgPoolOptions::new()
        .max_connections(config.pool_size)
        .connect(url)
        .await
        .map_err(|error| format!("failed to connect to PostgreSQL: {error}"))?;
    let options = PostgresBackendOptions::new(
        NonZeroUsize::new(config.adapter_batch_items)
            .ok_or_else(|| "adapter batch items must be positive".to_string())?,
    );
    let backend = PostgresBackend::new_with_options(pool, options);
    backend
        .initialize_schema()
        .await
        .map_err(|error| format!("failed to initialize PostgreSQL schema: {error}"))?;
    sqlx::query("TRUNCATE TABLE prolly_nodes, prolly_hints, prolly_roots")
        .execute(backend.pool())
        .await
        .map_err(|error| format!("failed to clear PostgreSQL benchmark state: {error}"))?;
    let workload = Workload::generate(config.workload)?;
    run_workload(RemoteProllyStore::new(backend), config, &workload).await
}

pub async fn run_mysql(config: &RunConfig, url: &str) -> Result<Vec<EvidenceRow>, String> {
    let pool = MySqlPoolOptions::new()
        .max_connections(config.pool_size)
        .connect(url)
        .await
        .map_err(|error| format!("failed to connect to MySQL: {error}"))?;
    let options = MySqlBackendOptions::new(
        NonZeroUsize::new(config.adapter_batch_items)
            .ok_or_else(|| "adapter batch items must be positive".to_string())?,
    );
    let backend = MySqlBackend::new_with_options(pool, options);
    backend
        .initialize_schema()
        .await
        .map_err(|error| format!("failed to initialize MySQL schema: {error}"))?;
    for table in [
        "prolly_hints",
        "prolly_roots",
        "prolly_nodes",
        "prolly_root_locks",
    ] {
        sqlx::query(&format!("TRUNCATE TABLE {table}"))
            .execute(backend.pool())
            .await
            .map_err(|error| format!("failed to clear MySQL table {table}: {error}"))?;
    }
    let workload = Workload::generate(config.workload)?;
    run_workload(RemoteProllyStore::new(backend), config, &workload).await
}

pub async fn run_postgres_service(
    config: &RunConfig,
    url: &str,
) -> Result<Vec<ServiceEvidenceRow>, String> {
    let pool = PgPoolOptions::new()
        .max_connections(config.pool_size)
        .connect(url)
        .await
        .map_err(|error| format!("failed to connect to PostgreSQL: {error}"))?;
    let backend = PostgresBackend::new_with_options(
        pool,
        PostgresBackendOptions::new(
            NonZeroUsize::new(config.adapter_batch_items)
                .ok_or_else(|| "adapter batch items must be positive".to_string())?,
        ),
    );
    backend
        .initialize_schema()
        .await
        .map_err(|error| format!("failed to initialize PostgreSQL schema: {error}"))?;
    sqlx::query("TRUNCATE TABLE prolly_nodes, prolly_hints, prolly_roots")
        .execute(backend.pool())
        .await
        .map_err(|error| format!("failed to clear PostgreSQL service state: {error}"))?;
    run_service_workload(backend, config).await
}

pub async fn run_mysql_service(
    config: &RunConfig,
    url: &str,
) -> Result<Vec<ServiceEvidenceRow>, String> {
    let pool = MySqlPoolOptions::new()
        .max_connections(config.pool_size)
        .connect(url)
        .await
        .map_err(|error| format!("failed to connect to MySQL: {error}"))?;
    let backend = MySqlBackend::new_with_options(
        pool,
        MySqlBackendOptions::new(
            NonZeroUsize::new(config.adapter_batch_items)
                .ok_or_else(|| "adapter batch items must be positive".to_string())?,
        ),
    );
    backend
        .initialize_schema()
        .await
        .map_err(|error| format!("failed to initialize MySQL schema: {error}"))?;
    for table in [
        "prolly_hints",
        "prolly_roots",
        "prolly_nodes",
        "prolly_root_locks",
    ] {
        sqlx::query(&format!("TRUNCATE TABLE {table}"))
            .execute(backend.pool())
            .await
            .map_err(|error| format!("failed to clear MySQL table {table}: {error}"))?;
    }
    run_service_workload(backend, config).await
}

#[cfg(feature = "spanner")]
async fn connect_spanner(config: &RunConfig, database: &str) -> Result<SpannerBackend, String> {
    use google_cloud_spanner::client::ClientConfig;

    let mut client_config = ClientConfig::default();
    if std::env::var_os("SPANNER_EMULATOR_HOST").is_none() {
        client_config = client_config
            .with_auth()
            .await
            .map_err(|error| format!("failed to configure Spanner authentication: {error}"))?;
    }
    SpannerBackend::connect_with_options(
        database,
        client_config,
        SpannerBackendOptions::default()
            .with_batch_read_items(config.adapter_batch_items)
            .with_read_parallelism(config.workload.concurrency),
    )
    .await
    .map_err(|error| format!("failed to connect to Spanner: {error}"))
}

#[cfg(feature = "spanner")]
async fn clear_spanner(backend: &SpannerBackend) -> Result<(), String> {
    use google_cloud_spanner::key::all_keys;
    use google_cloud_spanner::mutation::delete;

    backend
        .client()
        .apply(vec![
            delete("ProllyHints", all_keys()),
            delete("ProllyRoots", all_keys()),
            delete("ProllyNodes", all_keys()),
        ])
        .await
        .map(|_| ())
        .map_err(|error| format!("failed to clear Spanner benchmark state: {error}"))
}

#[cfg(feature = "spanner")]
pub async fn run_spanner(config: &RunConfig, database: &str) -> Result<Vec<EvidenceRow>, String> {
    let backend = connect_spanner(config, database).await?;
    clear_spanner(&backend).await?;
    let workload = Workload::generate(config.workload)?;
    let result = run_workload(RemoteProllyStore::new(backend.clone()), config, &workload).await;
    backend.client().clone().close().await;
    result
}

#[cfg(feature = "spanner")]
pub async fn run_spanner_service(
    config: &RunConfig,
    database: &str,
) -> Result<Vec<ServiceEvidenceRow>, String> {
    let backend = connect_spanner(config, database).await?;
    clear_spanner(&backend).await?;
    let result = run_service_workload(backend.clone(), config).await;
    backend.client().clone().close().await;
    result
}

#[cfg(feature = "dynamodb")]
pub async fn run_dynamodb(
    config: &RunConfig,
    connection: &DynamoDbConnection,
) -> Result<Vec<EvidenceRow>, String> {
    let sdk_config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-west-2"))
        .endpoint_url(&connection.endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .build();
    let prefix = format!("prolly:comparison:{}:{}:", config.run_id, config.repetition).into_bytes();
    let backend = DynamoDbBackend::new(
        aws_sdk_dynamodb::Client::from_conf(sdk_config),
        &connection.table,
    )
    .with_key_prefix(prefix)
    .with_read_parallelism(connection.read_parallelism)
    .with_batch_get_parallelism(connection.batch_get_parallelism)
    .with_batch_write_parallelism(connection.batch_write_parallelism)
    .with_scan_parallelism(connection.scan_parallelism);
    backend
        .initialize_schema()
        .await
        .map_err(|error| format!("failed to initialize DynamoDB Local schema: {error}"))?;
    backend
        .clear_namespace()
        .await
        .map_err(|error| format!("failed to clear DynamoDB Local namespace: {error}"))?;
    let workload = Workload::generate(config.workload)?;
    let result = run_workload(RemoteProllyStore::new(backend.clone()), config, &workload).await;
    let cleanup = backend
        .clear_namespace()
        .await
        .map_err(|error| format!("failed to clean DynamoDB Local namespace: {error}"));
    match (result, cleanup) {
        (Ok(rows), Ok(())) => Ok(rows),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}
