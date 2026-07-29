use std::num::NonZeroUsize;

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use prolly::RemoteProllyStore;
use prolly_backend_workload_contract::Workload;
use prolly_store_dynamodb::DynamoDbBackend;
use prolly_store_mysql::{MySqlBackend, MySqlBackendOptions};
use prolly_store_postgres::{PostgresBackend, PostgresBackendOptions};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::postgres::PgPoolOptions;

use crate::{
    run_service_workload, run_workload, DynamoDbConnection, EvidenceRow, RunConfig,
    ServiceEvidenceRow,
};

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
