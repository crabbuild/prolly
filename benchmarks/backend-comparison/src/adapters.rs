use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use prolly::RemoteProllyStore;
use prolly_backend_workload_contract::Workload;
use prolly_store_dynamodb::DynamoDbBackend;
use prolly_store_postgres::PostgresBackend;

use crate::{run_workload, DynamoDbConnection, EvidenceRow, RunConfig};

pub async fn run_postgres(config: &RunConfig, url: &str) -> Result<Vec<EvidenceRow>, String> {
    let backend = PostgresBackend::connect(url)
        .await
        .map_err(|error| format!("failed to connect to PostgreSQL: {error}"))?;
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
