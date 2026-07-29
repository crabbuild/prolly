use std::path::PathBuf;

use prolly_backend_comparison::{run_dynamodb, Backend, DynamoDbConnection, RunConfig};
use prolly_backend_workload_contract::{WorkloadSpec, DEFAULT_SEED};

#[tokio::test]
#[ignore = "requires PROLLY_BACKEND_DYNAMODB_TEST_ENDPOINT"]
async fn dynamodb_executes_the_common_comparison() {
    let connection = DynamoDbConnection {
        endpoint: std::env::var("PROLLY_BACKEND_DYNAMODB_TEST_ENDPOINT").unwrap(),
        table: "prolly_backend_comparison_test".to_string(),
        read_parallelism: 4,
        batch_get_parallelism: 4,
        batch_write_parallelism: 4,
        scan_parallelism: 2,
    };
    let config = config(Backend::DynamoDbLocal);
    let rows = run_dynamodb(&config, &connection).await.unwrap();
    assert_eq!(rows.len(), 6);
    assert!(rows.iter().all(|row| row.backend == Backend::DynamoDbLocal));
}

fn config(backend: Backend) -> RunConfig {
    RunConfig {
        backend,
        output: PathBuf::from("unused.csv"),
        run_id: "dynamodb-smoke".to_string(),
        repetition: 1,
        revision: "a".repeat(40),
        tree_hash: "b".repeat(40),
        binary_sha256: "c".repeat(64),
        pool_size: 4,
        adapter_batch_items: 32,
        workload: WorkloadSpec {
            records: 100,
            value_bytes: 27,
            changes: 10,
            samples: 10,
            concurrency: 4,
            seed: DEFAULT_SEED,
        },
    }
}
