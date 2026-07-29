use std::path::PathBuf;

use prolly_backend_comparison::{run_postgres, Backend, RunConfig};
use prolly_backend_workload_contract::{WorkloadSpec, DEFAULT_SEED};

#[tokio::test]
#[ignore = "requires PROLLY_BACKEND_POSTGRES_TEST_URL"]
async fn postgres_executes_the_common_comparison() {
    let url = std::env::var("PROLLY_BACKEND_POSTGRES_TEST_URL").unwrap();
    let config = config(Backend::Postgres);
    let rows = run_postgres(&config, &url).await.unwrap();
    assert_eq!(rows.len(), 6);
    assert!(rows.iter().all(|row| row.backend == Backend::Postgres));
}

fn config(backend: Backend) -> RunConfig {
    RunConfig {
        backend,
        output: PathBuf::from("unused.csv"),
        run_id: "postgres-smoke".to_string(),
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
