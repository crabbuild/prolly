use std::path::PathBuf;

use prolly_backend_comparison::{run_spanner, Backend, RunConfig};
use prolly_backend_workload_contract::{WorkloadSpec, DEFAULT_SEED};

#[tokio::test]
#[ignore = "requires PROLLY_BACKEND_SPANNER_TEST_DATABASE"]
async fn spanner_executes_the_common_comparison() {
    let database = std::env::var("PROLLY_BACKEND_SPANNER_TEST_DATABASE").unwrap();
    let config = RunConfig {
        backend: Backend::Spanner,
        output: PathBuf::from("unused.csv"),
        run_id: "spanner-smoke".to_string(),
        repetition: 1,
        revision: "a".repeat(40),
        tree_hash: "b".repeat(40),
        binary_sha256: "c".repeat(64),
        pool_size: 4,
        adapter_batch_items: 7,
        workload: WorkloadSpec {
            records: 100,
            value_bytes: 27,
            changes: 10,
            samples: 10,
            concurrency: 4,
            seed: DEFAULT_SEED,
        },
    };

    let rows = run_spanner(&config, &database).await.unwrap();
    assert_eq!(rows.len(), 6);
    assert!(rows.iter().all(|row| row.backend == Backend::Spanner));
    assert!(rows.iter().all(|row| row.validated));
}
