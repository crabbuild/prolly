use std::collections::BTreeSet;

use prolly_sqlite_turso_local_bench::harness::run_matrix;
use prolly_sqlite_turso_local_bench::measurement::{FixtureRow, RawRow};
use prolly_sqlite_turso_local_bench::model::{Adapter, Api, Pattern, RunConfig};

#[tokio::test]
async fn local_smoke_runs_every_adapter_api_and_pattern_without_sync() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = RunConfig::smoke(temp.path().to_path_buf());
    config.revision = "smoke-test".to_string();
    config.dirty = false;

    let stats = run_matrix(config.clone()).await.unwrap();
    assert_eq!(stats.measured, 24);
    assert_eq!(stats.fixtures, 2);

    let raw = csv::Reader::from_path(config.output.join("raw-results.csv"))
        .unwrap()
        .deserialize::<RawRow>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(raw.len(), 24);
    assert!(raw.iter().all(|row| row.validated && row.error.is_empty()));
    assert_eq!(
        raw.iter().map(|row| row.adapter).collect::<BTreeSet<_>>(),
        Adapter::ALL.into_iter().collect()
    );
    assert_eq!(
        raw.iter().map(|row| row.api).collect::<BTreeSet<_>>(),
        Api::ALL.into_iter().collect()
    );
    assert_eq!(
        raw.iter().map(|row| row.pattern).collect::<BTreeSet<_>>(),
        Pattern::ALL.into_iter().collect()
    );

    let fixtures = csv::Reader::from_path(config.output.join("fixture-results.csv"))
        .unwrap()
        .deserialize::<FixtureRow>()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(fixtures.len(), 2);
    assert!(fixtures.iter().all(|row| row.validated));
    assert_eq!(
        std::fs::read_to_string(config.output.join("run-status.txt")).unwrap(),
        "complete\n"
    );

    let resumed = run_matrix(config).await.unwrap();
    assert_eq!(resumed.measured, 0);
    assert_eq!(resumed.skipped, 24);
}

