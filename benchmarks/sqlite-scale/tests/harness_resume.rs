use prolly_sqlite_scale_bench::harness::run_matrix;
use prolly_sqlite_scale_bench::model::{Operation, Pattern, RunConfig};

fn tiny_config(output: std::path::PathBuf) -> RunConfig {
    let mut config = RunConfig::smoke(output);
    config.operations = vec![Operation::Put];
    config.patterns = vec![Pattern::Append];
    config
}

#[test]
fn manifest_rejects_an_incompatible_resume() {
    let temp = tempfile::tempdir().unwrap();
    let config = tiny_config(temp.path().join("run"));
    let first = run_matrix(config.clone()).unwrap();
    assert_eq!(first.measured, 1);

    let second = run_matrix(config.clone()).unwrap();
    assert_eq!(second.measured, 0);
    assert_eq!(second.skipped, 1);

    let mut incompatible = config;
    incompatible.read_samples = 9;
    let error = run_matrix(incompatible).unwrap_err();
    assert!(error.contains("manifest"));
}

#[test]
fn disk_guard_fails_before_starting_the_run() {
    let temp = tempfile::tempdir().unwrap();
    let mut config = tiny_config(temp.path().join("run"));
    config.min_free_bytes = u64::MAX;
    let error = run_matrix(config).unwrap_err();
    assert!(error.contains("insufficient free space"));
}
