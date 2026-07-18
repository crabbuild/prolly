use prolly_sqlite_pattern_bench::harness::enumerate_matrix;
use prolly_sqlite_pattern_bench::model::RunConfig;

#[test]
fn full_matrix_has_expected_cardinality() {
    let config = RunConfig::full("results".into(), "revision".into(), false);
    let plan = enumerate_matrix(&config);
    assert_eq!(plan.fixtures.len(), 15);
    assert_eq!(plan.cells.len(), 225);
}

#[test]
fn smoke_matrix_has_fifteen_cells() {
    let plan = enumerate_matrix(&RunConfig::smoke("smoke".into()));
    assert_eq!(plan.fixtures.len(), 1);
    assert_eq!(plan.cells.len(), 15);
}
