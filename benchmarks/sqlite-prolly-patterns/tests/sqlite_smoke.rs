use prolly_sqlite_pattern_bench::fixture::FixtureLayout;
use prolly_sqlite_pattern_bench::model::{enumerate_cells, FixtureSpec, RunConfig};
use prolly_sqlite_pattern_bench::sqlite_runner::{build_fixture, run_cell};

#[test]
fn every_smoke_cell_validates() {
    let temp = tempfile::tempdir().unwrap();
    let config = RunConfig::smoke(temp.path().to_path_buf());
    let layout = FixtureLayout::new(config.output.clone(), 100, 1);
    let fixture = build_fixture(&FixtureSpec::from_config(&config, 100, 1), &layout).unwrap();
    assert!(fixture.validated);
    let cells = enumerate_cells(&config, 100, 1);
    assert_eq!(cells.len(), 15);
    for cell in cells {
        layout.clone_for(&cell).unwrap();
        let row = run_cell(&cell, &layout).unwrap();
        assert!(row.validated, "{cell:?}");
        layout.remove_cell(&cell).unwrap();
    }
}
