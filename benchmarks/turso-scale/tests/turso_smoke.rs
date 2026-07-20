use prolly_turso_scale_bench::fixture::FixtureLayout;
use prolly_turso_scale_bench::model::{enumerate_cells, FixtureSpec, RunConfig};
use prolly_turso_scale_bench::turso_runner::{build_fixture, run_cell};

#[tokio::test(flavor = "multi_thread")]
async fn every_smoke_cell_validates() {
    let temp = tempfile::tempdir().unwrap();
    let config = RunConfig::smoke(temp.path().to_path_buf());
    let layout = FixtureLayout::new(config.output.clone(), 100, 1);
    let fixture = build_fixture(&FixtureSpec::from_config(&config, 100, 1), &layout)
        .await
        .unwrap();
    assert!(fixture.validated);
    let cells = enumerate_cells(&config, 100, 1);
    assert_eq!(cells.len(), 25);
    for cell in cells {
        layout.clone_for(&cell).unwrap();
        let row = run_cell(&cell, &layout).await.unwrap();
        assert!(row.validated, "{cell:?}");
        assert_eq!(
            row.logical_operations,
            cell.logical_operations(),
            "{cell:?}"
        );
        assert_eq!(row.expected_entries, row.observed_entries, "{cell:?}");
        assert_eq!(row.observed_items, cell.logical_operations(), "{cell:?}");
        layout.remove_cell(&cell).unwrap();
    }
}
