use prolly_turso_scale_bench::harness::enumerate_matrix;
use prolly_turso_scale_bench::model::{enumerate_cells, Operation, Pattern, RunConfig};

#[test]
fn full_matrix_has_three_fixtures_and_complete_cells() {
    let config = RunConfig::full("results".into(), "revision".into(), false);
    let plan = enumerate_matrix(&config);
    assert_eq!(plan.fixtures.len(), 3);
    assert_eq!(plan.cells.len(), 75);

    let cells = enumerate_cells(&config, 1_000_000, 1);
    for operation in Operation::ALL {
        let count = cells
            .iter()
            .filter(|cell| cell.operation == operation)
            .count();
        let expected = if operation == Operation::FullScan {
            1
        } else {
            3
        };
        assert_eq!(count, expected, "{operation:?}");
    }
}

#[test]
fn smoke_matrix_uses_single_put_and_separate_read_and_change_counts() {
    let config = RunConfig::smoke("smoke".into());
    let cells = enumerate_cells(&config, 100, 1);
    assert_eq!(cells.len(), 25);
    for cell in cells {
        assert_eq!(cell.changes, 10);
        assert_eq!(cell.read_samples, 10);
        assert_eq!(
            cell.logical_operations(),
            if cell.operation == Operation::Put {
                1
            } else if matches!(
                cell.operation,
                Operation::GetCold | Operation::GetWarm | Operation::Query | Operation::Scan
            ) {
                10
            } else if cell.operation == Operation::FullScan {
                100
            } else {
                10
            }
        );
        let expected_entries = match (cell.operation, cell.pattern) {
            (Operation::Put, Pattern::Append) => 101,
            (Operation::Batch | Operation::Diff | Operation::Merge, Pattern::Append) => 110,
            _ => 100,
        };
        assert_eq!(cell.expected_entries(), expected_entries);
    }
}
