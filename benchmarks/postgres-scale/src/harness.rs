use std::collections::BTreeSet;
use std::path::Path;

use prolly_store_postgres::PostgresBackend;

use crate::cli::RunConfig;
use crate::measurement::{CellKey, CsvSink, RawRow, SCHEMA_VERSION};
use crate::model::{change_count, Operation, Pattern};
use crate::workloads::{build_fixture, run_cell, CellSpec, RunMeta};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunStats {
    pub measured: usize,
    pub skipped: usize,
    pub fixtures_built: usize,
}

pub fn enumerate_cells(config: &RunConfig, records: usize) -> Vec<CellSpec> {
    let changes = config.changes.unwrap_or_else(|| change_count(records));
    let mut cells = Vec::new();
    for repetition in 1..=config.runs {
        let mut patterns = config.patterns.clone();
        if !patterns.is_empty() {
            let pattern_len = patterns.len();
            patterns.rotate_left((repetition as usize - 1) % pattern_len);
        }
        for operation in &config.operations {
            if *operation == Operation::FullScan {
                if repetition == 1 {
                    cells.push(CellSpec {
                        operation: *operation,
                        pattern: Pattern::Append,
                        repetition,
                        changes,
                    });
                }
                continue;
            }
            for pattern in &patterns {
                if *operation == Operation::Scan && *pattern == Pattern::Random {
                    continue;
                }
                cells.push(CellSpec {
                    operation: *operation,
                    pattern: *pattern,
                    repetition,
                    changes,
                });
            }
        }
    }
    cells
}

pub async fn run_matrix(config: RunConfig) -> Result<RunStats, String> {
    config.validate()?;
    std::fs::create_dir_all(&config.output).map_err(|error| {
        format!(
            "failed to create output directory {}: {error}",
            config.output.display()
        )
    })?;
    let raw_path = config.output.join("raw-results.csv");
    let existing = read_rows(&raw_path)?;
    let mut completed = BTreeSet::new();
    for row in &existing {
        row.validate()?;
        if row.revision != config.revision || row.dirty != config.dirty {
            return Err(format!("existing row provenance differs: {:?}", row.key()));
        }
        if !completed.insert(row.key()) {
            return Err(format!("duplicate existing row: {:?}", row.key()));
        }
    }
    let mut sink = CsvSink::open(&raw_path)?;
    let backend = PostgresBackend::connect(&config.url)
        .await
        .map_err(|error| format!("failed to connect to PostgreSQL: {error}"))?;
    let meta = RunMeta {
        revision: config.revision.clone(),
        dirty: config.dirty,
    };
    let mut stats = RunStats::default();

    for records in &config.sizes {
        let cells = enumerate_cells(&config, *records);
        let build_key = CellKey {
            records: *records as u64,
            repetition: 1,
            operation: Operation::Build.as_str().to_string(),
            pattern: "base".to_string(),
            cache_state: "cold-manager".to_string(),
        };
        let pending_cells = cells
            .iter()
            .filter(|cell| !completed.contains(&cell_key(*records, cell)))
            .count();
        if completed.contains(&build_key) && pending_cells == 0 {
            stats.skipped += 1;
            stats.skipped += cells.len();
            continue;
        }

        let fixture = if completed.contains(&build_key) {
            match crate::workloads::load_fixture(backend.clone(), *records).await {
                Ok(fixture) => fixture,
                Err(load_error) => {
                    eprintln!(
                        "snapshot cannot resume records={records} ({load_error}); rebuilding fixture"
                    );
                    let (fixture, _) = build_fixture(backend.clone(), *records, &meta).await?;
                    stats.fixtures_built += 1;
                    fixture
                }
            }
        } else {
            eprintln!("building base fixture: records={records}");
            let (fixture, build_row) = build_fixture(backend.clone(), *records, &meta).await?;
            sink.append(&build_row)?;
            completed.insert(build_row.key());
            stats.measured += 1;
            stats.fixtures_built += 1;
            fixture
        };
        check_free_space(&config)?;

        for cell in cells {
            let key = cell_key(*records, &cell);
            if completed.contains(&key) {
                stats.skipped += 1;
                continue;
            }
            crate::postgres::restore_base(backend.pool())
                .await
                .map_err(|error| format!("failed to restore base before disk guard: {error}"))?;
            check_free_space(&config)?;
            eprintln!(
                "measuring: records={} repetition={} operation={} pattern={}",
                records,
                cell.repetition,
                cell.operation.as_str(),
                cell.pattern.as_str()
            );
            let row = run_cell(&fixture, cell, &meta).await?;
            if row.key() != key {
                return Err(format!(
                    "workload returned unexpected key: expected {key:?}, got {:?}",
                    row.key()
                ));
            }
            sink.append(&row)?;
            completed.insert(key);
            stats.measured += 1;
        }
    }
    Ok(stats)
}

fn cell_key(records: usize, cell: &CellSpec) -> CellKey {
    CellKey {
        records: records as u64,
        repetition: cell.repetition,
        operation: cell.operation.as_str().to_string(),
        pattern: cell.pattern.as_str().to_string(),
        cache_state: cache_state(cell.operation).to_string(),
    }
}

fn cache_state(operation: Operation) -> &'static str {
    if operation == Operation::GetWarm {
        "warm-manager"
    } else {
        "cold-manager"
    }
}

fn read_rows(path: &Path) -> Result<Vec<RawRow>, String> {
    if !path.exists() || path.metadata().is_ok_and(|metadata| metadata.len() == 0) {
        return Ok(Vec::new());
    }
    let mut reader = csv::Reader::from_path(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
    reader
        .deserialize::<RawRow>()
        .map(|row| {
            let row = row.map_err(|error| format!("failed to parse raw row: {error}"))?;
            if row.schema != SCHEMA_VERSION {
                return Err(format!("unsupported raw schema: {}", row.schema));
            }
            Ok(row)
        })
        .collect()
}

fn check_free_space(config: &RunConfig) -> Result<(), String> {
    if config.min_free_bytes == 0 {
        return Ok(());
    }
    let available = fs2::available_space(&config.output).map_err(|error| {
        format!(
            "failed to inspect free space for {}: {error}",
            config.output.display()
        )
    })?;
    if available < config.min_free_bytes {
        return Err(format!(
            "disk guard stopped run: available {available} bytes, required {} bytes",
            config.min_free_bytes
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::RunConfig;
    use crate::model::{Operation, Pattern};

    #[test]
    fn matrix_has_unique_cells_and_only_meaningful_scans() {
        let config = RunConfig::smoke();
        let cells = enumerate_cells(&config, 1_000);
        let keys = cells
            .iter()
            .map(|cell| (cell.operation, cell.pattern, cell.repetition))
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(keys.len(), cells.len());
        assert!(!cells
            .iter()
            .any(|cell| { cell.operation == Operation::Scan && cell.pattern == Pattern::Random }));
        assert_eq!(
            cells
                .iter()
                .filter(|cell| cell.operation == Operation::FullScan)
                .count(),
            1
        );
    }

    #[tokio::test]
    #[ignore = "requires PROLLY_STORE_POSTGRES_URL"]
    async fn smoke_matrix_persists_and_resumes_every_cell() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = RunConfig::smoke();
        config.url = std::env::var("PROLLY_STORE_POSTGRES_URL").unwrap();
        config.output = temp.path().to_path_buf();
        let first = run_matrix(config.clone()).await.unwrap();
        assert!(first.measured > 0);
        assert_eq!(first.fixtures_built, 1);
        let second = run_matrix(config).await.unwrap();
        assert_eq!(second.measured, 0);
        assert_eq!(second.skipped, first.measured);
        assert_eq!(second.fixtures_built, 0);
    }
}
