//! Serial matrix orchestration, resume, guards, and durable run manifests.

use std::collections::BTreeSet;
use std::fs::{self, File};
use std::path::Path;
use std::time::{Duration, Instant};
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::available_space;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::fixture::{clone_fixture, remove_cell_dir, remove_source_dir, FixtureLayout};
use crate::measurement::{CellKey, CsvSink, FixtureRow, RawRow, ResumeState, SCHEMA_VERSION};
use crate::model::{change_count, Adapter, CellSpec, FixtureSpec, RunConfig, RANDOM_SEED};
use crate::sqlite_runner::{build_sqlite_fixture, run_sqlite_cell};
use crate::turso_runner::{build_turso_fixture, run_turso_cell};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct FixtureKey {
    pub adapter: Adapter,
    pub records: usize,
    pub repetition: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MatrixPlan {
    pub fixtures: Vec<FixtureKey>,
    pub cells: Vec<CellKey>,
}

pub fn enumerate_matrix(config: &RunConfig) -> MatrixPlan {
    let mut fixtures = Vec::new();
    let mut cells = Vec::new();

    for repetition in 1..=config.runs {
        for &records in &config.sizes {
            let adapters: Vec<_> = if repetition % 2 == 0 {
                config.adapters.iter().rev().copied().collect()
            } else {
                config.adapters.clone()
            };

            for adapter in adapters {
                fixtures.push(FixtureKey {
                    adapter,
                    records,
                    repetition,
                });

                for &api in &config.apis {
                    for &pattern in &config.patterns {
                        cells.push(CellKey {
                            adapter,
                            records,
                            repetition,
                            api,
                            pattern,
                        });
                    }
                }
            }
        }
    }

    MatrixPlan { fixtures, cells }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunStats {
    pub fixtures: usize,
    pub measured: usize,
    pub skipped: usize,
    pub failed: usize,
    pub stopped_by_guard: bool,
}

pub async fn run_matrix(config: RunConfig) -> Result<RunStats, String> {
    config.validate()?;
    fs::create_dir_all(&config.output).map_err(|error| {
        format!(
            "failed to create benchmark output {}: {error}",
            config.output.display()
        )
    })?;
    ensure_manifest(&config)?;
    write_status(&config.output, "running")?;

    let raw_path = config.output.join("raw-results.csv");
    let fixture_path = config.output.join("fixture-results.csv");
    let mut raw_rows: Vec<RawRow> = read_csv(&raw_path)?;
    validate_raw_rows(&config, &raw_rows)?;
    let failed_rows = raw_rows.iter().filter(|row| !row.validated).count();
    let resume = ResumeState::from_rows(&raw_rows)?;

    let mut fixture_rows: Vec<FixtureRow> = read_csv(&fixture_path)?;
    validate_fixture_rows(&config, &fixture_rows)?;
    let completed_fixtures: BTreeSet<_> = fixture_rows
        .iter()
        .filter(|row| row.validated)
        .map(|row| FixtureKey {
            adapter: row.adapter,
            records: row.records,
            repetition: row.repetition,
        })
        .collect();

    let plan = enumerate_matrix(&config);
    let started = Instant::now();
    let mut stats = RunStats {
        failed: failed_rows,
        ..RunStats::default()
    };

    for fixture_key in plan.fixtures {
        let fixture_cells: Vec<_> = plan
            .cells
            .iter()
            .copied()
            .filter(|cell| {
                cell.adapter == fixture_key.adapter
                    && cell.records == fixture_key.records
                    && cell.repetition == fixture_key.repetition
            })
            .collect();
        let pending: Vec<_> = fixture_cells
            .iter()
            .copied()
            .filter(|cell| !resume.contains(cell))
            .collect();
        stats.skipped += fixture_cells.len() - pending.len();

        let layout = FixtureLayout::new(
            config.output.clone(),
            fixture_key.adapter,
            fixture_key.records,
            fixture_key.repetition,
        );
        layout.validate_source_destination()?;
        if pending.is_empty() {
            if !config.keep_fixtures {
                remove_source_dir(&layout)?;
            }
            continue;
        }
        check_guard(&config, started.elapsed())?;

        let fixture_already_recorded = completed_fixtures.contains(&fixture_key);
        if !fixture_already_recorded && layout.source_dir().exists() {
            remove_source_dir(&layout)?;
        }
        if !layout.source_dir().exists() {
            let spec = FixtureSpec {
                adapter: fixture_key.adapter,
                records: fixture_key.records,
                repetition: fixture_key.repetition,
                revision: config.revision.clone(),
                dirty: config.dirty,
                build_batch_size: config.build_batch_size,
            };
            let result = match fixture_key.adapter {
                Adapter::SqliteSync => build_sqlite_fixture(&spec, &layout),
                Adapter::TursoAsync => build_turso_fixture(&spec, &layout).await,
            };
            match result {
                Ok(row) => {
                    if !fixture_already_recorded {
                        persist_fixture_row(&fixture_path, &mut fixture_rows, row)?;
                    }
                    stats.fixtures += 1;
                }
                Err(error) => {
                    persist_fixture_row(
                        &fixture_path,
                        &mut fixture_rows,
                        failed_fixture_row(&spec, error.clone()),
                    )?;
                    write_status(
                        &config.output,
                        &format!("failed: fixture {fixture_key:?}: {error}"),
                    )?;
                    return Err(format!("fixture {fixture_key:?} failed: {error}"));
                }
            }
        }

        for cell in pending {
            check_guard(&config, started.elapsed())?;
            layout.validate_cell_destination(cell.api, cell.pattern)?;
            let cell_dir = layout.cell_dir(cell.api, cell.pattern);
            let changes = config
                .explicit_changes
                .unwrap_or_else(|| change_count(cell.records));
            let spec = CellSpec {
                adapter: cell.adapter,
                records: cell.records,
                repetition: cell.repetition,
                api: cell.api,
                pattern: cell.pattern,
                changes,
                revision: config.revision.clone(),
                dirty: config.dirty,
                measurement_samples: config.measurement_samples,
            };
            if let Err(error) = remove_cell_dir(&layout, &cell_dir)
                .and_then(|()| clone_fixture(&layout.source_dir(), &cell_dir))
            {
                persist_raw_row(&raw_path, &mut raw_rows, failed_row(&spec, error.clone()))?;
                write_status(&config.output, &format!("failed: {cell:?}: {error}"))?;
                return Err(format!("cell {cell:?} failed before execution: {error}"));
            }
            let result = match cell.adapter {
                Adapter::SqliteSync => run_sqlite_cell(&spec, &layout),
                Adapter::TursoAsync => run_turso_cell(&spec, &layout).await,
            };
            match result {
                Ok(row) => {
                    persist_raw_row(&raw_path, &mut raw_rows, row)?;
                    stats.measured += 1;
                    if !config.keep_fixtures {
                        remove_cell_dir(&layout, &cell_dir)?;
                    }
                }
                Err(error) => {
                    let row = failed_row(&spec, error.clone());
                    persist_raw_row(&raw_path, &mut raw_rows, row)?;
                    write_status(&config.output, &format!("failed: {cell:?}: {error}"))?;
                    return Err(format!("cell {cell:?} failed: {error}"));
                }
            }
        }

        if !config.keep_fixtures {
            remove_source_dir(&layout)?;
        }
    }

    write_status(&config.output, "complete")?;
    Ok(stats)
}

fn manifest_contract(config: &RunConfig) -> String {
    let adapters = config
        .adapters
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let sizes = config
        .sizes
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let apis = config
        .apis
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let patterns = config
        .patterns
        .iter()
        .map(|value| value.as_str())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "schema={SCHEMA_VERSION}\nrevision={}\ndirty={}\nseed={RANDOM_SEED}\nadapters={adapters}\nsizes={sizes}\nruns={}\napis={apis}\npatterns={patterns}\nchanges={}\nmeasurement_samples={}\ntokio_workers={}\nbuild_batch_size={}\nexecution=sqlite-sync-vs-turso-async-serial-local-only\nsqlite_config=default-wal-synchronous-normal-busy-timeout-5000ms\nturso_config=native-0.7-local-defaults-no-cloud-sync\n",
        config.revision,
        config.dirty,
        config.runs,
        config
            .explicit_changes
            .map_or_else(|| "automatic".to_string(), |value| value.to_string()),
        config.measurement_samples,
        config.tokio_workers,
        config.build_batch_size,
    )
}

fn ensure_manifest(config: &RunConfig) -> Result<(), String> {
    let path = config.output.join("run-manifest.txt");
    let expected = manifest_contract(config);
    if path.exists() {
        let actual = fs::read_to_string(&path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        if !actual.starts_with(&expected) {
            return Err(format!(
                "existing run manifest does not match requested benchmark configuration: {}",
                path.display()
            ));
        }
        if actual == expected {
            fs::write(
                &path,
                format!(
                    "{expected}started_unix={}\nstatus=running\nupdated_unix={}\n",
                    unix_seconds(),
                    unix_seconds()
                ),
            )
            .map_err(|error| format!("failed to update {}: {error}", path.display()))?;
        }
        return Ok(());
    }
    fs::write(
        &path,
        format!(
            "{expected}started_unix={}\nstatus=running\nupdated_unix={}\n",
            unix_seconds(),
            unix_seconds()
        ),
    )
    .map_err(|error| format!("failed to write {}: {error}", path.display()))
}

fn guard_reason(config: &RunConfig, elapsed: Duration, free_bytes: u64) -> Option<String> {
    if config
        .max_seconds
        .is_some_and(|seconds| elapsed >= Duration::from_secs(seconds))
    {
        return Some("maximum elapsed time reached".to_string());
    }
    if free_bytes < config.min_free_bytes {
        return Some("minimum free disk space reached".to_string());
    }
    None
}

fn check_guard(config: &RunConfig, elapsed: Duration) -> Result<(), String> {
    let free_bytes = available_space(&config.output).map_err(|error| {
        format!(
            "failed to inspect free space for {}: {error}",
            config.output.display()
        )
    })?;
    if let Some(reason) = guard_reason(config, elapsed, free_bytes) {
        write_status(&config.output, &format!("stopped: {reason}"))?;
        return Err(reason);
    }
    Ok(())
}

fn write_status(output: &Path, status: &str) -> Result<(), String> {
    let path = output.join("run-status.txt");
    fs::write(&path, format!("{status}\n"))
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    let manifest_path = output.join("run-manifest.txt");
    let manifest = fs::read_to_string(&manifest_path)
        .map_err(|error| format!("failed to read {}: {error}", manifest_path.display()))?;
    let stable = manifest
        .lines()
        .filter(|line| !line.starts_with("status=") && !line.starts_with("updated_unix="))
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(
        &manifest_path,
        format!(
            "{stable}\nstatus={status}\nupdated_unix={}\n",
            unix_seconds()
        ),
    )
    .map_err(|error| format!("failed to update {}: {error}", manifest_path.display()))
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn read_csv<T: DeserializeOwned>(path: &Path) -> Result<Vec<T>, String> {
    if !path.exists() || path.metadata().map(|value| value.len()).unwrap_or(0) == 0 {
        return Ok(Vec::new());
    }
    csv::Reader::from_path(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?
        .deserialize()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to read {}: {error}", path.display()))
}

fn rewrite_csv<T: Serialize>(path: &Path, rows: &[T]) -> Result<(), String> {
    let temporary = path.with_extension("csv.tmp");
    let file = File::create(&temporary)
        .map_err(|error| format!("failed to create {}: {error}", temporary.display()))?;
    let mut writer = csv::Writer::from_writer(file);
    for row in rows {
        writer
            .serialize(row)
            .map_err(|error| format!("failed to serialize {}: {error}", temporary.display()))?;
    }
    writer
        .flush()
        .map_err(|error| format!("failed to flush {}: {error}", temporary.display()))?;
    writer
        .get_ref()
        .sync_data()
        .map_err(|error| format!("failed to sync {}: {error}", temporary.display()))?;
    drop(writer);
    fs::rename(&temporary, path).map_err(|error| {
        format!(
            "failed to replace {} with {}: {error}",
            path.display(),
            temporary.display()
        )
    })
}

fn persist_raw_row(path: &Path, rows: &mut Vec<RawRow>, row: RawRow) -> Result<(), String> {
    if let Some(position) = rows.iter().position(|existing| existing.key() == row.key()) {
        rows[position] = row;
        rewrite_csv(path, rows)
    } else {
        CsvSink::open(path)?.append(&row)?;
        rows.push(row);
        Ok(())
    }
}

fn persist_fixture_row(
    path: &Path,
    rows: &mut Vec<FixtureRow>,
    row: FixtureRow,
) -> Result<(), String> {
    let key = (row.adapter, row.records, row.repetition);
    if let Some(position) = rows
        .iter()
        .position(|existing| (existing.adapter, existing.records, existing.repetition) == key)
    {
        rows[position] = row;
        rewrite_csv(path, rows)
    } else {
        CsvSink::open(path)?.append(&row)?;
        rows.push(row);
        Ok(())
    }
}

fn validate_raw_rows(config: &RunConfig, rows: &[RawRow]) -> Result<(), String> {
    let expected: BTreeSet<_> = enumerate_matrix(config).cells.into_iter().collect();
    for row in rows {
        let key = row.key();
        if row.revision != config.revision || row.dirty != config.dirty {
            return Err(format!(
                "measurement provenance does not match manifest: {key:?}"
            ));
        }
        if !expected.contains(&key) {
            return Err(format!("measurement is outside requested matrix: {key:?}"));
        }
        let expected_changes = config
            .explicit_changes
            .unwrap_or_else(|| change_count(row.records));
        if row.configured_changes != expected_changes {
            return Err(format!(
                "measurement change count does not match manifest: {key:?}"
            ));
        }
        if !row.validated {
            continue;
        }
        let expected_operations = match row.api {
            crate::model::Api::Merge => expected_changes.saturating_mul(2),
            crate::model::Api::Put | crate::model::Api::Batch | crate::model::Api::Diff => {
                expected_changes
            }
        };
        let expected_records = match (row.api, row.pattern) {
            (crate::model::Api::Merge, crate::model::Pattern::Append) => row
                .records
                .saturating_add(expected_changes.saturating_mul(2)),
            (_, crate::model::Pattern::Append) => row.records.saturating_add(expected_changes),
            _ => row.records,
        };
        if row.observed_changes != expected_operations
            || row.expected_records != expected_records
            || row.observed_records != expected_records
            || row.total_ns == 0
            || !row.operations_per_sec.is_finite()
            || row.operations_per_sec <= 0.0
        {
            return Err(format!("measurement contract is invalid: {key:?}"));
        }
        let calculated_rate = expected_operations as f64 * 1_000_000_000.0 / row.total_ns as f64;
        let relative_error = (row.operations_per_sec - calculated_rate).abs() / calculated_rate;
        if relative_error > 1e-9 {
            return Err(format!("measurement throughput is inconsistent: {key:?}"));
        }
        let percentiles = [row.p50_ns, row.p95_ns, row.p99_ns, row.max_ns];
        if percentiles
            .iter()
            .any(|value| value.is_none_or(|value| value == 0))
        {
            return Err(format!("measurement percentiles are missing: {key:?}"));
        }
        let values = percentiles.map(Option::unwrap);
        if !values.windows(2).all(|window| window[0] <= window[1]) {
            return Err(format!("measurement percentiles are unordered: {key:?}"));
        }
    }
    Ok(())
}

fn validate_fixture_rows(config: &RunConfig, rows: &[FixtureRow]) -> Result<(), String> {
    let expected: BTreeSet<_> = enumerate_matrix(config).fixtures.into_iter().collect();
    let mut seen = BTreeSet::new();
    for row in rows {
        if row.schema != SCHEMA_VERSION {
            return Err(format!("unexpected fixture schema: {}", row.schema));
        }
        let key = FixtureKey {
            adapter: row.adapter,
            records: row.records,
            repetition: row.repetition,
        };
        if row.revision != config.revision || row.dirty != config.dirty {
            return Err(format!(
                "fixture provenance does not match manifest: {key:?}"
            ));
        }
        if !expected.contains(&key) {
            return Err(format!("fixture is outside requested matrix: {key:?}"));
        }
        if !seen.insert(key) {
            return Err(format!("duplicate fixture row: {key:?}"));
        }
        if row.validated {
            if !row.error.is_empty() {
                return Err(format!("validated fixture contains an error: {key:?}"));
            }
            if row.build_ns == 0
                || !row.records_per_sec.is_finite()
                || row.records_per_sec <= 0.0
                || row.database_bytes == 0
                || row.observed_records != row.records
            {
                return Err(format!("validated fixture contract is invalid: {key:?}"));
            }
            let expected_rate = row.records as f64 * 1_000_000_000.0 / row.build_ns as f64;
            if (row.records_per_sec - expected_rate).abs() / expected_rate > 1e-9 {
                return Err(format!("fixture throughput is inconsistent: {key:?}"));
            }
        } else if row.error.is_empty() {
            return Err(format!("failed fixture is missing an error: {key:?}"));
        }
    }
    Ok(())
}

fn failed_fixture_row(spec: &FixtureSpec, error: String) -> FixtureRow {
    FixtureRow {
        schema: SCHEMA_VERSION.to_string(),
        revision: spec.revision.clone(),
        dirty: spec.dirty,
        adapter: spec.adapter,
        records: spec.records,
        repetition: spec.repetition,
        build_ns: 0,
        records_per_sec: 0.0,
        database_bytes: 0,
        observed_records: 0,
        validated: false,
        error,
    }
}

fn failed_row(spec: &CellSpec, error: String) -> RawRow {
    RawRow {
        schema: SCHEMA_VERSION.to_string(),
        revision: spec.revision.clone(),
        dirty: spec.dirty,
        adapter: spec.adapter,
        records: spec.records,
        repetition: spec.repetition,
        api: spec.api,
        pattern: spec.pattern,
        configured_changes: spec.changes,
        observed_changes: 0,
        total_ns: 0,
        operations_per_sec: 0.0,
        p50_ns: None,
        p95_ns: None,
        p99_ns: None,
        max_ns: None,
        db_bytes_before: 0,
        db_bytes_after: 0,
        expected_records: spec.expected_records(),
        observed_records: 0,
        validated: false,
        error,
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use crate::model::{Api, Pattern, FULL_SIZES};

    use super::*;

    #[test]
    fn full_matrix_has_approved_cardinality_and_alternating_adapter_order() {
        let mut config = RunConfig::smoke(PathBuf::from("output"));
        config.sizes = FULL_SIZES.to_vec();
        config.runs = 3;
        config.explicit_changes = None;

        let plan = enumerate_matrix(&config);

        assert_eq!(plan.fixtures.len(), 36);
        assert_eq!(plan.cells.len(), 432);
        assert_eq!(plan.fixtures[0].adapter, Adapter::SqliteSync);
        assert_eq!(plan.fixtures[1].adapter, Adapter::TursoAsync);
        let run_two = plan
            .fixtures
            .iter()
            .position(|key| key.repetition == 2)
            .unwrap();
        assert_eq!(plan.fixtures[run_two].adapter, Adapter::TursoAsync);
        assert_eq!(plan.fixtures[run_two + 1].adapter, Adapter::SqliteSync);
        assert_eq!(
            plan.cells
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            432
        );
    }

    #[test]
    fn smoke_matrix_has_two_fixtures_and_twenty_four_cells() {
        let config = RunConfig::smoke(PathBuf::from("output"));
        let plan = enumerate_matrix(&config);
        assert_eq!(plan.fixtures.len(), 2);
        assert_eq!(plan.cells.len(), 24);
        assert!(plan.cells.iter().any(|key| key.api == Api::Merge));
        assert!(plan
            .cells
            .iter()
            .any(|key| key.pattern == Pattern::Clustered));
    }

    #[test]
    fn manifest_rejects_a_changed_resume_contract() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = RunConfig::smoke(temp.path().to_path_buf());
        config.revision = "revision-a".to_string();
        ensure_manifest(&config).unwrap();
        ensure_manifest(&config).unwrap();

        config.tokio_workers += 1;
        assert!(ensure_manifest(&config)
            .unwrap_err()
            .contains("does not match"));
    }

    #[test]
    fn guards_stop_only_after_limits_are_crossed() {
        let config = RunConfig::smoke(PathBuf::from("output"));
        assert_eq!(guard_reason(&config, Duration::ZERO, u64::MAX), None);

        let mut time_limited = config.clone();
        time_limited.max_seconds = Some(5);
        assert_eq!(
            guard_reason(&time_limited, Duration::from_secs(5), u64::MAX),
            Some("maximum elapsed time reached".to_string())
        );

        let mut disk_limited = config;
        disk_limited.min_free_bytes = 1_000;
        assert_eq!(
            guard_reason(&disk_limited, Duration::ZERO, 999),
            Some("minimum free disk space reached".to_string())
        );
    }

    #[test]
    fn resume_rows_must_match_manifest_provenance() {
        let mut config = RunConfig::smoke(PathBuf::from("output"));
        config.revision = "revision-a".to_string();
        let mut row = RawRow::example();
        row.revision = "revision-b".to_string();
        assert!(validate_raw_rows(&config, &[row])
            .unwrap_err()
            .contains("provenance"));
    }

    #[test]
    fn resume_rejects_a_corrupt_successful_measurement() {
        let config = RunConfig::smoke(PathBuf::from("output"));
        let mut row = RawRow::example();
        row.revision = config.revision.clone();
        row.dirty = config.dirty;
        row.observed_changes = 9;
        assert!(validate_raw_rows(&config, &[row])
            .unwrap_err()
            .contains("contract"));
    }
}
