use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use crate::fixture::{compact_aof, validate_strong_durability, FixtureLayout};
use crate::measurement::{read_csv, CellKey, CsvSink, FixtureRow, RawRow, SCHEMA_VERSION};
use crate::model::{enumerate_cells, CellSpec, FixtureSpec, RunConfig};
use crate::redis_runner::{build_fixture, run_cell};
use crate::report::{summarize, write_report, write_summary};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FixtureKey {
    pub records: usize,
    pub repetition: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MatrixPlan {
    pub fixtures: Vec<FixtureKey>,
    pub cells: Vec<CellKey>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RunStats {
    pub fixtures: usize,
    pub measured: usize,
    pub skipped: usize,
}

pub fn enumerate_matrix(config: &RunConfig) -> MatrixPlan {
    let mut fixtures = Vec::new();
    let mut cells = Vec::new();
    for repetition in 1..=config.runs {
        for &records in &config.sizes {
            fixtures.push(FixtureKey {
                records,
                repetition,
            });
            let mut specs = enumerate_cells(config, records, repetition);
            if repetition % 2 == 0 {
                specs.reverse();
            }
            cells.extend(specs.into_iter().map(|spec| CellKey {
                records: spec.records,
                repetition: spec.repetition,
                operation: spec.operation,
                pattern: spec.pattern,
                cache_state: spec.cache_state,
            }));
        }
    }
    MatrixPlan { fixtures, cells }
}

pub async fn run_matrix(config: RunConfig) -> Result<RunStats, String> {
    config.validate()?;
    fs::create_dir_all(&config.output).map_err(|error| {
        format!(
            "failed to create output {}: {error}",
            config.output.display()
        )
    })?;
    require_free_space(&config)?;
    validate_strong_durability(&config.redis_url).await?;
    ensure_manifest(&config)?;
    write_status(&config.output, "running")?;

    let raw_path = config.output.join("raw-results.csv");
    let fixture_path = config.output.join("fixture-results.csv");
    let mut raw_rows: Vec<RawRow> = read_csv(&raw_path)?;
    validate_raw_rows(&config, &raw_rows)?;
    let completed_cells = raw_rows
        .iter()
        .filter(|row| row.validated)
        .map(RawRow::key)
        .collect::<BTreeSet<_>>();
    let mut fixture_rows: Vec<FixtureRow> = read_csv(&fixture_path)?;
    validate_fixture_rows(&config, &fixture_rows)?;
    let completed_fixtures = fixture_rows
        .iter()
        .filter(|row| row.validated)
        .map(|row| FixtureKey {
            records: row.records,
            repetition: row.repetition,
        })
        .collect::<BTreeSet<_>>();

    let plan = enumerate_matrix(&config);
    let mut stats = RunStats::default();
    for fixture_key in plan.fixtures {
        let mut pending = enumerate_cells(&config, fixture_key.records, fixture_key.repetition);
        if fixture_key.repetition % 2 == 0 {
            pending.reverse();
        }
        let before = pending.len();
        pending.retain(|spec| !completed_cells.contains(&cell_key(spec)));
        stats.skipped += before - pending.len();
        if pending.is_empty() {
            continue;
        }

        let layout = FixtureLayout::new(
            config.redis_url.clone(),
            fixture_key.records,
            fixture_key.repetition,
        );
        layout.clear_source().await?;
        let fixture_spec =
            FixtureSpec::from_config(&config, fixture_key.records, fixture_key.repetition);
        let fixture = match build_fixture(&fixture_spec, &layout).await {
            Ok(row) => row,
            Err(error) => {
                write_status(&config.output, &format!("failed: {error}"))?;
                return Err(error);
            }
        };
        if !completed_fixtures.contains(&fixture_key) {
            CsvSink::open(&fixture_path)?.append(&fixture)?;
            fixture_rows.push(fixture);
        }
        stats.fixtures += 1;

        for spec in pending {
            layout.clear_cell(&spec).await?;
            layout.clone_for(&spec).await?;
            match run_cell(&spec, &layout).await {
                Ok(row) => {
                    CsvSink::open(&raw_path)?.append(&row)?;
                    raw_rows.push(row);
                    stats.measured += 1;
                    if !config.keep_fixtures {
                        layout.clear_cell(&spec).await?;
                        compact_aof(&config.redis_url).await?;
                    }
                }
                Err(error) => {
                    let failed = failed_row(&spec, &error);
                    CsvSink::open(&raw_path)?.append(&failed)?;
                    write_status(&config.output, &format!("failed: {error}"))?;
                    return Err(format!("cell {:?} failed: {error}", cell_key(&spec)));
                }
            }
        }
        if !config.keep_fixtures {
            layout.clear_source().await?;
            compact_aof(&config.redis_url).await?;
        }
    }

    let summaries = summarize(&raw_rows, config.runs)?;
    write_summary(&config.output.join("summary.csv"), &summaries)?;
    write_report(
        &config.output.join("report.md"),
        &summaries,
        &fixture_rows,
        &config,
    )?;
    write_status(&config.output, "complete")?;
    Ok(stats)
}

fn cell_key(spec: &CellSpec) -> CellKey {
    CellKey {
        records: spec.records,
        repetition: spec.repetition,
        operation: spec.operation,
        pattern: spec.pattern,
        cache_state: spec.cache_state,
    }
}

fn failed_row(spec: &CellSpec, error: &str) -> RawRow {
    RawRow {
        schema: SCHEMA_VERSION.to_string(),
        revision: spec.revision.clone(),
        dirty: spec.dirty,
        records: spec.records,
        repetition: spec.repetition,
        operation: spec.operation,
        pattern: spec.pattern,
        cache_state: spec.cache_state,
        sample_count: 0,
        logical_operations: spec.logical_operations(),
        observed_items: 0,
        total_ns: 0,
        ns_per_operation: 0.0,
        operations_per_sec: 0.0,
        p50_ns: None,
        p95_ns: None,
        p99_ns: None,
        max_ns: None,
        nodes_read: 0,
        nodes_written: 0,
        bytes_read: 0,
        bytes_written: 0,
        cache_hits: 0,
        cache_misses: 0,
        cache_evictions: 0,
        result_entries: 0,
        num_nodes: 0,
        num_leaves: 0,
        num_internal: 0,
        height: 0,
        tree_bytes: 0,
        redis_used_memory_bytes: 0,
        redis_used_memory_rss_bytes: 0,
        redis_used_memory_dataset_bytes: 0,
        aof_current_size_bytes: 0,
        namespace_keys: 0,
        expected_entries: spec.expected_entries(),
        observed_entries: 0,
        error: error.to_string(),
        validated: false,
    }
}

fn manifest(config: &RunConfig) -> String {
    let sizes = config
        .sizes
        .iter()
        .map(usize::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let operations = config
        .operations
        .iter()
        .map(|operation| operation.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let patterns = config
        .patterns
        .iter()
        .map(|pattern| pattern.as_str())
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "schema={SCHEMA_VERSION}\nrevision={}\ndirty={}\nsizes={sizes}\nruns={}\noperations={operations}\npatterns={patterns}\nchanges={}\nread_samples={}\ntokio_workers={}\nmerge_changes_semantics=total_split_evenly\nrandom_merge_branch_distribution=interleaved\nkey_bytes=24\nvalue_bytes=100\nrandom_seed=0x6a09e667f3bcc909\nredis_mode=docker-local-tcp\nredis_durability=aof-appendfsync-always\nrdb_snapshots=disabled\nauto_aof_rewrite=disabled\nmanual_aof_rewrite=after-cell-cleanup-outside-timing\nfixture_clone=server-side-copy-before-timing\nmanager_cache=cold-per-get-or-warmed\n",
        config.revision,
        config.dirty,
        config.runs,
        config
            .changes
            .map(|value| value.to_string())
            .unwrap_or_else(|| "auto-30-percent".to_string()),
        config.read_samples,
        config.tokio_workers,
    )
}

fn require_free_space(config: &RunConfig) -> Result<(), String> {
    let available = fs2::available_space(&config.output)
        .map_err(|error| format!("failed to inspect free space: {error}"))?;
    if available < config.min_free_bytes {
        return Err(format!(
            "insufficient free space: available {available} bytes, require {} bytes",
            config.min_free_bytes
        ));
    }
    Ok(())
}

fn ensure_manifest(config: &RunConfig) -> Result<(), String> {
    let path = config.output.join("run-manifest.txt");
    let expected = manifest(config);
    if path.exists() {
        let observed = fs::read_to_string(&path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        if observed != expected {
            return Err(format!(
                "existing run manifest does not match requested benchmark: {}",
                path.display()
            ));
        }
        return Ok(());
    }
    fs::write(&path, expected)
        .map_err(|error| format!("failed to write {}: {error}", path.display()))
}

fn write_status(output: &Path, status: &str) -> Result<(), String> {
    fs::write(output.join("run-status.txt"), status)
        .map_err(|error| format!("failed to write run status: {error}"))
}

fn validate_raw_rows(config: &RunConfig, rows: &[RawRow]) -> Result<(), String> {
    let mut successful = BTreeSet::new();
    for row in rows {
        if row.schema != SCHEMA_VERSION
            || row.revision != config.revision
            || row.dirty != config.dirty
            || !config.sizes.contains(&row.records)
            || row.repetition == 0
            || row.repetition > config.runs
        {
            return Err("raw results do not match the requested run manifest".to_string());
        }
        if row.validated && !successful.insert(row.key()) {
            return Err(format!("duplicate validated row: {:?}", row.key()));
        }
    }
    Ok(())
}

fn validate_fixture_rows(config: &RunConfig, rows: &[FixtureRow]) -> Result<(), String> {
    let mut successful = BTreeSet::new();
    for row in rows {
        let key = FixtureKey {
            records: row.records,
            repetition: row.repetition,
        };
        if row.schema != SCHEMA_VERSION
            || row.revision != config.revision
            || row.dirty != config.dirty
            || !config.sizes.contains(&row.records)
            || row.repetition == 0
            || row.repetition > config.runs
        {
            return Err("fixture results do not match the requested run manifest".to_string());
        }
        if row.validated && !successful.insert(key) {
            return Err(format!("duplicate validated fixture row: {key:?}"));
        }
    }
    Ok(())
}
