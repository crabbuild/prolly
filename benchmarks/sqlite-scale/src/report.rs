use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::measurement::{FixtureRow, RawRow};
use crate::model::{CacheState, Operation, Pattern, RunConfig};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SummaryRow {
    pub records: usize,
    pub operation: Operation,
    pub pattern: Pattern,
    pub cache_state: CacheState,
    pub repetitions: usize,
    pub median_total_ns: u128,
    pub min_total_ns: u128,
    pub max_total_ns: u128,
    pub median_ns_per_operation: f64,
    pub median_operations_per_sec: f64,
    pub min_operations_per_sec: f64,
    pub max_operations_per_sec: f64,
}

pub fn summarize(rows: &[RawRow], runs: usize) -> Result<Vec<SummaryRow>, String> {
    let mut grouped: BTreeMap<(usize, Operation, Pattern, CacheState), Vec<&RawRow>> =
        BTreeMap::new();
    for row in rows.iter().filter(|row| row.validated) {
        grouped
            .entry((row.records, row.operation, row.pattern, row.cache_state))
            .or_default()
            .push(row);
    }
    let mut summaries = Vec::with_capacity(grouped.len());
    for ((records, operation, pattern, cache_state), group) in grouped {
        if group.len() != runs {
            return Err(format!(
                "incomplete summary group {records}/{operation:?}/{pattern:?}/{cache_state:?}: observed {}, expected {runs}",
                group.len()
            ));
        }
        let mut totals = group.iter().map(|row| row.total_ns).collect::<Vec<_>>();
        let mut per_operation = group
            .iter()
            .map(|row| row.ns_per_operation)
            .collect::<Vec<_>>();
        let mut rates = group
            .iter()
            .map(|row| row.operations_per_sec)
            .collect::<Vec<_>>();
        totals.sort_unstable();
        per_operation.sort_by(f64::total_cmp);
        rates.sort_by(f64::total_cmp);
        let middle = totals.len() / 2;
        summaries.push(SummaryRow {
            records,
            operation,
            pattern,
            cache_state,
            repetitions: group.len(),
            median_total_ns: totals[middle],
            min_total_ns: totals[0],
            max_total_ns: *totals.last().expect("non-empty validated group"),
            median_ns_per_operation: per_operation[middle],
            median_operations_per_sec: rates[middle],
            min_operations_per_sec: rates[0],
            max_operations_per_sec: *rates.last().expect("non-empty validated group"),
        });
    }
    Ok(summaries)
}

pub fn write_summary(path: &Path, rows: &[SummaryRow]) -> Result<(), String> {
    let file = File::create(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))?;
    let mut writer = csv::Writer::from_writer(file);
    for row in rows {
        writer
            .serialize(row)
            .map_err(|error| format!("failed to write summary: {error}"))?;
    }
    writer
        .flush()
        .map_err(|error| format!("failed to flush summary: {error}"))
}

pub fn write_report(
    path: &Path,
    summaries: &[SummaryRow],
    fixtures: &[FixtureRow],
    config: &RunConfig,
) -> Result<(), String> {
    let mut file = File::create(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))?;
    writeln!(file, "# SQLite prolly scale baseline\n").map_err(|error| error.to_string())?;
    writeln!(
        file,
        "All values below are medians of independent repetitions.\n"
    )
    .map_err(|error| error.to_string())?;
    writeln!(file, "## Workload contract\n").map_err(|error| error.to_string())?;
    writeln!(
        file,
        "- Sizes: {} records; repetitions: {}.",
        config
            .sizes
            .iter()
            .map(usize::to_string)
            .collect::<Vec<_>>()
            .join(", "),
        config.runs
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        file,
        "- Mutations: {}; read/scan samples: {}.",
        config
            .changes
            .map(|changes| changes.to_string())
            .unwrap_or_else(|| "30% of each base".to_string()),
        config.read_samples
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        file,
        "- Merge changes are the total across two equal, disjoint branches.\n"
    )
    .map_err(|error| error.to_string())?;
    writeln!(file, "## Workloads\n").map_err(|error| error.to_string())?;
    writeln!(
        file,
        "| Records | Operation | Pattern | Cache | Median ns/op | Median ops/s |"
    )
    .map_err(|error| error.to_string())?;
    writeln!(file, "|---:|---|---|---|---:|---:|").map_err(|error| error.to_string())?;
    for row in summaries {
        writeln!(
            file,
            "| {} | {} | {} | {} | {:.1} | {:.1} |",
            row.records,
            row.operation.as_str(),
            row.pattern.as_str(),
            row.cache_state.as_str(),
            row.median_ns_per_operation,
            row.median_operations_per_sec
        )
        .map_err(|error| error.to_string())?;
    }
    writeln!(file, "\n## Fixture context\n").map_err(|error| error.to_string())?;
    writeln!(
        file,
        "| Records | Repetitions | Median build ms | Median records/s | Median database MiB |"
    )
    .map_err(|error| error.to_string())?;
    writeln!(file, "|---:|---:|---:|---:|---:|").map_err(|error| error.to_string())?;
    let mut grouped = BTreeMap::<usize, Vec<&FixtureRow>>::new();
    for fixture in fixtures.iter().filter(|fixture| fixture.validated) {
        grouped.entry(fixture.records).or_default().push(fixture);
    }
    for (records, group) in grouped {
        let mut build_ns = group.iter().map(|row| row.build_ns).collect::<Vec<_>>();
        let mut rates = group
            .iter()
            .map(|row| row.records_per_sec)
            .collect::<Vec<_>>();
        let mut bytes = group
            .iter()
            .map(|row| row.database_bytes)
            .collect::<Vec<_>>();
        build_ns.sort_unstable();
        rates.sort_by(f64::total_cmp);
        bytes.sort_unstable();
        let middle = group.len() / 2;
        writeln!(
            file,
            "| {records} | {} | {:.3} | {:.1} | {:.2} |",
            group.len(),
            build_ns[middle] as f64 / 1_000_000.0,
            rates[middle],
            bytes[middle] as f64 / (1024.0 * 1024.0)
        )
        .map_err(|error| error.to_string())?;
    }
    writeln!(file).map_err(|error| error.to_string())?;
    writeln!(file, "## Measurement boundaries\n").map_err(|error| error.to_string())?;
    writeln!(file, "- Fixture cloning, diff/merge branch setup, validation, stats, publication, and reopen checks are outside timed intervals.")
        .map_err(|error| error.to_string())?;
    writeln!(file, "- Scans include full iterator consumption; cold point gets clear the manager cache before every lookup.")
        .map_err(|error| error.to_string())?;
    writeln!(
        file,
        "- Each workload cell uses an isolated clone of a closed SQLite fixture.\n"
    )
    .map_err(|error| error.to_string())?;
    writeln!(file, "## Interpretation limits\n").map_err(|error| error.to_string())?;
    writeln!(
        file,
        "- End-to-end synchronous `Prolly<SqliteStore>` on one workload thread; this matrix does not measure read-pool concurrency or group commit."
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        file,
        "- SQLite uses WAL and `synchronous=NORMAL`; this is not `FULL` durability."
    )
    .map_err(|error| error.to_string())?;
    writeln!(
        file,
        "- Manager cache state is controlled, but the operating-system filesystem cache is not."
    )
    .map_err(|error| error.to_string())?;
    writeln!(file, "- Keys are 24 bytes and values are 100 bytes. Results do not predict concurrent writers, remote filesystems, or raw SQLite.")
        .map_err(|error| error.to_string())
}
