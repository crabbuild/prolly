use std::collections::BTreeMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::measurement::{FixtureRow, RawRow};
use crate::model::{CacheState, Operation, Pattern};

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
) -> Result<(), String> {
    let mut file = File::create(path)
        .map_err(|error| format!("failed to create {}: {error}", path.display()))?;
    writeln!(file, "# SQLite-backed prolly key-pattern benchmark\n")
        .map_err(|error| error.to_string())?;
    writeln!(
        file,
        "All values below are medians of independent repetitions.\n"
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
        "Validated fixture rows: {}.\n",
        fixtures.iter().filter(|row| row.validated).count()
    )
    .map_err(|error| error.to_string())?;
    writeln!(file, "## Interpretation limits\n").map_err(|error| error.to_string())?;
    writeln!(
        file,
        "- End-to-end synchronous `Prolly<SqliteStore>` on one local connection."
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
