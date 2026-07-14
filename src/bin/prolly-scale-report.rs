use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
struct Row {
    version: String,
    records: usize,
    workload: String,
    operations: usize,
    total_ns: u128,
    ns_per_op: f64,
    validated: bool,
    nodes_read: u64,
    nodes_written: u64,
    bytes_read: u64,
    bytes_written: u64,
    num_nodes: usize,
    num_leaves: usize,
    num_internal: usize,
    height: u8,
    tree_bytes: usize,
}

#[derive(Clone, Debug)]
struct Sample {
    row: Row,
    peak_rss_bytes: u64,
}

#[derive(Clone, Debug)]
struct ManifestEntry {
    version: String,
    records: usize,
    run: usize,
    exit_status: i32,
    csv: PathBuf,
    timing: PathBuf,
    stderr: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Failure {
    version: String,
    records: usize,
    run: usize,
    exit_status: i32,
}

#[derive(Clone, Debug)]
struct Aggregate {
    operations: usize,
    runs: usize,
    min_ns: f64,
    median_ns: f64,
    max_ns: f64,
    median_total_ns: f64,
    median_nodes_read: f64,
    median_nodes_written: f64,
    median_bytes_read: f64,
    median_bytes_written: f64,
    median_num_nodes: f64,
    median_num_leaves: f64,
    median_num_internal: f64,
    median_height: f64,
    median_tree_bytes: f64,
    median_peak_rss: f64,
}

#[derive(Clone, Debug)]
struct Comparison {
    records: usize,
    workload: String,
    original: Aggregate,
    improved: Aggregate,
    change_pct: f64,
    classification: &'static str,
}

fn main() {
    let directory = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("performance-results/scale-2026-07-14"));
    if let Err(error) = generate_report(&directory) {
        eprintln!("scale report failed: {error}");
        std::process::exit(1);
    }
}

fn generate_report(directory: &Path) -> Result<(), String> {
    let manifest_path = directory.join("run-manifest.csv");
    let manifest = fs::read_to_string(&manifest_path)
        .map_err(|error| format!("read {}: {error}", manifest_path.display()))?;
    let mut failures = Vec::new();
    let mut samples = Vec::new();

    for line in manifest
        .lines()
        .skip(1)
        .filter(|line| !line.trim().is_empty())
    {
        let entry = parse_manifest_entry(line)?;
        if entry.exit_status != 0 {
            failures.push(Failure {
                version: entry.version,
                records: entry.records,
                run: entry.run,
                exit_status: entry.exit_status,
            });
            continue;
        }
        let peak_rss_bytes = parse_peak_rss(
            &fs::read_to_string(&entry.timing)
                .map_err(|error| format!("read timing {}: {error}", entry.timing.display()))?,
        )?;
        let csv = fs::read_to_string(&entry.csv)
            .map_err(|error| format!("read CSV {}: {error}", entry.csv.display()))?;
        for row_line in csv.lines().skip(1).filter(|line| !line.trim().is_empty()) {
            let row = parse_row(row_line)?;
            if row.version != entry.version || row.records != entry.records {
                return Err(format!(
                    "manifest/row mismatch for {} run {}",
                    entry.version, entry.run
                ));
            }
            if !row.validated {
                return Err(format!(
                    "unvalidated row: {} {} {}",
                    row.version, row.records, row.workload
                ));
            }
            samples.push(Sample {
                row,
                peak_rss_bytes,
            });
        }
        let stderr = fs::read_to_string(&entry.stderr)
            .map_err(|error| format!("read stderr {}: {error}", entry.stderr.display()))?;
        if !stderr.trim().is_empty() {
            return Err(format!(
                "non-empty stderr for {}: {stderr}",
                entry.csv.display()
            ));
        }
    }

    let comparisons = compare_samples(&samples)?;
    fs::write(
        directory.join("results.csv"),
        render_results_csv(&comparisons),
    )
    .map_err(|error| format!("write results.csv: {error}"))?;
    let machine = fs::read_to_string(directory.join("machine.txt")).unwrap_or_default();
    fs::write(
        directory.join("report.md"),
        render_markdown(&comparisons, &failures, &machine),
    )
    .map_err(|error| format!("write report.md: {error}"))?;
    Ok(())
}

fn parse_row(line: &str) -> Result<Row, String> {
    let fields = line.split(',').collect::<Vec<_>>();
    if fields.len() != 16 {
        return Err(format!(
            "expected 16 row fields, got {}: {line}",
            fields.len()
        ));
    }
    Ok(Row {
        version: fields[0].to_string(),
        records: parse(fields[1], "records")?,
        workload: fields[2].to_string(),
        operations: parse(fields[3], "operations")?,
        total_ns: parse(fields[4], "total_ns")?,
        ns_per_op: parse(fields[5], "ns_per_op")?,
        validated: parse(fields[6], "validated")?,
        nodes_read: parse(fields[7], "nodes_read")?,
        nodes_written: parse(fields[8], "nodes_written")?,
        bytes_read: parse(fields[9], "bytes_read")?,
        bytes_written: parse(fields[10], "bytes_written")?,
        num_nodes: parse(fields[11], "num_nodes")?,
        num_leaves: parse(fields[12], "num_leaves")?,
        num_internal: parse(fields[13], "num_internal")?,
        height: parse(fields[14], "height")?,
        tree_bytes: parse(fields[15], "tree_bytes")?,
    })
}

fn parse_manifest_entry(line: &str) -> Result<ManifestEntry, String> {
    let fields = line.split(',').collect::<Vec<_>>();
    if fields.len() != 7 {
        return Err(format!(
            "expected 7 manifest fields, got {}: {line}",
            fields.len()
        ));
    }
    Ok(ManifestEntry {
        version: fields[0].to_string(),
        records: parse(fields[1], "manifest records")?,
        run: parse(fields[2], "manifest run")?,
        exit_status: parse(fields[3], "manifest exit status")?,
        csv: PathBuf::from(fields[4]),
        timing: PathBuf::from(fields[5]),
        stderr: PathBuf::from(fields[6]),
    })
}

#[cfg(test)]
fn parse_manifest_failure(line: &str) -> Result<Failure, String> {
    let entry = parse_manifest_entry(line)?;
    if entry.exit_status == 0 {
        return Err("manifest entry succeeded".to_string());
    }
    Ok(Failure {
        version: entry.version,
        records: entry.records,
        run: entry.run,
        exit_status: entry.exit_status,
    })
}

fn parse_peak_rss(timing: &str) -> Result<u64, String> {
    timing
        .lines()
        .find(|line| line.contains("maximum resident set size"))
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| "timing output has no maximum resident set size".to_string())?
        .parse()
        .map_err(|error| format!("invalid maximum resident set size: {error}"))
}

fn parse<T: std::str::FromStr>(value: &str, field: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {field} '{value}': {error}"))
}

fn compare_samples(samples: &[Sample]) -> Result<Vec<Comparison>, String> {
    let mut grouped: BTreeMap<(usize, String, String), Vec<Sample>> = BTreeMap::new();
    for sample in samples {
        grouped
            .entry((
                sample.row.records,
                sample.row.workload.clone(),
                sample.row.version.clone(),
            ))
            .or_default()
            .push(sample.clone());
    }

    let mut workload_keys = BTreeMap::<(usize, String), ()>::new();
    for records_workload_version in grouped.keys() {
        workload_keys.insert(
            (
                records_workload_version.0,
                records_workload_version.1.clone(),
            ),
            (),
        );
    }

    let mut comparisons = Vec::new();
    for ((records, workload), ()) in workload_keys {
        let original_samples = grouped
            .get(&(records, workload.clone(), "original".to_string()))
            .ok_or_else(|| format!("missing original samples for {records} {workload}"))?;
        let improved_samples = grouped
            .get(&(records, workload.clone(), "improved".to_string()))
            .ok_or_else(|| format!("missing improved samples for {records} {workload}"))?;
        let original = aggregate(original_samples)?;
        let improved = aggregate(improved_samples)?;
        if original.operations != improved.operations {
            return Err(format!("operation mismatch for {records} {workload}"));
        }
        let change_pct = percent_change(original.median_ns, improved.median_ns);
        let classification = classify(
            original.median_ns,
            improved.median_ns,
            original.median_total_ns,
            improved.median_total_ns,
        );
        comparisons.push(Comparison {
            records,
            workload,
            classification,
            original,
            improved,
            change_pct,
        });
    }
    Ok(comparisons)
}

fn aggregate(samples: &[Sample]) -> Result<Aggregate, String> {
    let first = samples
        .first()
        .ok_or_else(|| "empty sample group".to_string())?;
    if samples
        .iter()
        .any(|sample| sample.row.operations != first.row.operations)
    {
        return Err("operation count changed between repetitions".to_string());
    }
    let ns = samples
        .iter()
        .map(|sample| sample.row.ns_per_op)
        .collect::<Vec<_>>();
    Ok(Aggregate {
        operations: first.row.operations,
        runs: samples.len(),
        min_ns: ns.iter().copied().fold(f64::INFINITY, f64::min),
        median_ns: median(&ns),
        max_ns: ns.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        median_total_ns: median_field(samples, |sample| sample.row.total_ns as f64),
        median_nodes_read: median_field(samples, |sample| sample.row.nodes_read as f64),
        median_nodes_written: median_field(samples, |sample| sample.row.nodes_written as f64),
        median_bytes_read: median_field(samples, |sample| sample.row.bytes_read as f64),
        median_bytes_written: median_field(samples, |sample| sample.row.bytes_written as f64),
        median_num_nodes: median_field(samples, |sample| sample.row.num_nodes as f64),
        median_num_leaves: median_field(samples, |sample| sample.row.num_leaves as f64),
        median_num_internal: median_field(samples, |sample| sample.row.num_internal as f64),
        median_height: median_field(samples, |sample| sample.row.height as f64),
        median_tree_bytes: median_field(samples, |sample| sample.row.tree_bytes as f64),
        median_peak_rss: median_field(samples, |sample| sample.peak_rss_bytes as f64),
    })
}

fn median_field(samples: &[Sample], field: impl Fn(&Sample) -> f64) -> f64 {
    median(&samples.iter().map(field).collect::<Vec<_>>())
}

fn median(values: &[f64]) -> f64 {
    assert!(!values.is_empty(), "median requires samples");
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn percent_change(original: f64, improved: f64) -> f64 {
    if original == 0.0 {
        0.0
    } else {
        (improved - original) * 100.0 / original
    }
}

fn classify(
    original: f64,
    improved: f64,
    original_total_ns: f64,
    improved_total_ns: f64,
) -> &'static str {
    let change = percent_change(original, improved);
    if original < 20.0
        || change.abs() < 3.0
        || original_total_ns.max(improved_total_ns) < 1_000_000.0
    {
        "noise-sensitive"
    } else if change > 0.0 {
        "regression"
    } else {
        "gain"
    }
}

fn render_results_csv(comparisons: &[Comparison]) -> String {
    let mut output = String::from(
        "records,workload,operations,runs,original_min_ns,original_median_ns,original_max_ns,improved_min_ns,improved_median_ns,improved_max_ns,delta_ns,change_pct,classification,original_total_ns,improved_total_ns,original_nodes_read,improved_nodes_read,original_nodes_written,improved_nodes_written,original_bytes_read,improved_bytes_read,original_bytes_written,improved_bytes_written,original_num_nodes,improved_num_nodes,original_num_leaves,improved_num_leaves,original_num_internal,improved_num_internal,original_height,improved_height,original_tree_bytes,improved_tree_bytes,original_peak_rss,improved_peak_rss\n",
    );
    for comparison in comparisons {
        let original = &comparison.original;
        let improved = &comparison.improved;
        output.push_str(&format!(
            "{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0}\n",
            comparison.records,
            comparison.workload,
            original.operations,
            original.runs.min(improved.runs),
            original.min_ns,
            original.median_ns,
            original.max_ns,
            improved.min_ns,
            improved.median_ns,
            improved.max_ns,
            improved.median_ns - original.median_ns,
            comparison.change_pct,
            comparison.classification,
            original.median_total_ns,
            improved.median_total_ns,
            original.median_nodes_read,
            improved.median_nodes_read,
            original.median_nodes_written,
            improved.median_nodes_written,
            original.median_bytes_read,
            improved.median_bytes_read,
            original.median_bytes_written,
            improved.median_bytes_written,
            original.median_num_nodes,
            improved.median_num_nodes,
            original.median_num_leaves,
            improved.median_num_leaves,
            original.median_num_internal,
            improved.median_num_internal,
            original.median_height,
            improved.median_height,
            original.median_tree_bytes,
            improved.median_tree_bytes,
            original.median_peak_rss,
            improved.median_peak_rss,
        ));
    }
    output
}

fn render_markdown(comparisons: &[Comparison], failures: &[Failure], machine: &str) -> String {
    let mut report = String::from("# Prolly Scale Performance Report\n\n");
    report.push_str("Lower latency is better. Percentage change is `(improved - original) / original`; negative values are gains. Results below 20 ns/op, below 3% absolute change, or whose entire measured workload is below 1 ms are labeled noise-sensitive.\n\n");
    report.push_str("## Regressions first\n\n");
    let regressions = comparisons
        .iter()
        .filter(|comparison| comparison.classification == "regression")
        .collect::<Vec<_>>();
    if regressions.is_empty() {
        report.push_str("No material latency regressions met the reporting threshold.\n\n");
    } else {
        report.push_str("| Records | Workload | Original ns/op | Improved ns/op | Change |\n|---:|---|---:|---:|---:|\n");
        for comparison in regressions {
            report.push_str(&format!(
                "| {} | {} | {:.3} | {:.3} | {:+.1}% |\n",
                format_count(comparison.records),
                comparison.workload,
                comparison.original.median_ns,
                comparison.improved.median_ns,
                comparison.change_pct,
            ));
        }
        report.push('\n');
    }

    report.push_str("## Workload-pattern summary\n\n");
    for (workload, label) in [
        ("append_mutations", "Append-only mutations"),
        ("random_mutations", "Randomly distributed mutations"),
        ("clustered_mutations", "Clustered mutations"),
        ("base_build", "Streaming base build"),
    ] {
        report.push_str(&format!("- **{label}:** "));
        let values = comparisons
            .iter()
            .filter(|comparison| comparison.workload == workload)
            .map(|comparison| {
                format!(
                    "{} {:+.1}%",
                    format_count(comparison.records),
                    comparison.change_pct
                )
            })
            .collect::<Vec<_>>();
        report.push_str(&values.join(", "));
        report.push_str(".\n");
    }
    if let Some(largest_random) = comparisons
        .iter()
        .filter(|comparison| comparison.workload == "random_mutations")
        .max_by_key(|comparison| comparison.records)
    {
        let interpretation = if largest_random.change_pct > 3.0 {
            "The counters indicate that the remaining latency regression is primarily CPU work rather than a large increase in store I/O; that attribution is an inference, not a CPU profile."
        } else if largest_random.change_pct < -3.0 {
            "The latency gain is not explained by a large reduction in store I/O, so it primarily reflects lower CPU overhead; that attribution is an inference, not a CPU profile."
        } else {
            "The latency change is inside the report's noise threshold."
        };
        report.push_str(&format!(
            "\nAt the largest tier, random-mutation latency changed {:+.1}% while nodes read changed {:+.1}%, nodes written {:+.1}%, bytes read {:+.1}%, and bytes written {:+.1}%. {interpretation}\n\n",
            largest_random.change_pct,
            percent_change(largest_random.original.median_nodes_read, largest_random.improved.median_nodes_read),
            percent_change(largest_random.original.median_nodes_written, largest_random.improved.median_nodes_written),
            percent_change(largest_random.original.median_bytes_read, largest_random.improved.median_bytes_read),
            percent_change(largest_random.original.median_bytes_written, largest_random.improved.median_bytes_written),
        ));
    }

    report.push_str("## Complete latency matrix\n\n");
    let mut current_size = None;
    for comparison in comparisons {
        if current_size != Some(comparison.records) {
            if current_size.is_some() {
                report.push('\n');
            }
            current_size = Some(comparison.records);
            report.push_str(&format!(
                "### {} records\n\n| Workload | Operations | Original ns/op [range] | Improved ns/op [range] | Original ops/s | Improved ops/s | Change | Classification |\n|---|---:|---:|---:|---:|---:|---:|---|\n",
                format_count(comparison.records)
            ));
        }
        report.push_str(&format!(
            "| {} | {} | {:.3} [{:.3}–{:.3}] | {:.3} [{:.3}–{:.3}] | {:.0} | {:.0} | {:+.1}% | {} |\n",
            comparison.workload,
            comparison.original.operations,
            comparison.original.median_ns,
            comparison.original.min_ns,
            comparison.original.max_ns,
            comparison.improved.median_ns,
            comparison.improved.min_ns,
            comparison.improved.max_ns,
            operations_per_second(comparison.original.median_ns),
            operations_per_second(comparison.improved.median_ns),
            comparison.change_pct,
            comparison.classification,
        ));
    }

    report.push_str("\n## Structure and process memory\n\n| Records | Original nodes | Improved nodes | Node change | Original tree bytes | Improved tree bytes | Byte change | Original peak RSS | Improved peak RSS |\n|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for comparison in comparisons
        .iter()
        .filter(|comparison| comparison.workload == "base_build")
    {
        report.push_str(&format!(
            "| {} | {:.0} | {:.0} | {:+.1}% | {:.0} | {:.0} | {:+.1}% | {} | {} |\n",
            format_count(comparison.records),
            comparison.original.median_num_nodes,
            comparison.improved.median_num_nodes,
            percent_change(
                comparison.original.median_num_nodes,
                comparison.improved.median_num_nodes
            ),
            comparison.original.median_tree_bytes,
            comparison.improved.median_tree_bytes,
            percent_change(
                comparison.original.median_tree_bytes,
                comparison.improved.median_tree_bytes
            ),
            format_bytes(comparison.original.median_peak_rss),
            format_bytes(comparison.improved.median_peak_rss),
        ));
    }

    report.push_str("\n## Captured failures\n\n");
    if failures.is_empty() {
        report.push_str("None.\n");
    } else {
        for failure in failures {
            report.push_str(&format!(
                "- {} {} records run {} exited {}.\n",
                failure.version, failure.records, failure.run, failure.exit_status
            ));
        }
    }
    let repetitions = comparisons
        .iter()
        .filter(|comparison| comparison.workload == "base_build")
        .map(|comparison| {
            format!(
                "{}: {}",
                format_count(comparison.records),
                comparison.original.runs.min(comparison.improved.runs)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    report.push_str("\n## Methodology and caveats\n\n- Base trees are streamed into `MemStore` with fixed-width deterministic keys and values.\n- Reads are warmed; mutations and diffs use fresh manager caches.\n- Mutation count is 1% of records, bounded to 100–10,000 operations.\n- The primary comparison uses each implementation's product default, so boundary-input changes are part of the measured product result.\n- A legacy-policy attribution run is not included because the exact shared source API at the original commit cannot select the new persisted policy object. Mixing different harness source would weaken the primary comparison.\n");
    report.push_str(&format!(
        "- Repetitions by record count are {repetitions}; tables show median and full measured range.\n"
    ));
    report.push_str("- Peak RSS covers the whole process, including the base store and all workload result nodes accumulated during the run.\n- Raw process CSV, `/usr/bin/time -l` output, stderr, and normalized aggregates are retained beside this report.\n\n");
    report.push_str("## Machine\n\n```text\n");
    report.push_str(machine.trim());
    report.push_str("\n```\n");
    report
}

fn format_count(value: usize) -> String {
    let digits = value.to_string();
    let mut output = String::new();
    for (index, character) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            output.push(',');
        }
        output.push(character);
    }
    output
}

fn format_bytes(value: f64) -> String {
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * MIB;
    if value >= GIB {
        format!("{:.2} GiB", value / GIB)
    } else {
        format!("{:.1} MiB", value / MIB)
    }
}

fn operations_per_second(ns_per_operation: f64) -> f64 {
    if ns_per_operation == 0.0 {
        0.0
    } else {
        1_000_000_000.0 / ns_per_operation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ROW: &str =
        "original,1000,random_reads,100000,33536000,335.360,true,0,0,0,0,9,8,1,1,33761";

    #[test]
    fn parses_harness_csv_row() {
        let row = parse_row(ROW).unwrap();
        assert_eq!(row.version, "original");
        assert_eq!(row.records, 1_000);
        assert_eq!(row.workload, "random_reads");
        assert_eq!(row.operations, 100_000);
        assert_eq!(row.num_nodes, 9);
        assert_eq!(row.tree_bytes, 33_761);
    }

    #[test]
    fn median_handles_odd_and_even_samples() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[4.0, 1.0, 2.0, 3.0]), 2.5);
    }

    #[test]
    fn classification_uses_lower_latency_as_gain() {
        assert_eq!(classify(100.0, 80.0, 2_000_000.0, 2_000_000.0), "gain");
        assert_eq!(
            classify(100.0, 120.0, 2_000_000.0, 2_000_000.0),
            "regression"
        );
        assert_eq!(
            classify(100.0, 101.0, 2_000_000.0, 2_000_000.0),
            "noise-sensitive"
        );
        assert_eq!(
            classify(10.0, 8.0, 2_000_000.0, 2_000_000.0),
            "noise-sensitive"
        );
        assert_eq!(
            classify(700.0, 780.0, 70_000.0, 78_000.0),
            "noise-sensitive"
        );
    }

    #[test]
    fn failed_manifest_entries_are_retained() {
        let line = "improved,10000000,2,137,/tmp/a.csv,/tmp/a.time,/tmp/a.stderr";
        let failure = parse_manifest_failure(line).unwrap();
        assert_eq!(failure.version, "improved");
        assert_eq!(failure.records, 10_000_000);
        assert_eq!(failure.run, 2);
        assert_eq!(failure.exit_status, 137);
    }
}
