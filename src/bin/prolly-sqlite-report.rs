use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

const MIB: f64 = 1024.0 * 1024.0;

#[derive(Clone, Debug)]
struct RawRow {
    version: String,
    profile: String,
    records: usize,
    workload: String,
    operations: usize,
    total_ns: f64,
    ns_per_op: f64,
    ops_per_sec: f64,
    nodes_read: f64,
    nodes_written: f64,
    bytes_read: f64,
    bytes_written: f64,
    result_entries: f64,
    num_nodes: f64,
    height: f64,
    tree_bytes: f64,
    fixture_bytes: f64,
    node_payload_bytes: f64,
    peak_rss: f64,
}

#[derive(Clone, Debug)]
struct Summary {
    profile: String,
    records: usize,
    workload: String,
    runs: usize,
    operations: usize,
    original_total: Stats,
    current_total: Stats,
    original_ns_per_op: f64,
    current_ns_per_op: f64,
    original_ops_per_sec: f64,
    current_ops_per_sec: f64,
    original_rss: f64,
    current_rss: f64,
    original_fixture: f64,
    current_fixture: f64,
    original_nodes_read: f64,
    current_nodes_read: f64,
    original_nodes_written: f64,
    current_nodes_written: f64,
    original_bytes_read: f64,
    current_bytes_read: f64,
    original_bytes_written: f64,
    current_bytes_written: f64,
    original_tree_bytes: f64,
    current_tree_bytes: f64,
    original_node_payload: f64,
    current_node_payload: f64,
    original_num_nodes: f64,
    current_num_nodes: f64,
    original_height: f64,
    current_height: f64,
    original_entries: f64,
    current_entries: f64,
    latency_class: &'static str,
    memory_regression: bool,
    size_regression: bool,
    io_regression: bool,
}

#[derive(Clone, Copy, Debug)]
struct Stats {
    median: f64,
    min: f64,
    max: f64,
}

fn main() {
    if let Err(error) = generate_from_args() {
        eprintln!("SQLite workload report failed: {error}");
        std::process::exit(1);
    }
}

fn generate_from_args() -> Result<(), String> {
    let directory = std::env::args()
        .nth(1)
        .ok_or_else(|| "usage: prolly-sqlite-report <result-directory>".to_string())?;
    generate_report(Path::new(&directory))
}

fn generate_report(directory: &Path) -> Result<(), String> {
    let manifest_path = directory.join("run-manifest.csv");
    let raw_path = directory.join("raw-results.csv");
    let machine_path = directory.join("machine.txt");
    let manifest = fs::read_to_string(&manifest_path)
        .map_err(|error| format!("read {}: {error}", manifest_path.display()))?;
    let raw = fs::read_to_string(&raw_path)
        .map_err(|error| format!("read {}: {error}", raw_path.display()))?;
    let machine = fs::read_to_string(&machine_path)
        .map_err(|error| format!("read {}: {error}", machine_path.display()))?;
    let (rss, failures) = parse_manifest(directory, &manifest)?;
    let rows = parse_raw_rows(&raw, &rss)?;
    let summaries = aggregate(&rows)?;
    let results = render_csv(&summaries);
    let report = render_markdown(&summaries, &failures, &machine);
    fs::write(directory.join("results.csv"), results).map_err(|error| error.to_string())?;
    fs::write(directory.join("report.md"), report).map_err(|error| error.to_string())?;
    Ok(())
}

type RunKey = (String, String, usize, usize, String);

fn parse_manifest(
    directory: &Path,
    input: &str,
) -> Result<(BTreeMap<RunKey, f64>, Vec<String>), String> {
    let mut rss = BTreeMap::new();
    let mut failures = Vec::new();
    for (line_number, line) in input.lines().enumerate().skip(1) {
        let fields = line.split(',').collect::<Vec<_>>();
        if fields.len() != 11 {
            return Err(format!(
                "manifest line {} has {} fields",
                line_number + 1,
                fields.len()
            ));
        }
        let records = parse(fields[2], "manifest records")?;
        let run = parse(fields[3], "manifest run")?;
        let key = (
            fields[0].to_string(),
            fields[1].to_string(),
            records,
            run,
            fields[4].to_string(),
        );
        if fields[7] != "ok" || fields[6] != "0" {
            failures.push(format!(
                "{} / {} / {} / run {} / {}: exit={}, validation={}",
                fields[0], fields[1], fields[2], fields[3], fields[4], fields[6], fields[7]
            ));
            continue;
        }
        let timing = fs::read_to_string(directory.join(fields[10]))
            .map_err(|error| format!("read timing {}: {error}", fields[10]))?;
        let peak = parse_peak_rss(&timing)?;
        if rss.insert(key, peak).is_some() {
            return Err(format!(
                "duplicate successful manifest row at line {}",
                line_number + 1
            ));
        }
    }
    Ok((rss, failures))
}

fn parse_peak_rss(input: &str) -> Result<f64, String> {
    input
        .lines()
        .find(|line| line.contains("maximum resident set size"))
        .and_then(|line| line.split_whitespace().next())
        .ok_or_else(|| "timing output is missing maximum resident set size".to_string())?
        .parse::<f64>()
        .map_err(|error| format!("invalid peak RSS: {error}"))
}

fn parse_raw_rows(input: &str, rss: &BTreeMap<RunKey, f64>) -> Result<Vec<RawRow>, String> {
    let expected_header = "version,profile,records,run,workload,operations,total_ns,ns_per_op,ops_per_sec,nodes_read,nodes_written,bytes_read,bytes_written,cache_hits,cache_misses,cache_evictions,result_entries,num_nodes,num_leaves,num_internal,height,tree_bytes,db_bytes_before,db_bytes_after,wal_bytes_after,shm_bytes_after,fixture_bytes_after,sqlite_node_count,sqlite_node_payload_bytes,validated,status";
    let mut lines = input.lines();
    if lines.next() != Some(expected_header) {
        return Err("raw results header does not match the benchmark schema".to_string());
    }
    let mut seen = BTreeSet::new();
    let mut rows = Vec::new();
    for (line_number, line) in lines.enumerate() {
        let fields = line.split(',').collect::<Vec<_>>();
        if fields.len() != 31 {
            return Err(format!(
                "raw line {} has {} fields",
                line_number + 2,
                fields.len()
            ));
        }
        if fields[29] != "true" || fields[30] != "ok" {
            return Err(format!("raw line {} is not validated", line_number + 2));
        }
        let records = parse(fields[2], "records")?;
        let run = parse(fields[3], "run")?;
        let key = (
            fields[0].to_string(),
            fields[1].to_string(),
            records,
            run,
            fields[4].to_string(),
        );
        if !seen.insert(key.clone()) {
            return Err(format!("duplicate raw tuple at line {}", line_number + 2));
        }
        let peak_rss = *rss
            .get(&key)
            .ok_or_else(|| format!("raw row has no successful manifest entry: {key:?}"))?;
        rows.push(RawRow {
            version: fields[0].to_string(),
            profile: fields[1].to_string(),
            records,
            workload: fields[4].to_string(),
            operations: parse(fields[5], "operations")?,
            total_ns: parse(fields[6], "total_ns")?,
            ns_per_op: parse(fields[7], "ns_per_op")?,
            ops_per_sec: parse(fields[8], "ops_per_sec")?,
            nodes_read: parse(fields[9], "nodes_read")?,
            nodes_written: parse(fields[10], "nodes_written")?,
            bytes_read: parse(fields[11], "bytes_read")?,
            bytes_written: parse(fields[12], "bytes_written")?,
            result_entries: parse(fields[16], "result_entries")?,
            num_nodes: parse(fields[17], "num_nodes")?,
            height: parse(fields[20], "height")?,
            tree_bytes: parse(fields[21], "tree_bytes")?,
            fixture_bytes: parse(fields[26], "fixture_bytes_after")?,
            node_payload_bytes: parse(fields[28], "sqlite_node_payload_bytes")?,
            peak_rss,
        });
    }
    Ok(rows)
}

fn parse<T: std::str::FromStr>(value: &str, field: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {field}: {error}"))
}

fn aggregate(rows: &[RawRow]) -> Result<Vec<Summary>, String> {
    type GroupKey = (String, usize, String);
    let mut groups: BTreeMap<GroupKey, (Vec<&RawRow>, Vec<&RawRow>)> = BTreeMap::new();
    for row in rows {
        let group = groups
            .entry((row.profile.clone(), row.records, row.workload.clone()))
            .or_default();
        match row.version.as_str() {
            "original" => group.0.push(row),
            "current" => group.1.push(row),
            other => return Err(format!("unexpected version label {other}")),
        }
    }
    let mut summaries = Vec::new();
    for ((profile, records, workload), (original, current)) in groups {
        if original.is_empty() || current.is_empty() || original.len() != current.len() {
            return Err(format!(
                "incomplete version pairing for {profile}/{records}/{workload}: original={}, current={}",
                original.len(), current.len()
            ));
        }
        let operations = original[0].operations;
        if original
            .iter()
            .chain(&current)
            .any(|row| row.operations != operations)
        {
            return Err(format!(
                "operation-count mismatch for {profile}/{records}/{workload}"
            ));
        }
        let original_total = stats(&original, |row| row.total_ns);
        let current_total = stats(&current, |row| row.total_ns);
        let broad_overlap = range_overlap_fraction(original_total, current_total) >= 0.5;
        let latency_class =
            classify_latency(original_total.median, current_total.median, broad_overlap);
        let original_rss = median_rows(&original, |row| row.peak_rss);
        let current_rss = median_rows(&current, |row| row.peak_rss);
        let original_fixture = median_rows(&original, |row| row.fixture_bytes);
        let current_fixture = median_rows(&current, |row| row.fixture_bytes);
        let original_nodes_read = median_rows(&original, |row| row.nodes_read);
        let current_nodes_read = median_rows(&current, |row| row.nodes_read);
        let original_nodes_written = median_rows(&original, |row| row.nodes_written);
        let current_nodes_written = median_rows(&current, |row| row.nodes_written);
        let original_bytes_read = median_rows(&original, |row| row.bytes_read);
        let current_bytes_read = median_rows(&current, |row| row.bytes_read);
        let original_bytes_written = median_rows(&original, |row| row.bytes_written);
        let current_bytes_written = median_rows(&current, |row| row.bytes_written);
        let io_regressed = io_regression(original_nodes_read, current_nodes_read)
            || io_regression(original_nodes_written, current_nodes_written)
            || io_regression(original_bytes_read, current_bytes_read)
            || io_regression(original_bytes_written, current_bytes_written);
        summaries.push(Summary {
            profile,
            records,
            workload,
            runs: original.len(),
            operations,
            original_total,
            current_total,
            original_ns_per_op: median_rows(&original, |row| row.ns_per_op),
            current_ns_per_op: median_rows(&current, |row| row.ns_per_op),
            original_ops_per_sec: median_rows(&original, |row| row.ops_per_sec),
            current_ops_per_sec: median_rows(&current, |row| row.ops_per_sec),
            original_rss,
            current_rss,
            original_fixture,
            current_fixture,
            original_nodes_read,
            current_nodes_read,
            original_nodes_written,
            current_nodes_written,
            original_bytes_read,
            current_bytes_read,
            original_bytes_written,
            current_bytes_written,
            original_tree_bytes: median_rows(&original, |row| row.tree_bytes),
            current_tree_bytes: median_rows(&current, |row| row.tree_bytes),
            original_node_payload: median_rows(&original, |row| row.node_payload_bytes),
            current_node_payload: median_rows(&current, |row| row.node_payload_bytes),
            original_num_nodes: median_rows(&original, |row| row.num_nodes),
            current_num_nodes: median_rows(&current, |row| row.num_nodes),
            original_height: median_rows(&original, |row| row.height),
            current_height: median_rows(&current, |row| row.height),
            original_entries: median_rows(&original, |row| row.result_entries),
            current_entries: median_rows(&current, |row| row.result_entries),
            latency_class,
            memory_regression: memory_regression(original_rss, current_rss),
            size_regression: size_regression(original_fixture, current_fixture),
            io_regression: io_regressed,
        });
    }
    Ok(summaries)
}

fn stats(rows: &[&RawRow], select: impl Fn(&RawRow) -> f64) -> Stats {
    let values = rows.iter().map(|row| select(row)).collect::<Vec<_>>();
    Stats {
        median: median(&values),
        min: values.iter().copied().fold(f64::INFINITY, f64::min),
        max: values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
    }
}

fn median_rows(rows: &[&RawRow], select: impl Fn(&RawRow) -> f64) -> f64 {
    median(&rows.iter().map(|row| select(row)).collect::<Vec<_>>())
}

fn median(values: &[f64]) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    if sorted.len() % 2 == 0 {
        (sorted[sorted.len() / 2 - 1] + sorted[sorted.len() / 2]) / 2.0
    } else {
        sorted[sorted.len() / 2]
    }
}

fn percent_delta(original: f64, current: f64) -> f64 {
    if original == 0.0 {
        if current == 0.0 {
            0.0
        } else {
            f64::INFINITY
        }
    } else {
        (current - original) / original * 100.0
    }
}

fn classify_latency(original: f64, current: f64, broad_overlap: bool) -> &'static str {
    let delta = percent_delta(original, current);
    if original < 1_000_000.0 || current < 1_000_000.0 || delta.abs() < 3.0 || broad_overlap {
        "noise-sensitive"
    } else if delta >= 3.0 {
        "material regression"
    } else {
        "material gain"
    }
}

fn memory_regression(original: f64, current: f64) -> bool {
    percent_delta(original, current) >= 5.0 && current - original >= 4.0 * MIB
}

fn size_regression(original: f64, current: f64) -> bool {
    percent_delta(original, current) >= 3.0 && current - original >= MIB
}

fn io_regression(original: f64, current: f64) -> bool {
    percent_delta(original, current) >= 3.0
}

fn range_overlap_fraction(left: Stats, right: Stats) -> f64 {
    let overlap = left.max.min(right.max) - left.min.max(right.min);
    let union = left.max.max(right.max) - left.min.min(right.min);
    if overlap <= 0.0 || union <= 0.0 {
        0.0
    } else {
        overlap / union
    }
}

fn render_csv(summaries: &[Summary]) -> String {
    let mut output = String::from("profile,records,workload,runs,operations,original_total_ns,current_total_ns,latency_delta_pct,latency_class,original_min_ns,original_max_ns,current_min_ns,current_max_ns,original_ns_per_op,current_ns_per_op,original_ops_per_sec,current_ops_per_sec,original_peak_rss,current_peak_rss,rss_delta_pct,memory_regression,original_fixture_bytes,current_fixture_bytes,fixture_delta_pct,size_regression,original_nodes_read,current_nodes_read,original_nodes_written,current_nodes_written,original_bytes_read,current_bytes_read,original_bytes_written,current_bytes_written,io_regression,original_tree_bytes,current_tree_bytes,original_node_payload,current_node_payload,original_num_nodes,current_num_nodes,original_height,current_height,original_entries,current_entries\n");
    for row in summaries {
        output.push_str(&format!(
            "{},{},{},{},{},{:.0},{:.0},{:.3},{},{:.0},{:.0},{:.0},{:.0},{:.3},{:.3},{:.3},{:.3},{:.0},{:.0},{:.3},{},{:.0},{:.0},{:.3},{},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0},{:.0}\n",
            row.profile, row.records, row.workload, row.runs, row.operations,
            row.original_total.median, row.current_total.median,
            percent_delta(row.original_total.median, row.current_total.median), row.latency_class,
            row.original_total.min, row.original_total.max, row.current_total.min, row.current_total.max,
            row.original_ns_per_op, row.current_ns_per_op, row.original_ops_per_sec, row.current_ops_per_sec,
            row.original_rss, row.current_rss, percent_delta(row.original_rss, row.current_rss), row.memory_regression,
            row.original_fixture, row.current_fixture, percent_delta(row.original_fixture, row.current_fixture), row.size_regression,
            row.original_nodes_read, row.current_nodes_read, row.original_nodes_written, row.current_nodes_written,
            row.original_bytes_read, row.current_bytes_read, row.original_bytes_written, row.current_bytes_written, row.io_regression,
            row.original_tree_bytes, row.current_tree_bytes, row.original_node_payload, row.current_node_payload,
            row.original_num_nodes, row.current_num_nodes, row.original_height, row.current_height,
            row.original_entries, row.current_entries
        ));
    }
    output
}

fn render_markdown(summaries: &[Summary], failures: &[String], machine: &str) -> String {
    let mut output = String::from("# SQLite-backed prolly tree performance evaluation\n\n");
    output.push_str("Lower latency, peak RSS, fixture size, and I/O are better. Deltas are `(current - original) / original`. Medians are shown with full measured ranges.\n\n");
    output.push_str("## Failures and invalid rows\n\n");
    if failures.is_empty() {
        output.push_str("None.\n\n");
    } else {
        for failure in failures {
            output.push_str(&format!("- {failure}\n"));
        }
        output.push('\n');
    }
    render_selection(
        &mut output,
        "Material latency regressions",
        summaries,
        |row| row.latency_class == "material regression",
    );
    render_selection(&mut output, "Memory regressions", summaries, |row| {
        row.memory_regression
    });
    render_selection(
        &mut output,
        "SQLite fixture-size regressions",
        summaries,
        |row| row.size_regression,
    );
    render_selection(&mut output, "Prolly I/O regressions", summaries, |row| {
        row.io_regression
    });
    render_selection(&mut output, "Material latency gains", summaries, |row| {
        row.latency_class == "material gain"
    });
    output.push_str("## Complete latency matrix\n\n");
    for profile in ["full", "normal"] {
        output.push_str(&format!("### WAL+{}\n\n", profile.to_uppercase()));
        output.push_str("| Records | Workload | Runs | Original median (range) | Current median (range) | Delta | Classification |\n|---:|---|---:|---:|---:|---:|---|\n");
        for row in summaries.iter().filter(|row| row.profile == profile) {
            output.push_str(&format!(
                "| {} | {} | {} | {} ({}) | {} ({}) | {:+.1}% | {} |\n",
                row.records,
                row.workload,
                row.runs,
                duration(row.original_total.median),
                range(row.original_total),
                duration(row.current_total.median),
                range(row.current_total),
                percent_delta(row.original_total.median, row.current_total.median),
                row.latency_class
            ));
        }
        output.push('\n');
    }
    output.push_str("## Structural, storage, memory, and I/O matrix\n\n");
    output.push_str("| Profile | Records | Workload | RSS O→C | Fixture O→C | Nodes read O→C | Nodes written O→C | Bytes read O→C | Bytes written O→C | Tree bytes O→C | Height O→C | Flags |\n|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|\n");
    for row in summaries {
        let mut flags = Vec::new();
        if row.memory_regression {
            flags.push("memory regression");
        }
        if row.size_regression {
            flags.push("size regression");
        }
        if row.io_regression {
            flags.push("I/O regression");
        }
        output.push_str(&format!("| {} | {} | {} | {}→{} | {}→{} | {:.0}→{:.0} | {:.0}→{:.0} | {:.0}→{:.0} | {:.0}→{:.0} | {:.0}→{:.0} | {:.0}→{:.0} | {} |\n",
            row.profile, row.records, row.workload, bytes(row.original_rss), bytes(row.current_rss),
            bytes(row.original_fixture), bytes(row.current_fixture), row.original_nodes_read, row.current_nodes_read,
            row.original_nodes_written, row.current_nodes_written, row.original_bytes_read, row.current_bytes_read,
            row.original_bytes_written, row.current_bytes_written, row.original_tree_bytes, row.current_tree_bytes,
            row.original_height, row.current_height, if flags.is_empty() { "—".to_string() } else { flags.join(", ") }));
    }
    output.push_str("\n## Methodology and limitations\n\n");
    output.push_str("The two revisions use byte-identical benchmark sources, deterministic keys and mutation sets, separate WAL+FULL and WAL+NORMAL profiles, alternating process order, and isolated SQLite fixture clones. Cold-manager means a fresh decoded-node cache; the operating-system page cache is not flushed. Diff and merge branch preparation is outside the timed interval, while process peak RSS includes that preparation. Validation and SQLite integrity checks are required before a row enters the aggregates.\n\n");
    output.push_str("Latency is material at ±3% only when both medians are at least 1 ms and measured ranges do not broadly overlap. Memory requires +5% and +4 MiB; fixture size requires +3% and +1 MiB; prolly I/O flags any +3% median increase.\n\n");
    output.push_str("## Machine and build metadata\n\n```text\n");
    output.push_str(machine.trim());
    output.push_str("\n```\n");
    output
}

fn render_selection(
    output: &mut String,
    title: &str,
    summaries: &[Summary],
    predicate: impl Fn(&Summary) -> bool,
) {
    output.push_str(&format!("## {title}\n\n"));
    let selected = summaries
        .iter()
        .filter(|row| predicate(row))
        .collect::<Vec<_>>();
    if selected.is_empty() {
        output.push_str("None.\n\n");
        return;
    }
    output.push_str("| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |\n|---|---:|---|---:|---:|---:|---:|---:|\n");
    for row in selected {
        output.push_str(&format!(
            "| {} | {} | {} | {} | {} | {:+.1}% | {:+.1}% | {:+.1}% |\n",
            row.profile,
            row.records,
            row.workload,
            duration(row.original_total.median),
            duration(row.current_total.median),
            percent_delta(row.original_total.median, row.current_total.median),
            percent_delta(row.original_rss, row.current_rss),
            percent_delta(row.original_fixture, row.current_fixture)
        ));
    }
    output.push('\n');
}

fn duration(nanoseconds: f64) -> String {
    if nanoseconds >= 1_000_000_000.0 {
        format!("{:.3}s", nanoseconds / 1_000_000_000.0)
    } else if nanoseconds >= 1_000_000.0 {
        format!("{:.3}ms", nanoseconds / 1_000_000.0)
    } else {
        format!("{:.3}µs", nanoseconds / 1_000.0)
    }
}

fn range(stats: Stats) -> String {
    format!("{}–{}", duration(stats.min), duration(stats.max))
}

fn bytes(value: f64) -> String {
    if value >= 1024.0 * MIB {
        format!("{:.2}GiB", value / (1024.0 * MIB))
    } else if value >= MIB {
        format!("{:.2}MiB", value / MIB)
    } else {
        format!("{:.1}KiB", value / 1024.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_handles_odd_and_even_samples() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), 2.0);
        assert_eq!(median(&[4.0, 1.0, 3.0, 2.0]), 2.5);
    }

    #[test]
    fn latency_classifier_applies_thresholds_and_duration_floor() {
        assert_eq!(
            classify_latency(1_000_000.0, 1_040_000.0, false),
            "material regression"
        );
        assert_eq!(
            classify_latency(1_100_000.0, 1_050_000.0, false),
            "material gain"
        );
        assert_eq!(
            classify_latency(900_000.0, 1_100_000.0, false),
            "noise-sensitive"
        );
        assert_eq!(
            classify_latency(2_000_000.0, 2_040_000.0, false),
            "noise-sensitive"
        );
        assert_eq!(
            classify_latency(2_000_000.0, 2_200_000.0, true),
            "noise-sensitive"
        );
    }

    #[test]
    fn resource_regressions_require_relative_and_absolute_thresholds() {
        assert!(memory_regression(100.0 * MIB, 110.0 * MIB));
        assert!(!memory_regression(10.0 * MIB, 11.0 * MIB));
        assert!(size_regression(20.0 * MIB, 22.0 * MIB));
        assert!(!size_regression(2.0 * MIB, 2.2 * MIB));
        assert!(io_regression(100.0, 104.0));
        assert!(!io_regression(100.0, 102.0));
    }

    #[test]
    fn percentage_delta_is_lower_is_better() {
        assert_eq!(percent_delta(100.0, 125.0), 25.0);
        assert_eq!(percent_delta(100.0, 75.0), -25.0);
    }
}
