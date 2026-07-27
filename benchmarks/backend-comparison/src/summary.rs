use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use serde::Serialize;

use crate::statistics::{
    classify_winner, coefficient_of_variation, median, median_absolute_deviation,
    paired_bootstrap_ratio_ci, Winner,
};
use crate::{Backend, EvidenceRow, Operation, RESULT_SCHEMA, TIMED_SCOPE_VERSION};

pub const MANIFEST_SCHEMA: &str = "backend-comparison-manifest-v1";
const BOOTSTRAP_RESAMPLES: usize = 10_000;
const BOOTSTRAP_SEED: u64 = 0x243f_6a88_85a3_08d3;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub schema: String,
    pub status: String,
    pub resumed: bool,
    pub dirty: bool,
    pub run_id: String,
    pub revision: String,
    pub tree_hash: String,
    pub contract_version: String,
    pub timed_scope_version: String,
    pub result_schema: String,
    pub repetitions: usize,
    pub lockfile_sha256: String,
    pub config_sha256: String,
    pub commands_sha256: String,
    pub postgres_binary_sha256: String,
    pub dynamodb_binary_sha256: String,
    pub summarizer_binary_sha256: String,
    pub postgres_image: String,
    pub postgres_image_id: String,
    pub dynamodb_image: String,
    pub dynamodb_image_id: String,
}

impl Manifest {
    pub fn from_path(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        let mut values = BTreeMap::new();
        for (line_number, line) in content.lines().enumerate() {
            let (key, value) = line.split_once('=').ok_or_else(|| {
                format!(
                    "{}:{} is not a key=value manifest line",
                    path.display(),
                    line_number + 1
                )
            })?;
            if values.insert(key.to_string(), value.to_string()).is_some() {
                return Err(format!("manifest key is duplicated: {key}"));
            }
        }
        let manifest = Self {
            schema: take(&mut values, "schema")?,
            status: take(&mut values, "status")?,
            resumed: boolean(&take(&mut values, "resumed")?, "resumed")?,
            dirty: boolean(&take(&mut values, "dirty")?, "dirty")?,
            run_id: take(&mut values, "run_id")?,
            revision: take(&mut values, "revision")?,
            tree_hash: take(&mut values, "tree_hash")?,
            contract_version: take(&mut values, "contract_version")?,
            timed_scope_version: take(&mut values, "timed_scope_version")?,
            result_schema: take(&mut values, "result_schema")?,
            repetitions: number(&take(&mut values, "repetitions")?, "repetitions")?,
            lockfile_sha256: take(&mut values, "lockfile_sha256")?,
            config_sha256: take(&mut values, "config_sha256")?,
            commands_sha256: take(&mut values, "commands_sha256")?,
            postgres_binary_sha256: take(&mut values, "postgres_binary_sha256")?,
            dynamodb_binary_sha256: take(&mut values, "dynamodb_binary_sha256")?,
            summarizer_binary_sha256: take(&mut values, "summarizer_binary_sha256")?,
            postgres_image: take(&mut values, "postgres_image")?,
            postgres_image_id: take(&mut values, "postgres_image_id")?,
            dynamodb_image: take(&mut values, "dynamodb_image")?,
            dynamodb_image_id: take(&mut values, "dynamodb_image_id")?,
        };
        if !values.is_empty() {
            return Err(format!(
                "unsupported manifest keys: {}",
                values.keys().cloned().collect::<Vec<_>>().join(", ")
            ));
        }
        manifest.validate()?;
        Ok(manifest)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.schema != MANIFEST_SCHEMA {
            return Err(format!("unsupported manifest schema: {}", self.schema));
        }
        if self.status != "complete" || self.resumed || self.dirty {
            return Err("publishable manifest must be complete, fresh, and clean".to_string());
        }
        if self.repetitions < 7 {
            return Err("publishable comparison requires at least seven repetitions".to_string());
        }
        if self.run_id.is_empty()
            || self.contract_version.is_empty()
            || self.postgres_image.is_empty()
            || self.postgres_image_id.is_empty()
            || self.dynamodb_image.is_empty()
            || self.dynamodb_image_id.is_empty()
        {
            return Err("manifest provenance values cannot be empty".to_string());
        }
        if self.result_schema != RESULT_SCHEMA || self.timed_scope_version != TIMED_SCOPE_VERSION {
            return Err("manifest result or timing contract is unsupported".to_string());
        }
        for (name, value, length) in [
            ("revision", self.revision.as_str(), 40),
            ("tree_hash", self.tree_hash.as_str(), 40),
            ("lockfile_sha256", self.lockfile_sha256.as_str(), 64),
            ("config_sha256", self.config_sha256.as_str(), 64),
            ("commands_sha256", self.commands_sha256.as_str(), 64),
            (
                "postgres_binary_sha256",
                self.postgres_binary_sha256.as_str(),
                64,
            ),
            (
                "dynamodb_binary_sha256",
                self.dynamodb_binary_sha256.as_str(),
                64,
            ),
            (
                "summarizer_binary_sha256",
                self.summarizer_binary_sha256.as_str(),
                64,
            ),
        ] {
            if value.len() != length || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(format!(
                    "{name} is not a {length}-character hexadecimal value"
                ));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    fn example() -> Self {
        Self {
            schema: MANIFEST_SCHEMA.to_string(),
            status: "complete".to_string(),
            resumed: false,
            dirty: false,
            run_id: "run-1".to_string(),
            revision: "a".repeat(40),
            tree_hash: "b".repeat(40),
            contract_version: "backend-workload-v1".to_string(),
            timed_scope_version: TIMED_SCOPE_VERSION.to_string(),
            result_schema: RESULT_SCHEMA.to_string(),
            repetitions: 7,
            lockfile_sha256: "c".repeat(64),
            config_sha256: "d".repeat(64),
            commands_sha256: "e".repeat(64),
            postgres_binary_sha256: "1".repeat(64),
            dynamodb_binary_sha256: "2".repeat(64),
            summarizer_binary_sha256: "3".repeat(64),
            postgres_image: "postgres@sha256:test".to_string(),
            postgres_image_id: "sha256:postgres".to_string(),
            dynamodb_image: "dynamodb@sha256:test".to_string(),
            dynamodb_image_id: "sha256:dynamodb".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SummaryRow {
    pub operation: Operation,
    pub logical_operations: u64,
    pub repetitions: usize,
    pub postgres_median_ms: f64,
    pub postgres_ops_per_sec: f64,
    pub postgres_min_ms: f64,
    pub postgres_max_ms: f64,
    pub postgres_mad_ms: f64,
    pub postgres_cv: f64,
    pub dynamodb_median_ms: f64,
    pub dynamodb_ops_per_sec: f64,
    pub dynamodb_min_ms: f64,
    pub dynamodb_max_ms: f64,
    pub dynamodb_mad_ms: f64,
    pub dynamodb_cv: f64,
    pub dynamodb_to_postgres_latency: f64,
    pub ratio_ci_low: f64,
    pub ratio_ci_high: f64,
    pub winner: Winner,
}

pub fn summarize_run(input: &Path, manifest_path: &Path, output: &Path) -> Result<(), String> {
    let manifest = Manifest::from_path(manifest_path)?;
    let rows = read_rows(input)?;
    let summaries = summarize_rows(&rows, &manifest)?;
    let csv = render_csv(&summaries)?;
    let report = render_report(&summaries, &manifest, &rows);
    std::fs::create_dir_all(output)
        .map_err(|error| format!("failed to create {}: {error}", output.display()))?;
    write_outputs_new(output, &csv, &report)
}

pub fn summarize_rows(
    rows: &[EvidenceRow],
    manifest: &Manifest,
) -> Result<Vec<SummaryRow>, String> {
    manifest.validate()?;
    let expected_rows = manifest.repetitions * Operation::ALL.len() * 2;
    if rows.len() != expected_rows {
        return Err(format!(
            "comparison matrix has {} rows, expected {expected_rows}",
            rows.len()
        ));
    }
    let mut indexed = BTreeMap::new();
    let mut binaries = BTreeMap::<Backend, BTreeSet<&str>>::new();
    for row in rows {
        row.validate()?;
        if row.run_id != manifest.run_id
            || row.revision != manifest.revision
            || row.tree_hash != manifest.tree_hash
            || row.contract_version != manifest.contract_version
            || row.timed_scope_version != manifest.timed_scope_version
            || row.schema != manifest.result_schema
        {
            return Err(format!(
                "row provenance differs for {} repetition {}",
                row.operation, row.repetition
            ));
        }
        let expected_binary = match row.backend {
            Backend::Postgres => &manifest.postgres_binary_sha256,
            Backend::DynamoDbLocal => &manifest.dynamodb_binary_sha256,
        };
        if &row.binary_sha256 != expected_binary {
            return Err(format!("binary provenance differs for {}", row.backend));
        }
        binaries
            .entry(row.backend)
            .or_default()
            .insert(&row.binary_sha256);
        let key = (row.backend, row.operation, row.repetition);
        if indexed.insert(key, row).is_some() {
            return Err(format!(
                "duplicate row for {} {} repetition {}",
                row.backend, row.operation, row.repetition
            ));
        }
    }
    if binaries.values().any(|values| values.len() != 1) {
        return Err("mixed binary provenance".to_string());
    }

    let mut summaries = Vec::with_capacity(Operation::ALL.len());
    for operation in Operation::ALL {
        let mut postgres = Vec::with_capacity(manifest.repetitions);
        let mut dynamodb = Vec::with_capacity(manifest.repetitions);
        let mut logical_operations = None;
        let mut identity = None;
        for repetition in 1..=manifest.repetitions as u32 {
            let pg = indexed
                .get(&(Backend::Postgres, operation, repetition))
                .ok_or_else(|| format!("missing PostgreSQL {operation} repetition {repetition}"))?;
            let ddb = indexed
                .get(&(Backend::DynamoDbLocal, operation, repetition))
                .ok_or_else(|| {
                    format!("missing DynamoDB Local {operation} repetition {repetition}")
                })?;
            require_equivalent(pg, ddb)?;
            if let Some(reference) = identity {
                require_same_workload(reference, pg)?;
            } else {
                identity = Some(*pg);
            }
            match logical_operations {
                Some(expected) if expected != pg.logical_operations => {
                    return Err(format!("{operation} logical operation count changed"))
                }
                None => logical_operations = Some(pg.logical_operations),
                _ => {}
            }
            postgres.push(pg.total_ns as f64);
            dynamodb.push(ddb.total_ns as f64);
        }
        let pg_median = median(&postgres);
        let ddb_median = median(&dynamodb);
        let ratio = ddb_median / pg_median;
        let interval = paired_bootstrap_ratio_ci(
            &postgres,
            &dynamodb,
            BOOTSTRAP_RESAMPLES,
            BOOTSTRAP_SEED ^ operation as u64,
        )?;
        let logical_operations = logical_operations.expect("each operation has rows");
        summaries.push(SummaryRow {
            operation,
            logical_operations,
            repetitions: manifest.repetitions,
            postgres_median_ms: pg_median / 1_000_000.0,
            postgres_ops_per_sec: logical_operations as f64 * 1_000_000_000.0 / pg_median,
            postgres_min_ms: min(&postgres) / 1_000_000.0,
            postgres_max_ms: max(&postgres) / 1_000_000.0,
            postgres_mad_ms: median_absolute_deviation(&postgres) / 1_000_000.0,
            postgres_cv: coefficient_of_variation(&postgres),
            dynamodb_median_ms: ddb_median / 1_000_000.0,
            dynamodb_ops_per_sec: logical_operations as f64 * 1_000_000_000.0 / ddb_median,
            dynamodb_min_ms: min(&dynamodb) / 1_000_000.0,
            dynamodb_max_ms: max(&dynamodb) / 1_000_000.0,
            dynamodb_mad_ms: median_absolute_deviation(&dynamodb) / 1_000_000.0,
            dynamodb_cv: coefficient_of_variation(&dynamodb),
            dynamodb_to_postgres_latency: ratio,
            ratio_ci_low: interval.low,
            ratio_ci_high: interval.high,
            winner: classify_winner(ratio, interval),
        });
    }
    Ok(summaries)
}

fn require_same_workload(reference: &EvidenceRow, row: &EvidenceRow) -> Result<(), String> {
    if reference.records != row.records
        || reference.value_bytes != row.value_bytes
        || reference.changes != row.changes
        || reference.samples != row.samples
        || reference.concurrency != row.concurrency
        || reference.seed != row.seed
        || reference.logical_operations != row.logical_operations
        || reference.observed_items != row.observed_items
        || reference.workload_digest != row.workload_digest
        || reference.outcome_digest != row.outcome_digest
        || reference.root != row.root
    {
        return Err(format!(
            "{} workload or outcome changed between repetitions",
            row.operation
        ));
    }
    Ok(())
}

fn require_equivalent(postgres: &EvidenceRow, dynamodb: &EvidenceRow) -> Result<(), String> {
    if postgres.records != dynamodb.records
        || postgres.value_bytes != dynamodb.value_bytes
        || postgres.changes != dynamodb.changes
        || postgres.samples != dynamodb.samples
        || postgres.concurrency != dynamodb.concurrency
        || postgres.seed != dynamodb.seed
        || postgres.logical_operations != dynamodb.logical_operations
        || postgres.observed_items != dynamodb.observed_items
        || postgres.workload_digest != dynamodb.workload_digest
        || postgres.outcome_digest != dynamodb.outcome_digest
        || postgres.root != dynamodb.root
    {
        return Err(format!(
            "{} repetition {} differs between backends",
            postgres.operation, postgres.repetition
        ));
    }
    Ok(())
}

fn read_rows(path: &Path) -> Result<Vec<EvidenceRow>, String> {
    let mut reader = csv::Reader::from_path(path)
        .map_err(|error| format!("failed to open {}: {error}", path.display()))?;
    reader
        .deserialize()
        .map(|row| row.map_err(|error| format!("failed to parse evidence row: {error}")))
        .collect()
}

fn render_csv(summaries: &[SummaryRow]) -> Result<Vec<u8>, String> {
    let mut writer = csv::WriterBuilder::new()
        .terminator(csv::Terminator::Any(b'\n'))
        .from_writer(Vec::new());
    for summary in summaries {
        writer
            .serialize(summary)
            .map_err(|error| format!("failed to serialize comparison: {error}"))?;
    }
    writer
        .into_inner()
        .map_err(|error| format!("failed to finish comparison CSV: {error}"))
}

fn render_report(summaries: &[SummaryRow], manifest: &Manifest, rows: &[EvidenceRow]) -> String {
    let first = &rows[0];
    let mut lines = vec![
        "# PostgreSQL vs DynamoDB Local".to_string(),
        String::new(),
        format!(
            "This local comparison uses {} records, {}-byte values, concurrency {}, and {} measured repetitions. Latency is lower; throughput is higher.",
            first.records, first.value_bytes, first.concurrency, manifest.repetitions
        ),
        String::new(),
        "| Operation | Logical ops | PostgreSQL median | PostgreSQL ops/s | DynamoDB Local median | DynamoDB Local ops/s | DDB/PG 95% CI | Result |".to_string(),
        "|---|---:|---:|---:|---:|---:|---:|---|".to_string(),
    ];
    for row in summaries {
        lines.push(format!(
            "| {} | {} | {:.2} ms | {:.2} | {:.2} ms | {:.2} | {:.3}–{:.3} | {} |",
            row.operation,
            row.logical_operations,
            row.postgres_median_ms,
            row.postgres_ops_per_sec,
            row.dynamodb_median_ms,
            row.dynamodb_ops_per_sec,
            row.ratio_ci_low,
            row.ratio_ci_high,
            row.winner
        ));
    }
    lines.extend([
        String::new(),
        "## Dispersion".to_string(),
        String::new(),
        "| Operation | PostgreSQL range | PostgreSQL MAD | PostgreSQL CV | DynamoDB Local range | DynamoDB Local MAD | DynamoDB Local CV |".to_string(),
        "|---|---:|---:|---:|---:|---:|---:|".to_string(),
    ]);
    for row in summaries {
        lines.push(format!(
            "| {} | {:.2}–{:.2} ms | {:.2} ms | {:.2}% | {:.2}–{:.2} ms | {:.2} ms | {:.2}% |",
            row.operation,
            row.postgres_min_ms,
            row.postgres_max_ms,
            row.postgres_mad_ms,
            row.postgres_cv * 100.0,
            row.dynamodb_min_ms,
            row.dynamodb_max_ms,
            row.dynamodb_mad_ms,
            row.dynamodb_cv * 100.0
        ));
    }
    lines.extend([
        String::new(),
        "## Interpretation".to_string(),
        String::new(),
        "- Each row uses byte-identical workloads and complete post-timing validation.".to_string(),
        "- A winner requires a paired bootstrap 95% confidence interval that excludes parity and a median effect above 5%.".to_string(),
        "- DynamoDB Local does not model Amazon DynamoDB network latency, throttling, partitions, capacity, or cost.".to_string(),
        "- These measurements compare local adapter implementations, not production services.".to_string(),
        String::new(),
        format!("Run ID: `{}`.", manifest.run_id),
    ]);
    lines.join("\n") + "\n"
}

fn write_outputs_new(output: &Path, csv: &[u8], report: &str) -> Result<(), String> {
    let csv_path = output.join("comparison.csv");
    let report_path = output.join("report.md");
    if csv_path.exists() || report_path.exists() {
        return Err("refusing to overwrite existing summary output".to_string());
    }
    let csv_temp = output.join(".comparison.csv.tmp");
    let report_temp = output.join(".report.md.tmp");
    write_new(&csv_temp, csv)?;
    if let Err(error) = write_new(&report_temp, report.as_bytes()) {
        let _ = std::fs::remove_file(&csv_temp);
        return Err(error);
    }
    std::fs::rename(&csv_temp, &csv_path)
        .map_err(|error| format!("failed to publish {}: {error}", csv_path.display()))?;
    std::fs::rename(&report_temp, &report_path)
        .map_err(|error| format!("failed to publish {}: {error}", report_path.display()))
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| format!("refusing to overwrite {}: {error}", path.display()))?;
    file.write_all(bytes)
        .map_err(|error| format!("failed to write {}: {error}", path.display()))?;
    file.sync_all()
        .map_err(|error| format!("failed to sync {}: {error}", path.display()))
}

fn min(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::INFINITY, f64::min)
}

fn max(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::NEG_INFINITY, f64::max)
}

fn take(values: &mut BTreeMap<String, String>, key: &str) -> Result<String, String> {
    values
        .remove(key)
        .ok_or_else(|| format!("manifest does not declare {key}"))
}

fn boolean(value: &str, key: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("manifest {key} must be true or false")),
    }
}

fn number<T: std::str::FromStr>(value: &str, key: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("manifest {key} is invalid: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_matrix_fails_closed() {
        let manifest = Manifest::example();
        assert!(summarize_rows(&[], &manifest)
            .unwrap_err()
            .contains("matrix"));
    }

    #[test]
    fn complete_equivalent_matrix_reports_postgres_winners() {
        let manifest = Manifest::example();
        let rows = fixture_rows(&manifest);
        let summaries = summarize_rows(&rows, &manifest).unwrap();
        assert_eq!(summaries.len(), Operation::ALL.len());
        assert!(summaries
            .iter()
            .all(|summary| summary.winner == Winner::Postgres));
        let report = render_report(&summaries, &manifest, &rows);
        assert!(report.contains("PostgreSQL ops/s"));
        assert!(report.contains("PostgreSQL median"));
    }

    #[test]
    fn outcome_mismatch_fails_closed() {
        let manifest = Manifest::example();
        let mut rows = fixture_rows(&manifest);
        rows.last_mut().unwrap().outcome_digest = "9".repeat(64);
        assert!(summarize_rows(&rows, &manifest)
            .unwrap_err()
            .contains("differs between backends"));
    }

    #[test]
    fn repetition_workload_drift_fails_closed() {
        let manifest = Manifest::example();
        let mut rows = fixture_rows(&manifest);
        for row in rows
            .iter_mut()
            .filter(|row| row.operation == Operation::Build && row.repetition == 7)
        {
            row.workload_digest = "8".repeat(64);
        }
        assert!(summarize_rows(&rows, &manifest)
            .unwrap_err()
            .contains("changed between repetitions"));
    }

    fn fixture_rows(manifest: &Manifest) -> Vec<EvidenceRow> {
        let mut rows = Vec::new();
        for operation in Operation::ALL {
            let operation_digit = char::from_digit(operation as u32 + 3, 16).unwrap();
            for repetition in 1..=manifest.repetitions as u32 {
                for backend in [Backend::Postgres, Backend::DynamoDbLocal] {
                    let mut row = EvidenceRow::example();
                    row.run_id.clone_from(&manifest.run_id);
                    row.revision.clone_from(&manifest.revision);
                    row.tree_hash.clone_from(&manifest.tree_hash);
                    row.contract_version.clone_from(&manifest.contract_version);
                    row.timed_scope_version
                        .clone_from(&manifest.timed_scope_version);
                    row.schema.clone_from(&manifest.result_schema);
                    row.backend = backend;
                    row.binary_sha256 = match backend {
                        Backend::Postgres => manifest.postgres_binary_sha256.clone(),
                        Backend::DynamoDbLocal => manifest.dynamodb_binary_sha256.clone(),
                    };
                    row.operation = operation;
                    row.repetition = repetition;
                    row.total_ns = match backend {
                        Backend::Postgres => 1_000 + repetition as u128,
                        Backend::DynamoDbLocal => 1_200 + repetition as u128,
                    };
                    row.ops_per_sec =
                        row.logical_operations as f64 * 1_000_000_000.0 / row.total_ns as f64;
                    row.root = operation_digit.to_string().repeat(64);
                    row.outcome_digest = operation_digit.to_string().repeat(64);
                    rows.push(row);
                }
            }
        }
        rows
    }
}
