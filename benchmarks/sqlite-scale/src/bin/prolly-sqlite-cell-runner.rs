use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;

use prolly_sqlite_scale_bench::fixture::FixtureLayout;
use prolly_sqlite_scale_bench::measurement::{FixtureRow, RawRow};
use prolly_sqlite_scale_bench::model::{CacheState, CellSpec, FixtureSpec, Operation, Pattern};
use prolly_sqlite_scale_bench::sqlite_runner::{build_fixture, run_cell};
use serde::Serialize;

const CONTRACT_VERSION: &str = "sqlite-scale-v2";

#[derive(Serialize)]
struct ProtocolRow {
    contract_version: &'static str,
    kind: &'static str,
    implementation: &'static str,
    revision: String,
    records: usize,
    repetition: usize,
    operation: String,
    pattern: String,
    cache_state: String,
    logical_operations: usize,
    observed_items: usize,
    total_ns: u128,
    ns_per_operation: f64,
    operations_per_second: f64,
    p50_ns: Option<u128>,
    p95_ns: Option<u128>,
    p99_ns: Option<u128>,
    max_ns: Option<u128>,
    chunk_reads: Option<u64>,
    chunk_writes: Option<u64>,
    bytes_read: Option<u64>,
    bytes_written: Option<u64>,
    result_entries: usize,
    db_bytes: u64,
    wal_bytes: u64,
    shm_bytes: u64,
    total_database_bytes: u64,
    expected_entries: usize,
    observed_entries: usize,
    query_strategy: Option<&'static str>,
    validated: bool,
    error: String,
}

enum Command {
    Fixture {
        output: PathBuf,
        records: usize,
        repetition: usize,
        revision: String,
    },
    Cell {
        output: PathBuf,
        spec: CellSpec,
    },
}

fn main() {
    let command = match parse(std::env::args().skip(1).collect()) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2);
        }
    };
    let result = execute(command);
    match result {
        Ok(row) => emit(&row),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(1);
        }
    }
}

fn execute(command: Command) -> Result<ProtocolRow, String> {
    match command {
        Command::Fixture {
            output,
            records,
            repetition,
            revision,
        } => {
            let layout = FixtureLayout::new(output, records, repetition);
            let fixture = build_fixture(
                &FixtureSpec {
                    records,
                    repetition,
                    revision,
                    dirty: false,
                },
                &layout,
            )?;
            Ok(fixture_row(fixture))
        }
        Command::Cell { output, spec } => {
            let layout = FixtureLayout::new(output, spec.records, spec.repetition);
            layout.clone_for(&spec)?;
            let result = run_cell(&spec, &layout);
            match result {
                Ok(row) => {
                    let protocol = cell_row(row);
                    layout.remove_cell(&spec)?;
                    Ok(protocol)
                }
                Err(error) => Err(error),
            }
        }
    }
}

fn fixture_row(row: FixtureRow) -> ProtocolRow {
    ProtocolRow {
        contract_version: CONTRACT_VERSION,
        kind: "fixture",
        implementation: "rust",
        revision: row.revision,
        records: row.records,
        repetition: row.repetition,
        operation: "build".to_string(),
        pattern: "n/a".to_string(),
        cache_state: "n/a".to_string(),
        logical_operations: row.records,
        observed_items: row.observed_records,
        total_ns: row.build_ns,
        ns_per_operation: row.build_ns as f64 / row.records.max(1) as f64,
        operations_per_second: row.records_per_sec,
        p50_ns: None,
        p95_ns: None,
        p99_ns: None,
        max_ns: None,
        chunk_reads: None,
        chunk_writes: None,
        bytes_read: None,
        bytes_written: None,
        result_entries: row.observed_records,
        db_bytes: row.database_bytes,
        wal_bytes: 0,
        shm_bytes: 0,
        total_database_bytes: row.database_bytes,
        expected_entries: row.records,
        observed_entries: row.observed_records,
        query_strategy: None,
        validated: row.validated,
        error: row.error,
    }
}

fn cell_row(row: RawRow) -> ProtocolRow {
    let query_strategy = (row.operation == Operation::Query).then_some("native_get_many");
    ProtocolRow {
        contract_version: CONTRACT_VERSION,
        kind: "cell",
        implementation: "rust",
        revision: row.revision,
        records: row.records,
        repetition: row.repetition,
        operation: row.operation.as_str().to_string(),
        pattern: row.pattern.as_str().to_string(),
        cache_state: row.cache_state.as_str().to_string(),
        logical_operations: row.logical_operations,
        observed_items: row.observed_items,
        total_ns: row.total_ns,
        ns_per_operation: row.ns_per_operation,
        operations_per_second: row.operations_per_sec,
        p50_ns: row.p50_ns,
        p95_ns: row.p95_ns,
        p99_ns: row.p99_ns,
        max_ns: row.max_ns,
        chunk_reads: Some(row.nodes_read),
        chunk_writes: Some(row.nodes_written),
        bytes_read: Some(row.bytes_read),
        bytes_written: Some(row.bytes_written),
        result_entries: row.result_entries,
        db_bytes: row.db_bytes,
        wal_bytes: row.wal_bytes,
        shm_bytes: row.shm_bytes,
        total_database_bytes: row.total_database_bytes,
        expected_entries: row.expected_entries,
        observed_entries: row.observed_entries,
        query_strategy,
        validated: row.validated,
        error: row.error,
    }
}

fn emit(row: &ProtocolRow) {
    let stdout = std::io::stdout();
    let mut locked = stdout.lock();
    serde_json::to_writer(&mut locked, row).expect("serialize protocol row");
    writeln!(locked).expect("finish protocol row");
}

fn parse(arguments: Vec<String>) -> Result<Command, String> {
    let kind = arguments
        .first()
        .ok_or_else(|| usage().to_string())?
        .clone();
    if kind != "fixture" && kind != "cell" {
        return Err(usage().to_string());
    }
    let mut output = None;
    let mut records = None;
    let mut repetition = None;
    let mut revision = None;
    let mut operation = None;
    let mut pattern = None;
    let mut changes = None;
    let mut read_samples = None;
    let mut index = 1;
    while index < arguments.len() {
        let flag = arguments[index].as_str();
        index += 1;
        let value = arguments
            .get(index)
            .ok_or_else(|| format!("{flag} requires a value"))?
            .clone();
        match flag {
            "--output" => output = Some(PathBuf::from(value)),
            "--records" => records = Some(number(&value, flag)?),
            "--repetition" => repetition = Some(number(&value, flag)?),
            "--revision" => revision = Some(value),
            "--operation" => operation = Some(Operation::from_str(&value)?),
            "--pattern" => pattern = Some(Pattern::from_str(&value)?),
            "--changes" => changes = Some(number(&value, flag)?),
            "--read-samples" => read_samples = Some(number(&value, flag)?),
            other => return Err(format!("unknown option {other}")),
        }
        index += 1;
    }
    let output = output.ok_or_else(|| "--output is required".to_string())?;
    let records = records
        .filter(|value| *value > 0)
        .ok_or_else(|| "--records must be positive".to_string())?;
    let repetition = repetition
        .filter(|value| *value > 0)
        .ok_or_else(|| "--repetition must be positive".to_string())?;
    let revision = revision
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "--revision is required".to_string())?;
    if kind == "fixture" {
        return Ok(Command::Fixture {
            output,
            records,
            repetition,
            revision,
        });
    }
    let operation = operation.ok_or_else(|| "--operation is required".to_string())?;
    let pattern = pattern.ok_or_else(|| "--pattern is required".to_string())?;
    let changes = changes
        .filter(|value| *value > 0 && *value <= records)
        .ok_or_else(|| "--changes must be positive and not exceed records".to_string())?;
    let read_samples = read_samples
        .filter(|value| *value > 0 && *value <= records)
        .ok_or_else(|| "--read-samples must be positive and not exceed records".to_string())?;
    if operation == Operation::Merge && changes % 2 != 0 {
        return Err("merge changes must be even".to_string());
    }
    let cache_state = match operation {
        Operation::GetCold => CacheState::ColdManager,
        Operation::GetWarm => CacheState::WarmManager,
        _ => CacheState::NotApplicable,
    };
    Ok(Command::Cell {
        output,
        spec: CellSpec {
            records,
            repetition,
            operation,
            pattern,
            cache_state,
            changes,
            read_samples,
            revision,
            dirty: false,
        },
    })
}

fn number<T: FromStr>(value: &str, flag: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {flag}: {error}"))
}

fn usage() -> &'static str {
    "usage: prolly-sqlite-cell-runner fixture|cell --output PATH --records N --repetition N --revision REV [--operation OP --pattern PATTERN --changes N --read-samples N]"
}
