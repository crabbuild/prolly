use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use futures_util::stream::{self, StreamExt, TryStreamExt};
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore, RemoteStoreBackend};
use prolly_store_dynamodb::{DynamoDbBackend, DynamoDbStore};
use serde::{Deserialize, Serialize};

const SCHEMA: &str = "dynamodb-local-scale-v2";

type Manager = AsyncProlly<DynamoDbStore>;

#[derive(Clone, Debug)]
struct Args {
    endpoint: String,
    table: String,
    output: PathBuf,
    records: usize,
    value_bytes: usize,
    raw_items: usize,
    samples: usize,
    changes: usize,
    roots: usize,
    conflicts: usize,
    concurrency: usize,
    concurrent_operations: usize,
    read_parallelism: usize,
    batch_get_parallelism: usize,
    batch_write_parallelism: usize,
    scan_parallelism: usize,
    cleanup_namespace: bool,
    runs: u32,
    revision: String,
    dirty: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Row {
    schema: String,
    revision: String,
    dirty: bool,
    records: u64,
    value_bytes: u64,
    concurrency: u64,
    read_parallelism: u64,
    batch_get_parallelism: u64,
    batch_write_parallelism: u64,
    scan_parallelism: u64,
    repetition: u32,
    operation: String,
    logical_operations: u64,
    observed_items: u64,
    total_ns: u128,
    ns_per_op: f64,
    ops_per_sec: f64,
    validated: bool,
    error: String,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(parse_args().unwrap_or_else(|error| {
        eprintln!("{error}");
        std::process::exit(2);
    }))
    .await
    {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), String> {
    validate_args(&args)?;
    std::fs::create_dir_all(&args.output).map_err(error)?;
    let raw_path = args.output.join("raw-results.csv");
    let existing = read_rows(&raw_path)?;
    let mut completed = existing
        .iter()
        .map(|row| (row.repetition, row.operation.clone()))
        .collect::<BTreeSet<_>>();
    for row in &existing {
        validate_row(row)?;
        if row.revision != args.revision
            || row.dirty != args.dirty
            || row.records != args.records as u64
            || row.value_bytes != args.value_bytes as u64
            || row.concurrency != args.concurrency as u64
            || row.read_parallelism != args.read_parallelism as u64
            || row.batch_get_parallelism != args.batch_get_parallelism as u64
            || row.batch_write_parallelism != args.batch_write_parallelism as u64
            || row.scan_parallelism != args.scan_parallelism as u64
        {
            return Err(format!(
                "existing row provenance differs for {} repetition {}",
                row.operation, row.repetition
            ));
        }
    }
    let has_rows = raw_path.metadata().is_ok_and(|metadata| metadata.len() > 0);
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&raw_path)
        .map_err(error)?;
    let mut writer = csv::WriterBuilder::new()
        .has_headers(!has_rows)
        .from_writer(file);

    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-west-2"))
        .endpoint_url(&args.endpoint)
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .build();
    let prefix = format!(
        "prolly:bench:{}:",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    )
    .into_bytes();
    let backend = DynamoDbBackend::new(aws_sdk_dynamodb::Client::from_conf(config), &args.table)
        .with_key_prefix(prefix)
        .with_read_parallelism(args.read_parallelism)
        .with_batch_get_parallelism(args.batch_get_parallelism)
        .with_batch_write_parallelism(args.batch_write_parallelism)
        .with_scan_parallelism(args.scan_parallelism);
    backend.initialize_schema().await.map_err(error)?;

    let raw_keys = (0..args.raw_items)
        .map(|index| format!("raw-{index:020}").into_bytes())
        .collect::<Vec<Vec<u8>>>();
    let raw_values = (0..args.raw_items)
        .map(|index| value(index, 0, args.value_bytes))
        .collect::<Vec<_>>();
    for repetition in 1..=args.runs {
        measure(
            &args,
            &backend,
            &mut writer,
            &mut completed,
            repetition,
            "raw_batch_put",
            args.raw_items,
            async {
                let entries = raw_keys
                    .iter()
                    .zip(&raw_values)
                    .map(|(key, value)| (key.as_slice(), value.as_slice()))
                    .collect::<Vec<_>>();
                backend.batch_put_nodes(&entries).await.map_err(error)?;
                Ok(args.raw_items)
            },
        )
        .await?;
        measure(
            &args,
            &backend,
            &mut writer,
            &mut completed,
            repetition,
            "raw_batch_get",
            args.raw_items,
            async {
                let keys = raw_keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
                let values = backend
                    .batch_get_nodes_ordered(&keys)
                    .await
                    .map_err(error)?;
                if values.iter().all(Option::is_some) {
                    Ok(values.len())
                } else {
                    Err("raw batch read returned a missing value".to_string())
                }
            },
        )
        .await?;
    }

    for root in 0..args.roots {
        backend
            .put_root_manifest(
                format!("root-{root:020}").as_bytes(),
                format!("manifest-{root:020}").as_bytes(),
            )
            .await
            .map_err(error)?;
    }
    measure(
        &args,
        &backend,
        &mut writer,
        &mut completed,
        1,
        "list_roots",
        args.roots,
        async {
            let roots = backend.list_root_manifests().await.map_err(error)?;
            Ok(roots.len())
        },
    )
    .await?;

    backend
        .put_root_manifest(b"conflict-root", b"current")
        .await
        .map_err(error)?;
    for repetition in 1..=args.runs {
        measure(
            &args,
            &backend,
            &mut writer,
            &mut completed,
            repetition,
            "cas_conflict",
            args.conflicts,
            async {
                let mut conflicts = 0;
                for _ in 0..args.conflicts {
                    if matches!(
                        backend
                            .compare_and_swap_root_manifest(
                                b"conflict-root",
                                Some(b"stale"),
                                Some(b"new"),
                            )
                            .await
                            .map_err(error)?,
                        prolly::RemoteManifestUpdate::Conflict { .. }
                    ) {
                        conflicts += 1;
                    }
                }
                Ok(conflicts)
            },
        )
        .await?;
    }

    let fixture_manager = manager(&backend);
    let base_mutations = (0..args.records)
        .map(|index| Mutation::Upsert {
            key: key(index),
            val: value(index, 0, args.value_bytes),
        })
        .collect::<Vec<_>>();
    let build_started = Instant::now();
    let base = fixture_manager
        .batch(&fixture_manager.create(), base_mutations)
        .await
        .map_err(error)?;
    if !completed.contains(&(1, "build".to_string())) {
        append_row(
            &args,
            &mut writer,
            &mut completed,
            make_row(
                &args,
                1,
                "build",
                args.records,
                args.records,
                build_started.elapsed().as_nanos(),
            ),
        )?;
    }
    let stats = fixture_manager.collect_stats(&base).await.map_err(error)?;
    if stats.total_key_value_pairs != args.records {
        return Err("base tree count mismatch".to_string());
    }
    let sample_ids = deterministic_ids(args.records, args.samples, 17);
    let sample_keys = sample_ids
        .iter()
        .map(|index| key(*index))
        .collect::<Vec<_>>();
    let change_ids = deterministic_ids(args.records, args.changes, 29);
    let changed = fixture_manager
        .batch(&base, mutations(&change_ids, 1, args.value_bytes))
        .await
        .map_err(error)?;
    let merge_ids = deterministic_ids(args.records, args.changes.saturating_mul(2), 53);
    let (left_ids, right_ids) = merge_ids.split_at(args.changes.min(merge_ids.len()));
    let left = fixture_manager
        .batch(&base, mutations(left_ids, 2, args.value_bytes))
        .await
        .map_err(error)?;
    let right = fixture_manager
        .batch(&base, mutations(right_ids, 2, args.value_bytes))
        .await
        .map_err(error)?;

    for repetition in 1..=args.runs {
        let concurrent_manager = manager(&backend);
        let concurrent_keys = deterministic_ids(
            args.records,
            args.concurrent_operations,
            repetition as usize + 71,
        )
        .into_iter()
        .map(key)
        .collect::<Vec<_>>();
        measure(
            &args,
            &backend,
            &mut writer,
            &mut completed,
            repetition,
            "concurrent_query",
            args.concurrent_operations,
            async {
                let values = stream::iter(concurrent_keys.iter())
                    .map(|key| concurrent_manager.get(&base, key))
                    .buffer_unordered(args.concurrency)
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(error)?;
                if values.iter().all(Option::is_some) {
                    Ok(values.len())
                } else {
                    Err("concurrent engine query returned a missing value".to_string())
                }
            },
        )
        .await?;
        let query_manager = manager(&backend);
        measure(
            &args,
            &backend,
            &mut writer,
            &mut completed,
            repetition,
            "query",
            args.samples,
            async {
                let values = query_manager
                    .get_many(&base, &sample_keys)
                    .await
                    .map_err(error)?;
                if values.iter().all(Option::is_some) {
                    Ok(values.len())
                } else {
                    Err("engine query returned a missing value".to_string())
                }
            },
        )
        .await?;
        let batch_manager = manager(&backend);
        measure(
            &args,
            &backend,
            &mut writer,
            &mut completed,
            repetition,
            "batch",
            args.changes,
            async {
                let tree = batch_manager
                    .batch(
                        &base,
                        mutations(&change_ids, repetition as u64 + 2, args.value_bytes),
                    )
                    .await
                    .map_err(error)?;
                Ok(batch_manager
                    .collect_stats(&tree)
                    .await
                    .map_err(error)?
                    .total_key_value_pairs)
            },
        )
        .await?;
        let diff_manager = manager(&backend);
        measure(
            &args,
            &backend,
            &mut writer,
            &mut completed,
            repetition,
            "diff",
            args.changes,
            async {
                Ok(diff_manager
                    .diff(&base, &changed)
                    .await
                    .map_err(error)?
                    .len())
            },
        )
        .await?;
        let merge_manager = manager(&backend);
        measure(
            &args,
            &backend,
            &mut writer,
            &mut completed,
            repetition,
            "merge",
            args.changes.saturating_mul(2),
            async {
                let merged = merge_manager
                    .merge(&base, &left, &right, None)
                    .await
                    .map_err(error)?;
                Ok(merge_manager
                    .collect_stats(&merged)
                    .await
                    .map_err(error)?
                    .total_key_value_pairs)
            },
        )
        .await?;
    }

    measure(
        &args,
        &backend,
        &mut writer,
        &mut completed,
        1,
        "list_roots_large_table",
        args.roots,
        async {
            let roots = backend.list_root_manifests().await.map_err(error)?;
            Ok(roots.len())
        },
    )
    .await?;

    if args.cleanup_namespace {
        backend.clear_namespace().await.map_err(error)?;
    }
    println!("DynamoDB Local benchmark complete: {}", raw_path.display());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn measure<F>(
    args: &Args,
    _backend: &DynamoDbBackend,
    writer: &mut csv::Writer<std::fs::File>,
    completed: &mut BTreeSet<(u32, String)>,
    repetition: u32,
    operation: &str,
    logical_operations: usize,
    future: F,
) -> Result<(), String>
where
    F: std::future::Future<Output = Result<usize, String>>,
{
    if completed.contains(&(repetition, operation.to_string())) {
        return Ok(());
    }
    eprintln!("measuring {operation} repetition={repetition}");
    let started = Instant::now();
    let observed = future.await?;
    let row = make_row(
        args,
        repetition,
        operation,
        logical_operations,
        observed,
        started.elapsed().as_nanos(),
    );
    append_row(args, writer, completed, row)
}

fn make_row(
    args: &Args,
    repetition: u32,
    operation: &str,
    logical_operations: usize,
    observed_items: usize,
    total_ns: u128,
) -> Row {
    let logical = logical_operations.max(1);
    Row {
        schema: SCHEMA.to_string(),
        revision: args.revision.clone(),
        dirty: args.dirty,
        records: args.records as u64,
        value_bytes: args.value_bytes as u64,
        concurrency: args.concurrency as u64,
        read_parallelism: args.read_parallelism as u64,
        batch_get_parallelism: args.batch_get_parallelism as u64,
        batch_write_parallelism: args.batch_write_parallelism as u64,
        scan_parallelism: args.scan_parallelism as u64,
        repetition,
        operation: operation.to_string(),
        logical_operations: logical as u64,
        observed_items: observed_items as u64,
        total_ns,
        ns_per_op: total_ns as f64 / logical as f64,
        ops_per_sec: logical as f64 * 1_000_000_000.0 / total_ns.max(1) as f64,
        validated: true,
        error: String::new(),
    }
}

fn append_row(
    _args: &Args,
    writer: &mut csv::Writer<std::fs::File>,
    completed: &mut BTreeSet<(u32, String)>,
    row: Row,
) -> Result<(), String> {
    validate_row(&row)?;
    writer.serialize(&row).map_err(error)?;
    writer.flush().map_err(error)?;
    writer.get_ref().sync_data().map_err(error)?;
    completed.insert((row.repetition, row.operation.clone()));
    Ok(())
}

fn validate_row(row: &Row) -> Result<(), String> {
    if row.schema != SCHEMA
        || !row.validated
        || !row.error.is_empty()
        || row.logical_operations == 0
        || row.observed_items == 0
        || row.total_ns == 0
    {
        return Err(format!(
            "invalid benchmark row {} repetition {}",
            row.operation, row.repetition
        ));
    }
    Ok(())
}

fn read_rows(path: &std::path::Path) -> Result<Vec<Row>, String> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    csv::Reader::from_path(path)
        .map_err(error)?
        .deserialize()
        .map(|row| row.map_err(error))
        .collect()
}

fn manager(backend: &DynamoDbBackend) -> Manager {
    AsyncProlly::new(RemoteProllyStore::new(backend.clone()), Config::default())
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:020}").into_bytes()
}

fn value(index: usize, generation: u64, bytes: usize) -> Vec<u8> {
    let mut seed = Vec::with_capacity(16);
    seed.extend_from_slice(&generation.to_le_bytes());
    seed.extend_from_slice(&(index as u64).to_le_bytes());
    seed.iter().copied().cycle().take(bytes).collect()
}

fn mutations(ids: &[usize], generation: u64, value_bytes: usize) -> Vec<Mutation> {
    ids.iter()
        .map(|index| Mutation::Upsert {
            key: key(*index),
            val: value(*index, generation, value_bytes),
        })
        .collect()
}

fn deterministic_ids(records: usize, count: usize, salt: usize) -> Vec<usize> {
    let mut step = 104_729 % records;
    if step == 0 {
        step = 1;
    }
    while greatest_common_divisor(step, records) != 1 {
        step = (step + 1) % records;
        if step == 0 {
            step = 1;
        }
    }
    let mut candidate = salt % records;
    let mut ids = Vec::with_capacity(count.min(records));
    for _ in 0..count.min(records) {
        ids.push(candidate);
        candidate = (candidate + step) % records;
    }
    ids.sort_unstable();
    ids
}

fn greatest_common_divisor(mut left: usize, mut right: usize) -> usize {
    while right != 0 {
        (left, right) = (right, left % right);
    }
    left
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        endpoint: "http://127.0.0.1:8000".to_string(),
        table: "prolly_benchmark".to_string(),
        output: PathBuf::from("performance-results/dynamodb-local"),
        records: 100_000,
        value_bytes: 256,
        raw_items: 2_500,
        samples: 10_000,
        changes: 10_000,
        roots: 1_000,
        conflicts: 100,
        concurrency: 32,
        concurrent_operations: 10_000,
        read_parallelism: 16,
        batch_get_parallelism: 8,
        batch_write_parallelism: 8,
        scan_parallelism: 8,
        cleanup_namespace: true,
        runs: 3,
        revision: "unknown".to_string(),
        dirty: true,
    };
    let values = std::env::args().collect::<Vec<_>>();
    let mut index = 1;
    while index < values.len() {
        let flag = values[index].as_str();
        match flag {
            "--endpoint" => args.endpoint = take(&values, &mut index, flag)?,
            "--table" => args.table = take(&values, &mut index, flag)?,
            "--output" => args.output = PathBuf::from(take(&values, &mut index, flag)?),
            "--records" => args.records = number(&take(&values, &mut index, flag)?, flag)?,
            "--value-bytes" => args.value_bytes = number(&take(&values, &mut index, flag)?, flag)?,
            "--raw-items" => args.raw_items = number(&take(&values, &mut index, flag)?, flag)?,
            "--samples" => args.samples = number(&take(&values, &mut index, flag)?, flag)?,
            "--changes" => args.changes = number(&take(&values, &mut index, flag)?, flag)?,
            "--roots" => args.roots = number(&take(&values, &mut index, flag)?, flag)?,
            "--conflicts" => args.conflicts = number(&take(&values, &mut index, flag)?, flag)?,
            "--concurrency" => args.concurrency = number(&take(&values, &mut index, flag)?, flag)?,
            "--concurrent-operations" => {
                args.concurrent_operations = number(&take(&values, &mut index, flag)?, flag)?
            }
            "--read-parallelism" => {
                args.read_parallelism = number(&take(&values, &mut index, flag)?, flag)?
            }
            "--batch-get-parallelism" => {
                args.batch_get_parallelism = number(&take(&values, &mut index, flag)?, flag)?
            }
            "--batch-write-parallelism" => {
                args.batch_write_parallelism = number(&take(&values, &mut index, flag)?, flag)?
            }
            "--scan-parallelism" => {
                args.scan_parallelism = number(&take(&values, &mut index, flag)?, flag)?
            }
            "--skip-namespace-cleanup" => args.cleanup_namespace = false,
            "--runs" => args.runs = number(&take(&values, &mut index, flag)?, flag)?,
            "--revision" => args.revision = take(&values, &mut index, flag)?,
            "--dirty" => args.dirty = true,
            "--clean" => args.dirty = false,
            "--help" | "-h" => {
                return Err("usage: prolly-dynamodb-scale-bench [--endpoint URL] [--table NAME] [--output PATH] [--records N] [--value-bytes N] [--raw-items N] [--samples N] [--changes N] [--roots N] [--conflicts N] [--concurrency N] [--concurrent-operations N] [--read-parallelism N] [--batch-get-parallelism N] [--batch-write-parallelism N] [--scan-parallelism N] [--skip-namespace-cleanup] [--runs N] [--revision REV] [--dirty|--clean]".to_string())
            }
            _ => return Err(format!("unknown option: {flag}")),
        }
        index += 1;
    }
    Ok(args)
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.endpoint.is_empty()
        || args.table.is_empty()
        || args.revision.is_empty()
        || args.records == 0
        || args.value_bytes == 0
        || args.raw_items == 0
        || args.samples == 0
        || args.changes == 0
        || args.roots == 0
        || args.conflicts == 0
        || args.concurrency == 0
        || args.concurrent_operations == 0
        || args.read_parallelism == 0
        || args.batch_get_parallelism == 0
        || args.batch_write_parallelism == 0
        || args.scan_parallelism == 0
        || args.runs == 0
        || args.samples > args.records
        || args.concurrent_operations > args.records
        || args.changes.saturating_mul(2) > args.records
    {
        return Err("benchmark counts and strings must be positive and in range".to_string());
    }
    Ok(())
}

fn take(values: &[String], index: &mut usize, flag: &str) -> Result<String, String> {
    *index += 1;
    values
        .get(*index)
        .cloned()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn number<T: std::str::FromStr>(value: &str, flag: &str) -> Result<T, String>
where
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|error| format!("invalid {flag}: {error}"))
}

fn error(value: impl std::fmt::Display) -> String {
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tiny_values_change_across_generations() {
        assert_ne!(value(7, 0, 1), value(7, 1, 1));
    }

    #[test]
    fn deterministic_ids_terminate_and_remain_unique_for_stride_multiple() {
        let ids = deterministic_ids(104_729, 104_729, 3);
        assert_eq!(ids.len(), 104_729);
        assert_eq!(ids.first(), Some(&0));
        assert_eq!(ids.last(), Some(&104_728));
    }
}
