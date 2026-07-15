mod sqlite_workload_support;

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use prolly::{
    BatchBuilder, Config, Diff, Error, ManifestStore, Mutation, Prolly, ProllyMetricsSnapshot,
    Resolution, RootManifest, SortedBatchBuilder, Tree, TreeStats,
};
use prolly_store_sqlite::{SqliteStore, SqliteStoreConfig};
use rusqlite::Connection;

use sqlite_workload_support::{
    clustered_indexes, expected_merge_entries, expected_result_entries, key, merge_branch_indexes,
    merge_count, mutation_indexes, random_indexes, right_edge_indexes, sample_count, shuffled_ids,
    value, BenchArgs, CsvRow, DurabilityProfile, Workload, RANDOM_SEED, READ_OPERATIONS,
};

const BASE_ROOT_NAME: &[u8] = b"sqlite-workload-base";
const RESULT_ROOT_NAME: &[u8] = b"sqlite-workload-result";

fn main() {
    if std::env::args().any(|argument| argument == "clustered_range_bounds") {
        if let Err(err) = clustered_range_bounds() {
            eprintln!("clustered range bounds validation failed: {err}");
            std::process::exit(1);
        }
        return;
    }
    if let Err(err) = run() {
        eprintln!("sqlite workload benchmark failed: {err}");
        std::process::exit(1);
    }
}

fn clustered_range_bounds() -> Result<(), String> {
    let records = 100_000;
    let count = sample_count(records);
    let ids = clustered_indexes(records, count);
    let start = *ids
        .first()
        .ok_or_else(|| "clustered delete range is unexpectedly empty".to_string())?;
    let (range_start, range_end) = clustered_delete_bounds(records, count)?;
    if range_start != key(start) {
        return Err("clustered range start does not match the first deleted key".to_string());
    }
    if range_end != key(start + count) {
        return Err("clustered range end does not exclude the first surviving key".to_string());
    }
    let survivors = expected_result_entries(Workload::ClusteredBatchDeletes, records, count);
    if survivors != records - count {
        return Err("clustered range delete has the wrong expected survivor count".to_string());
    }
    Ok(())
}

fn clustered_delete_bounds(records: usize, count: usize) -> Result<(Vec<u8>, Vec<u8>), String> {
    let ids = clustered_indexes(records, count);
    let start = *ids
        .first()
        .ok_or_else(|| "clustered delete range is unexpectedly empty".to_string())?;
    Ok((key(start), key(start + ids.len())))
}

#[allow(dead_code)]
trait BenchmarkRangeDelete {
    fn delete_range(&self, tree: &Tree, start: &[u8], end: &[u8]) -> Result<Tree, Error>;
}

impl BenchmarkRangeDelete for Prolly<Arc<SqliteStore>> {
    fn delete_range(&self, tree: &Tree, start: &[u8], end: &[u8]) -> Result<Tree, Error> {
        let start = benchmark_key_id(start)?;
        let end = benchmark_key_id(end)?;
        if start >= end {
            return Ok(tree.clone());
        }
        self.batch(
            tree,
            (start..end)
                .map(|id| Mutation::Delete { key: key(id) })
                .collect(),
        )
    }
}

#[allow(dead_code)]
fn benchmark_key_id(key_bytes: &[u8]) -> Result<usize, Error> {
    let digits = key_bytes
        .strip_prefix(b"key-")
        .filter(|digits| digits.len() == 20)
        .ok_or_else(|| Error::Deserialize("invalid benchmark range key".to_string()))?;
    let id = std::str::from_utf8(digits)
        .map_err(|_| Error::Deserialize("invalid benchmark range key".to_string()))?
        .parse()
        .map_err(|_| Error::Deserialize("invalid benchmark range key".to_string()))?;
    if key(id) != key_bytes {
        return Err(Error::Deserialize(
            "invalid benchmark range key".to_string(),
        ));
    }
    Ok(id)
}

fn run() -> Result<(), String> {
    let args = BenchArgs::from_env()?;
    let row = match args.workload {
        Workload::SortedStreamBuild => run_sorted_build(&args)?,
        Workload::ShuffledBatchBuild => run_shuffled_build(&args)?,
        Workload::RandomReadsColdManager
        | Workload::RandomReadsWarmManager
        | Workload::ClusteredReadsColdManager
        | Workload::ClusteredReadsWarmManager
        | Workload::RightEdgeReadsColdManager
        | Workload::RightEdgeReadsWarmManager => run_reads(&args)?,
        Workload::AppendBatchUpserts
        | Workload::RandomBatchUpdates
        | Workload::ClusteredBatchUpdates
        | Workload::RandomBatchDeletes
        | Workload::ClusteredBatchDeletes => run_mutations(&args)?,
        Workload::IdenticalDiff
        | Workload::AppendSparseDiff
        | Workload::RandomSparseDiff
        | Workload::ClusteredSparseDiff
        | Workload::RandomDeleteDiff
        | Workload::ClusteredDeleteDiff => run_diff(&args)?,
        Workload::AppendDisjointSparseMerge
        | Workload::RandomDisjointSparseMerge
        | Workload::ClusteredDisjointSparseMerge
        | Workload::RandomConflictResolvedMerge
        | Workload::ClusteredConflictResolvedMerge => run_merge(&args)?,
    };
    println!("{}", CsvRow::header());
    println!("{}", row.to_csv());
    Ok(())
}

fn run_diff(args: &BenchArgs) -> Result<CsvRow, String> {
    let db_before = sqlite_files(&args.db_path).total;
    let (store, base) = open_base(args)?;
    let preparation = Prolly::new(store.clone(), base.config.clone());
    let count = sample_count(args.records);
    let (changed, ids, expected_kind, generation) = match args.workload {
        Workload::IdenticalDiff => (base.clone(), Vec::new(), DiffKind::Changed, 0),
        Workload::AppendSparseDiff => {
            let ids = (args.records..args.records + count).collect::<Vec<_>>();
            let changed = apply_upserts(&preparation, &base, &ids, 1, true)?;
            (changed, ids, DiffKind::Added, 1)
        }
        Workload::RandomSparseDiff => {
            let ids = random_indexes(args.records, count, RANDOM_SEED);
            let changed = apply_upserts(&preparation, &base, &ids, 1, false)?;
            (changed, ids, DiffKind::Changed, 1)
        }
        Workload::ClusteredSparseDiff => {
            let ids = clustered_indexes(args.records, count);
            let changed = apply_upserts(&preparation, &base, &ids, 2, false)?;
            (changed, ids, DiffKind::Changed, 2)
        }
        Workload::RandomDeleteDiff => {
            let ids = random_indexes(args.records, count, RANDOM_SEED);
            let changed = apply_deletes(&preparation, &base, &ids)?;
            (changed, ids, DiffKind::Removed, 0)
        }
        Workload::ClusteredDeleteDiff => {
            let ids = clustered_indexes(args.records, count);
            let changed = apply_deletes(&preparation, &base, &ids)?;
            (changed, ids, DiffKind::Removed, 0)
        }
        _ => return Err("diff runner received a non-diff workload".to_string()),
    };
    drop(preparation);
    drop(store);

    let timed_store = Arc::new(open_store(&args.db_path, args.profile)?);
    let manager = Prolly::new(timed_store, base.config.clone());
    manager.reset_metrics();
    let started = Instant::now();
    let diffs = manager
        .diff(&base, &changed)
        .map_err(|err| err.to_string())?;
    let total_ns = started.elapsed().as_nanos();
    let metrics = manager.metrics();
    validate_diffs(&diffs, &ids, expected_kind, generation)?;
    let stats = manager
        .collect_stats(&changed)
        .map_err(|err| err.to_string())?;
    make_row(args, ids.len().max(1), total_ns, metrics, &stats, db_before)
}

#[derive(Clone, Copy)]
enum DiffKind {
    Added,
    Changed,
    Removed,
}

fn validate_diffs(
    diffs: &[Diff],
    ids: &[usize],
    expected_kind: DiffKind,
    generation: u8,
) -> Result<(), String> {
    if diffs.len() != ids.len() {
        return Err(format!(
            "diff cardinality mismatch: expected {}, observed {}",
            ids.len(),
            diffs.len()
        ));
    }
    for (diff, id) in diffs.iter().zip(ids) {
        let expected_key = key(*id);
        let valid = match (expected_kind, diff) {
            (DiffKind::Added, Diff::Added { key, val }) => {
                key == &expected_key && val == &value(*id, generation)
            }
            (DiffKind::Changed, Diff::Changed { key, old, new }) => {
                key == &expected_key && old == &value(*id, 0) && new == &value(*id, generation)
            }
            (DiffKind::Removed, Diff::Removed { key, val }) => {
                key == &expected_key && val == &value(*id, 0)
            }
            _ => false,
        };
        if !valid {
            return Err(format!("diff validation failed for record {id}"));
        }
    }
    Ok(())
}

fn run_merge(args: &BenchArgs) -> Result<CsvRow, String> {
    let db_before = sqlite_files(&args.db_path).total;
    let (store, base) = open_base(args)?;
    let preparation = Prolly::new(store.clone(), base.config.clone());
    let count = merge_count(args.records);
    let (left_ids, right_ids) = merge_branch_indexes(args.workload, args.records, count);
    let append = matches!(args.workload, Workload::AppendDisjointSparseMerge);
    let left = apply_upserts(&preparation, &base, &left_ids, 1, append)?;
    let right = apply_upserts(&preparation, &base, &right_ids, 2, append)?;
    drop(preparation);
    drop(store);

    let conflicts = matches!(
        args.workload,
        Workload::RandomConflictResolvedMerge | Workload::ClusteredConflictResolvedMerge
    );
    let resolver = conflicts.then(|| {
        Box::new(|conflict: &prolly::Conflict| match &conflict.right {
            Some(right) => Resolution::value(right.clone()),
            None => Resolution::delete(),
        }) as prolly::Resolver
    });
    let timed_store = Arc::new(open_store(&args.db_path, args.profile)?);
    let manager = Prolly::new(timed_store.clone(), base.config.clone());
    manager.reset_metrics();
    let started = Instant::now();
    let merged = manager
        .merge(&base, &left, &right, resolver)
        .map_err(|err| err.to_string())?;
    let total_ns = started.elapsed().as_nanos();
    let metrics = manager.metrics();
    validate_merge_result(
        &manager,
        &merged,
        &left_ids,
        &right_ids,
        conflicts,
        expected_merge_entries(args.workload, args.records, count),
    )?;
    timed_store
        .put_root(RESULT_ROOT_NAME, &RootManifest::from_tree(&merged))
        .map_err(|err| err.to_string())?;
    let stats = manager
        .collect_stats(&merged)
        .map_err(|err| err.to_string())?;
    drop(manager);
    drop(timed_store);
    verify_reopened_merge(
        args,
        &left_ids,
        &right_ids,
        conflicts,
        stats.total_key_value_pairs,
    )?;
    let operations = if conflicts {
        left_ids.len()
    } else {
        left_ids.len() + right_ids.len()
    };
    make_row(
        args,
        operations.max(1),
        total_ns,
        metrics,
        &stats,
        db_before,
    )
}

fn apply_upserts(
    manager: &Prolly<Arc<SqliteStore>>,
    base: &Tree,
    ids: &[usize],
    generation: u8,
    append: bool,
) -> Result<Tree, String> {
    let mutations = ids
        .iter()
        .map(|id| Mutation::Upsert {
            key: key(*id),
            val: value(*id, generation),
        })
        .collect::<Vec<_>>();
    if append {
        manager
            .append_batch(base, mutations)
            .map_err(|err| err.to_string())
    } else {
        manager
            .batch(base, mutations)
            .map_err(|err| err.to_string())
    }
}

fn apply_deletes(
    manager: &Prolly<Arc<SqliteStore>>,
    base: &Tree,
    ids: &[usize],
) -> Result<Tree, String> {
    manager
        .batch(
            base,
            ids.iter()
                .map(|id| Mutation::Delete { key: key(*id) })
                .collect(),
        )
        .map_err(|err| err.to_string())
}

fn validate_merge_result(
    manager: &Prolly<Arc<SqliteStore>>,
    merged: &Tree,
    left_ids: &[usize],
    right_ids: &[usize],
    conflicts: bool,
    expected_entries: usize,
) -> Result<(), String> {
    if !conflicts {
        validate_values(manager, merged, left_ids, 1, "left merge")?;
    }
    validate_values(manager, merged, right_ids, 2, "right merge")?;
    let stats = manager
        .collect_stats(merged)
        .map_err(|err| err.to_string())?;
    if stats.total_key_value_pairs != expected_entries {
        return Err(format!(
            "merge cardinality mismatch: expected {expected_entries}, observed {}",
            stats.total_key_value_pairs
        ));
    }
    Ok(())
}

fn validate_values(
    manager: &Prolly<Arc<SqliteStore>>,
    tree: &Tree,
    ids: &[usize],
    generation: u8,
    context: &str,
) -> Result<(), String> {
    for id in ids {
        let observed = manager
            .get(tree, &key(*id))
            .map_err(|err| err.to_string())?;
        if observed != Some(value(*id, generation)) {
            return Err(format!("{context} validation failed for record {id}"));
        }
    }
    Ok(())
}

fn verify_reopened_merge(
    args: &BenchArgs,
    left_ids: &[usize],
    right_ids: &[usize],
    conflicts: bool,
    expected_entries: usize,
) -> Result<(), String> {
    let store = Arc::new(open_store(&args.db_path, args.profile)?);
    let tree = store
        .get_root(RESULT_ROOT_NAME)
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "workload database is missing the named merge root".to_string())?
        .to_tree();
    let manager = Prolly::new(store, tree.config.clone());
    validate_merge_result(
        &manager,
        &tree,
        left_ids,
        right_ids,
        conflicts,
        expected_entries,
    )
}

fn run_reads(args: &BenchArgs) -> Result<CsvRow, String> {
    let db_before = sqlite_files(&args.db_path).total;
    let (store, tree) = open_base(args)?;
    let manager = Prolly::new(store, tree.config.clone());
    let sample_ids = match args.workload {
        Workload::RandomReadsColdManager | Workload::RandomReadsWarmManager => {
            random_indexes(args.records, args.records.min(10_000), RANDOM_SEED)
        }
        Workload::ClusteredReadsColdManager | Workload::ClusteredReadsWarmManager => {
            clustered_indexes(args.records, args.records.min(10_000))
        }
        Workload::RightEdgeReadsColdManager | Workload::RightEdgeReadsWarmManager => {
            right_edge_indexes(args.records, args.records.min(10_000))
        }
        _ => return Err("read runner received a non-read workload".to_string()),
    };
    let warm = matches!(
        args.workload,
        Workload::RandomReadsWarmManager
            | Workload::ClusteredReadsWarmManager
            | Workload::RightEdgeReadsWarmManager
    );
    if warm {
        execute_reads(&manager, &tree, &sample_ids, READ_OPERATIONS)?;
    }
    manager.reset_metrics();
    let started = Instant::now();
    execute_reads(&manager, &tree, &sample_ids, READ_OPERATIONS)?;
    let total_ns = started.elapsed().as_nanos();
    let metrics = manager.metrics();
    let stats = manager
        .collect_stats(&tree)
        .map_err(|err| err.to_string())?;
    make_row(args, READ_OPERATIONS, total_ns, metrics, &stats, db_before)
}

fn execute_reads(
    manager: &Prolly<Arc<SqliteStore>>,
    tree: &Tree,
    sample_ids: &[usize],
    operations: usize,
) -> Result<(), String> {
    let mut observed_bytes = 0usize;
    for operation in 0..operations {
        let id = sample_ids[operation % sample_ids.len()];
        let observed = manager
            .get(tree, black_box(&key(id)))
            .map_err(|err| err.to_string())?
            .ok_or_else(|| format!("read workload did not find record {id}"))?;
        let expected = value(id, 0);
        if observed != expected {
            return Err(format!(
                "read workload returned the wrong value for record {id}"
            ));
        }
        observed_bytes = observed_bytes.wrapping_add(observed.len());
        black_box(&observed);
    }
    black_box(observed_bytes);
    Ok(())
}

fn run_mutations(args: &BenchArgs) -> Result<CsvRow, String> {
    let db_before = sqlite_files(&args.db_path).total;
    let (store, base) = open_base(args)?;
    let manager = Prolly::new(store.clone(), base.config.clone());
    let count = sample_count(args.records);
    let ids = mutation_indexes(args.workload, args.records, count);
    let is_delete = matches!(
        args.workload,
        Workload::RandomBatchDeletes | Workload::ClusteredBatchDeletes
    );
    let generation = if matches!(args.workload, Workload::ClusteredBatchUpdates) {
        2
    } else {
        1
    };
    let range_bounds = matches!(args.workload, Workload::ClusteredBatchDeletes)
        .then(|| clustered_delete_bounds(args.records, count))
        .transpose()?;
    let mutations = (!matches!(args.workload, Workload::ClusteredBatchDeletes))
        .then(|| mutation_batch(&ids, is_delete, generation));
    manager.reset_metrics();
    let started = Instant::now();
    let changed = match args.workload {
        Workload::ClusteredBatchDeletes => {
            let (start, end) = range_bounds
                .as_ref()
                .expect("clustered range delete has bounds");
            manager
                .delete_range(
                    &base,
                    black_box(start.as_slice()),
                    black_box(end.as_slice()),
                )
                .map_err(|err| err.to_string())?
        }
        Workload::AppendBatchUpserts => manager
            .append_batch(
                &base,
                black_box(mutations.expect("non-range mutation has a batch")),
            )
            .map_err(|err| err.to_string())?,
        _ => manager
            .batch(
                &base,
                black_box(mutations.expect("non-range mutation has a batch")),
            )
            .map_err(|err| err.to_string())?,
    };
    let total_ns = started.elapsed().as_nanos();
    let metrics = manager.metrics();
    validate_mutation_result(
        &manager,
        &changed,
        &ids,
        is_delete,
        generation,
        expected_result_entries(args.workload, args.records, count),
    )?;
    if matches!(args.workload, Workload::ClusteredBatchDeletes) {
        validate_clustered_range_delete_samples(&manager, &changed, &ids, args.records)?;
    }
    store
        .put_root(RESULT_ROOT_NAME, &RootManifest::from_tree(&changed))
        .map_err(|err| err.to_string())?;
    let stats = manager
        .collect_stats(&changed)
        .map_err(|err| err.to_string())?;
    drop(manager);
    drop(store);
    verify_reopened_result(
        args,
        &ids,
        is_delete,
        generation,
        stats.total_key_value_pairs,
    )?;
    make_row(args, count, total_ns, metrics, &stats, db_before)
}

fn mutation_batch(ids: &[usize], is_delete: bool, generation: u8) -> Vec<Mutation> {
    ids.iter()
        .map(|id| {
            if is_delete {
                Mutation::Delete { key: key(*id) }
            } else {
                Mutation::Upsert {
                    key: key(*id),
                    val: value(*id, generation),
                }
            }
        })
        .collect()
}

fn validate_mutation_result(
    manager: &Prolly<Arc<SqliteStore>>,
    tree: &Tree,
    ids: &[usize],
    is_delete: bool,
    generation: u8,
    expected_entries: usize,
) -> Result<(), String> {
    for id in ids {
        let observed = manager
            .get(tree, &key(*id))
            .map_err(|err| err.to_string())?;
        let expected = if is_delete {
            None
        } else {
            Some(value(*id, generation))
        };
        if observed != expected {
            return Err(format!("mutation validation failed for record {id}"));
        }
    }
    let stats = manager.collect_stats(tree).map_err(|err| err.to_string())?;
    if stats.total_key_value_pairs != expected_entries {
        return Err(format!(
            "mutation cardinality mismatch: expected {expected_entries}, observed {}",
            stats.total_key_value_pairs
        ));
    }
    Ok(())
}

fn validate_clustered_range_delete_samples(
    manager: &Prolly<Arc<SqliteStore>>,
    tree: &Tree,
    ids: &[usize],
    records: usize,
) -> Result<(), String> {
    let start = *ids
        .first()
        .ok_or_else(|| "clustered delete range is unexpectedly empty".to_string())?;
    let end = ids
        .last()
        .copied()
        .ok_or_else(|| "clustered delete range is unexpectedly empty".to_string())?
        + 1;
    let interior = ids[ids.len() / 2];
    if manager
        .get(tree, &key(interior))
        .map_err(|err| err.to_string())?
        .is_some()
    {
        return Err(format!(
            "clustered range delete retained interior record {interior}"
        ));
    }
    if start > 0 {
        let id = start - 1;
        if manager.get(tree, &key(id)).map_err(|err| err.to_string())? != Some(value(id, 0)) {
            return Err(format!(
                "clustered range delete removed left neighboring record {id}"
            ));
        }
    }
    if end < records
        && manager
            .get(tree, &key(end))
            .map_err(|err| err.to_string())?
            != Some(value(end, 0))
    {
        return Err(format!(
            "clustered range delete removed right neighboring record {end}"
        ));
    }
    Ok(())
}

fn verify_reopened_result(
    args: &BenchArgs,
    ids: &[usize],
    is_delete: bool,
    generation: u8,
    expected_entries: usize,
) -> Result<(), String> {
    let store = Arc::new(open_store(&args.db_path, args.profile)?);
    let manifest = store
        .get_root(RESULT_ROOT_NAME)
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "workload database is missing the named result root".to_string())?;
    let tree = manifest.to_tree();
    let manager = Prolly::new(store, tree.config.clone());
    for id in ids {
        let observed = manager
            .get(&tree, &key(*id))
            .map_err(|err| err.to_string())?;
        let expected = if is_delete {
            None
        } else {
            Some(value(*id, generation))
        };
        if observed != expected {
            return Err(format!(
                "reopened mutation validation failed for record {id}"
            ));
        }
    }
    let stats = manager
        .collect_stats(&tree)
        .map_err(|err| err.to_string())?;
    if stats.total_key_value_pairs != expected_entries {
        return Err("reopened mutation tree has the wrong cardinality".to_string());
    }
    if matches!(args.workload, Workload::ClusteredBatchDeletes) {
        validate_clustered_range_delete_samples(&manager, &tree, ids, args.records)?;
    }
    Ok(())
}

fn open_base(args: &BenchArgs) -> Result<(Arc<SqliteStore>, Tree), String> {
    let store = Arc::new(open_store(&args.db_path, args.profile)?);
    let tree = store
        .get_root(BASE_ROOT_NAME)
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "workload fixture is missing the named base root".to_string())?
        .to_tree();
    Ok((store, tree))
}

fn run_sorted_build(args: &BenchArgs) -> Result<CsvRow, String> {
    remove_sqlite_files(&args.db_path);
    let db_before = sqlite_files(&args.db_path).total;
    let config = bench_config();
    let store = Arc::new(open_store(&args.db_path, args.profile)?);
    let started = Instant::now();
    let mut builder = SortedBatchBuilder::new(store.clone(), config.clone());
    for id in 0..args.records {
        builder
            .add(key(id), value(id, 0))
            .map_err(|err| err.to_string())?;
    }
    let tree = builder.build().map_err(|err| err.to_string())?;
    store
        .put_root(BASE_ROOT_NAME, &RootManifest::from_tree(&tree))
        .map_err(|err| err.to_string())?;
    let elapsed = started.elapsed();
    finalize_build_row(args, store, tree, elapsed.as_nanos(), db_before)
}

fn run_shuffled_build(args: &BenchArgs) -> Result<CsvRow, String> {
    remove_sqlite_files(&args.db_path);
    let db_before = sqlite_files(&args.db_path).total;
    let config = bench_config();
    let store = Arc::new(open_store(&args.db_path, args.profile)?);
    let started = Instant::now();
    let mut builder = BatchBuilder::new(store.clone(), config.clone());
    for id in shuffled_ids(args.records, RANDOM_SEED) {
        builder.add(key(id), value(id, 0));
    }
    let tree = builder.build().map_err(|err| err.to_string())?;
    store
        .put_root(BASE_ROOT_NAME, &RootManifest::from_tree(&tree))
        .map_err(|err| err.to_string())?;
    let elapsed = started.elapsed();
    finalize_build_row(args, store, tree, elapsed.as_nanos(), db_before)
}

fn finalize_build_row(
    args: &BenchArgs,
    store: Arc<SqliteStore>,
    tree: Tree,
    total_ns: u128,
    db_before: u64,
) -> Result<CsvRow, String> {
    let manager = Prolly::new(store.clone(), tree.config.clone());
    let stats = manager
        .collect_stats(&tree)
        .map_err(|err| err.to_string())?;
    if stats.total_key_value_pairs != args.records {
        return Err(format!(
            "build cardinality mismatch: expected {}, observed {}",
            args.records, stats.total_key_value_pairs
        ));
    }
    drop(manager);
    drop(store);
    verify_reopened_tree(&args.db_path, args.profile, args.records)?;
    make_row(
        args,
        args.records,
        total_ns,
        ProllyMetricsSnapshot::default(),
        &stats,
        db_before,
    )
}

fn make_row(
    args: &BenchArgs,
    operations: usize,
    total_ns: u128,
    metrics: ProllyMetricsSnapshot,
    stats: &TreeStats,
    db_before: u64,
) -> Result<CsvRow, String> {
    let files = sqlite_files(&args.db_path);
    let (sqlite_node_count, sqlite_node_payload_bytes) = sqlite_node_stats(&args.db_path)?;
    let ns_per_op = if operations == 0 {
        0.0
    } else {
        total_ns as f64 / operations as f64
    };
    let ops_per_sec = if total_ns == 0 {
        0.0
    } else {
        operations as f64 / (total_ns as f64 / 1_000_000_000.0)
    };
    Ok(CsvRow {
        version: args.version.clone(),
        profile: args.profile.as_str().to_string(),
        records: args.records,
        run: args.run,
        workload: args.workload.as_str().to_string(),
        operations,
        total_ns,
        ns_per_op,
        ops_per_sec,
        nodes_read: metrics.nodes_read,
        nodes_written: metrics.nodes_written,
        bytes_read: metrics.bytes_read,
        bytes_written: metrics.bytes_written,
        cache_hits: metrics.node_cache_hits,
        cache_misses: metrics.node_cache_misses,
        cache_evictions: metrics.node_cache_evictions,
        result_entries: stats.total_key_value_pairs,
        num_nodes: stats.num_nodes,
        num_leaves: stats.num_leaves,
        num_internal: stats.num_internal_nodes,
        height: usize::from(stats.tree_height),
        tree_bytes: stats.total_tree_size_bytes,
        db_bytes_before: db_before,
        db_bytes_after: files.db,
        wal_bytes_after: files.wal,
        shm_bytes_after: files.shm,
        fixture_bytes_after: files.total,
        sqlite_node_count,
        sqlite_node_payload_bytes,
        validated: true,
        status: "ok".to_string(),
    })
}

fn verify_reopened_tree(
    path: &Path,
    profile: DurabilityProfile,
    records: usize,
) -> Result<(), String> {
    let store = Arc::new(open_store(path, profile)?);
    let manifest = store
        .get_root(BASE_ROOT_NAME)
        .map_err(|err| err.to_string())?
        .ok_or_else(|| "prepared database is missing the named base root".to_string())?;
    let tree = manifest.to_tree();
    let manager = Prolly::new(store, tree.config.clone());
    for id in [0, records / 2, records - 1] {
        let observed = manager
            .get(&tree, &key(id))
            .map_err(|err| err.to_string())?;
        if observed.as_deref() != Some(value(id, 0).as_slice()) {
            return Err(format!("reopen verification failed for record {id}"));
        }
    }
    Ok(())
}

fn bench_config() -> Config {
    Config::builder()
        .min_chunk_size(64)
        .max_chunk_size(512)
        .chunking_factor(256)
        .hash_seed(0xC0DA)
        .build()
}

fn open_store(path: &Path, profile: DurabilityProfile) -> Result<SqliteStore, String> {
    SqliteStore::open_with_config(
        path,
        SqliteStoreConfig {
            busy_timeout_ms: 5_000,
            enable_wal: true,
            synchronous_normal: matches!(profile, DurabilityProfile::Normal),
        },
    )
    .map_err(|err| err.to_string())
}

#[derive(Clone, Copy, Debug, Default)]
struct SqliteFiles {
    db: u64,
    wal: u64,
    shm: u64,
    total: u64,
}

fn sqlite_files(path: &Path) -> SqliteFiles {
    let size = |path: &Path| std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0);
    let wal_path = PathBuf::from(format!("{}-wal", path.display()));
    let shm_path = PathBuf::from(format!("{}-shm", path.display()));
    let db = size(path);
    let wal = size(&wal_path);
    let shm = size(&shm_path);
    SqliteFiles {
        db,
        wal,
        shm,
        total: db.saturating_add(wal).saturating_add(shm),
    }
}

fn sqlite_node_stats(path: &Path) -> Result<(u64, u64), String> {
    let connection = Connection::open(path).map_err(|err| err.to_string())?;
    connection
        .query_row(
            "SELECT count(*), COALESCE(sum(length(node)), 0) FROM prolly_nodes",
            [],
            |row| Ok((row.get::<_, u64>(0)?, row.get::<_, u64>(1)?)),
        )
        .map_err(|err| err.to_string())
}

fn remove_sqlite_files(path: &Path) {
    for path in [
        path.to_path_buf(),
        PathBuf::from(format!("{}-wal", path.display())),
        PathBuf::from(format!("{}-shm", path.display())),
    ] {
        let _ = std::fs::remove_file(path);
    }
}
