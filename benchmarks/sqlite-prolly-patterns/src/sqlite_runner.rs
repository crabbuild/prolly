use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use std::time::Instant;

use prolly::{Config, Mutation, Prolly, ProllyMetricsSnapshot, SortedBatchBuilder, Tree, TreeStats};
use prolly_store_sqlite::SqliteStore;

use crate::fixture::{directory_bytes, sqlite_file_bytes, FixtureLayout};
use crate::measurement::{nearest_rank, rate, FixtureRow, RawRow, SCHEMA_VERSION};
use crate::model::{
    key, mutation_ids, range_bounds, range_ids, read_ids, value, CacheState, CellSpec,
    FixtureSpec, Operation,
};

const BASE_ROOT: &[u8] = b"sqlite-pattern-base";
const RESULT_ROOT: &[u8] = b"sqlite-pattern-result";

pub fn build_fixture(spec: &FixtureSpec, layout: &FixtureLayout) -> Result<FixtureRow, String> {
    if layout.source_dir().exists() {
        return Err(format!(
            "fixture already exists: {}",
            layout.source_dir().display()
        ));
    }
    fs::create_dir_all(layout.source_dir()).map_err(|error| {
        format!(
            "failed to create fixture {}: {error}",
            layout.source_dir().display()
        )
    })?;
    let store = Arc::new(open_store(&layout.source_database())?);
    let config = Config::default();
    let started = Instant::now();
    let mut builder = SortedBatchBuilder::new(store.clone(), config.clone());
    for id in 0..spec.records {
        builder
            .add(key(id), value(id, 0))
            .map_err(|error| format!("failed to add fixture record {id}: {error}"))?;
    }
    let tree = builder
        .build()
        .map_err(|error| format!("failed to build fixture: {error}"))?;
    let build_ns = started.elapsed().as_nanos().max(1);
    let manager = Prolly::new(store.clone(), config);
    validate_tree(&manager, &tree, spec.records, &BTreeMap::new())?;
    manager
        .publish_named_root(BASE_ROOT, &tree)
        .map_err(|error| format!("failed to publish fixture root: {error}"))?;
    let stats = manager
        .collect_stats(&tree)
        .map_err(|error| format!("failed to collect fixture stats: {error}"))?;
    drop(manager);
    drop(store);

    let reopened_store = Arc::new(open_store(&layout.source_database())?);
    let reopened = Prolly::new(reopened_store.clone(), tree.config.clone());
    let loaded = reopened
        .load_named_root(BASE_ROOT)
        .map_err(|error| format!("failed to reload fixture root: {error}"))?
        .ok_or_else(|| "fixture root is missing after reopen".to_string())?;
    validate_tree(&reopened, &loaded, spec.records, &BTreeMap::new())?;
    drop(reopened);
    drop(reopened_store);

    Ok(FixtureRow {
        schema: SCHEMA_VERSION.to_string(),
        revision: spec.revision.clone(),
        dirty: spec.dirty,
        records: spec.records,
        repetition: spec.repetition,
        build_ns,
        records_per_sec: rate(spec.records, build_ns),
        num_nodes: stats.num_nodes,
        num_leaves: stats.num_leaves,
        num_internal: stats.num_internal_nodes,
        height: usize::from(stats.tree_height),
        tree_bytes: stats.total_tree_size_bytes,
        database_bytes: directory_bytes(&layout.source_dir())?,
        observed_records: spec.records,
        error: String::new(),
        validated: true,
    })
}

pub fn run_cell(spec: &CellSpec, layout: &FixtureLayout) -> Result<RawRow, String> {
    let database = layout.cell_database(spec);
    let store = Arc::new(open_store(&database)?);
    let manager = Prolly::new(store.clone(), Config::default());
    let base = manager
        .load_named_root(BASE_ROOT)
        .map_err(|error| format!("failed to load base root: {error}"))?
        .ok_or_else(|| "base root is missing".to_string())?;

    let outcome = match spec.operation {
        Operation::Put => run_put(&manager, &base, spec)?,
        Operation::Batch => run_batch(&manager, &base, spec)?,
        Operation::PointRead => run_point_reads(&manager, &base, spec)?,
        Operation::RangeScan => run_range_scan(&manager, &base, spec)?,
    };
    let observed_entries = manager
        .len(&outcome.tree)
        .map_err(|error| format!("failed to count result entries: {error}"))?
        as usize;
    if observed_entries != spec.expected_entries() {
        return Err(format!(
            "result cardinality mismatch: observed {observed_entries}, expected {}",
            spec.expected_entries()
        ));
    }
    let stats = manager
        .collect_stats(&outcome.tree)
        .map_err(|error| format!("failed to collect result stats: {error}"))?;

    if matches!(spec.operation, Operation::Put | Operation::Batch) {
        validate_tree(
            &manager,
            &outcome.tree,
            spec.expected_entries(),
            &outcome.changed_values,
        )?;
        manager
            .publish_named_root(RESULT_ROOT, &outcome.tree)
            .map_err(|error| format!("failed to publish result root: {error}"))?;
    }
    let files = sqlite_file_bytes(&database)?;
    drop(manager);
    drop(store);

    if matches!(spec.operation, Operation::Put | Operation::Batch) {
        let reopened_store = Arc::new(open_store(&database)?);
        let reopened = Prolly::new(reopened_store.clone(), outcome.tree.config.clone());
        let persisted = reopened
            .load_named_root(RESULT_ROOT)
            .map_err(|error| format!("failed to reload result root: {error}"))?
            .ok_or_else(|| "result root is missing after reopen".to_string())?;
        validate_tree(
            &reopened,
            &persisted,
            spec.expected_entries(),
            &outcome.changed_values,
        )?;
    }

    Ok(make_row(
        spec,
        &outcome,
        &stats,
        observed_entries,
        files,
    ))
}

struct Outcome {
    tree: Tree,
    changed_values: BTreeMap<usize, u8>,
    observed_operations: usize,
    total_ns: u128,
    latencies: Vec<u128>,
    metrics: ProllyMetricsSnapshot,
}

fn run_put(
    manager: &Prolly<Arc<SqliteStore>>,
    base: &Tree,
    spec: &CellSpec,
) -> Result<Outcome, String> {
    let ids = mutation_ids(spec.pattern, spec.records, spec.operations);
    let mut tree = base.clone();
    let mut latencies = Vec::with_capacity(ids.len());
    manager.reset_metrics();
    let total_started = Instant::now();
    for id in &ids {
        let started = Instant::now();
        tree = manager
            .put(&tree, key(*id), value(*id, 1))
            .map_err(|error| format!("put failed for {id}: {error}"))?;
        latencies.push(started.elapsed().as_nanos().max(1));
    }
    let total_ns = total_started.elapsed().as_nanos().max(1);
    Ok(Outcome {
        tree,
        changed_values: ids.into_iter().map(|id| (id, 1)).collect(),
        observed_operations: latencies.len(),
        total_ns,
        latencies,
        metrics: manager.metrics(),
    })
}

fn run_batch(
    manager: &Prolly<Arc<SqliteStore>>,
    base: &Tree,
    spec: &CellSpec,
) -> Result<Outcome, String> {
    let ids = mutation_ids(spec.pattern, spec.records, spec.operations);
    let mutations = ids
        .iter()
        .map(|id| Mutation::Upsert {
            key: key(*id),
            val: value(*id, 1),
        })
        .collect();
    manager.reset_metrics();
    let started = Instant::now();
    let tree = manager
        .batch(base, mutations)
        .map_err(|error| format!("batch failed: {error}"))?;
    let total_ns = started.elapsed().as_nanos().max(1);
    Ok(Outcome {
        tree,
        changed_values: ids.iter().copied().map(|id| (id, 1)).collect(),
        observed_operations: ids.len(),
        total_ns,
        latencies: Vec::new(),
        metrics: manager.metrics(),
    })
}

fn run_point_reads(
    manager: &Prolly<Arc<SqliteStore>>,
    base: &Tree,
    spec: &CellSpec,
) -> Result<Outcome, String> {
    let ids = read_ids(spec.pattern, spec.records, spec.operations);
    if spec.cache_state == CacheState::WarmManager {
        for id in &ids {
            let observed = manager
                .get(base, &key(*id))
                .map_err(|error| format!("warmup read failed for {id}: {error}"))?;
            if observed.as_deref() != Some(value(*id, 0).as_slice()) {
                return Err(format!("warmup read returned the wrong value for {id}"));
            }
        }
    }
    manager.reset_metrics();
    let mut observed = Vec::with_capacity(ids.len());
    let mut latencies = Vec::with_capacity(ids.len());
    let total_started = Instant::now();
    for id in &ids {
        let started = Instant::now();
        observed.push(
            manager
                .get(base, &key(*id))
                .map_err(|error| format!("point read failed for {id}: {error}"))?,
        );
        latencies.push(started.elapsed().as_nanos().max(1));
    }
    let total_ns = total_started.elapsed().as_nanos().max(1);
    let metrics = manager.metrics();
    for (id, observed) in ids.iter().zip(observed) {
        if observed.as_deref() != Some(value(*id, 0).as_slice()) {
            return Err(format!("point read returned the wrong value for {id}"));
        }
    }
    Ok(Outcome {
        tree: base.clone(),
        changed_values: BTreeMap::new(),
        observed_operations: ids.len(),
        total_ns,
        latencies,
        metrics,
    })
}

fn run_range_scan(
    manager: &Prolly<Arc<SqliteStore>>,
    base: &Tree,
    spec: &CellSpec,
) -> Result<Outcome, String> {
    let expected_ids = range_ids(spec.pattern, spec.records, spec.operations);
    let (start, end) = range_bounds(spec.pattern, spec.records, spec.operations);
    manager.reset_metrics();
    let started = Instant::now();
    let observed = manager
        .range(base, &start, Some(&end))
        .map_err(|error| format!("failed to start range scan: {error}"))?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("range scan failed: {error}"))?;
    let total_ns = started.elapsed().as_nanos().max(1);
    let metrics = manager.metrics();
    if observed.len() != expected_ids.len() {
        return Err(format!(
            "range scan returned {} rows, expected {}",
            observed.len(),
            expected_ids.len()
        ));
    }
    for ((observed_key, observed_value), id) in observed.iter().zip(&expected_ids) {
        if observed_key != &key(*id) || observed_value != &value(*id, 0) {
            return Err(format!("range scan returned the wrong record for {id}"));
        }
    }
    Ok(Outcome {
        tree: base.clone(),
        changed_values: BTreeMap::new(),
        observed_operations: observed.len(),
        total_ns,
        latencies: Vec::new(),
        metrics,
    })
}

fn validate_tree(
    manager: &Prolly<Arc<SqliteStore>>,
    tree: &Tree,
    expected_entries: usize,
    changed_values: &BTreeMap<usize, u8>,
) -> Result<(), String> {
    let observed = manager
        .len(tree)
        .map_err(|error| format!("failed to count tree: {error}"))?
        as usize;
    if observed != expected_entries {
        return Err(format!(
            "tree contains {observed} entries, expected {expected_entries}"
        ));
    }
    if expected_entries > 0 {
        for id in [0, expected_entries / 2, expected_entries - 1] {
            let generation = changed_values.get(&id).copied().unwrap_or(0);
            let observed = manager
                .get(tree, &key(id))
                .map_err(|error| format!("failed to validate record {id}: {error}"))?;
            if observed.as_deref() != Some(value(id, generation).as_slice()) {
                return Err(format!("tree validation failed for record {id}"));
            }
        }
    }
    for (id, generation) in changed_values {
        let observed = manager
            .get(tree, &key(*id))
            .map_err(|error| format!("failed to validate changed record {id}: {error}"))?;
        if observed.as_deref() != Some(value(*id, *generation).as_slice()) {
            return Err(format!("changed record validation failed for {id}"));
        }
    }
    Ok(())
}

fn make_row(
    spec: &CellSpec,
    outcome: &Outcome,
    stats: &TreeStats,
    observed_entries: usize,
    files: (u64, u64, u64, u64),
) -> RawRow {
    let (db_bytes, wal_bytes, shm_bytes, total_database_bytes) = files;
    let operations = outcome.observed_operations;
    RawRow {
        schema: SCHEMA_VERSION.to_string(),
        revision: spec.revision.clone(),
        dirty: spec.dirty,
        records: spec.records,
        repetition: spec.repetition,
        operation: spec.operation,
        pattern: spec.pattern,
        cache_state: spec.cache_state,
        configured_operations: spec.operations,
        observed_operations: operations,
        total_ns: outcome.total_ns,
        ns_per_operation: outcome.total_ns as f64 / operations.max(1) as f64,
        operations_per_sec: rate(operations, outcome.total_ns),
        p50_ns: nearest_rank(&outcome.latencies, 0.50),
        p95_ns: nearest_rank(&outcome.latencies, 0.95),
        p99_ns: nearest_rank(&outcome.latencies, 0.99),
        max_ns: outcome.latencies.iter().max().copied(),
        nodes_read: outcome.metrics.nodes_read,
        nodes_written: outcome.metrics.nodes_written,
        bytes_read: outcome.metrics.bytes_read,
        bytes_written: outcome.metrics.bytes_written,
        cache_hits: outcome.metrics.node_cache_hits,
        cache_misses: outcome.metrics.node_cache_misses,
        cache_evictions: outcome.metrics.node_cache_evictions,
        result_entries: stats.total_key_value_pairs,
        num_nodes: stats.num_nodes,
        num_leaves: stats.num_leaves,
        num_internal: stats.num_internal_nodes,
        height: usize::from(stats.tree_height),
        tree_bytes: stats.total_tree_size_bytes,
        db_bytes,
        wal_bytes,
        shm_bytes,
        total_database_bytes,
        expected_entries: spec.expected_entries(),
        observed_entries,
        error: String::new(),
        validated: true,
    }
}

fn open_store(path: &std::path::Path) -> Result<SqliteStore, String> {
    SqliteStore::open(path).map_err(|error| error.to_string())
}
