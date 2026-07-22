use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::sync::Arc;
use std::time::Instant;

use prolly::{
    Config, Diff, Mutation, Prolly, ProllyMetricsSnapshot, SortedBatchBuilder, Tree, TreeStats,
};
use prolly_store_sqlite::{SqliteStore, SqliteStoreConfig};

use crate::fixture::{directory_bytes, sqlite_file_bytes, FixtureLayout};
use crate::measurement::{nearest_rank, rate, FixtureRow, RawRow, SCHEMA_VERSION};
use crate::model::{
    key, merge_ids, mutation_ids, range_bounds, range_ids, read_ids, value, CellSpec, FixtureSpec,
    Operation,
};

const BASE_ROOT: &[u8] = b"sqlite-scale/base";
const RESULT_ROOT: &[u8] = b"sqlite-scale/result";

type Manager = Prolly<Arc<SqliteStore>>;

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
    // About 5 KiB of logical leaf data per chunk for this 100-byte-value
    // comparison. This keeps the million-row tree at height three without the
    // oversized nodes that made cold SQLite reads and structural diffs slower.
    let config = Config::builder().chunking_factor(52).build();
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
    if loaded.root != tree.root {
        return Err("fixture root changed after reopen".to_string());
    }
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
    let loader = Prolly::new(store.clone(), Config::default());
    let base = loader
        .load_named_root(BASE_ROOT)
        .map_err(|error| format!("failed to load base root: {error}"))?
        .ok_or_else(|| "base root is missing".to_string())?;

    let outcome = match spec.operation {
        Operation::Build => return Err("build is measured by build_fixture".to_string()),
        Operation::Put => run_put(store.clone(), &base, spec)?,
        Operation::Batch => run_batch(store.clone(), &base, spec)?,
        Operation::GetCold | Operation::GetWarm => run_point_reads(store.clone(), &base, spec)?,
        Operation::Query => run_query(store.clone(), &base, spec)?,
        Operation::Scan | Operation::FullScan => run_scan(store.clone(), &base, spec)?,
        Operation::Diff => run_diff(store.clone(), &base, spec)?,
        Operation::Merge => run_merge(store.clone(), &base, spec)?,
    };
    let manager = manager(store.clone(), &outcome.tree);
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

    if matches!(
        spec.operation,
        Operation::Put | Operation::Batch | Operation::Merge
    ) {
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
    drop(loader);
    drop(manager);
    drop(store);

    if matches!(
        spec.operation,
        Operation::Put | Operation::Batch | Operation::Merge
    ) {
        let reopened_store = Arc::new(open_store(&database)?);
        let reopened = Prolly::new(reopened_store.clone(), outcome.tree.config.clone());
        let persisted = reopened
            .load_named_root(RESULT_ROOT)
            .map_err(|error| format!("failed to reload result root: {error}"))?
            .ok_or_else(|| "result root is missing after reopen".to_string())?;
        if persisted.root != outcome.tree.root {
            return Err("result root changed after reopen".to_string());
        }
        validate_tree(
            &reopened,
            &persisted,
            spec.expected_entries(),
            &outcome.changed_values,
        )?;
    }

    Ok(make_row(spec, &outcome, &stats, observed_entries, files))
}

struct Outcome {
    tree: Tree,
    changed_values: BTreeMap<usize, u8>,
    observed_items: usize,
    total_ns: u128,
    latencies: Vec<u128>,
    metrics: ProllyMetricsSnapshot,
}

fn run_put(store: Arc<SqliteStore>, base: &Tree, spec: &CellSpec) -> Result<Outcome, String> {
    let id = mutation_ids(spec.pattern, spec.records, 1, 1)[0];
    let manager = operation_manager(store, base)?;
    manager.reset_metrics();
    let started = Instant::now();
    let tree = manager
        .put(base, key(id), value(id, 1))
        .map_err(|error| format!("put failed for {id}: {error}"))?;
    let total_ns = started.elapsed().as_nanos().max(1);
    Ok(Outcome {
        tree,
        changed_values: [(id, 1)].into_iter().collect(),
        observed_items: 1,
        total_ns,
        latencies: vec![total_ns],
        metrics: manager.metrics(),
    })
}

fn run_batch(store: Arc<SqliteStore>, base: &Tree, spec: &CellSpec) -> Result<Outcome, String> {
    let ids = mutation_ids(spec.pattern, spec.records, spec.changes, 1);
    let mutations = mutations(&ids, 1);
    let manager = operation_manager(store, base)?;
    manager.reset_metrics();
    let started = Instant::now();
    let tree = manager
        .batch(base, mutations)
        .map_err(|error| format!("batch failed: {error}"))?;
    let total_ns = started.elapsed().as_nanos().max(1);
    Ok(Outcome {
        tree,
        changed_values: ids.iter().copied().map(|id| (id, 1)).collect(),
        observed_items: ids.len(),
        total_ns,
        latencies: Vec::new(),
        metrics: manager.metrics(),
    })
}

fn run_point_reads(
    store: Arc<SqliteStore>,
    base: &Tree,
    spec: &CellSpec,
) -> Result<Outcome, String> {
    let ids = read_ids(spec.pattern, spec.records, spec.read_samples);
    let manager = operation_manager(store, base)?;
    if spec.operation == Operation::GetWarm {
        for id in &ids {
            assert_value(&manager, base, *id, 0)?;
        }
    } else {
        manager
            .pin_tree_root(base)
            .map_err(|error| format!("failed to retain cold-read root: {error}"))?;
        manager.clear_unpinned_cache();
    }
    manager.reset_metrics();
    let mut observed = Vec::with_capacity(ids.len());
    let mut latencies = Vec::with_capacity(ids.len());
    let total_started = Instant::now();
    for id in &ids {
        if spec.operation == Operation::GetCold {
            manager.clear_unpinned_cache();
        }
        let started = Instant::now();
        observed.push(
            manager
                .get(base, &key(*id))
                .map_err(|error| format!("point get failed for {id}: {error}"))?,
        );
        latencies.push(started.elapsed().as_nanos().max(1));
    }
    let total_ns = total_started.elapsed().as_nanos().max(1);
    let metrics = manager.metrics();
    for (id, observed) in ids.iter().zip(observed) {
        if observed.as_deref() != Some(value(*id, 0).as_slice()) {
            return Err(format!("point get returned the wrong value for {id}"));
        }
    }
    Ok(Outcome {
        tree: base.clone(),
        changed_values: BTreeMap::new(),
        observed_items: ids.len(),
        total_ns,
        latencies,
        metrics,
    })
}

fn run_query(store: Arc<SqliteStore>, base: &Tree, spec: &CellSpec) -> Result<Outcome, String> {
    let ids = read_ids(spec.pattern, spec.records, spec.read_samples);
    let keys = ids.iter().map(|id| key(*id)).collect::<Vec<_>>();
    let manager = operation_manager(store, base)?;
    manager.reset_metrics();
    let mut observed_items = 0usize;
    let mut wrong_position = None;
    let started = Instant::now();
    manager
        .get_many_with(base, &keys, |position, observed_key, observed_value| {
            if observed_key != keys[position].as_slice() || observed_value.is_none() {
                wrong_position.get_or_insert(position);
            }
            observed_items += usize::from(observed_value.is_some());
        })
        .map_err(|error| format!("query failed: {error}"))?;
    let total_ns = started.elapsed().as_nanos().max(1);
    let metrics = manager.metrics();
    if let Some(position) = wrong_position {
        return Err(format!(
            "query returned the wrong result at position {position}"
        ));
    }
    if observed_items != ids.len() {
        return Err(format!(
            "query returned {observed_items} values, expected {}",
            ids.len()
        ));
    }
    for position in [0, ids.len() / 2, ids.len() - 1] {
        assert_value(&manager, base, ids[position], 0)?;
    }
    Ok(Outcome {
        tree: base.clone(),
        changed_values: BTreeMap::new(),
        observed_items,
        total_ns,
        latencies: Vec::new(),
        metrics,
    })
}

fn run_scan(store: Arc<SqliteStore>, base: &Tree, spec: &CellSpec) -> Result<Outcome, String> {
    let expected_ids = (spec.operation != Operation::FullScan)
        .then(|| range_ids(spec.pattern, spec.records, spec.read_samples));
    let expected_len = expected_ids.as_ref().map_or(spec.records, Vec::len);
    let bounds = if spec.operation == Operation::FullScan {
        (Vec::new(), None)
    } else {
        let (start, end) = range_bounds(spec.pattern, spec.records, spec.read_samples);
        (start, Some(end))
    };
    let manager = operation_manager(store, base)?;
    manager.reset_metrics();
    let started = Instant::now();
    let mut wrong = None;
    let mut callback_index = 0usize;
    let observed_items = manager
        .scan_range(base, &bounds.0, bounds.1.as_deref(), |entry| {
            if callback_index < expected_len {
                let id = expected_ids
                    .as_ref()
                    .map_or(callback_index, |ids| ids[callback_index]);
                if entry.key() != key(id) || entry.value() != value(id, 0) {
                    wrong.get_or_insert(id);
                }
            }
            callback_index += 1;
        })
        .map_err(|error| format!("scan failed: {error}"))? as usize;
    if callback_index > expected_len {
        return Err(format!("scan returned more than {expected_len} rows"));
    }
    let total_ns = started.elapsed().as_nanos().max(1);
    let metrics = manager.metrics();
    if let Some(id) = wrong {
        return Err(format!("scan returned the wrong record for {id}"));
    }
    if observed_items != expected_len {
        return Err(format!(
            "scan returned {observed_items} rows, expected {}",
            expected_len
        ));
    }
    Ok(Outcome {
        tree: base.clone(),
        changed_values: BTreeMap::new(),
        observed_items,
        total_ns,
        latencies: Vec::new(),
        metrics,
    })
}

fn run_diff(store: Arc<SqliteStore>, base: &Tree, spec: &CellSpec) -> Result<Outcome, String> {
    let ids = mutation_ids(spec.pattern, spec.records, spec.changes, 2);
    let manager = operation_manager(store, base)?;
    let changed = manager
        .batch(base, mutations(&ids, 1))
        .map_err(|error| format!("diff setup failed: {error}"))?;
    manager.reset_metrics();
    let started = Instant::now();
    let diffs = manager
        .diff(base, &changed)
        .map_err(|error| format!("diff failed: {error}"))?;
    let total_ns = started.elapsed().as_nanos().max(1);
    let metrics = manager.metrics();
    validate_diffs(&ids, &diffs)?;
    Ok(Outcome {
        tree: changed,
        changed_values: BTreeMap::new(),
        observed_items: diffs.len(),
        total_ns,
        latencies: Vec::new(),
        metrics,
    })
}

fn run_merge(store: Arc<SqliteStore>, base: &Tree, spec: &CellSpec) -> Result<Outcome, String> {
    let (left_ids, right_ids) = merge_ids(spec.records, spec.changes, spec.pattern);
    let manager = operation_manager(store, base)?;
    let build_branch = |ids: &[usize], generation| {
        let mutations = mutations(ids, generation);
        if spec.pattern == crate::model::Pattern::Clustered {
            manager.batch(base, mutations)
        } else {
            manager.batch_with_lineage(base, Arc::new(mutations))
        }
    };
    let left =
        build_branch(&left_ids, 1).map_err(|error| format!("left merge setup failed: {error}"))?;
    let right = build_branch(&right_ids, 2)
        .map_err(|error| format!("right merge setup failed: {error}"))?;
    manager.reset_metrics();
    let started = Instant::now();
    let merged = manager
        .merge(base, &left, &right, None)
        .map_err(|error| format!("merge failed: {error}"))?;
    let total_ns = started.elapsed().as_nanos().max(1);
    let metrics = manager.metrics();
    for id in &left_ids {
        assert_value(&manager, &merged, *id, 1)?;
    }
    for id in &right_ids {
        assert_value(&manager, &merged, *id, 2)?;
    }
    let changed_values = left_ids
        .iter()
        .copied()
        .map(|id| (id, 1))
        .chain(right_ids.iter().copied().map(|id| (id, 2)))
        .collect();
    Ok(Outcome {
        tree: merged,
        changed_values,
        observed_items: left_ids.len() + right_ids.len(),
        total_ns,
        latencies: Vec::new(),
        metrics,
    })
}

fn mutations(ids: &[usize], generation: u8) -> Vec<Mutation> {
    ids.iter()
        .map(|id| Mutation::Upsert {
            key: key(*id),
            val: value(*id, generation),
        })
        .collect()
}

fn validate_diffs(ids: &[usize], diffs: &[Diff]) -> Result<(), String> {
    let expected = ids.iter().map(|id| key(*id)).collect::<BTreeSet<_>>();
    let observed = diffs
        .iter()
        .map(|diff| diff.key().to_vec())
        .collect::<BTreeSet<_>>();
    if diffs.len() != ids.len() || observed != expected {
        return Err("diff key set differs from requested mutations".to_string());
    }
    for diff in diffs {
        match diff {
            Diff::Added {
                key: observed_key,
                val,
            } => {
                let id = parse_key(observed_key)?;
                if val != &value(id, 1) {
                    return Err(format!("diff added the wrong value for {id}"));
                }
            }
            Diff::Changed {
                key: observed_key,
                old,
                new,
            } => {
                let id = parse_key(observed_key)?;
                if old != &value(id, 0) || new != &value(id, 1) {
                    return Err(format!("diff changed the wrong value for {id}"));
                }
            }
            Diff::Removed { .. } => return Err("upsert workload produced a removal".to_string()),
        }
    }
    Ok(())
}

fn parse_key(bytes: &[u8]) -> Result<usize, String> {
    let text = std::str::from_utf8(bytes).map_err(|error| format!("invalid key: {error}"))?;
    text.strip_prefix("key-")
        .ok_or_else(|| "invalid key prefix".to_string())?
        .parse()
        .map_err(|error| format!("invalid key number: {error}"))
}

fn validate_tree(
    manager: &Manager,
    tree: &Tree,
    expected_entries: usize,
    changed_values: &BTreeMap<usize, u8>,
) -> Result<(), String> {
    let observed = manager
        .len(tree)
        .map_err(|error| format!("failed to count tree: {error}"))? as usize;
    if observed != expected_entries {
        return Err(format!(
            "tree contains {observed} entries, expected {expected_entries}"
        ));
    }
    if expected_entries > 0 {
        for id in [0, expected_entries / 2, expected_entries - 1] {
            let generation = changed_values.get(&id).copied().unwrap_or(0);
            assert_value(manager, tree, id, generation)?;
        }
    }
    for (id, generation) in changed_values {
        assert_value(manager, tree, *id, *generation)?;
    }
    Ok(())
}

fn assert_value(manager: &Manager, tree: &Tree, id: usize, generation: u8) -> Result<(), String> {
    let observed = manager
        .get(tree, &key(id))
        .map_err(|error| format!("failed to validate record {id}: {error}"))?;
    if observed.as_deref() != Some(value(id, generation).as_slice()) {
        return Err(format!("tree validation failed for record {id}"));
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
    let operations = spec.logical_operations();
    RawRow {
        schema: SCHEMA_VERSION.to_string(),
        revision: spec.revision.clone(),
        dirty: spec.dirty,
        records: spec.records,
        repetition: spec.repetition,
        operation: spec.operation,
        pattern: spec.pattern,
        cache_state: spec.cache_state,
        sample_count: outcome.latencies.len().max(1),
        logical_operations: operations,
        observed_items: outcome.observed_items,
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

fn manager(store: Arc<SqliteStore>, tree: &Tree) -> Manager {
    Prolly::new(store, tree.config.clone())
}

fn operation_manager(store: Arc<SqliteStore>, tree: &Tree) -> Result<Manager, String> {
    let manager = manager(store, tree);
    manager
        .pin_tree_root(tree)
        .map_err(|error| format!("failed to retain operation root: {error}"))?;
    Ok(manager)
}

fn open_store(path: &std::path::Path) -> Result<SqliteStore, String> {
    SqliteStore::open_with_config(
        path,
        SqliteStoreConfig {
            page_size_bytes: 32 * 1024,
            node_compression_min_bytes: 256,
            ..SqliteStoreConfig::default()
        },
    )
    .map_err(|error| error.to_string())
}
