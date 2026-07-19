use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Instant;

use prolly::{AsyncProlly, SnapshotBundle, Tree};
use prolly_store_turso::{TursoBackend, TursoStore};

use crate::model::{
    build_entries, expected_records, key, merge_ids, mutation_ids, mutations, percentile, root_hex,
    value, Api, CellSpec, ResultRow,
};

const BASE_ROOT: &[u8] = b"publication-bench-base";
const RESULT_ROOT: &[u8] = b"publication-bench-result";

struct Outcome {
    tree: Tree,
    expected_values: BTreeMap<usize, u8>,
    default_generation: u8,
    operations: usize,
    total_ns: u128,
    latencies: Vec<u128>,
}

pub async fn run(
    revision: &str,
    spec: CellSpec,
    fixture: &SnapshotBundle,
    database_path: &Path,
) -> Result<ResultRow, String> {
    if spec.api == Api::Reopen {
        return run_reopen(revision, spec, fixture, database_path).await;
    }

    let store = open(database_path).await?;
    let manager = AsyncProlly::new(store.clone(), prolly::Config::default());
    let base = manager
        .import_snapshot(fixture)
        .await
        .map_err(|error| format!("Turso fixture import failed: {error}"))?;
    manager
        .publish_named_root(BASE_ROOT, &base)
        .await
        .map_err(|error| format!("Turso base-root publish failed: {error}"))?;
    let outcome = execute(&manager, &base, spec).await?;
    let (node_count, byte_count) = validate(
        &manager,
        &outcome.tree,
        expected_records(&spec),
        &outcome.expected_values,
        outcome.default_generation,
        spec.records,
    )
    .await?;
    manager
        .publish_named_root(RESULT_ROOT, &outcome.tree)
        .await
        .map_err(|error| format!("Turso result-root publish failed: {error}"))?;
    let expected_root = outcome.tree.root.clone();
    drop(manager);
    drop(store);

    let reopened_store = open(database_path).await?;
    let reopened = AsyncProlly::new(reopened_store.clone(), prolly::Config::default());
    let persisted = reopened
        .load_named_root(RESULT_ROOT)
        .await
        .map_err(|error| format!("Turso result reopen failed: {error}"))?
        .ok_or_else(|| "Turso result root missing after reopen".to_string())?;
    if persisted.root != expected_root {
        return Err("Turso root changed after reopen".to_string());
    }
    validate(
        &reopened,
        &persisted,
        expected_records(&spec),
        &outcome.expected_values,
        outcome.default_generation,
        spec.records,
    )
    .await?;

    Ok(row_from_outcome(
        revision, spec, outcome, node_count, byte_count,
    ))
}

async fn run_reopen(
    revision: &str,
    spec: CellSpec,
    fixture: &SnapshotBundle,
    database_path: &Path,
) -> Result<ResultRow, String> {
    let store = open(database_path).await?;
    let manager = AsyncProlly::new(store.clone(), prolly::Config::default());
    let base = manager
        .import_snapshot(fixture)
        .await
        .map_err(|error| format!("Turso fixture import failed: {error}"))?;
    manager
        .publish_named_root(RESULT_ROOT, &base)
        .await
        .map_err(|error| format!("Turso root publish failed: {error}"))?;
    let expected_root = base.root.clone();
    drop(manager);
    drop(store);

    let started = Instant::now();
    let reopened_store = open(database_path).await?;
    let reopened = AsyncProlly::new(reopened_store.clone(), prolly::Config::default());
    let persisted = reopened
        .load_named_root(RESULT_ROOT)
        .await
        .map_err(|error| format!("Turso timed reopen failed: {error}"))?
        .ok_or_else(|| "Turso root missing during timed reopen".to_string())?;
    let total_ns = started.elapsed().as_nanos().max(1);
    if persisted.root != expected_root {
        return Err("Turso root changed during reopen".to_string());
    }
    let (node_count, byte_count) = validate(
        &reopened,
        &persisted,
        spec.records,
        &BTreeMap::new(),
        0,
        spec.records,
    )
    .await?;
    Ok(ResultRow {
        revision: revision.to_string(),
        adapter: spec.adapter,
        records: spec.records,
        changes: spec.changes,
        api: spec.api,
        pattern: spec.pattern,
        run: spec.run,
        total_ns,
        operations_per_sec: rate(1, total_ns),
        p50_ns: total_ns,
        p95_ns: total_ns,
        root: root_hex(&persisted),
        node_count,
        byte_count,
        value_valid: true,
        count_valid: true,
        root_valid: true,
        reopen_valid: true,
    })
}

async fn execute(
    manager: &AsyncProlly<TursoStore>,
    base: &Tree,
    spec: CellSpec,
) -> Result<Outcome, String> {
    match spec.api {
        Api::Put => {
            let ids = mutation_ids(spec.pattern, spec.records, spec.changes);
            let mut result = base.clone();
            let mut latencies = Vec::with_capacity(ids.len());
            let total_started = Instant::now();
            for id in &ids {
                let started = Instant::now();
                result = manager
                    .put(&result, key(*id), value(*id, 1))
                    .await
                    .map_err(|error| format!("Turso put failed: {error}"))?;
                latencies.push(started.elapsed().as_nanos().max(1));
            }
            Ok(Outcome {
                tree: result,
                expected_values: expected_values(&ids, 1),
                default_generation: 0,
                operations: ids.len(),
                total_ns: total_started.elapsed().as_nanos().max(1),
                latencies,
            })
        }
        Api::Batch => {
            let ids = mutation_ids(spec.pattern, spec.records, spec.changes);
            let started = Instant::now();
            let tree = manager
                .batch(base, mutations(&ids, 1))
                .await
                .map_err(|error| format!("Turso batch failed: {error}"))?;
            Ok(single_call(
                tree,
                expected_values(&ids, 1),
                0,
                ids.len(),
                started,
            ))
        }
        Api::Build => {
            let entries = build_entries(spec.pattern, spec.records);
            let started = Instant::now();
            let tree = manager
                .build_from_entries(entries)
                .await
                .map_err(|error| format!("Turso build failed: {error}"))?;
            Ok(single_call(tree, BTreeMap::new(), 4, spec.records, started))
        }
        Api::Diff => {
            let ids = mutation_ids(spec.pattern, spec.records, spec.changes);
            let changed = manager
                .batch(base, mutations(&ids, 1))
                .await
                .map_err(|error| format!("Turso diff fixture failed: {error}"))?;
            let expected_keys = ids.iter().map(|id| key(*id)).collect::<BTreeSet<_>>();
            let started = Instant::now();
            let diffs = manager
                .diff(base, &changed)
                .await
                .map_err(|error| format!("Turso diff failed: {error}"))?;
            let total_ns = started.elapsed().as_nanos().max(1);
            let actual_keys = diffs
                .iter()
                .map(|diff| diff.key().to_vec())
                .collect::<BTreeSet<_>>();
            if actual_keys != expected_keys || diffs.len() != ids.len() {
                return Err(format!(
                    "Turso diff returned {} changes, expected {}",
                    diffs.len(),
                    ids.len()
                ));
            }
            Ok(Outcome {
                tree: changed,
                expected_values: expected_values(&ids, 1),
                default_generation: 0,
                operations: diffs.len(),
                total_ns,
                latencies: vec![total_ns],
            })
        }
        Api::Merge => {
            let (left_ids, right_ids) = merge_ids(spec.pattern, spec.records, spec.changes);
            let left = manager
                .batch(base, mutations(&left_ids, 1))
                .await
                .map_err(|error| format!("Turso left branch failed: {error}"))?;
            let right = manager
                .batch(base, mutations(&right_ids, 2))
                .await
                .map_err(|error| format!("Turso right branch failed: {error}"))?;
            let started = Instant::now();
            let tree = manager
                .merge(base, &left, &right, None)
                .await
                .map_err(|error| format!("Turso merge failed: {error}"))?;
            let mut values = expected_values(&left_ids, 1);
            values.extend(expected_values(&right_ids, 2));
            Ok(single_call(
                tree,
                values,
                0,
                left_ids.len() + right_ids.len(),
                started,
            ))
        }
        Api::Reopen => unreachable!("reopen handled separately"),
    }
}

fn single_call(
    tree: Tree,
    expected_values: BTreeMap<usize, u8>,
    default_generation: u8,
    operations: usize,
    started: Instant,
) -> Outcome {
    let total_ns = started.elapsed().as_nanos().max(1);
    Outcome {
        tree,
        expected_values,
        default_generation,
        operations,
        total_ns,
        latencies: vec![total_ns],
    }
}

async fn validate(
    manager: &AsyncProlly<TursoStore>,
    tree: &Tree,
    expected_count: usize,
    expected_values: &BTreeMap<usize, u8>,
    default_generation: u8,
    base_records: usize,
) -> Result<(usize, usize), String> {
    let stats = manager
        .collect_stats(tree)
        .await
        .map_err(|error| format!("Turso count validation failed: {error}"))?;
    if stats.total_key_value_pairs != expected_count {
        return Err(format!(
            "Turso tree has {} records, expected {expected_count}",
            stats.total_key_value_pairs
        ));
    }
    for (id, generation) in expected_values {
        expect_value(manager, tree, *id, *generation).await?;
    }
    if base_records > 0 {
        for id in [0, base_records / 2, base_records - 1] {
            let generation = expected_values
                .get(&id)
                .copied()
                .unwrap_or(default_generation);
            expect_value(manager, tree, id, generation).await?;
        }
    }
    let bundle = manager
        .export_snapshot(tree)
        .await
        .map_err(|error| format!("Turso snapshot export failed: {error}"))?;
    let verification = bundle
        .verify()
        .map_err(|error| format!("Turso snapshot verification failed: {error}"))?;
    if !verification.valid {
        return Err("Turso snapshot reachability verification failed".to_string());
    }
    Ok((bundle.node_count(), bundle.byte_count()))
}

async fn expect_value(
    manager: &AsyncProlly<TursoStore>,
    tree: &Tree,
    id: usize,
    generation: u8,
) -> Result<(), String> {
    let actual = manager
        .get(tree, &key(id))
        .await
        .map_err(|error| format!("Turso key {id} validation failed: {error}"))?;
    if actual != Some(value(id, generation)) {
        return Err(format!("Turso key {id} has an unexpected value"));
    }
    Ok(())
}

async fn open(path: &Path) -> Result<TursoStore, String> {
    let backend = TursoBackend::open(path)
        .await
        .map_err(|error| format!("failed to open local Turso database: {error}"))?;
    if backend.is_synced() {
        return Err("refusing to benchmark a cloud-synchronized Turso database".to_string());
    }
    Ok(TursoStore::new(backend))
}

fn expected_values(ids: &[usize], generation: u8) -> BTreeMap<usize, u8> {
    ids.iter().map(|id| (*id, generation)).collect()
}

fn row_from_outcome(
    revision: &str,
    spec: CellSpec,
    outcome: Outcome,
    node_count: usize,
    byte_count: usize,
) -> ResultRow {
    ResultRow {
        revision: revision.to_string(),
        adapter: spec.adapter,
        records: spec.records,
        changes: spec.changes,
        api: spec.api,
        pattern: spec.pattern,
        run: spec.run,
        total_ns: outcome.total_ns,
        operations_per_sec: rate(outcome.operations, outcome.total_ns),
        p50_ns: percentile(&outcome.latencies, 50),
        p95_ns: percentile(&outcome.latencies, 95),
        root: root_hex(&outcome.tree),
        node_count,
        byte_count,
        value_valid: true,
        count_valid: true,
        root_valid: true,
        reopen_valid: true,
    }
}

fn rate(operations: usize, total_ns: u128) -> f64 {
    operations as f64 / (total_ns as f64 / 1_000_000_000.0)
}
