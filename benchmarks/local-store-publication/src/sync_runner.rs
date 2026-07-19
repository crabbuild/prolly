use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Instant;

use prolly::{ManifestStore, Prolly, SnapshotBundle, Store, Tree};

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

pub fn run<S, Open>(
    revision: &str,
    spec: CellSpec,
    fixture: &SnapshotBundle,
    open: Open,
) -> Result<ResultRow, String>
where
    S: Store + ManifestStore<Error = <S as Store>::Error>,
    Open: Fn() -> Result<Arc<S>, String>,
{
    if spec.api == Api::Reopen {
        return run_reopen(revision, spec, fixture, open);
    }

    let store = open()?;
    let manager = Prolly::new(store.clone(), prolly::Config::default());
    let base = manager
        .import_snapshot(fixture)
        .map_err(|error| format!("{} fixture import failed: {error}", spec.adapter.as_str()))?;
    manager
        .publish_named_root(BASE_ROOT, &base)
        .map_err(|error| {
            format!(
                "{} base-root publish failed: {error}",
                spec.adapter.as_str()
            )
        })?;
    let outcome = execute(&manager, &base, spec)?;
    let (node_count, byte_count) = validate(
        &manager,
        &outcome.tree,
        expected_records(&spec),
        &outcome.expected_values,
        outcome.default_generation,
        spec.records,
    )?;
    manager
        .publish_named_root(RESULT_ROOT, &outcome.tree)
        .map_err(|error| {
            format!(
                "{} result-root publish failed: {error}",
                spec.adapter.as_str()
            )
        })?;
    let expected_root = outcome.tree.root.clone();
    drop(manager);
    drop(store);

    let reopened_store = open()?;
    let reopened = Prolly::new(reopened_store.clone(), prolly::Config::default());
    let persisted = reopened
        .load_named_root(RESULT_ROOT)
        .map_err(|error| format!("{} result reopen failed: {error}", spec.adapter.as_str()))?
        .ok_or_else(|| format!("{} result root missing after reopen", spec.adapter.as_str()))?;
    if persisted.root != expected_root {
        return Err(format!(
            "{} root changed after reopen",
            spec.adapter.as_str()
        ));
    }
    validate(
        &reopened,
        &persisted,
        expected_records(&spec),
        &outcome.expected_values,
        outcome.default_generation,
        spec.records,
    )?;

    Ok(row_from_outcome(
        revision, spec, outcome, node_count, byte_count,
    ))
}

fn run_reopen<S, Open>(
    revision: &str,
    spec: CellSpec,
    fixture: &SnapshotBundle,
    open: Open,
) -> Result<ResultRow, String>
where
    S: Store + ManifestStore<Error = <S as Store>::Error>,
    Open: Fn() -> Result<Arc<S>, String>,
{
    let store = open()?;
    let manager = Prolly::new(store.clone(), prolly::Config::default());
    let base = manager
        .import_snapshot(fixture)
        .map_err(|error| format!("{} fixture import failed: {error}", spec.adapter.as_str()))?;
    manager
        .publish_named_root(RESULT_ROOT, &base)
        .map_err(|error| format!("{} root publish failed: {error}", spec.adapter.as_str()))?;
    let expected_root = base.root.clone();
    drop(manager);
    drop(store);

    let started = Instant::now();
    let reopened_store = open()?;
    let reopened = Prolly::new(reopened_store.clone(), prolly::Config::default());
    let persisted = reopened
        .load_named_root(RESULT_ROOT)
        .map_err(|error| format!("{} timed reopen failed: {error}", spec.adapter.as_str()))?
        .ok_or_else(|| format!("{} root missing during timed reopen", spec.adapter.as_str()))?;
    let total_ns = started.elapsed().as_nanos().max(1);
    if persisted.root != expected_root {
        return Err(format!(
            "{} root changed during reopen",
            spec.adapter.as_str()
        ));
    }
    let (node_count, byte_count) = validate(
        &reopened,
        &persisted,
        spec.records,
        &BTreeMap::new(),
        0,
        spec.records,
    )?;
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

fn execute<S>(manager: &Prolly<Arc<S>>, base: &Tree, spec: CellSpec) -> Result<Outcome, String>
where
    S: Store,
{
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
                    .map_err(|error| format!("{} put failed: {error}", spec.adapter.as_str()))?;
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
                .map_err(|error| format!("{} batch failed: {error}", spec.adapter.as_str()))?;
            single_call(tree, expected_values(&ids, 1), 0, ids.len(), started)
        }
        Api::Build => {
            let entries = build_entries(spec.pattern, spec.records);
            let started = Instant::now();
            let tree = manager
                .build_from_entries(entries)
                .map_err(|error| format!("{} build failed: {error}", spec.adapter.as_str()))?;
            single_call(tree, BTreeMap::new(), 4, spec.records, started)
        }
        Api::Diff => {
            let ids = mutation_ids(spec.pattern, spec.records, spec.changes);
            let changed = manager.batch(base, mutations(&ids, 1)).map_err(|error| {
                format!("{} diff fixture failed: {error}", spec.adapter.as_str())
            })?;
            let expected_keys = ids.iter().map(|id| key(*id)).collect::<BTreeSet<_>>();
            let started = Instant::now();
            let diffs = manager
                .diff(base, &changed)
                .map_err(|error| format!("{} diff failed: {error}", spec.adapter.as_str()))?;
            let total_ns = started.elapsed().as_nanos().max(1);
            let actual_keys = diffs
                .iter()
                .map(|diff| diff.key().to_vec())
                .collect::<BTreeSet<_>>();
            if actual_keys != expected_keys || diffs.len() != ids.len() {
                return Err(format!(
                    "{} diff returned {} changes, expected {}",
                    spec.adapter.as_str(),
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
                .map_err(|error| {
                    format!("{} left branch failed: {error}", spec.adapter.as_str())
                })?;
            let right = manager
                .batch(base, mutations(&right_ids, 2))
                .map_err(|error| {
                    format!("{} right branch failed: {error}", spec.adapter.as_str())
                })?;
            let started = Instant::now();
            let tree = manager
                .merge(base, &left, &right, None)
                .map_err(|error| format!("{} merge failed: {error}", spec.adapter.as_str()))?;
            let mut values = expected_values(&left_ids, 1);
            values.extend(expected_values(&right_ids, 2));
            single_call(tree, values, 0, left_ids.len() + right_ids.len(), started)
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
) -> Result<Outcome, String> {
    let total_ns = started.elapsed().as_nanos().max(1);
    Ok(Outcome {
        tree,
        expected_values,
        default_generation,
        operations,
        total_ns,
        latencies: vec![total_ns],
    })
}

fn validate<S>(
    manager: &Prolly<Arc<S>>,
    tree: &Tree,
    expected_count: usize,
    expected_values: &BTreeMap<usize, u8>,
    default_generation: u8,
    base_records: usize,
) -> Result<(usize, usize), String>
where
    S: Store,
{
    let count = manager
        .len(tree)
        .map_err(|error| format!("tree count validation failed: {error}"))?
        as usize;
    if count != expected_count {
        return Err(format!(
            "tree has {count} records, expected {expected_count}"
        ));
    }
    for (id, generation) in expected_values {
        expect_value(manager, tree, *id, *generation)?;
    }
    if base_records > 0 {
        for id in [0, base_records / 2, base_records - 1] {
            let generation = expected_values
                .get(&id)
                .copied()
                .unwrap_or(default_generation);
            expect_value(manager, tree, id, generation)?;
        }
    }
    let bundle = manager
        .export_snapshot(tree)
        .map_err(|error| format!("snapshot export failed: {error}"))?;
    let verification = bundle
        .verify()
        .map_err(|error| format!("snapshot verification failed: {error}"))?;
    if !verification.valid {
        return Err("snapshot reachability verification failed".to_string());
    }
    Ok((bundle.node_count(), bundle.byte_count()))
}

fn expect_value<S>(
    manager: &Prolly<Arc<S>>,
    tree: &Tree,
    id: usize,
    generation: u8,
) -> Result<(), String>
where
    S: Store,
{
    let actual = manager
        .get(tree, &key(id))
        .map_err(|error| format!("key {id} validation failed: {error}"))?;
    if actual != Some(value(id, generation)) {
        return Err(format!("key {id} has an unexpected value"));
    }
    Ok(())
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
