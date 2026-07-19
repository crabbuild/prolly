//! Native asynchronous Turso fixture and benchmark-cell execution.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::time::Instant;

use prolly::{AsyncProlly, AsyncStore, Config, Mutation, Node, Tree};
use prolly_store_turso::{TursoBackend, TursoStore};

use crate::fixture::{directory_bytes, FixtureLayout};
use crate::measurement::{nearest_rank, FixtureRow, RawRow, SCHEMA_VERSION};
use crate::model::{key, merge_ids, mutation_ids, value, Adapter, Api, CellSpec, FixtureSpec};
use crate::sqlite_runner::{BASE_ROOT_NAME, RESULT_ROOT_NAME};

pub async fn build_turso_fixture(
    spec: &FixtureSpec,
    layout: &FixtureLayout,
) -> Result<FixtureRow, String> {
    if spec.adapter != Adapter::TursoAsync {
        return Err("Turso fixture received a non-Turso specification".to_string());
    }
    if layout.source_dir().exists() {
        return Err(format!(
            "fixture source already exists: {}",
            layout.source_dir().display()
        ));
    }
    fs::create_dir_all(layout.source_dir()).map_err(|error| {
        format!(
            "failed to create Turso fixture {}: {error}",
            layout.source_dir().display()
        )
    })?;

    let store = open_turso(&layout.source_database()).await?;
    let prolly = AsyncProlly::new(store.clone(), bench_config());
    let mut tree = prolly.create();
    let started = Instant::now();
    let batch_size = spec.build_batch_size.max(1);
    for start in (0..spec.records).step_by(batch_size) {
        let end = start.saturating_add(batch_size).min(spec.records);
        tree = prolly
            .batch(&tree, mutations(start..end, 0))
            .await
            .map_err(|error| format!("failed to build Turso fixture: {error}"))?;
    }
    let build_ns = started.elapsed().as_nanos().max(1);
    prolly
        .publish_named_root(BASE_ROOT_NAME, &tree)
        .await
        .map_err(|error| format!("failed to publish Turso fixture root: {error}"))?;
    validate_async_tree(&prolly, &tree, spec.records, &BTreeMap::new()).await?;
    drop(prolly);
    drop(store);

    let reopened_store = open_turso(&layout.source_database()).await?;
    let reopened = AsyncProlly::new(reopened_store.clone(), bench_config());
    let loaded = reopened
        .load_named_root(BASE_ROOT_NAME)
        .await
        .map_err(|error| format!("failed to reopen Turso fixture root: {error}"))?
        .ok_or_else(|| "Turso fixture root is missing after reopen".to_string())?;
    validate_async_tree(&reopened, &loaded, spec.records, &BTreeMap::new()).await?;
    drop(reopened);
    drop(reopened_store);

    Ok(FixtureRow {
        schema: SCHEMA_VERSION.to_string(),
        revision: spec.revision.clone(),
        dirty: spec.dirty,
        adapter: spec.adapter,
        records: spec.records,
        repetition: spec.repetition,
        build_ns,
        records_per_sec: rate(spec.records, build_ns),
        database_bytes: directory_bytes(&layout.source_dir())?,
        observed_records: spec.records,
        validated: true,
        error: String::new(),
    })
}

pub async fn run_turso_cell(spec: &CellSpec, layout: &FixtureLayout) -> Result<RawRow, String> {
    if spec.adapter != Adapter::TursoAsync {
        return Err("Turso runner received a non-Turso cell".to_string());
    }
    let cell_dir = layout.cell_dir(spec.api, spec.pattern);
    let database_path = layout.cell_database(spec.api, spec.pattern);
    let db_bytes_before = directory_bytes(&cell_dir)?;
    let store = open_turso(&database_path).await?;
    let config = bench_config();
    let base_manager = AsyncProlly::new(store.clone(), config.clone());
    let base = base_manager
        .load_named_root(BASE_ROOT_NAME)
        .await
        .map_err(|error| format!("failed to load Turso base root: {error}"))?
        .ok_or_else(|| "Turso base root is missing".to_string())?;

    let outcome = match spec.api {
        Api::Put => run_put(&base_manager, &base, spec).await?,
        Api::Batch => run_batch(&base_manager, &base, spec).await?,
        Api::Diff => {
            let ids = mutation_ids(
                spec.pattern,
                spec.records,
                spec.changes,
                crate::model::RANDOM_SEED,
            );
            let changed = base_manager
                .batch(&base, mutations_for_ids(&ids, 1))
                .await
                .map_err(|error| format!("failed to prepare Turso diff tree: {error}"))?;
            drop(base_manager);
            let timed_manager = AsyncProlly::new(store.clone(), config.clone());
            run_diff(
                &timed_manager,
                &base,
                &changed,
                ids,
                spec.measurement_samples,
            )
            .await?
        }
        Api::Merge => {
            let (left_ids, right_ids) = merge_ids(
                spec.pattern,
                spec.records,
                spec.changes,
                crate::model::RANDOM_SEED,
            );
            let left = base_manager
                .batch(&base, mutations_for_ids(&left_ids, 1))
                .await
                .map_err(|error| format!("failed to prepare Turso left branch: {error}"))?;
            let right = base_manager
                .batch(&base, mutations_for_ids(&right_ids, 2))
                .await
                .map_err(|error| format!("failed to prepare Turso right branch: {error}"))?;
            drop(base_manager);
            let timed_manager = AsyncProlly::new(store.clone(), config.clone());
            run_merge(
                &timed_manager,
                &base,
                &left,
                &right,
                left_ids,
                right_ids,
                spec.measurement_samples,
            )
            .await?
        }
    };

    let result_manager = AsyncProlly::new(store.clone(), config.clone());
    validate_async_tree(
        &result_manager,
        &outcome.result,
        spec.expected_records(),
        &outcome.changed_values,
    )
    .await?;
    result_manager
        .publish_named_root(RESULT_ROOT_NAME, &outcome.result)
        .await
        .map_err(|error| format!("failed to publish Turso result root: {error}"))?;
    drop(result_manager);
    drop(store);

    let reopened_store = open_turso(&database_path).await?;
    let reopened = AsyncProlly::new(reopened_store.clone(), config);
    let persisted = reopened
        .load_named_root(RESULT_ROOT_NAME)
        .await
        .map_err(|error| format!("failed to reopen Turso result root: {error}"))?
        .ok_or_else(|| "Turso result root is missing after reopen".to_string())?;
    validate_async_tree(
        &reopened,
        &persisted,
        spec.expected_records(),
        &outcome.changed_values,
    )
    .await?;
    let observed_records = async_tree_len(&reopened, &persisted).await?;
    drop(reopened);
    drop(reopened_store);

    Ok(RawRow {
        schema: SCHEMA_VERSION.to_string(),
        revision: spec.revision.clone(),
        dirty: spec.dirty,
        adapter: spec.adapter,
        records: spec.records,
        repetition: spec.repetition,
        api: spec.api,
        pattern: spec.pattern,
        configured_changes: spec.changes,
        observed_changes: outcome.observed_changes,
        total_ns: outcome.total_ns,
        operations_per_sec: rate(outcome.observed_changes, outcome.total_ns),
        p50_ns: outcome.p50_ns,
        p95_ns: outcome.p95_ns,
        p99_ns: outcome.p99_ns,
        max_ns: outcome.max_ns,
        db_bytes_before,
        db_bytes_after: directory_bytes(&cell_dir)?,
        expected_records: spec.expected_records(),
        observed_records,
        validated: true,
        error: String::new(),
    })
}

struct CellOutcome {
    result: Tree,
    changed_values: BTreeMap<usize, u8>,
    observed_changes: usize,
    total_ns: u128,
    p50_ns: Option<u128>,
    p95_ns: Option<u128>,
    p99_ns: Option<u128>,
    max_ns: Option<u128>,
}

async fn run_put(
    prolly: &AsyncProlly<TursoStore>,
    base: &Tree,
    spec: &CellSpec,
) -> Result<CellOutcome, String> {
    let ids = mutation_ids(
        spec.pattern,
        spec.records,
        spec.changes,
        crate::model::RANDOM_SEED,
    );
    let mut result = base.clone();
    let mut latencies = Vec::with_capacity(ids.len().saturating_mul(spec.measurement_samples));
    let mut totals = Vec::with_capacity(spec.measurement_samples);
    for sample in 0..spec.measurement_samples {
        prolly.clear_cache();
        let generation = sample_generation(sample);
        let mut sample_result = base.clone();
        let total_started = Instant::now();
        for id in &ids {
            let started = Instant::now();
            sample_result = prolly
                .put(&sample_result, key(*id), value(*id, generation))
                .await
                .map_err(|error| format!("Turso put failed: {error}"))?;
            latencies.push(started.elapsed().as_nanos().max(1));
        }
        totals.push(total_started.elapsed().as_nanos().max(1));
        result = sample_result;
    }
    let generation = sample_generation(spec.measurement_samples - 1);
    Ok(CellOutcome {
        result,
        changed_values: expected_values(&ids, generation),
        observed_changes: ids.len(),
        total_ns: nearest_rank(&totals, 0.50).unwrap_or(1),
        p50_ns: nearest_rank(&latencies, 0.50),
        p95_ns: nearest_rank(&latencies, 0.95),
        p99_ns: nearest_rank(&latencies, 0.99),
        max_ns: latencies.iter().max().copied(),
    })
}

async fn run_batch(
    prolly: &AsyncProlly<TursoStore>,
    base: &Tree,
    spec: &CellSpec,
) -> Result<CellOutcome, String> {
    let ids = mutation_ids(
        spec.pattern,
        spec.records,
        spec.changes,
        crate::model::RANDOM_SEED,
    );
    let mut totals = Vec::with_capacity(spec.measurement_samples);
    let mut result = base.clone();
    for sample in 0..spec.measurement_samples {
        prolly.clear_cache();
        let generation = sample_generation(sample);
        let started = Instant::now();
        result = prolly
            .batch(base, mutations_for_ids(&ids, generation))
            .await
            .map_err(|error| format!("Turso batch failed: {error}"))?;
        totals.push(started.elapsed().as_nanos().max(1));
    }
    Ok(sampled_call_outcome(
        result,
        expected_values(&ids, sample_generation(spec.measurement_samples - 1)),
        ids.len(),
        &totals,
    ))
}

async fn run_diff(
    prolly: &AsyncProlly<TursoStore>,
    base: &Tree,
    changed: &Tree,
    ids: Vec<usize>,
    measurement_samples: usize,
) -> Result<CellOutcome, String> {
    let expected_keys = ids.iter().map(|id| key(*id)).collect::<BTreeSet<_>>();
    let mut totals = Vec::with_capacity(measurement_samples);
    for _ in 0..measurement_samples {
        prolly.clear_cache();
        let started = Instant::now();
        let diffs = prolly
            .diff(base, changed)
            .await
            .map_err(|error| format!("Turso diff failed: {error}"))?;
        totals.push(started.elapsed().as_nanos().max(1));
        let observed_keys = diffs
            .iter()
            .map(|diff| diff.key().to_vec())
            .collect::<BTreeSet<_>>();
        if observed_keys != expected_keys || diffs.len() != ids.len() {
            return Err(format!(
                "Turso diff returned {} changes, expected {}",
                diffs.len(),
                ids.len()
            ));
        }
    }
    Ok(sampled_call_outcome(
        changed.clone(),
        expected_values(&ids, 1),
        ids.len(),
        &totals,
    ))
}

async fn run_merge(
    prolly: &AsyncProlly<TursoStore>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    left_ids: Vec<usize>,
    right_ids: Vec<usize>,
    measurement_samples: usize,
) -> Result<CellOutcome, String> {
    let mut totals = Vec::with_capacity(measurement_samples);
    let mut result = base.clone();
    for _ in 0..measurement_samples {
        prolly.clear_cache();
        let started = Instant::now();
        result = prolly
            .merge(base, left, right, None)
            .await
            .map_err(|error| format!("Turso merge failed: {error}"))?;
        totals.push(started.elapsed().as_nanos().max(1));
    }
    let mut changed_values = expected_values(&left_ids, 1);
    changed_values.extend(expected_values(&right_ids, 2));
    Ok(sampled_call_outcome(
        result,
        changed_values,
        left_ids.len().saturating_add(right_ids.len()),
        &totals,
    ))
}

fn sampled_call_outcome(
    result: Tree,
    changed_values: BTreeMap<usize, u8>,
    observed_changes: usize,
    totals: &[u128],
) -> CellOutcome {
    CellOutcome {
        result,
        changed_values,
        observed_changes,
        total_ns: nearest_rank(totals, 0.50).unwrap_or(1),
        p50_ns: nearest_rank(totals, 0.50),
        p95_ns: nearest_rank(totals, 0.95),
        p99_ns: nearest_rank(totals, 0.99),
        max_ns: totals.iter().max().copied(),
    }
}

fn sample_generation(sample: usize) -> u8 {
    u8::try_from(sample + 1).expect("validated measurement sample count")
}

async fn validate_async_tree(
    prolly: &AsyncProlly<TursoStore>,
    tree: &Tree,
    expected_records: usize,
    changed_values: &BTreeMap<usize, u8>,
) -> Result<(), String> {
    let observed = async_tree_len(prolly, tree).await?;
    if observed != expected_records {
        return Err(format!(
            "Turso tree contains {observed} records, expected {expected_records}"
        ));
    }
    for (id, generation) in changed_values {
        expect_async_value(prolly, tree, *id, *generation).await?;
    }
    for id in sample_ids(expected_records) {
        let generation = changed_values.get(&id).copied().unwrap_or(0);
        expect_async_value(prolly, tree, id, generation).await?;
    }
    Ok(())
}

async fn async_tree_len(prolly: &AsyncProlly<TursoStore>, tree: &Tree) -> Result<usize, String> {
    let Some(root) = tree.root.as_ref() else {
        return Ok(0);
    };
    let bytes = prolly
        .store()
        .get(root.as_bytes())
        .await
        .map_err(|error| format!("failed to load Turso root node: {error}"))?
        .ok_or_else(|| "Turso root node is missing".to_string())?;
    let node = Node::from_bytes_with_format(&bytes, &tree.config.format)
        .map_err(|error| format!("failed to decode Turso root node: {error}"))?;
    if node.leaf {
        return Ok(node.len());
    }
    if node.child_counts.len() == node.len() && !node.child_counts.contains(&0) {
        return node
            .child_counts
            .iter()
            .try_fold(0usize, |total, count| {
                usize::try_from(*count)
                    .ok()
                    .and_then(|count| total.checked_add(count))
            })
            .ok_or_else(|| "Turso tree count exceeds usize".to_string());
    }
    Ok(prolly
        .collect_stats(tree)
        .await
        .map_err(|error| format!("failed to count legacy Turso tree: {error}"))?
        .total_key_value_pairs)
}

async fn expect_async_value(
    prolly: &AsyncProlly<TursoStore>,
    tree: &Tree,
    id: usize,
    generation: u8,
) -> Result<(), String> {
    let actual = prolly
        .get(tree, &key(id))
        .await
        .map_err(|error| format!("failed to read Turso key {id}: {error}"))?;
    if actual != Some(value(id, generation)) {
        return Err(format!("Turso value mismatch for key {id}"));
    }
    Ok(())
}

fn sample_ids(records: usize) -> Vec<usize> {
    if records == 0 {
        Vec::new()
    } else {
        vec![0, records / 2, records - 1]
    }
}

fn expected_values(ids: &[usize], generation: u8) -> BTreeMap<usize, u8> {
    ids.iter().map(|id| (*id, generation)).collect()
}

fn mutations_for_ids(ids: &[usize], generation: u8) -> Vec<Mutation> {
    ids.iter()
        .map(|id| Mutation::Upsert {
            key: key(*id),
            val: value(*id, generation),
        })
        .collect()
}

fn mutations(ids: impl IntoIterator<Item = usize>, generation: u8) -> Vec<Mutation> {
    ids.into_iter()
        .map(|id| Mutation::Upsert {
            key: key(id),
            val: value(id, generation),
        })
        .collect()
}

async fn open_turso(path: &std::path::Path) -> Result<TursoStore, String> {
    let backend = TursoBackend::open(path)
        .await
        .map_err(|error| format!("failed to open Turso database {}: {error}", path.display()))?;
    Ok(TursoStore::new(backend))
}

fn bench_config() -> Config {
    Config::builder()
        .min_chunk_size(64)
        .max_chunk_size(512)
        .chunking_factor(256)
        .hash_seed(0xC0DA)
        .build()
}

fn rate(operations: usize, nanoseconds: u128) -> f64 {
    operations as f64 * 1_000_000_000.0 / nanoseconds.max(1) as f64
}

#[cfg(test)]
mod tests {
    use crate::fixture::{clone_fixture, remove_cell_dir};
    use crate::model::{Adapter, Api, Pattern};

    use super::*;

    fn fixture_spec() -> FixtureSpec {
        FixtureSpec {
            adapter: Adapter::TursoAsync,
            records: 100,
            repetition: 1,
            revision: "test".to_string(),
            dirty: false,
            build_batch_size: 25,
        }
    }

    fn cell_spec(api: Api, pattern: Pattern) -> CellSpec {
        CellSpec {
            adapter: Adapter::TursoAsync,
            records: 100,
            repetition: 1,
            api,
            pattern,
            changes: 10,
            revision: "test".to_string(),
            dirty: false,
            measurement_samples: 3,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn turso_fixture_reopens_with_named_root_and_expected_count() {
        let temp = tempfile::tempdir().unwrap();
        let layout = FixtureLayout::new(temp.path().to_path_buf(), Adapter::TursoAsync, 100, 1);

        let row = build_turso_fixture(&fixture_spec(), &layout).await.unwrap();

        assert!(row.validated, "{}", row.error);
        assert_eq!(row.observed_records, 100);
        assert!(layout.source_database().exists());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn turso_cells_validate_all_apis_and_patterns() {
        let temp = tempfile::tempdir().unwrap();
        let layout = FixtureLayout::new(temp.path().to_path_buf(), Adapter::TursoAsync, 100, 1);
        build_turso_fixture(&fixture_spec(), &layout).await.unwrap();

        for api in Api::ALL {
            for pattern in Pattern::ALL {
                let cell_dir = layout.cell_dir(api, pattern);
                clone_fixture(&layout.source_dir(), &cell_dir).unwrap();
                let spec = cell_spec(api, pattern);
                let row = run_turso_cell(&spec, &layout).await.unwrap();
                assert!(row.validated, "{api:?}/{pattern:?}: {}", row.error);
                assert_eq!(row.observed_changes, spec.expected_operations());
                assert_eq!(row.observed_records, spec.expected_records());
                assert!(row.p50_ns.is_some());
                assert!(row.p95_ns.is_some());
                assert!(row.p99_ns.is_some());
                assert!(row.max_ns.is_some());
                remove_cell_dir(&layout, &cell_dir).unwrap();
            }
        }
    }
}
