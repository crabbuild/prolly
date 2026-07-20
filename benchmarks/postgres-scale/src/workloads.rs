use std::collections::BTreeSet;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use prolly::{
    AsyncProlly, Config, Diff, Mutation, ProllyMetricsSnapshot, RemoteProllyStore, Tree, TreeStats,
};
use prolly_store_postgres::{PostgresBackend, PostgresStore};

use crate::measurement::{percentile, PgMetrics, PhysicalSize, RawRow, SCHEMA_VERSION};
use crate::model::{key, merge_ids, pattern_ids, value, Operation, Pattern};
use crate::postgres::{
    clear_all, initialize_benchmark_schema, read_pg_metrics, read_physical_size, reset_pg_stats,
    restore_base, snapshot_base,
};

const BASE_ROOT: &[u8] = b"benchmark/base";

#[derive(Clone, Debug)]
pub struct RunMeta {
    pub revision: String,
    pub dirty: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct CellSpec {
    pub operation: Operation,
    pub pattern: Pattern,
    pub repetition: u32,
    pub changes: usize,
    pub read_samples: usize,
}

#[derive(Clone, Debug)]
pub struct Fixture {
    backend: PostgresBackend,
    base: Tree,
    stats: TreeStats,
    records: usize,
}

type Manager = AsyncProlly<PostgresStore>;

pub async fn load_fixture(backend: PostgresBackend, records: usize) -> Result<Fixture, String> {
    backend.initialize_schema().await.map_err(error)?;
    initialize_benchmark_schema(backend.pool())
        .await
        .map_err(error)?;
    restore_base(backend.pool()).await.map_err(error)?;
    let manager = manager(&backend);
    let base = manager
        .load_named_root(BASE_ROOT)
        .await
        .map_err(error)?
        .ok_or_else(|| "snapshotted base root is missing".to_string())?;
    let stats = manager.collect_stats(&base).await.map_err(error)?;
    require_count(&stats, records)?;
    Ok(Fixture {
        backend,
        base,
        stats,
        records,
    })
}

pub async fn build_fixture(
    backend: PostgresBackend,
    records: usize,
    meta: &RunMeta,
) -> Result<(Fixture, RawRow), String> {
    backend.initialize_schema().await.map_err(error)?;
    initialize_benchmark_schema(backend.pool())
        .await
        .map_err(error)?;
    clear_all(backend.pool()).await.map_err(error)?;

    let mutations = (0..records)
        .map(|id| Mutation::Upsert {
            key: key(id),
            val: value(id, 0),
        })
        .collect::<Vec<_>>();
    let build_manager = manager(&backend);
    let before = read_physical_size(backend.pool()).await.map_err(error)?;
    reset_pg_stats(backend.pool()).await.map_err(error)?;
    build_manager.reset_metrics();
    let started = Instant::now();
    let base = build_manager
        .batch(&build_manager.create(), mutations)
        .await
        .map_err(error)?;
    let elapsed = started.elapsed();
    let metrics = build_manager.metrics();
    let pg = read_pg_metrics(backend.pool()).await.map_err(error)?;
    let after = read_physical_size(backend.pool()).await.map_err(error)?;

    build_manager
        .publish_named_root(BASE_ROOT, &base)
        .await
        .map_err(error)?;
    let stats = build_manager.collect_stats(&base).await.map_err(error)?;
    if stats.total_key_value_pairs != records {
        return Err(format!(
            "base record count mismatch: expected {records}, observed {}",
            stats.total_key_value_pairs
        ));
    }
    validate_samples(&build_manager, &base, records, 0).await?;
    let reopened = manager(&backend)
        .load_named_root(BASE_ROOT)
        .await
        .map_err(error)?
        .ok_or_else(|| "published base root is missing".to_string())?;
    if reopened.root != base.root {
        return Err("reopened base root differs".to_string());
    }
    snapshot_base(backend.pool()).await.map_err(error)?;

    let row = make_row(RowInput {
        meta,
        records,
        repetition: 1,
        operation: Operation::Build,
        pattern: "base",
        cache_state: "cold-manager",
        sample_count: 1,
        logical_operations: records,
        observed_items: records,
        elapsed_ns: elapsed.as_nanos(),
        latency_samples: &[],
        metrics,
        stats: &stats,
        pg: &pg,
        before: &before,
        after: &after,
    });
    row.validate()?;
    Ok((
        Fixture {
            backend,
            base,
            stats,
            records,
        },
        row,
    ))
}

pub async fn run_cell(fixture: &Fixture, spec: CellSpec, meta: &RunMeta) -> Result<RawRow, String> {
    restore_base(fixture.backend.pool()).await.map_err(error)?;
    let loader = manager(&fixture.backend);
    let base = loader
        .load_named_root(BASE_ROOT)
        .await
        .map_err(error)?
        .ok_or_else(|| "restored base root is missing".to_string())?;
    if base.root != fixture.base.root {
        return Err("restored base root differs from fixture".to_string());
    }

    match spec.operation {
        Operation::Build => Err("build is created by build_fixture".to_string()),
        Operation::Put => run_put(fixture, &base, spec, meta).await,
        Operation::Batch => run_batch(fixture, &base, spec, meta).await,
        Operation::GetCold | Operation::GetWarm => run_get(fixture, &base, spec, meta).await,
        Operation::Query => run_query(fixture, &base, spec, meta).await,
        Operation::Scan | Operation::FullScan => run_scan(fixture, &base, spec, meta).await,
        Operation::Diff => run_diff(fixture, &base, spec, meta).await,
        Operation::Merge => run_merge(fixture, &base, spec, meta).await,
    }
}

async fn run_put(
    fixture: &Fixture,
    base: &Tree,
    spec: CellSpec,
    meta: &RunMeta,
) -> Result<RawRow, String> {
    let id = pattern_ids(fixture.records, 1, spec.pattern, 0)[0];
    let manager = manager(&fixture.backend);
    let before = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    reset_pg_stats(fixture.backend.pool())
        .await
        .map_err(error)?;
    manager.reset_metrics();
    let started = Instant::now();
    let changed = manager
        .put(base, key(id), value(id, 1))
        .await
        .map_err(error)?;
    let elapsed = started.elapsed();
    let metrics = manager.metrics();
    let pg = read_pg_metrics(fixture.backend.pool())
        .await
        .map_err(error)?;
    let after = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    assert_value(&manager, &changed, id, 1).await?;
    let stats = manager.collect_stats(&changed).await.map_err(error)?;
    let expected = fixture.records + usize::from(spec.pattern == Pattern::Append);
    require_count(&stats, expected)?;
    finish_cell(
        fixture,
        spec,
        meta,
        1,
        1,
        elapsed.as_nanos(),
        &[],
        metrics,
        &stats,
        &pg,
        &before,
        &after,
        "cold-manager",
    )
}

async fn run_batch(
    fixture: &Fixture,
    base: &Tree,
    spec: CellSpec,
    meta: &RunMeta,
) -> Result<RawRow, String> {
    let ids = pattern_ids(fixture.records, spec.changes, spec.pattern, 1);
    let mutations = mutations(&ids, 1);
    let manager = manager(&fixture.backend);
    let before = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    reset_pg_stats(fixture.backend.pool())
        .await
        .map_err(error)?;
    manager.reset_metrics();
    let started = Instant::now();
    let changed = manager.batch(base, mutations).await.map_err(error)?;
    let elapsed = started.elapsed();
    let metrics = manager.metrics();
    let pg = read_pg_metrics(fixture.backend.pool())
        .await
        .map_err(error)?;
    let after = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    for id in &ids {
        assert_value(&manager, &changed, *id, 1).await?;
    }
    let stats = manager.collect_stats(&changed).await.map_err(error)?;
    let expected = fixture.records
        + if spec.pattern == Pattern::Append {
            ids.len()
        } else {
            0
        };
    require_count(&stats, expected)?;
    finish_cell(
        fixture,
        spec,
        meta,
        ids.len(),
        ids.len(),
        elapsed.as_nanos(),
        &[],
        metrics,
        &stats,
        &pg,
        &before,
        &after,
        "cold-manager",
    )
}

async fn run_get(
    fixture: &Fixture,
    base: &Tree,
    spec: CellSpec,
    meta: &RunMeta,
) -> Result<RawRow, String> {
    let ids = lookup_ids(fixture.records, spec.read_samples, spec.pattern);
    let manager = manager(&fixture.backend);
    if spec.operation == Operation::GetWarm {
        for id in &ids {
            assert_value(&manager, base, *id, 0).await?;
        }
    }
    let before = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    reset_pg_stats(fixture.backend.pool())
        .await
        .map_err(error)?;
    manager.reset_metrics();
    let total_started = Instant::now();
    let mut samples = Vec::with_capacity(ids.len());
    let mut hits = 0usize;
    for id in &ids {
        if spec.operation == Operation::GetCold {
            manager.clear_cache();
        }
        let started = Instant::now();
        let found = manager.get(base, &key(*id)).await.map_err(error)?;
        samples.push(started.elapsed().as_nanos());
        if found.as_deref() == Some(value(*id, 0).as_slice()) {
            hits += 1;
        }
    }
    let elapsed_ns = total_started.elapsed().as_nanos();
    if hits != ids.len() {
        return Err(format!(
            "get hits mismatch: expected {}, observed {hits}",
            ids.len()
        ));
    }
    let metrics = manager.metrics();
    let pg = read_pg_metrics(fixture.backend.pool())
        .await
        .map_err(error)?;
    let after = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    finish_cell(
        fixture,
        spec,
        meta,
        ids.len(),
        hits,
        elapsed_ns,
        &samples,
        metrics,
        &fixture.stats,
        &pg,
        &before,
        &after,
        if spec.operation == Operation::GetWarm {
            "warm-manager"
        } else {
            "cold-manager"
        },
    )
}

async fn run_query(
    fixture: &Fixture,
    base: &Tree,
    spec: CellSpec,
    meta: &RunMeta,
) -> Result<RawRow, String> {
    let ids = lookup_ids(fixture.records, spec.read_samples, spec.pattern);
    let keys = ids.iter().map(|id| key(*id)).collect::<Vec<_>>();
    let manager = manager(&fixture.backend);
    let before = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    reset_pg_stats(fixture.backend.pool())
        .await
        .map_err(error)?;
    manager.reset_metrics();
    let started = Instant::now();
    let values = manager.get_many(base, &keys).await.map_err(error)?;
    let elapsed = started.elapsed();
    let metrics = manager.metrics();
    let pg = read_pg_metrics(fixture.backend.pool())
        .await
        .map_err(error)?;
    let after = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    let hits = values
        .iter()
        .zip(&ids)
        .filter(|(found, id)| found.as_deref() == Some(value(**id, 0).as_slice()))
        .count();
    if hits != ids.len() {
        return Err(format!(
            "query hits mismatch: expected {}, observed {hits}",
            ids.len()
        ));
    }
    finish_cell(
        fixture,
        spec,
        meta,
        ids.len(),
        hits,
        elapsed.as_nanos(),
        &[],
        metrics,
        &fixture.stats,
        &pg,
        &before,
        &after,
        "cold-manager",
    )
}

async fn run_scan(
    fixture: &Fixture,
    base: &Tree,
    spec: CellSpec,
    meta: &RunMeta,
) -> Result<RawRow, String> {
    if spec.operation == Operation::Scan && spec.pattern == Pattern::Random {
        return Err("random contiguous scan is undefined".to_string());
    }
    let (start_key, end_key, expected) = if spec.operation == Operation::FullScan {
        (Vec::new(), None, fixture.records)
    } else {
        let ids = lookup_ids(fixture.records, spec.read_samples, spec.pattern);
        let start = *ids
            .first()
            .ok_or_else(|| "scan IDs are empty".to_string())?;
        let end = ids.last().copied().unwrap() + 1;
        (key(start), Some(key(end)), ids.len())
    };
    let manager = manager(&fixture.backend);
    let before = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    reset_pg_stats(fixture.backend.pool())
        .await
        .map_err(error)?;
    manager.reset_metrics();
    let started = Instant::now();
    let mut iter = manager
        .range(base, &start_key, end_key.as_deref())
        .await
        .map_err(error)?;
    let mut observed = 0usize;
    let mut checksum = 0u64;
    let mut previous: Option<Vec<u8>> = None;
    while let Some(entry) = iter.next().await {
        let (entry_key, entry_value) = entry.map_err(error)?;
        if previous.as_ref().is_some_and(|prior| prior >= &entry_key) {
            return Err("scan keys are not strictly ordered".to_string());
        }
        checksum = checksum
            .wrapping_mul(1_099_511_628_211)
            .wrapping_add(entry_key.len() as u64)
            .wrapping_add(entry_value.len() as u64);
        previous = Some(entry_key);
        observed += 1;
    }
    let elapsed = started.elapsed();
    std::hint::black_box(checksum);
    if observed != expected {
        return Err(format!(
            "scan count mismatch: expected {expected}, observed {observed}"
        ));
    }
    let metrics = manager.metrics();
    let pg = read_pg_metrics(fixture.backend.pool())
        .await
        .map_err(error)?;
    let after = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    finish_cell(
        fixture,
        spec,
        meta,
        expected,
        observed,
        elapsed.as_nanos(),
        &[],
        metrics,
        &fixture.stats,
        &pg,
        &before,
        &after,
        "cold-manager",
    )
}

async fn run_diff(
    fixture: &Fixture,
    base: &Tree,
    spec: CellSpec,
    meta: &RunMeta,
) -> Result<RawRow, String> {
    let ids = pattern_ids(fixture.records, spec.changes, spec.pattern, 2);
    let changed = manager(&fixture.backend)
        .batch(base, mutations(&ids, 1))
        .await
        .map_err(error)?;
    let manager = manager(&fixture.backend);
    let before = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    reset_pg_stats(fixture.backend.pool())
        .await
        .map_err(error)?;
    manager.reset_metrics();
    let started = Instant::now();
    let diffs = manager.diff(base, &changed).await.map_err(error)?;
    let elapsed = started.elapsed();
    let metrics = manager.metrics();
    let pg = read_pg_metrics(fixture.backend.pool())
        .await
        .map_err(error)?;
    let after = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    let observed = diffs
        .iter()
        .map(|diff| match diff {
            Diff::Added { key, .. } | Diff::Removed { key, .. } | Diff::Changed { key, .. } => {
                key.clone()
            }
        })
        .collect::<BTreeSet<_>>();
    let expected = ids.iter().map(|id| key(*id)).collect::<BTreeSet<_>>();
    if observed != expected || diffs.len() != ids.len() {
        return Err("diff key set differs from requested mutations".to_string());
    }
    let stats = manager.collect_stats(&changed).await.map_err(error)?;
    finish_cell(
        fixture,
        spec,
        meta,
        ids.len(),
        diffs.len(),
        elapsed.as_nanos(),
        &[],
        metrics,
        &stats,
        &pg,
        &before,
        &after,
        "cold-manager",
    )
}

async fn run_merge(
    fixture: &Fixture,
    base: &Tree,
    spec: CellSpec,
    meta: &RunMeta,
) -> Result<RawRow, String> {
    if spec.changes % 2 != 0 {
        return Err("merge requires an even total change count".to_string());
    }
    let (left_ids, right_ids) = merge_ids(fixture.records, spec.changes, spec.pattern);
    let builder = manager(&fixture.backend);
    let left = builder
        .batch(base, mutations(&left_ids, 1))
        .await
        .map_err(error)?;
    let right = builder
        .batch(base, mutations(&right_ids, 2))
        .await
        .map_err(error)?;
    let manager = manager(&fixture.backend);
    let before = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    reset_pg_stats(fixture.backend.pool())
        .await
        .map_err(error)?;
    manager.reset_metrics();
    let started = Instant::now();
    let merged = manager
        .merge(base, &left, &right, None)
        .await
        .map_err(error)?;
    let elapsed = started.elapsed();
    let metrics = manager.metrics();
    let pg = read_pg_metrics(fixture.backend.pool())
        .await
        .map_err(error)?;
    let after = read_physical_size(fixture.backend.pool())
        .await
        .map_err(error)?;
    for id in &left_ids {
        assert_value(&manager, &merged, *id, 1).await?;
    }
    for id in &right_ids {
        assert_value(&manager, &merged, *id, 2).await?;
    }
    let stats = manager.collect_stats(&merged).await.map_err(error)?;
    let expected = fixture.records
        + if spec.pattern == Pattern::Append {
            left_ids.len() + right_ids.len()
        } else {
            0
        };
    require_count(&stats, expected)?;
    validate_unaffected_sample(&manager, &merged, fixture.records, &left_ids, &right_ids).await?;
    finish_cell(
        fixture,
        spec,
        meta,
        left_ids.len() + right_ids.len(),
        left_ids.len() + right_ids.len(),
        elapsed.as_nanos(),
        &[],
        metrics,
        &stats,
        &pg,
        &before,
        &after,
        "cold-manager",
    )
}

#[allow(clippy::too_many_arguments)]
fn finish_cell(
    fixture: &Fixture,
    spec: CellSpec,
    meta: &RunMeta,
    logical_operations: usize,
    observed_items: usize,
    elapsed_ns: u128,
    latency_samples: &[u128],
    metrics: ProllyMetricsSnapshot,
    stats: &TreeStats,
    pg: &PgMetrics,
    before: &PhysicalSize,
    after: &PhysicalSize,
    cache_state: &str,
) -> Result<RawRow, String> {
    let row = make_row(RowInput {
        meta,
        records: fixture.records,
        repetition: spec.repetition,
        operation: spec.operation,
        pattern: spec.pattern.as_str(),
        cache_state,
        sample_count: if latency_samples.is_empty() {
            1
        } else {
            latency_samples.len()
        },
        logical_operations,
        observed_items,
        elapsed_ns,
        latency_samples,
        metrics,
        stats,
        pg,
        before,
        after,
    });
    row.validate()?;
    Ok(row)
}

struct RowInput<'a> {
    meta: &'a RunMeta,
    records: usize,
    repetition: u32,
    operation: Operation,
    pattern: &'a str,
    cache_state: &'a str,
    sample_count: usize,
    logical_operations: usize,
    observed_items: usize,
    elapsed_ns: u128,
    latency_samples: &'a [u128],
    metrics: ProllyMetricsSnapshot,
    stats: &'a TreeStats,
    pg: &'a PgMetrics,
    before: &'a PhysicalSize,
    after: &'a PhysicalSize,
}

fn make_row(input: RowInput<'_>) -> RawRow {
    let logical_operations = input.logical_operations.max(1);
    RawRow {
        schema: SCHEMA_VERSION.to_string(),
        revision: input.meta.revision.clone(),
        dirty: input.meta.dirty,
        timestamp_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        records: input.records as u64,
        repetition: input.repetition,
        operation: input.operation.as_str().to_string(),
        pattern: input.pattern.to_string(),
        cache_state: input.cache_state.to_string(),
        sample_count: input.sample_count as u64,
        logical_operations: input.logical_operations as u64,
        observed_items: input.observed_items as u64,
        total_ns: input.elapsed_ns,
        ns_per_op: input.elapsed_ns as f64 / logical_operations as f64,
        ops_per_sec: logical_operations as f64 * 1_000_000_000.0 / input.elapsed_ns.max(1) as f64,
        p50_ns: percentile(input.latency_samples, 0.50),
        p95_ns: percentile(input.latency_samples, 0.95),
        p99_ns: percentile(input.latency_samples, 0.99),
        max_ns: input.latency_samples.iter().copied().max(),
        node_cache_hits: input.metrics.node_cache_hits,
        node_cache_misses: input.metrics.node_cache_misses,
        node_cache_evictions: input.metrics.node_cache_evictions,
        nodes_read: input.metrics.nodes_read,
        bytes_read: input.metrics.bytes_read,
        nodes_written: input.metrics.nodes_written,
        bytes_written: input.metrics.bytes_written,
        store_get_calls: input.metrics.store_get_calls,
        store_batch_get_calls: input.metrics.store_batch_get_calls,
        store_batch_get_keys: input.metrics.store_batch_get_keys,
        store_put_calls: input.metrics.store_put_calls,
        store_batch_put_calls: input.metrics.store_batch_put_calls,
        store_batch_put_nodes: input.metrics.store_batch_put_nodes,
        tree_nodes: input.stats.num_nodes as u64,
        tree_leaves: input.stats.num_leaves as u64,
        tree_internal_nodes: input.stats.num_internal_nodes as u64,
        tree_height: input.stats.tree_height,
        tree_records: input.stats.total_key_value_pairs as u64,
        tree_bytes: input.stats.total_tree_size_bytes as u64,
        pg_statement_calls: input.pg.statement_calls,
        pg_execution_ms: input.pg.execution_ms,
        pg_shared_blks_hit: input.pg.shared_blks_hit,
        pg_shared_blks_read: input.pg.shared_blks_read,
        pg_shared_blks_dirtied: input.pg.shared_blks_dirtied,
        pg_shared_blks_written: input.pg.shared_blks_written,
        pg_temp_blks_read: input.pg.temp_blks_read,
        pg_temp_blks_written: input.pg.temp_blks_written,
        pg_wal_bytes: input.pg.wal_bytes,
        pg_commits: input.pg.commits,
        pg_rollbacks: input.pg.rollbacks,
        database_bytes_before: input.before.database_bytes,
        database_bytes_after: input.after.database_bytes,
        prolly_table_bytes_before: input.before.prolly_table_bytes,
        prolly_table_bytes_after: input.after.prolly_table_bytes,
        prolly_index_bytes_before: input.before.prolly_index_bytes,
        prolly_index_bytes_after: input.after.prolly_index_bytes,
        validated: true,
        error: String::new(),
    }
}

fn manager(backend: &PostgresBackend) -> Manager {
    AsyncProlly::new(RemoteProllyStore::new(backend.clone()), Config::default())
}

fn mutations(ids: &[usize], generation: u8) -> Vec<Mutation> {
    ids.iter()
        .map(|id| Mutation::Upsert {
            key: key(*id),
            val: value(*id, generation),
        })
        .collect()
}

fn lookup_ids(records: usize, count: usize, pattern: Pattern) -> Vec<usize> {
    let count = count.min(records);
    match pattern {
        Pattern::Append => (records - count..records).collect(),
        Pattern::Clustered => pattern_ids(records, count, pattern, 0),
        Pattern::Random => {
            let mut ids = pattern_ids(records, count, pattern, 0x7265_6164);
            let mut state = 0x243f_6a88_85a3_08d3u64 ^ records as u64;
            for index in (1..ids.len()).rev() {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                ids.swap(index, (state as usize) % (index + 1));
            }
            ids
        }
    }
}

async fn assert_value(
    manager: &Manager,
    tree: &Tree,
    id: usize,
    generation: u8,
) -> Result<(), String> {
    let found = manager.get(tree, &key(id)).await.map_err(error)?;
    if found.as_deref() != Some(value(id, generation).as_slice()) {
        return Err(format!(
            "value mismatch for id {id}, generation {generation}"
        ));
    }
    Ok(())
}

async fn validate_samples(
    manager: &Manager,
    tree: &Tree,
    records: usize,
    generation: u8,
) -> Result<(), String> {
    for id in [0, records / 2, records - 1] {
        assert_value(manager, tree, id, generation).await?;
    }
    Ok(())
}

async fn validate_unaffected_sample(
    manager: &Manager,
    tree: &Tree,
    records: usize,
    left: &[usize],
    right: &[usize],
) -> Result<(), String> {
    for candidate in [0, records / 4, records / 2, records.saturating_sub(1)] {
        if !left.contains(&candidate) && !right.contains(&candidate) {
            assert_value(manager, tree, candidate, 0).await?;
            return Ok(());
        }
    }
    Err("unable to select an unaffected validation key".to_string())
}

fn require_count(stats: &TreeStats, expected: usize) -> Result<(), String> {
    if stats.total_key_value_pairs != expected {
        return Err(format!(
            "tree count mismatch: expected {expected}, observed {}",
            stats.total_key_value_pairs
        ));
    }
    Ok(())
}

fn error(value: impl std::fmt::Display) -> String {
    value.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Operation, Pattern};
    use prolly_store_postgres::PostgresBackend;

    #[tokio::test]
    #[ignore = "requires PROLLY_STORE_POSTGRES_URL"]
    async fn smoke_exercises_every_postgres_prolly_operation() {
        let url = std::env::var("PROLLY_STORE_POSTGRES_URL").unwrap();
        let backend = PostgresBackend::connect(&url).await.unwrap();
        let meta = RunMeta {
            revision: "test".to_string(),
            dirty: true,
        };
        let (fixture, build) = build_fixture(backend, 1_000, &meta).await.unwrap();
        assert!(build.validated);
        assert_eq!(build.observed_items, 1_000);

        let operations = [
            Operation::Put,
            Operation::Batch,
            Operation::GetCold,
            Operation::GetWarm,
            Operation::Query,
            Operation::Scan,
            Operation::Diff,
            Operation::Merge,
        ];
        for operation in operations {
            for pattern in Pattern::ALL {
                if operation == Operation::Scan && pattern == Pattern::Random {
                    continue;
                }
                let row = run_cell(
                    &fixture,
                    CellSpec {
                        operation,
                        pattern,
                        repetition: 1,
                        changes: 100,
                        read_samples: 100,
                    },
                    &meta,
                )
                .await
                .unwrap();
                row.validate().unwrap();
            }
        }

        let full = run_cell(
            &fixture,
            CellSpec {
                operation: Operation::FullScan,
                pattern: Pattern::Append,
                repetition: 1,
                changes: 100,
                read_samples: 100,
            },
            &meta,
        )
        .await
        .unwrap();
        assert_eq!(full.observed_items, 1_000);
        full.validate().unwrap();
    }
}
