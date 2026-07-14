use std::collections::BTreeSet;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prolly::{
    Config, Diff, MemStore, Mutation, Prolly, ProllyMetricsSnapshot, SortedBatchBuilder, Tree,
    TreeStats,
};

const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
const READ_OPERATIONS: usize = 1_000_000;

fn main() {
    let records = env_usize("SCALE_RECORDS", 1_000).max(1_000);
    let version = std::env::var("SCALE_VERSION").unwrap_or_else(|_| "unknown".to_string());
    let mutations = sample_count(records);

    println!(
        "version,records,workload,operations,total_ns,ns_per_op,validated,nodes_read,nodes_written,bytes_read,bytes_written,num_nodes,num_leaves,num_internal,height,tree_bytes"
    );

    let config = Config::default();
    let store = Arc::new(MemStore::new());
    let build_started = Instant::now();
    let mut builder = SortedBatchBuilder::new(store.clone(), config.clone());
    for id in 0..records {
        builder
            .add(key(id), value(id, 0))
            .expect("generated base keys are sorted");
    }
    let base = builder.build().expect("streamed base build succeeds");
    let build_elapsed = build_started.elapsed();
    let base_stats = collect_stats(store.clone(), &config, &base);
    assert_eq!(base_stats.total_key_value_pairs, records);
    emit(
        &version,
        records,
        "base_build",
        records,
        build_elapsed,
        ProllyMetricsSnapshot::default(),
        &base_stats,
    );

    let random_read_ids = random_read_indexes(records);
    let clustered_read_ids = clustered_read_indexes(records);
    bench_reads(
        &version,
        records,
        "random_reads",
        store.clone(),
        &config,
        &base,
        &base_stats,
        &random_read_ids,
    );
    bench_reads(
        &version,
        records,
        "clustered_reads",
        store.clone(),
        &config,
        &base,
        &base_stats,
        &clustered_read_ids,
    );

    let append_mutations = (records..records + mutations)
        .map(|id| Mutation::Upsert {
            key: key(id),
            val: value(id, 1),
        })
        .collect::<Vec<_>>();
    let random_update_ids = random_indexes(records, mutations);
    let random_mutations = update_mutations(&random_update_ids, 1);
    let clustered_update_ids = clustered_indexes(records, mutations);
    let clustered_mutations = update_mutations(&clustered_update_ids, 2);

    let appended = bench_mutation(
        &version,
        records,
        "append_mutations",
        store.clone(),
        &config,
        &base,
        append_mutations,
        true,
        records + mutations,
        1,
    );
    let random_updated = bench_mutation(
        &version,
        records,
        "random_mutations",
        store.clone(),
        &config,
        &base,
        random_mutations,
        false,
        records,
        1,
    );
    let clustered_updated = bench_mutation(
        &version,
        records,
        "clustered_mutations",
        store.clone(),
        &config,
        &base,
        clustered_mutations,
        false,
        records,
        2,
    );

    let append_ids = (records..records + mutations).collect::<Vec<_>>();
    bench_diff(
        &version,
        records,
        "append_diff",
        store.clone(),
        &config,
        &base,
        &appended,
        &append_ids,
    );
    bench_diff(
        &version,
        records,
        "random_diff",
        store.clone(),
        &config,
        &base,
        &random_updated,
        &random_update_ids,
    );
    bench_diff(
        &version,
        records,
        "clustered_diff",
        store,
        &config,
        &base,
        &clustered_updated,
        &clustered_update_ids,
    );
}

#[allow(clippy::too_many_arguments)]
fn bench_reads(
    version: &str,
    records: usize,
    workload: &str,
    store: Arc<MemStore>,
    config: &Config,
    tree: &Tree,
    stats: &TreeStats,
    ids: &[usize],
) {
    let manager = Prolly::new(store, config.clone());
    let entries = ids
        .iter()
        .map(|id| (key(*id), value(*id, 0)))
        .collect::<Vec<_>>();

    for (key, expected) in &entries {
        assert_eq!(
            manager.get(tree, key).expect("warm read succeeds"),
            Some(expected.clone())
        );
    }
    manager.reset_metrics();

    let started = Instant::now();
    let mut observed_bytes = 0usize;
    for (key, _) in &entries {
        let found = manager
            .get(tree, black_box(key))
            .expect("measured read succeeds")
            .expect("generated key exists");
        observed_bytes = observed_bytes.wrapping_add(found.len());
        black_box(&found);
    }
    let elapsed = started.elapsed();
    let expected_bytes = entries.iter().map(|(_, value)| value.len()).sum::<usize>();
    assert_eq!(observed_bytes, expected_bytes);
    emit(
        version,
        records,
        workload,
        entries.len(),
        elapsed,
        manager.metrics(),
        stats,
    );
}

#[allow(clippy::too_many_arguments)]
fn bench_mutation(
    version: &str,
    records: usize,
    workload: &str,
    store: Arc<MemStore>,
    config: &Config,
    base: &Tree,
    mutations: Vec<Mutation>,
    append: bool,
    expected_count: usize,
    generation: u8,
) -> Tree {
    let manager = Prolly::new(store.clone(), config.clone());
    manager.reset_metrics();
    let mutation_count = mutations.len();
    let expected = mutations
        .iter()
        .map(|mutation| {
            let id = id_from_key(mutation.key());
            (mutation.key().to_vec(), value(id, generation))
        })
        .collect::<Vec<_>>();

    let started = Instant::now();
    let changed = if append {
        manager
            .append_batch(base, black_box(mutations))
            .expect("append mutations succeed")
    } else {
        manager
            .batch(base, black_box(mutations))
            .expect("batch mutations succeed")
    };
    let elapsed = started.elapsed();
    let metrics = manager.metrics();

    for (key, value) in &expected {
        assert_eq!(
            manager
                .get(&changed, key)
                .expect("validation read succeeds"),
            Some(value.clone())
        );
    }
    let stats = collect_stats(store, config, &changed);
    assert_eq!(stats.total_key_value_pairs, expected_count);
    emit(
        version,
        records,
        workload,
        mutation_count,
        elapsed,
        metrics,
        &stats,
    );
    changed
}

#[allow(clippy::too_many_arguments)]
fn bench_diff(
    version: &str,
    records: usize,
    workload: &str,
    store: Arc<MemStore>,
    config: &Config,
    base: &Tree,
    changed: &Tree,
    expected_ids: &[usize],
) {
    let manager = Prolly::new(store.clone(), config.clone());
    manager.reset_metrics();
    let started = Instant::now();
    let diffs = manager
        .diff(base, changed)
        .expect("structural diff succeeds");
    let elapsed = started.elapsed();
    let metrics = manager.metrics();

    let actual_keys = diffs
        .iter()
        .map(|diff| match diff {
            Diff::Added { key, .. } | Diff::Removed { key, .. } | Diff::Changed { key, .. } => {
                key.clone()
            }
        })
        .collect::<BTreeSet<_>>();
    let expected_keys = expected_ids
        .iter()
        .map(|id| key(*id))
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_keys, expected_keys);
    assert_eq!(diffs.len(), expected_ids.len());
    let stats = collect_stats(store, config, changed);
    emit(
        version,
        records,
        workload,
        expected_ids.len(),
        elapsed,
        metrics,
        &stats,
    );
}

fn collect_stats(store: Arc<MemStore>, config: &Config, tree: &Tree) -> TreeStats {
    Prolly::new(store, config.clone())
        .collect_stats(tree)
        .expect("tree statistics succeed")
}

fn emit(
    version: &str,
    records: usize,
    workload: &str,
    operations: usize,
    elapsed: Duration,
    metrics: ProllyMetricsSnapshot,
    stats: &TreeStats,
) {
    let total_ns = elapsed.as_nanos();
    let ns_per_op = total_ns as f64 / operations.max(1) as f64;
    println!(
        "{version},{records},{workload},{operations},{total_ns},{ns_per_op:.3},true,{},{},{},{},{},{},{},{},{}",
        metrics.nodes_read,
        metrics.nodes_written,
        metrics.bytes_read,
        metrics.bytes_written,
        stats.num_nodes,
        stats.num_leaves,
        stats.num_internal_nodes,
        stats.tree_height,
        stats.total_tree_size_bytes,
    );
}

fn key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

fn value(id: usize, generation: u8) -> Vec<u8> {
    format!("value-{id:020}-{generation:02}").into_bytes()
}

fn id_from_key(key: &[u8]) -> usize {
    std::str::from_utf8(key)
        .expect("generated key is UTF-8")
        .strip_prefix("key-")
        .expect("generated key prefix")
        .parse()
        .expect("generated key ID")
}

fn sample_count(records: usize) -> usize {
    records.min((records / 100).max(100)).min(10_000)
}

fn update_mutations(ids: &[usize], generation: u8) -> Vec<Mutation> {
    ids.iter()
        .map(|id| Mutation::Upsert {
            key: key(*id),
            val: value(*id, generation),
        })
        .collect()
}

fn random_indexes(records: usize, count: usize) -> Vec<usize> {
    if count >= records {
        return (0..records).collect();
    }
    let mut state = RANDOM_SEED ^ records as u64 ^ (count as u64).rotate_left(17);
    let mut indexes = BTreeSet::new();
    while indexes.len() < count {
        indexes.insert((next_random(&mut state) as usize) % records);
    }
    indexes.into_iter().collect()
}

fn clustered_indexes(records: usize, count: usize) -> Vec<usize> {
    let count = count.min(records);
    let start = records.saturating_sub(count) / 2;
    (start..start + count).collect()
}

fn random_read_indexes(records: usize) -> Vec<usize> {
    let mut state = RANDOM_SEED ^ (records as u64).rotate_left(29);
    (0..READ_OPERATIONS)
        .map(|_| (next_random(&mut state) as usize) % records)
        .collect()
}

fn clustered_read_indexes(records: usize) -> Vec<usize> {
    let width = sample_count(records);
    let start = records.saturating_sub(width) / 2;
    (0..READ_OPERATIONS)
        .map(|offset| start + offset % width)
        .collect()
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
