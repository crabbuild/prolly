use prolly::{
    DistanceMetric, MemStore, ProximityConfig, ProximityMap, ProximityMutation, ProximityRecord,
    SearchRequest,
};
use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let records = env_usize("PROLLY_PROXIMITY_BENCH_RECORDS").unwrap_or(1_000);
    let dimensions = env_dimensions().unwrap_or_else(|| vec![8, 128, 768, 1_536]);
    println!("prolly proximity benchmark");
    println!("records={records}");
    println!("dimensions,build_ms,search_us,recall_at_10,nodes_read,bytes_read,distance_evaluations,mutation_us,nodes_written,nodes_reused,full_rebuild");
    for dimension in dimensions {
        bench_case(records, dimension);
    }
}

fn bench_case(count: usize, dimensions: usize) {
    let store = Arc::new(MemStore::new());
    let mut config = ProximityConfig::new(dimensions as u32);
    config.metric = DistanceMetric::L2Squared;
    config.hierarchy.level_hash_seed = 42;
    config.overflow.max_page_bytes = 64 * 1024 * 1024;
    let records = make_records(count, dimensions);
    let build_start = Instant::now();
    let map = ProximityMap::build(store, config, black_box(records.clone())).unwrap();
    let build = build_start.elapsed();

    let query = make_vector(count / 3, dimensions);
    let search_start = Instant::now();
    let result = map
        .search(SearchRequest::exact(
            black_box(&query),
            10.min(count.max(1)),
        ))
        .unwrap();
    let search = search_start.elapsed();
    let exact = brute_force(&records, &query, 10.min(count));
    let actual: HashSet<_> = result
        .neighbors
        .iter()
        .map(|item| item.key.clone())
        .collect();
    let recall =
        exact.iter().filter(|key| actual.contains(*key)).count() as f64 / exact.len().max(1) as f64;

    let key = format!("record-{:08}", count / 2).into_bytes();
    let mutation_start = Instant::now();
    let (_, mutation) = map
        .mutate_batch([ProximityMutation {
            key,
            value: Some((make_vector(count + 1, dimensions), b"updated".to_vec())),
        }])
        .unwrap();
    let mutation_elapsed = mutation_start.elapsed();

    println!(
        "{dimensions},{:.3},{:.3},{recall:.4},{},{},{},{:.3},{},{},{}",
        build.as_secs_f64() * 1_000.0,
        search.as_secs_f64() * 1_000_000.0,
        result.stats.nodes_read,
        result.stats.bytes_read,
        result.stats.distance_evaluations,
        mutation_elapsed.as_secs_f64() * 1_000_000.0,
        mutation.nodes_written,
        mutation.nodes_reused,
        mutation.full_proximity_rebuild,
    );
}

fn make_records(count: usize, dimensions: usize) -> Vec<ProximityRecord> {
    (0..count)
        .map(|index| ProximityRecord {
            key: format!("record-{index:08}").into_bytes(),
            vector: make_vector(index, dimensions),
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

fn make_vector(index: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|component| {
            let mixed = index
                .wrapping_mul(1_000_003)
                .wrapping_add(component.wrapping_mul(97_409));
            ((mixed % 20_003) as f32 - 10_001.0) / 1_000.0
        })
        .collect()
}

fn brute_force(records: &[ProximityRecord], query: &[f32], k: usize) -> Vec<Vec<u8>> {
    let mut scored: Vec<_> = records
        .iter()
        .map(|record| {
            let distance = record
                .vector
                .iter()
                .zip(query)
                .map(|(&left, &right)| {
                    let delta = f64::from(left) - f64::from(right);
                    delta * delta
                })
                .sum::<f64>();
            (distance, record.key.clone())
        })
        .collect();
    scored.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    scored.into_iter().take(k).map(|(_, key)| key).collect()
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

fn env_dimensions() -> Option<Vec<usize>> {
    let value = std::env::var("PROLLY_PROXIMITY_BENCH_DIMENSIONS").ok()?;
    let dimensions: Vec<_> = value
        .split(',')
        .filter_map(|item| item.trim().parse().ok())
        .collect();
    (!dimensions.is_empty()).then_some(dimensions)
}
