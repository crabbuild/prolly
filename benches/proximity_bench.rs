use prolly::{
    copy_content_graph, plan_content_gc, AdaptiveQuality, BuildParallelism, ContentGraphLimits,
    DistanceMetric, HnswConfig, HnswIndex, MemStore, ProductQuantizationConfig, ProductQuantizer,
    ProximityConfig, ProximityFilter, ProximityMap, ProximityMutation, ProximityRecord,
    QueryKernel, ScalarQuantizationConfig, SearchBackend, SearchPolicy, SearchRequest,
    TypedContentRoot,
};
#[cfg(feature = "async-store")]
use prolly::{AsyncProximityMap, AsyncSearchControl, SyncStoreAsAsync};
use std::collections::HashSet;
#[cfg(feature = "async-store")]
use std::future::Future;
use std::hint::black_box;
use std::sync::Arc;
#[cfg(feature = "async-store")]
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

fn main() {
    let records = env_usize("PROLLY_PROXIMITY_BENCH_RECORDS").unwrap_or(1_000);
    let dimensions =
        env_list("PROLLY_PROXIMITY_BENCH_DIMENSIONS").unwrap_or_else(|| vec![8, 128, 768, 1_536]);
    let threads = env_list("PROLLY_PROXIMITY_BENCH_THREADS").unwrap_or_else(|| vec![1, 2, 4]);
    println!("prolly proximity v2 benchmark");
    println!("records={records}");
    println!("operation,dimensions,threads,micros,metric_a,metric_b");
    for dimension in dimensions {
        bench_case(records, dimension, &threads);
    }
}

fn bench_case(count: usize, dimensions: usize, threads: &[usize]) {
    let records = make_records(count, dimensions);
    for &workers in threads {
        let store = Arc::new(MemStore::new());
        let config = config(dimensions);
        let started = Instant::now();
        let (_, stats) = ProximityMap::build_with_parallelism(
            store,
            config,
            black_box(records.clone()),
            BuildParallelism::new(workers).unwrap(),
        )
        .unwrap();
        row(
            "build",
            dimensions,
            workers,
            started.elapsed(),
            stats.distance_evaluations,
            stats.proximity_objects_written,
        );
    }

    let store = Arc::new(MemStore::new());
    let map = ProximityMap::build(store.clone(), config(dimensions), records.clone()).unwrap();
    let query = make_vector(count / 3, dimensions);
    let k = 10.min(count.max(1));

    for (name, policy, kernel) in [
        (
            "search_exact_scalar",
            SearchPolicy::Exact,
            QueryKernel::ScalarDeterministic,
        ),
        (
            "search_exact_simd",
            SearchPolicy::Exact,
            QueryKernel::SimdDeterministic,
        ),
        (
            "search_adaptive_sq8",
            SearchPolicy::Adaptive(AdaptiveQuality::Balanced),
            QueryKernel::AutoDeterministic,
        ),
    ] {
        let mut request = SearchRequest::exact(&query, k);
        request.policy = policy;
        request.kernel = kernel;
        request.filter = ProximityFilter::Prefix(b"record-");
        let started = Instant::now();
        let result = map.search(request).unwrap();
        row(
            name,
            dimensions,
            0,
            started.elapsed(),
            result.stats.nodes_read,
            result.stats.distance_evaluations + result.stats.quantized_distance_evaluations,
        );
        if name == "search_exact_scalar" {
            let recall = recall_at_k(&records, &query, &result, k);
            println!("recall_exact,{dimensions},0,0,{:.6},0", recall);
        }
    }

    let key = format!("record-{:08}", count / 2).into_bytes();
    let started = Instant::now();
    let (_, stats) = map
        .mutate_batch([ProximityMutation {
            key,
            value: Some((make_vector(count + 1, dimensions), b"updated".to_vec())),
        }])
        .unwrap();
    row(
        "localized_mutation",
        dimensions,
        0,
        started.elapsed(),
        stats.nodes_written,
        stats.nodes_reused,
    );

    let limits = ContentGraphLimits::default();
    let root = TypedContentRoot::proximity_descriptor(map.tree().descriptor.clone());
    let replica = MemStore::new();
    let started = Instant::now();
    let copied = copy_content_graph(&store, &replica, root.clone(), &limits).unwrap();
    row(
        "content_graph_copy",
        dimensions,
        0,
        started.elapsed(),
        copied.copied_objects,
        copied.copied_bytes,
    );
    let walk = prolly::walk_content_graph(&store, std::slice::from_ref(&root), &limits).unwrap();
    let candidates: Vec<_> = walk
        .objects
        .iter()
        .map(|object| object.root.cid.clone())
        .collect();
    let started = Instant::now();
    let gc = plan_content_gc(&store, &[root], &candidates, &limits).unwrap();
    row(
        "content_graph_gc_plan",
        dimensions,
        0,
        started.elapsed(),
        gc.live_objects,
        gc.reclaimable_cids.len(),
    );

    let started = Instant::now();
    let proof = map
        .prove_search(SearchRequest::exact(&query, k), &limits)
        .unwrap();
    let generated = started.elapsed();
    let started = Instant::now();
    let verified = proof.verify(&limits).unwrap();
    row(
        "search_proof_generate",
        dimensions,
        0,
        generated,
        proof.events.len(),
        proof.source.objects.len(),
    );
    row(
        "search_proof_verify",
        dimensions,
        0,
        started.elapsed(),
        verified.replayed_events,
        verified.result.neighbors.len(),
    );

    if dimensions <= 128 && count >= 16 {
        bench_accelerators(&map, &query, k, dimensions);
    }
    #[cfg(feature = "async-store")]
    bench_async(&map, store, &query, k, dimensions);
}

fn bench_accelerators<S>(map: &ProximityMap<S>, query: &[f32], k: usize, dimensions: usize)
where
    S: prolly::Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let started = Instant::now();
    let (pq, stats) = ProductQuantizer::build(
        map,
        ProductQuantizationConfig {
            subquantizers: (dimensions as u32).min(8),
            centroids_per_subquantizer: 16,
            training_iterations: 4,
            rerank_multiplier: 8,
            seed: 17,
        },
        BuildParallelism::new(2).unwrap(),
    )
    .unwrap();
    row(
        "pq_build",
        dimensions,
        2,
        started.elapsed(),
        stats.training_distance_evaluations,
        stats.encoded_vectors,
    );
    let mut request = SearchRequest::exact(query, k);
    request.policy = SearchPolicy::FixedBudget;
    request.backend = SearchBackend::ProductQuantized;
    let started = Instant::now();
    let result = pq.search(map, request).unwrap();
    row(
        "pq_search",
        dimensions,
        0,
        started.elapsed(),
        result.stats.distance_evaluations,
        result.stats.reranked_candidates,
    );

    let started = Instant::now();
    let (hnsw, stats) = HnswIndex::build(map, HnswConfig::default()).unwrap();
    row(
        "hnsw_build",
        dimensions,
        0,
        started.elapsed(),
        stats.distance_evaluations,
        stats.directed_edges,
    );
    let mut request = SearchRequest::exact(query, k);
    request.policy = SearchPolicy::FixedBudget;
    request.backend = SearchBackend::Hnsw;
    let started = Instant::now();
    let result = hnsw.search(map, request).unwrap();
    row(
        "hnsw_search",
        dimensions,
        0,
        started.elapsed(),
        result.stats.nodes_read,
        result.stats.distance_evaluations,
    );
}

#[cfg(feature = "async-store")]
fn bench_async(
    map: &ProximityMap<Arc<MemStore>>,
    store: Arc<MemStore>,
    query: &[f32],
    k: usize,
    dimensions: usize,
) {
    let async_map = block_on(AsyncProximityMap::load(
        SyncStoreAsAsync::new(store),
        map.tree().descriptor.clone(),
    ))
    .unwrap();
    let started = Instant::now();
    let result = block_on(async_map.search(
        SearchRequest::exact(query, k),
        AsyncSearchControl::default(),
    ))
    .unwrap();
    row(
        "async_search",
        dimensions,
        0,
        started.elapsed(),
        result.stats.nodes_read,
        result.stats.distance_evaluations,
    );
}

#[cfg(feature = "async-store")]
fn block_on<F: Future>(future: F) -> F::Output {
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn config(dimensions: usize) -> ProximityConfig {
    let mut config = ProximityConfig::new(dimensions as u32);
    config.metric = DistanceMetric::L2Squared;
    config.hierarchy.level_hash_seed = 42;
    config.overflow.min_page_bytes = 4 * 1024;
    config.overflow.target_page_bytes = 16 * 1024;
    config.overflow.max_page_bytes = 64 * 1024;
    config.vector_storage.inline_threshold_bytes = 4 * 1024;
    config.scalar_quantization = Some(ScalarQuantizationConfig {
        group_size: (dimensions as u32).min(32),
    });
    config
}

fn row(
    operation: &str,
    dimensions: usize,
    threads: usize,
    duration: Duration,
    metric_a: usize,
    metric_b: usize,
) {
    println!(
        "{operation},{dimensions},{threads},{:.3},{metric_a},{metric_b}",
        duration.as_secs_f64() * 1_000_000.0
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

fn recall_at_k(
    records: &[ProximityRecord],
    query: &[f32],
    result: &prolly::SearchResult,
    k: usize,
) -> f64 {
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
    let exact: HashSet<_> = scored.into_iter().take(k).map(|(_, key)| key).collect();
    result
        .neighbors
        .iter()
        .filter(|neighbor| exact.contains(&neighbor.key))
        .count() as f64
        / k.max(1) as f64
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

fn env_list(name: &str) -> Option<Vec<usize>> {
    let value = std::env::var(name).ok()?;
    let values: Vec<_> = value
        .split(',')
        .filter_map(|item| item.trim().parse().ok())
        .collect();
    (!values.is_empty()).then_some(values)
}
