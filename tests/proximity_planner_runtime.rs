use prolly::{
    AcceleratorSet, BatchOp, BuildParallelism, Config, FileNodeStore, HnswConfig, HnswIndex,
    MemStore, ProductQuantizationConfig, ProductQuantizer, Prolly, ProximityConfig,
    ProximityFilter, ProximityMap, ProximityRecord, SearchBackend, SearchIo, SearchPolicy,
    SearchRequest, SearchRuntime, Store,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

fn records(count: usize) -> Vec<ProximityRecord> {
    (0..count)
        .map(|index| ProximityRecord {
            key: format!("key-{index:04}").into_bytes(),
            vector: vec![index as f32, (index % 7) as f32, (index % 11) as f32, 1.0],
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

#[test]
fn auto_plans_hnsw_pq_and_non_copying_eligible_exact_deterministically() {
    let store = Arc::new(MemStore::new());
    let map = ProximityMap::build(store.clone(), ProximityConfig::new(4), records(128)).unwrap();
    let (hnsw, _) = HnswIndex::build(
        &map,
        HnswConfig {
            max_connections: 12,
            ef_construction: 64,
            ef_search: 48,
            level_bits: 4,
            overfetch_multiplier: 8,
            seed: 17,
            ..HnswConfig::default()
        },
    )
    .unwrap();
    let (pq, _) = ProductQuantizer::build(
        &map,
        ProductQuantizationConfig {
            subquantizers: 2,
            centroids_per_subquantizer: 8,
            training_iterations: 4,
            rerank_multiplier: 8,
            seed: 19,
            max_training_vectors: 64,
        },
        BuildParallelism::new(2).unwrap(),
    )
    .unwrap();
    let accelerators = AcceleratorSet::try_new(map.tree(), Some(hnsw), Some(pq)).unwrap();
    let io = SearchIo::new(store, Arc::new(SearchRuntime::default()));
    let query = [63.0, 0.0, 8.0, 1.0];

    let mut automatic = SearchRequest::exact(&query, 10);
    automatic.policy = SearchPolicy::FixedBudget;
    let hnsw_result = map
        .search_with(&accelerators, &io, automatic.clone())
        .unwrap();
    assert_eq!(hnsw_result.plan.backend, SearchBackend::Hnsw);
    assert!(hnsw_result.plan.expansion_target.unwrap() >= 48);
    let warm_hnsw = map.search_with(&accelerators, &io, automatic).unwrap();
    assert_eq!(warm_hnsw.plan, hnsw_result.plan);
    assert_eq!(warm_hnsw.neighbors, hnsw_result.neighbors);
    assert_eq!(warm_hnsw.completion, hnsw_result.completion);
    let mut cold_logical = hnsw_result.stats.clone();
    let mut warm_logical = warm_hnsw.stats.clone();
    cold_logical.physical_bytes_read = 0;
    warm_logical.physical_bytes_read = 0;
    assert_eq!(warm_logical, cold_logical);

    let mut pq_request = SearchRequest::exact(&query, 10);
    pq_request.policy = SearchPolicy::FixedBudget;
    pq_request.options.planner.approximate_preference =
        prolly::ApproximatePreference::ProductQuantizedFirst;
    let pq_result = map.search_with(&accelerators, &io, pq_request).unwrap();
    assert_eq!(pq_result.plan.backend, SearchBackend::ProductQuantized);
    assert!(pq_result.stats.reranked_candidates <= 80);

    let eligible = vec![b"key-0062".to_vec(), b"key-0063".to_vec()];
    let mut exact_subset = SearchRequest::exact(&query, 1);
    exact_subset.policy = SearchPolicy::FixedBudget;
    exact_subset.filter = ProximityFilter::EligibleKeys(&eligible);
    let exact_result = map.search_with(&accelerators, &io, exact_subset).unwrap();
    assert_eq!(exact_result.plan.eligible_exact_records, Some(2));
    assert_eq!(exact_result.stats.distance_evaluations, 2);
    assert!(
        exact_result.neighbors[0].key == eligible[0]
            || exact_result.neighbors[0].key == eligible[1]
    );
}

#[derive(Clone)]
struct CountingStore {
    inner: Arc<MemStore>,
    reads: Arc<AtomicUsize>,
    first_read_gate: Arc<Barrier>,
}

impl Store for CountingStore {
    type Error = prolly::MemStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
            self.first_read_gate.wait();
        }
        Store::get(&self.inner, key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        Store::put(&self.inner, key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        Store::delete(&self.inner, key)
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        Store::batch(&self.inner, ops)
    }
}

#[test]
fn runtime_coalesces_one_physical_load_and_clones_share_namespace() {
    let inner = Arc::new(MemStore::new());
    let manager = Prolly::new(inner.clone(), Config::default());
    let tree = manager
        .put(&manager.create(), b"key".to_vec(), b"value".to_vec())
        .unwrap();
    let cid = tree.root.unwrap();
    let bytes = Store::get(&inner, cid.as_bytes()).unwrap().unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Barrier::new(2));
    let store = CountingStore {
        inner,
        reads: reads.clone(),
        first_read_gate: gate.clone(),
    };
    let io = SearchIo::new(store, Arc::new(SearchRuntime::default()));
    let mut workers = Vec::new();
    for _ in 0..8 {
        let io = io.clone();
        let cid = cid.clone();
        workers.push(std::thread::spawn(move || {
            Store::get(&io, cid.as_bytes()).unwrap()
        }));
    }
    gate.wait();
    for worker in workers {
        assert_eq!(worker.join().unwrap(), Some(bytes.clone()));
    }
    assert_eq!(reads.load(Ordering::SeqCst), 1);
    assert_eq!(io.physical_reads(), 1);
    assert_eq!(io.physical_bytes_read(), bytes.len());
}

#[test]
fn unified_accelerated_search_is_store_neutral_on_local_durable_content() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "prolly-proximity-runtime-{}-{nonce}",
        std::process::id()
    ));
    let store = Arc::new(FileNodeStore::open(&path).unwrap());
    let map = ProximityMap::build(store.clone(), ProximityConfig::new(4), records(64)).unwrap();
    let (hnsw, _) = HnswIndex::build(&map, HnswConfig::default()).unwrap();
    let accelerators = AcceleratorSet::try_new(map.tree(), Some(hnsw), None).unwrap();
    let io = SearchIo::new(store, Arc::new(SearchRuntime::default()));
    let query = [31.0, 3.0, 9.0, 1.0];
    let mut request = SearchRequest::exact(&query, 5);
    request.policy = SearchPolicy::FixedBudget;
    let result = map.search_with(&accelerators, &io, request).unwrap();
    assert_eq!(result.plan.backend, SearchBackend::Hnsw);
    assert_eq!(result.neighbors.len(), 5);
    drop(map);
    drop(io);
    std::fs::remove_dir_all(path).unwrap();
}
