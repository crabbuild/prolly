use prolly::{
    HnswBuildLimits, HnswConfig, HnswIndex, MemStore, ProximityConfig, ProximityFilter,
    ProximityMap, ProximityMutation, ProximityRecord, SearchBackend, SearchPolicy, SearchRequest,
    Store,
};
use std::collections::HashSet;
use std::sync::Arc;

fn records() -> Vec<ProximityRecord> {
    (0usize..384)
        .map(|index| ProximityRecord {
            key: format!("hnsw-{index:04}").into_bytes(),
            vector: (0..8)
                .map(|dimension| {
                    let cluster = (index % 13) as f32 * 4.0;
                    cluster + ((index * 31 + dimension * 17) % 97) as f32 / 97.0
                })
                .collect(),
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

fn map(store: Arc<MemStore>, input: Vec<ProximityRecord>) -> ProximityMap<Arc<MemStore>> {
    let mut config = ProximityConfig::new(8);
    config.hierarchy.log_chunk_size = 2;
    config.hierarchy.level_hash_seed = 41;
    ProximityMap::build(store, config, input).unwrap()
}

fn config() -> HnswConfig {
    HnswConfig {
        max_connections: 16,
        ef_construction: 128,
        ef_search: 192,
        level_bits: 4,
        overfetch_multiplier: 16,
        seed: 0x5eed,
        routing_vector_encoding: prolly::HnswRoutingVectorEncoding::FullF32,
    }
}

#[test]
fn clean_graph_is_permutation_identical_loadable_and_disposable_mode_is_explicit() {
    let forward = records();
    let mut reverse = forward.clone();
    reverse.reverse();
    let forward_store = Arc::new(MemStore::new());
    let reverse_store = Arc::new(MemStore::new());
    let forward_map = map(forward_store.clone(), forward);
    let reverse_map = map(reverse_store, reverse);
    assert_eq!(forward_map.tree().descriptor, reverse_map.tree().descriptor);

    let (first, first_stats) = HnswIndex::build(&forward_map, config()).unwrap();
    let (second, second_stats) = HnswIndex::build_with_limits(
        &reverse_map,
        config(),
        HnswBuildLimits {
            worker_threads: 4,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(first.manifest_cid(), second.manifest_cid());
    assert_eq!(first_stats, second_stats);
    assert!(first.is_canonical());
    assert!(first_stats.directed_edges > 0);
    assert!(matches!(
        HnswIndex::build_with_limits(
            &forward_map,
            config(),
            HnswBuildLimits {
                max_records: Some(1),
                ..Default::default()
            },
        ),
        Err(prolly::Error::ProximityResourceLimitExceeded { .. })
    ));

    let loaded = HnswIndex::load(forward_store.clone(), first.manifest_cid().clone()).unwrap();
    assert_eq!(loaded.source_descriptor(), &forward_map.tree().descriptor);
    assert!(loaded.is_canonical());

    let (disposable, _) = HnswIndex::build_disposable(&forward_map, config()).unwrap();
    assert!(!disposable.is_canonical());
    assert_ne!(disposable.manifest_cid(), first.manifest_cid());

    Store::put(
        &forward_store,
        first.manifest_cid().as_bytes(),
        b"corrupt manifest",
    )
    .unwrap();
    assert!(matches!(
        HnswIndex::load(forward_store, first.manifest_cid().clone()),
        Err(prolly::Error::CidMismatch { .. })
    ));
}

#[test]
fn hnsw_has_fixed_seed_recall_enforces_filters_and_resolves_authoritative_values() {
    let store = Arc::new(MemStore::new());
    let map = map(store, records());
    let (index, _) = HnswIndex::build(&map, config()).unwrap();
    let query = [20.1, 20.2, 20.3, 20.4, 20.5, 20.6, 20.7, 20.8];

    let exact = map.search(SearchRequest::exact(&query, 10)).unwrap();
    let mut request = SearchRequest::exact(&query, 10);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::Hnsw;
    let first = index.search(&map, request.clone()).unwrap();
    let second = index.search(&map, request).unwrap();
    assert_eq!(first.neighbors, second.neighbors);
    assert_eq!(first.stats, second.stats);
    assert!(first.stats.reranked_candidates > first.neighbors.len());
    assert_eq!(
        first.stats.candidate_handles_peak,
        first.stats.reranked_candidates
    );
    assert!(first.stats.candidate_retained_bytes_peak > 0);
    let exact_keys: HashSet<_> = exact.neighbors.iter().map(|hit| hit.key.clone()).collect();
    let overlap = first
        .neighbors
        .iter()
        .filter(|hit| exact_keys.contains(&hit.key))
        .count();
    assert_eq!(overlap, 10, "fixed-seed recall was {overlap}/10");
    for hit in &first.neighbors {
        assert_eq!(map.get(&hit.key).unwrap().unwrap().1, hit.value);
    }

    let mut filtered_exact = SearchRequest::exact(&query, 8);
    filtered_exact.filter = ProximityFilter::Prefix(b"hnsw-00");
    let expected = map.search(filtered_exact).unwrap();
    let mut filtered = SearchRequest::exact(&query, 8);
    filtered.policy = SearchPolicy::FixedBudget;
    filtered.options.backend = SearchBackend::Hnsw;
    filtered.filter = ProximityFilter::Prefix(b"hnsw-00");
    let actual = index.search(&map, filtered).unwrap();
    assert_eq!(actual.neighbors.len(), 8);
    assert!(actual
        .neighbors
        .iter()
        .all(|neighbor| neighbor.key.starts_with(b"hnsw-00")));
    let expected_keys: HashSet<_> = expected
        .neighbors
        .iter()
        .map(|neighbor| neighbor.key.clone())
        .collect();
    assert!(
        actual
            .neighbors
            .iter()
            .filter(|neighbor| expected_keys.contains(&neighbor.key))
            .count()
            >= 6
    );
}

#[test]
fn construction_and_search_ef_increase_logical_work_and_preserve_recall_floor() {
    let source = records();
    let store = Arc::new(MemStore::new());
    let map = map(store, source);
    let mut low_config = config();
    low_config.ef_construction = 32;
    let mut high_config = low_config.clone();
    high_config.ef_construction = 128;
    let (low_index, low_build) = HnswIndex::build(&map, low_config).unwrap();
    let (index, high_build) = HnswIndex::build(&map, high_config).unwrap();
    assert!(high_build.distance_evaluations >= low_build.distance_evaluations);

    let query = [20.1, 20.2, 20.3, 20.4, 20.5, 20.6, 20.7, 20.8];
    let exact = map.search(SearchRequest::exact(&query, 10)).unwrap();
    let exact_keys: HashSet<_> = exact.neighbors.iter().map(|hit| hit.key.clone()).collect();
    let mut low_construction_request = SearchRequest::exact(&query, 10);
    low_construction_request.policy = SearchPolicy::FixedBudget;
    low_construction_request.options.backend = SearchBackend::Hnsw;
    let low_construction_result = low_index.search(&map, low_construction_request).unwrap();
    assert_eq!(
        low_construction_result
            .neighbors
            .iter()
            .filter(|hit| exact_keys.contains(&hit.key))
            .count(),
        10
    );
    let mut low = SearchRequest::exact(&query, 10);
    low.policy = SearchPolicy::FixedBudget;
    low.options.backend = SearchBackend::Hnsw;
    low.options.hnsw.ef_search = Some(16);
    let mut high = low.clone();
    high.options.hnsw.ef_search = Some(128);
    let low = index.search(&map, low).unwrap();
    let high = index.search(&map, high).unwrap();
    assert!(high.stats.distance_evaluations >= low.stats.distance_evaluations);
    assert!(high.stats.reranked_candidates >= low.stats.reranked_candidates);
    let low_recall = low
        .neighbors
        .iter()
        .filter(|hit| exact_keys.contains(&hit.key))
        .count();
    let high_recall = high
        .neighbors
        .iter()
        .filter(|hit| exact_keys.contains(&hit.key))
        .count();
    assert!(high_recall >= low_recall);
    assert_eq!(high_recall, 10);
}

#[test]
fn explicit_hnsw_rejects_exact_and_stale_while_auto_falls_back_to_native() {
    let store = Arc::new(MemStore::new());
    let map = map(store, records());
    let (index, _) = HnswIndex::build(&map, config()).unwrap();
    let query = [4.0; 8];
    let mut exact_hnsw = SearchRequest::exact(&query, 5);
    exact_hnsw.options.backend = SearchBackend::Hnsw;
    assert!(matches!(
        index.search(&map, exact_hnsw),
        Err(prolly::Error::InvalidProximitySearch { .. })
    ));

    let (mutated, _) = map
        .mutate_batch([ProximityMutation {
            key: b"hnsw-0001".to_vec(),
            value: None,
        }])
        .unwrap();
    let mut stale = SearchRequest::exact(&query, 5);
    stale.policy = SearchPolicy::FixedBudget;
    stale.options.backend = SearchBackend::Hnsw;
    assert!(matches!(
        index.search(&mutated, stale),
        Err(prolly::Error::InvalidProximitySearch { .. })
    ));

    let mut automatic = SearchRequest::exact(&query, 5);
    automatic.policy = SearchPolicy::FixedBudget;
    automatic.options.backend = SearchBackend::Auto;
    let fallback = index.search(&mutated, automatic.clone()).unwrap();
    automatic.options.backend = SearchBackend::Native;
    let native = mutated.search(automatic).unwrap();
    assert_eq!(fallback.neighbors, native.neighbors);
    assert_eq!(fallback.completion, native.completion);

    let mut missing_sidecar = SearchRequest::exact(&query, 5);
    missing_sidecar.policy = SearchPolicy::FixedBudget;
    missing_sidecar.options.backend = SearchBackend::Auto;
    assert_eq!(
        mutated.search(missing_sidecar.clone()).unwrap().neighbors,
        mutated
            .search({
                missing_sidecar.options.backend = SearchBackend::Native;
                missing_sidecar
            })
            .unwrap()
            .neighbors
    );
}
