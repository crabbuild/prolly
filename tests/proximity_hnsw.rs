use prolly::{
    HnswConfig, HnswIndex, MemStore, ProximityConfig, ProximityFilter, ProximityMap,
    ProximityMutation, ProximityRecord, SearchBackend, SearchPolicy, SearchRequest, Store,
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
    let (second, second_stats) = HnswIndex::build(&reverse_map, config()).unwrap();
    assert_eq!(first.manifest_cid(), second.manifest_cid());
    assert_eq!(first_stats, second_stats);
    assert!(first.is_canonical());
    assert!(first_stats.directed_edges > 0);

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

    let exact = map.search(SearchRequest::exact(&query, 12)).unwrap();
    let mut request = SearchRequest::exact(&query, 12);
    request.policy = SearchPolicy::FixedBudget;
    request.backend = SearchBackend::Hnsw;
    let first = index.search(&map, request.clone()).unwrap();
    let second = index.search(&map, request).unwrap();
    assert_eq!(first.neighbors, second.neighbors);
    assert_eq!(first.stats, second.stats);
    let exact_keys: HashSet<_> = exact.neighbors.iter().map(|hit| hit.key.clone()).collect();
    let overlap = first
        .neighbors
        .iter()
        .filter(|hit| exact_keys.contains(&hit.key))
        .count();
    assert!(overlap >= 10, "fixed-seed recall was {overlap}/12");
    for hit in &first.neighbors {
        assert_eq!(map.get(&hit.key).unwrap().unwrap().1, hit.value);
    }

    let mut filtered_exact = SearchRequest::exact(&query, 8);
    filtered_exact.filter = ProximityFilter::Prefix(b"hnsw-00");
    let expected = map.search(filtered_exact).unwrap();
    let mut filtered = SearchRequest::exact(&query, 8);
    filtered.policy = SearchPolicy::FixedBudget;
    filtered.backend = SearchBackend::Hnsw;
    filtered.filter = ProximityFilter::Prefix(b"hnsw-00");
    let actual = index.search(&map, filtered).unwrap();
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
fn explicit_hnsw_rejects_exact_and_stale_while_auto_falls_back_to_native() {
    let store = Arc::new(MemStore::new());
    let map = map(store, records());
    let (index, _) = HnswIndex::build(&map, config()).unwrap();
    let query = [4.0; 8];
    let mut exact_hnsw = SearchRequest::exact(&query, 5);
    exact_hnsw.backend = SearchBackend::Hnsw;
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
    stale.backend = SearchBackend::Hnsw;
    assert!(matches!(
        index.search(&mutated, stale),
        Err(prolly::Error::InvalidProximitySearch { .. })
    ));

    let mut automatic = SearchRequest::exact(&query, 5);
    automatic.policy = SearchPolicy::FixedBudget;
    automatic.backend = SearchBackend::Auto;
    let fallback = index.search(&mutated, automatic.clone()).unwrap();
    automatic.backend = SearchBackend::Native;
    let native = mutated.search(automatic).unwrap();
    assert_eq!(fallback.neighbors, native.neighbors);
    assert_eq!(fallback.completion, native.completion);

    let mut missing_sidecar = SearchRequest::exact(&query, 5);
    missing_sidecar.policy = SearchPolicy::FixedBudget;
    missing_sidecar.backend = SearchBackend::Auto;
    assert_eq!(
        mutated.search(missing_sidecar.clone()).unwrap().neighbors,
        mutated
            .search({
                missing_sidecar.backend = SearchBackend::Native;
                missing_sidecar
            })
            .unwrap()
            .neighbors
    );
}
