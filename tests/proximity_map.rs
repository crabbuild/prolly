use prolly::{
    DistanceMetric, Error, MemStore, ProximityConfig, ProximityFilter, ProximityMap,
    ProximityRecord, SearchBackend, SearchBudget, SearchCompletion, SearchPolicy, SearchRequest,
};
use std::sync::Arc;

fn config(dimensions: u32) -> ProximityConfig {
    let mut config = ProximityConfig::new(dimensions);
    config.metric = DistanceMetric::L2Squared;
    config.hierarchy.level_hash_seed = 42;
    config.overflow.max_page_bytes = 256 * 1024;
    config
}

#[test]
fn proximity_search_returns_distance_then_key_ordered_neighbors() {
    let store = Arc::new(MemStore::new());
    let map = ProximityMap::build(
        store,
        config(2),
        [
            ProximityRecord {
                key: b"b".to_vec(),
                vector: vec![1.0, 0.0],
                value: b"B".to_vec(),
            },
            ProximityRecord {
                key: b"a".to_vec(),
                vector: vec![-1.0, 0.0],
                value: b"A".to_vec(),
            },
            ProximityRecord {
                key: b"far".to_vec(),
                vector: vec![10.0, 0.0],
                value: b"F".to_vec(),
            },
        ],
    )
    .unwrap();

    let result = map.search(SearchRequest::exact(&[0.0, 0.0], 2)).unwrap();

    assert_eq!(
        result
            .neighbors
            .iter()
            .map(|neighbor| neighbor.key.as_slice())
            .collect::<Vec<_>>(),
        vec![b"a".as_slice(), b"b".as_slice()]
    );
    assert_eq!(result.neighbors[0].distance, 1.0);
    assert_eq!(result.stats.distance_evaluations, 3);
}

#[test]
fn proximity_config_rejects_zero_dimensions() {
    let config = config(0);

    assert!(matches!(
        config.validate(),
        Err(Error::InvalidProximityConfig { .. })
    ));
}

#[test]
fn proximity_build_supports_exact_present_and_absent_key_lookup() {
    let store = Arc::new(MemStore::new());
    let map = ProximityMap::build(
        store,
        config(2),
        [ProximityRecord {
            key: b"document-1".to_vec(),
            vector: vec![1.0, -0.0],
            value: b"payload".to_vec(),
        }],
    )
    .unwrap();

    let (vector, value) = map.get(b"document-1").unwrap().unwrap();
    assert_eq!(vector, vec![1.0, 0.0]);
    assert_eq!(value, b"payload".to_vec());
    assert_eq!(map.get(b"absent").unwrap(), None);
    assert!(!map.contains_key(b"absent").unwrap());
}

#[test]
fn empty_map_and_duplicate_vectors_have_unambiguous_semantics() {
    let empty = ProximityMap::build(Arc::new(MemStore::new()), config(2), []).unwrap();
    assert_eq!(empty.get(b"absent").unwrap(), None);
    assert!(empty
        .search(SearchRequest::exact(&[0.0, 0.0], 1))
        .unwrap()
        .neighbors
        .is_empty());

    let map = ProximityMap::build(
        Arc::new(MemStore::new()),
        config(2),
        [
            ProximityRecord {
                key: b"a".to_vec(),
                vector: vec![1.0, 1.0],
                value: Vec::new(),
            },
            ProximityRecord {
                key: b"b".to_vec(),
                vector: vec![1.0, 1.0],
                value: vec![7; 128 * 1024],
            },
        ],
    )
    .unwrap();
    let result = map.search(SearchRequest::exact(&[1.0, 1.0], 2)).unwrap();
    assert_eq!(
        result
            .neighbors
            .iter()
            .map(|neighbor| neighbor.key.as_slice())
            .collect::<Vec<_>>(),
        [b"a".as_slice(), b"b".as_slice()]
    );
    assert!(map.get(b"a").unwrap().unwrap().1.is_empty());
    assert_eq!(map.get(b"b").unwrap().unwrap().1.len(), 128 * 1024);
}

#[test]
fn proximity_map_loads_from_its_descriptor_cid() {
    let store = Arc::new(MemStore::new());
    let map = ProximityMap::build(
        store.clone(),
        config(2),
        [ProximityRecord {
            key: b"document-1".to_vec(),
            vector: vec![1.0, 2.0],
            value: b"payload".to_vec(),
        }],
    )
    .unwrap();
    let descriptor = map.tree().descriptor.clone();

    let loaded = ProximityMap::load(store, descriptor).unwrap();
    assert_eq!(loaded.tree(), map.tree());
    assert_eq!(
        loaded.get(b"document-1").unwrap(),
        Some((vec![1.0, 2.0], b"payload".to_vec()))
    );
}

#[test]
fn proximity_verify_checks_a_multilevel_hierarchy() {
    let store = Arc::new(MemStore::new());
    let mut multilevel = config(2);
    multilevel.hierarchy.log_chunk_size = 1;
    let records = (0..64).map(|index| ProximityRecord {
        key: format!("key-{index:03}").into_bytes(),
        vector: vec![index as f32, (index % 5) as f32],
        value: vec![index as u8],
    });
    let map = ProximityMap::build(store, multilevel, records).unwrap();

    let verification = map.verify().unwrap();
    assert_eq!(verification.record_count, 64);
    assert!(verification.proximity_node_count > 1);
    assert!(verification.maximum_level > 0);
    assert!(verification.distance_checks > 0);
}

#[test]
fn exhaustive_beam_matches_brute_force_on_multilevel_map() {
    let store = Arc::new(MemStore::new());
    let mut multilevel = config(3);
    multilevel.hierarchy.log_chunk_size = 1;
    let records: Vec<_> = (0..128)
        .map(|index| ProximityRecord {
            key: format!("key-{index:03}").into_bytes(),
            vector: vec![index as f32 / 3.0, (index % 13) as f32, (index % 7) as f32],
            value: vec![index as u8],
        })
        .collect();
    let query = [17.25, 4.5, 2.0];
    let map = ProximityMap::build(store, multilevel, records.clone()).unwrap();

    let result = map.search(SearchRequest::exact(&query, 10)).unwrap();
    let mut expected = records;
    expected.sort_by(|left, right| {
        let distance = |vector: &[f32]| {
            vector
                .iter()
                .zip(query)
                .map(|(&left, right)| {
                    let delta = f64::from(left) - f64::from(right);
                    delta * delta
                })
                .sum::<f64>()
        };
        distance(&left.vector)
            .total_cmp(&distance(&right.vector))
            .then_with(|| left.key.cmp(&right.key))
    });

    assert_eq!(
        result
            .neighbors
            .iter()
            .map(|neighbor| neighbor.key.clone())
            .collect::<Vec<_>>(),
        expected
            .into_iter()
            .take(10)
            .map(|record| record.key)
            .collect::<Vec<_>>()
    );
}

#[test]
fn search_distance_budget_is_deterministic_and_reported() {
    let store = Arc::new(MemStore::new());
    let mut multilevel = config(2);
    multilevel.hierarchy.log_chunk_size = 1;
    let map = ProximityMap::build(
        store,
        multilevel,
        (0..64).map(|index| ProximityRecord {
            key: format!("key-{index:03}").into_bytes(),
            vector: vec![index as f32, 0.0],
            value: Vec::new(),
        }),
    )
    .unwrap();
    let query = [0.0, 0.0];
    let request = SearchRequest {
        query: &query,
        k: 1,
        policy: SearchPolicy::FixedBudget,
        budget: SearchBudget {
            max_distance_evaluations: Some(5),
            ..SearchBudget::default()
        },
        filter: ProximityFilter::All,
        backend: SearchBackend::Native,
        kernel: prolly::QueryKernel::AutoDeterministic,
    };

    let first = map.search(request.clone()).unwrap();
    let second = map.search(request).unwrap();
    assert_eq!(first.completion, SearchCompletion::BudgetExhausted);
    assert_eq!(first.stats.distance_evaluations, 5);
    assert_eq!(first.neighbors, second.neighbors);
    assert_eq!(first.stats.nodes_read, second.stats.nodes_read);
    assert_eq!(
        first.stats.distance_evaluations,
        second.stats.distance_evaluations
    );
    assert_eq!(first.completion, second.completion);
    assert_eq!(first.stats.bytes_read, second.stats.bytes_read);
}
