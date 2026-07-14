use prolly::{
    MemStore, ProximityConfig, ProximityMap, ProximityMutation, ProximityRecord, SearchRequest,
};
use std::sync::Arc;

fn records(count: usize, dimensions: usize) -> Vec<ProximityRecord> {
    (0..count)
        .map(|index| ProximityRecord {
            key: format!("key-{index:04}").into_bytes(),
            vector: (0..dimensions)
                .map(|dimension| ((index * 31 + dimension * 17) % 257) as f32 / 257.0)
                .collect(),
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

fn overflow_config(dimensions: u32) -> ProximityConfig {
    let mut config = ProximityConfig::new(dimensions);
    config.hierarchy.log_chunk_size = 63;
    config.vector_storage.inline_threshold_bytes = 64;
    config.overflow.min_page_bytes = 160;
    config.overflow.target_page_bytes = 240;
    config.overflow.max_page_bytes = 384;
    config.overflow.hash_seed = 0xfeed_cafe;
    config
}

#[test]
fn external_vectors_and_recursive_overflow_are_transparent() {
    let store = Arc::new(MemStore::new());
    let input = records(96, 256);
    let map = ProximityMap::build(store.clone(), overflow_config(256), input.clone()).unwrap();

    assert_eq!(
        map.get(&input[37].key).unwrap().unwrap().0,
        input[37].vector
    );
    let result = map
        .search(SearchRequest::exact(&input[37].vector, 5))
        .unwrap();
    assert_eq!(result.neighbors[0].key, input[37].key);

    let verification = map.verify().unwrap();
    assert!(verification.external_vector_count > 0);
    assert!(verification.overflow_page_count > 1);
    assert!(verification.overflow_directory_count > 1);
    assert!(verification.maximum_node_bytes <= 384);
}

#[test]
fn overflow_layout_is_canonical_across_input_permutations() {
    let store = Arc::new(MemStore::new());
    let mut reversed = records(64, 128);
    let forward = reversed.clone();
    reversed.reverse();

    let left = ProximityMap::build(store.clone(), overflow_config(128), forward).unwrap();
    let right = ProximityMap::build(store, overflow_config(128), reversed).unwrap();
    assert_eq!(left.tree().proximity_root, right.tree().proximity_root);
    assert_eq!(left.tree().descriptor, right.tree().descriptor);
}

#[test]
fn mutation_of_an_overflowed_hierarchy_remains_clean_build_equivalent() {
    let store = Arc::new(MemStore::new());
    let input = records(96, 256);
    let map = ProximityMap::build(store.clone(), overflow_config(256), input.clone()).unwrap();
    let mutation = ProximityMutation {
        key: input[41].key.clone(),
        value: Some((vec![0.25; 256], b"changed".to_vec())),
    };
    let (mutated, stats) = map.mutate_batch([mutation.clone()]).unwrap();

    let mut expected = input;
    expected[41].vector = vec![0.25; 256];
    expected[41].value = b"changed".to_vec();
    let clean = ProximityMap::build(store, overflow_config(256), expected).unwrap();
    assert_eq!(mutated.tree().proximity_root, clean.tree().proximity_root);
    assert_eq!(mutated.tree().directory.root, clean.tree().directory.root);
    assert!(stats.full_proximity_rebuild);
    mutated.verify().unwrap();
}
