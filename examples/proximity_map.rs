use prolly::{
    DistanceMetric, MemStore, ProximityConfig, ProximityMap, ProximityMutation, ProximityRecord,
    SearchOptions,
};
use std::error::Error;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn Error>> {
    let store = Arc::new(MemStore::new());
    let mut config = ProximityConfig::new(3);
    config.metric = DistanceMetric::L2Squared;
    config.hierarchy.level_hash_seed = 42;
    config.overflow.max_page_bytes = 256 * 1024;
    let map = ProximityMap::build(
        store.clone(),
        config,
        [
            record("document-a", [0.0, 1.0, 0.0], "first"),
            record("document-b", [1.0, 0.0, 0.0], "second"),
            record("document-c", [0.0, 0.0, 1.0], "third"),
        ],
    )?;

    let result = map.search(
        &[0.1, 0.9, 0.0],
        SearchOptions {
            k: 2,
            beam_width: 32,
            max_nodes: None,
            max_distance_evaluations: None,
        },
    )?;
    for neighbor in result.neighbors {
        println!(
            "{} distance={} value={}",
            String::from_utf8_lossy(&neighbor.key),
            neighbor.distance,
            String::from_utf8_lossy(&neighbor.value)
        );
    }

    let (updated, stats) = map.mutate_batch([ProximityMutation {
        key: b"document-b".to_vec(),
        value: Some((vec![0.2, 0.8, 0.0], b"second-updated".to_vec())),
    }])?;
    updated.verify()?;
    println!("descriptor={:?}", updated.tree().descriptor);
    println!("proximity nodes written={}", stats.nodes_written);

    let reopened = ProximityMap::load(store, updated.tree().descriptor.clone())?;
    assert!(reopened.contains_key(b"document-b")?);
    Ok(())
}

fn record(key: &str, vector: [f32; 3], value: &str) -> ProximityRecord {
    ProximityRecord {
        key: key.as_bytes().to_vec(),
        vector: vector.to_vec(),
        value: value.as_bytes().to_vec(),
    }
}
