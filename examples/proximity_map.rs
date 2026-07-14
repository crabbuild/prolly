use prolly::{
    copy_and_publish_content_graph, AdaptiveQuality, ContentGraphLimits, ContentRootManifest,
    DistanceMetric, MemStore, ProximityConfig, ProximityFilter, ProximityMap, ProximityMutation,
    ProximityRecord, SearchPolicy, SearchRequest, TypedContentRoot,
};
use std::collections::BTreeMap;
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

    let result = map.search(SearchRequest::exact(&[0.1, 0.9, 0.0], 2))?;
    for neighbor in result.neighbors {
        println!(
            "{} distance={} value={}",
            String::from_utf8_lossy(&neighbor.key),
            neighbor.distance,
            String::from_utf8_lossy(&neighbor.value)
        );
    }

    let query = [0.1, 0.9, 0.0];
    let mut filtered = SearchRequest::exact(&query, 2);
    filtered.policy = SearchPolicy::Adaptive(AdaptiveQuality::HighRecall);
    filtered.filter = ProximityFilter::Prefix(b"document-");
    println!("filtered completion={:?}", map.search(filtered)?.completion);

    let limits = ContentGraphLimits::default();
    let membership = map.prove_membership(b"document-a")?;
    membership.verify_for(&map.tree().descriptor)?;
    map.prove_search(SearchRequest::exact(&query, 2), &limits)?
        .verify_for_source(&map.tree().descriptor, &limits)?;

    let (updated, stats) = map.mutate_batch([ProximityMutation {
        key: b"document-b".to_vec(),
        value: Some((vec![0.2, 0.8, 0.0], b"second-updated".to_vec())),
    }])?;
    updated.verify()?;
    println!("descriptor={:?}", updated.tree().descriptor);
    println!("proximity nodes written={}", stats.nodes_written);

    let replica = MemStore::new();
    let manifest = ContentRootManifest {
        root: TypedContentRoot::proximity_descriptor(updated.tree().descriptor.clone()),
        logical_version: 1,
        created_at_millis: 0,
        metadata: BTreeMap::new(),
    };
    copy_and_publish_content_graph(&store, &replica, b"proximity/main", manifest, &limits)?;

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
