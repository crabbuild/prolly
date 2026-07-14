use prolly::{
    DistanceMetric, MemStore, ProximityConfig, ProximityMap, ProximityMutation, ProximityRecord,
};
use std::sync::Arc;
use xxhash_rust::xxh64::xxh64;

fn config() -> ProximityConfig {
    ProximityConfig {
        dimensions: 2,
        metric: DistanceMetric::L2Squared,
        log_chunk_size: 1,
        level_hash_seed: 7,
        max_node_bytes: 256 * 1024,
    }
}

fn records() -> Vec<ProximityRecord> {
    (0..64)
        .map(|index| ProximityRecord {
            key: format!("key-{index:03}").into_bytes(),
            vector: vec![index as f32, (index % 7) as f32],
            value: format!("value-{index}").into_bytes(),
        })
        .collect()
}

#[test]
fn value_only_mutation_reuses_proximity_root_and_matches_clean_rebuild() {
    let store = Arc::new(MemStore::new());
    let map = ProximityMap::build(store, config(), records()).unwrap();
    let mutation = ProximityMutation {
        key: b"key-010".to_vec(),
        value: Some((vec![10.0, 3.0], b"replacement".to_vec())),
    };

    let (localized, stats) = map.mutate_batch([mutation.clone()]).unwrap();
    let rebuilt = map.rebuild_batch([mutation]).unwrap();

    assert_eq!(localized.tree().descriptor, rebuilt.tree().descriptor);
    assert_eq!(localized.tree().proximity_root, map.tree().proximity_root);
    assert_eq!(stats.nodes_written, 0);
    assert!(!stats.full_proximity_rebuild);
    assert_eq!(
        localized.get(b"key-010").unwrap().unwrap().1,
        b"replacement".to_vec()
    );
    assert_eq!(
        map.get(b"key-010").unwrap().unwrap().1,
        b"value-10".to_vec()
    );
}

#[test]
fn localized_structural_mutation_matches_clean_rebuild_and_reuses_nodes() {
    let store = Arc::new(MemStore::new());
    let map = ProximityMap::build(store, config(), records()).unwrap();
    let mutation = ProximityMutation {
        key: b"key-010".to_vec(),
        value: Some((vec![10.25, 3.0], b"moved".to_vec())),
    };

    let (localized, stats) = map.mutate_batch([mutation.clone()]).unwrap();
    let rebuilt = map.rebuild_batch([mutation]).unwrap();

    assert_eq!(localized.tree().descriptor, rebuilt.tree().descriptor);
    assert!(stats.nodes_reused > 0);
    assert!(stats.nodes_written > 0);
    assert!(stats.nodes_written < stats.nodes_written + stats.nodes_reused);
}

#[test]
fn localized_mutation_matches_clean_rebuild_across_mixed_sequence() {
    let store = Arc::new(MemStore::new());
    let mut map = ProximityMap::build(store, config(), records()).unwrap();
    let mut state = 0x9e37_79b9_u64;

    for step in 0..100 {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        let index = (state % 80) as usize;
        let key = format!("key-{index:03}").into_bytes();
        let mutation = if step % 11 == 0 {
            ProximityMutation { key, value: None }
        } else {
            ProximityMutation {
                key,
                value: Some((
                    vec![index as f32 + step as f32 / 100.0, (index % 7) as f32],
                    format!("step-{step}").into_bytes(),
                )),
            }
        };

        let clean = map.rebuild_batch([mutation.clone()]).unwrap();
        let (localized, _) = map.mutate_batch([mutation.clone()]).unwrap();
        assert_eq!(
            localized.tree().directory.root,
            clean.tree().directory.root,
            "directory mismatch at step {step}: {mutation:?}"
        );
        assert_eq!(
            localized.tree().proximity_root,
            clean.tree().proximity_root,
            "proximity mismatch at step {step}: {mutation:?}"
        );
        assert_eq!(
            localized.tree().descriptor,
            clean.tree().descriptor,
            "descriptor mismatch at step {step}: {mutation:?}"
        );
        localized.verify().unwrap();
        map = localized;
    }
}

#[test]
fn root_representative_update_uses_documented_full_rebuild_fallback() {
    let store = Arc::new(MemStore::new());
    let source = records();
    let root_key = source
        .iter()
        .max_by_key(|record| xxh64(&record.key, 7).leading_zeros())
        .unwrap()
        .key
        .clone();
    let map = ProximityMap::build(store, config(), source).unwrap();
    let mutation = ProximityMutation {
        key: root_key,
        value: Some((vec![999.0, 999.0], b"root-moved".to_vec())),
    };

    let (localized, stats) = map.mutate_batch([mutation.clone()]).unwrap();
    let clean = map.rebuild_batch([mutation]).unwrap();
    assert!(stats.full_proximity_rebuild);
    assert_eq!(localized.tree().descriptor, clean.tree().descriptor);
}
