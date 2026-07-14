use prolly::{
    DistanceMetric, MemStore, ProximityConfig, ProximityMap, ProximityMutation, ProximityRecord,
};
use std::sync::Arc;
use xxhash_rust::xxh64::xxh64;

fn config() -> ProximityConfig {
    let mut config = ProximityConfig::new(2);
    config.metric = DistanceMetric::L2Squared;
    config.hierarchy.log_chunk_size = 1;
    config.hierarchy.level_hash_seed = 7;
    config.overflow.max_page_bytes = 256 * 1024;
    config
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

#[test]
fn exact_directory_mutation_resynchronizes_before_the_full_suffix() {
    let store = Arc::new(MemStore::new());
    let mut local_config = config();
    local_config.hierarchy.log_chunk_size = 63;
    let source: Vec<_> = (0usize..10_000)
        .map(|index| ProximityRecord {
            key: format!("wide-{index:05}").into_bytes(),
            vector: vec![index as f32, 0.0],
            value: index.to_le_bytes().to_vec(),
        })
        .collect();
    let map = ProximityMap::build(store, local_config, source).unwrap();
    let (mutated, stats) = map
        .mutate_batch([ProximityMutation {
            key: b"wide-05000".to_vec(),
            value: Some((vec![5000.0, 0.0], b"replacement".to_vec())),
        }])
        .unwrap();

    assert_eq!(mutated.tree().proximity_root, map.tree().proximity_root);
    assert!(stats.directory_entries_scanned < 10_000);
    assert!(stats.directory_nodes_reused > 0);
    assert!(stats.directory_nodes_written > 0);
}

#[test]
fn localized_mutation_traverses_overflow_without_changing_the_clean_oracle() {
    let store = Arc::new(MemStore::new());
    let mut overflow = ProximityConfig::new(32);
    overflow.hierarchy.log_chunk_size = 2;
    overflow.hierarchy.level_hash_seed = 7;
    overflow.vector_storage.inline_threshold_bytes = 64;
    overflow.overflow.min_page_bytes = 220;
    overflow.overflow.target_page_bytes = 340;
    overflow.overflow.max_page_bytes = 512;
    let source: Vec<_> = (0usize..256)
        .map(|index| ProximityRecord {
            key: format!("overflow-{index:04}").into_bytes(),
            vector: (0..32)
                .map(|dimension| (index * 13 + dimension * 7) as f32)
                .collect(),
            value: index.to_le_bytes().to_vec(),
        })
        .collect();
    let key = source
        .iter()
        .find(|record| xxh64(&record.key, 7).leading_zeros() < 2)
        .unwrap()
        .key
        .clone();
    let map = ProximityMap::build(store, overflow, source).unwrap();
    assert!(map.verify().unwrap().overflow_page_count > 0);
    let mutation = ProximityMutation {
        key,
        value: Some((vec![0.125; 32], b"moved".to_vec())),
    };

    let clean = map.rebuild_batch([mutation.clone()]).unwrap();
    let (localized, stats) = map.mutate_batch([mutation]).unwrap();
    assert!(!stats.full_proximity_rebuild);
    assert!(stats.nodes_reused > 0);
    assert_eq!(localized.tree().directory.root, clean.tree().directory.root);
    assert_eq!(localized.tree().proximity_root, clean.tree().proximity_root);
    assert_eq!(localized.tree().descriptor, clean.tree().descriptor);
    localized.verify().unwrap();
}

#[test]
fn localized_batches_are_clean_equivalent_for_every_canonical_metric() {
    for metric in [
        DistanceMetric::L2Squared,
        DistanceMetric::Cosine,
        DistanceMetric::InnerProduct,
    ] {
        let store = Arc::new(MemStore::new());
        let mut metric_config = ProximityConfig::new(2);
        metric_config.metric = metric;
        metric_config.hierarchy.log_chunk_size = 2;
        metric_config.hierarchy.level_hash_seed = 7;
        let source: Vec<_> = (0usize..128)
            .map(|index| ProximityRecord {
                key: format!("metric-{index:04}").into_bytes(),
                vector: vec![index as f32 + 1.0, (index % 11) as f32 + 1.0],
                value: index.to_le_bytes().to_vec(),
            })
            .collect();
        let movable: Vec<_> = source
            .iter()
            .filter(|record| xxh64(&record.key, 7).leading_zeros() < 2)
            .take(2)
            .map(|record| record.key.clone())
            .collect();
        let map = ProximityMap::build(store, metric_config, source).unwrap();
        map.verify()
            .unwrap_or_else(|error| panic!("source metric {metric:?}: {error:?}"));
        let mutations = vec![
            ProximityMutation {
                key: movable[0].clone(),
                value: Some((vec![0.25, 0.75], b"first".to_vec())),
            },
            ProximityMutation {
                key: movable[1].clone(),
                value: None,
            },
            ProximityMutation {
                key: b"metric-new".to_vec(),
                value: Some((vec![0.75, 0.25], b"new".to_vec())),
            },
        ];

        let clean = map.rebuild_batch(mutations.clone()).unwrap();
        let (localized, _) = map.mutate_batch(mutations).unwrap();
        assert_eq!(
            localized.tree().descriptor,
            clean.tree().descriptor,
            "metric {metric:?}"
        );
        localized
            .verify()
            .unwrap_or_else(|error| panic!("metric {metric:?}: {error:?}"));
    }
}
