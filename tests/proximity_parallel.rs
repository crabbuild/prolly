use prolly::{
    BuildParallelism, DistanceMetric, MemStore, ProximityConfig, ProximityMap, ProximityRecord,
};
use std::sync::Arc;

fn records() -> Vec<ProximityRecord> {
    (0usize..512)
        .map(|index| ProximityRecord {
            key: format!("parallel-{index:05}").into_bytes(),
            vector: (0..17)
                .map(|dimension| ((index * 31 + dimension * 19) % 997) as f32 + 1.0)
                .collect(),
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

#[test]
fn thread_counts_produce_identical_roots_and_logical_statistics() {
    for metric in [
        DistanceMetric::L2Squared,
        DistanceMetric::Cosine,
        DistanceMetric::InnerProduct,
    ] {
        let mut config = ProximityConfig::new(17);
        config.metric = metric;
        config.hierarchy.log_chunk_size = 3;
        config.hierarchy.level_hash_seed = 73;
        let mut expected = None;
        let mut thread_counts = vec![
            1,
            2,
            4,
            std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
        ];
        thread_counts.sort_unstable();
        thread_counts.dedup();
        for threads in thread_counts {
            let (map, stats) = ProximityMap::build_with_parallelism(
                Arc::new(MemStore::new()),
                config.clone(),
                records(),
                BuildParallelism::new(threads).unwrap(),
            )
            .unwrap();
            map.verify().unwrap();
            let actual = (map.tree().clone(), stats);
            if let Some(expected) = &expected {
                assert_eq!(&actual, expected, "metric={metric:?} threads={threads}");
            } else {
                expected = Some(actual);
            }
        }
    }
}

#[test]
fn invalid_worker_count_is_rejected_before_construction() {
    assert!(BuildParallelism::new(0).is_err());
}

#[test]
fn validation_reports_the_lowest_key_error_for_every_worker_count() {
    let records = vec![
        ProximityRecord {
            key: b"b".to_vec(),
            vector: vec![f32::NAN; 3],
            value: Vec::new(),
        },
        ProximityRecord {
            key: b"a".to_vec(),
            vector: vec![1.0; 2],
            value: Vec::new(),
        },
    ];
    for threads in [1, 4] {
        let error = ProximityMap::build_with_parallelism(
            Arc::new(MemStore::new()),
            ProximityConfig::new(3),
            records.clone(),
            BuildParallelism::new(threads).unwrap(),
        )
        .err()
        .expect("invalid input must fail");
        assert!(format!("{error}").contains("expected 3 dimensions, received 2"));
    }
}
