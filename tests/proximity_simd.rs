use prolly::{
    DistanceMetric, MemStore, ProximityConfig, ProximityMap, ProximityRecord, QueryKernel,
    SearchRequest,
};
use std::sync::Arc;

fn records(dimensions: usize) -> Vec<ProximityRecord> {
    (0..257)
        .map(|index| {
            let vector = (0..dimensions)
                .map(|dimension| {
                    let base = ((index * 37 + dimension * 19) % 101) as f32 - 50.0;
                    if index % 17 == 0 && dimension == dimensions / 2 {
                        base + f32::EPSILON
                    } else {
                        base
                    }
                })
                .collect();
            ProximityRecord {
                key: format!("simd-{index:04}").into_bytes(),
                vector,
                value: index.to_le_bytes().to_vec(),
            }
        })
        .collect()
}

#[test]
fn scalar_simd_and_auto_searches_are_bit_identical() {
    for metric in [
        DistanceMetric::L2Squared,
        DistanceMetric::Cosine,
        DistanceMetric::InnerProduct,
    ] {
        let dimensions = 65;
        let mut config = ProximityConfig::new(dimensions as u32);
        config.metric = metric;
        config.hierarchy.log_chunk_size = 2;
        config.hierarchy.level_hash_seed = 0x5eed;
        let map =
            ProximityMap::build(Arc::new(MemStore::new()), config, records(dimensions)).unwrap();
        let query: Vec<_> = (0..dimensions)
            .map(|dimension| (dimension as f32 * 0.25) - 8.0)
            .collect();

        let run = |kernel| {
            let mut request = SearchRequest::exact(&query, 23);
            request.kernel = kernel;
            map.search(request).unwrap()
        };
        let scalar = run(QueryKernel::ScalarDeterministic);
        let simd = run(QueryKernel::SimdDeterministic);
        let automatic = run(QueryKernel::AutoDeterministic);

        assert_eq!(scalar.completion, simd.completion, "metric={metric:?}");
        assert_eq!(scalar.completion, automatic.completion, "metric={metric:?}");
        assert_eq!(scalar.stats, simd.stats, "metric={metric:?}");
        assert_eq!(scalar.stats, automatic.stats, "metric={metric:?}");
        for (expected, actual) in scalar.neighbors.iter().zip(&simd.neighbors) {
            assert_eq!(expected.key, actual.key, "metric={metric:?}");
            assert_eq!(expected.value, actual.value, "metric={metric:?}");
            assert_eq!(
                expected.distance.to_bits(),
                actual.distance.to_bits(),
                "metric={metric:?} key={:?}",
                expected.key
            );
        }
        for (expected, actual) in scalar.neighbors.iter().zip(&automatic.neighbors) {
            assert_eq!(expected.key, actual.key, "metric={metric:?}");
            assert_eq!(expected.value, actual.value, "metric={metric:?}");
            assert_eq!(
                expected.distance.to_bits(),
                actual.distance.to_bits(),
                "metric={metric:?} key={:?}",
                expected.key
            );
        }
    }
}
