use prolly::{
    BuildParallelism, DistanceMetric, MemStore, ProductQuantizationBuildLimits,
    ProductQuantizationConfig, ProductQuantizer, ProximityConfig, ProximityFilter, ProximityMap,
    ProximityMutation, ProximityRecord, QueryKernel, ScalarQuantizationConfig, SearchBackend,
    SearchPolicy, SearchRequest,
};
use std::sync::Arc;

fn records(count: usize, dimensions: usize) -> Vec<ProximityRecord> {
    (0..count)
        .map(|index| ProximityRecord {
            key: format!("vector-{index:04}").into_bytes(),
            vector: (0..dimensions)
                .map(|dimension| {
                    let cluster = (index % 11) as f32 * 3.0;
                    cluster + ((index * 17 + dimension * 29) % 37) as f32 / 37.0
                })
                .collect(),
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

#[test]
fn local_sq8_is_committed_verified_reranked_and_locally_rewritten() {
    let store = Arc::new(MemStore::new());
    let source = records(257, 7);
    for metric in [
        DistanceMetric::L2Squared,
        DistanceMetric::Cosine,
        DistanceMetric::InnerProduct,
    ] {
        let mut config = ProximityConfig::new(7);
        config.metric = metric;
        config.hierarchy.log_chunk_size = 2;
        config.hierarchy.level_hash_seed = 91;
        config.scalar_quantization = Some(ScalarQuantizationConfig { group_size: 3 });
        let map = ProximityMap::build(store.clone(), config.clone(), source.clone()).unwrap();
        let verification = map.verify().unwrap();
        assert_eq!(
            verification.quantized_node_count,
            verification.proximity_node_count
        );
        assert!(verification.scalar_quantizer_count > 0);
        assert!(verification.scalar_quantizer_count <= verification.quantized_node_count);

        let query = [8.25, 8.5, 8.75, 9.0, 9.25, 9.5, 9.75];
        let mut exact_request = SearchRequest::exact(&query, 17);
        exact_request.kernel = QueryKernel::ScalarDeterministic;
        let exact = map.search(exact_request).unwrap();
        assert_eq!(exact.stats.quantized_distance_evaluations, 0);

        let mut accelerated_request = SearchRequest::exact(&query, 17);
        accelerated_request.policy = SearchPolicy::FixedBudget;
        accelerated_request.kernel = QueryKernel::ScalarDeterministic;
        let accelerated = map.search(accelerated_request).unwrap();
        assert!(accelerated.stats.quantized_distance_evaluations > 0);
        assert_eq!(accelerated.stats.reranked_candidates, 17);
        assert_eq!(accelerated.neighbors.len(), exact.neighbors.len());
        for (expected, actual) in exact.neighbors.iter().zip(&accelerated.neighbors) {
            assert_eq!(expected.key, actual.key, "metric={metric:?}");
            assert_eq!(
                expected.distance.to_bits(),
                actual.distance.to_bits(),
                "metric={metric:?}"
            );
        }

        let replacement = vec![4.0, 4.25, 4.5, 4.75, 5.0, 5.25, 5.5];
        let mutation = ProximityMutation {
            key: b"vector-0128".to_vec(),
            value: Some((replacement.clone(), b"changed".to_vec())),
        };
        let (mutated, stats) = map.mutate_batch([mutation.clone()]).unwrap();
        let clean = map.rebuild_batch([mutation]).unwrap();
        assert_eq!(mutated.tree().descriptor, clean.tree().descriptor);
        assert!(stats.nodes_reused > 0);
        assert_eq!(
            mutated.get(b"vector-0128").unwrap().unwrap().0,
            if metric == DistanceMetric::Cosine {
                clean.get(b"vector-0128").unwrap().unwrap().0
            } else {
                replacement
            }
        );
        assert_eq!(
            mutated.verify().unwrap().quantized_node_count,
            mutated.verify().unwrap().proximity_node_count
        );
    }
}

#[test]
fn product_quantization_is_thread_identical_source_bound_and_full_precision_reranked() {
    let store = Arc::new(MemStore::new());
    let mut map_config = ProximityConfig::new(7);
    map_config.hierarchy.log_chunk_size = 2;
    map_config.hierarchy.level_hash_seed = 17;
    let map = ProximityMap::build(store.clone(), map_config, records(193, 7)).unwrap();
    let config = ProductQuantizationConfig {
        subquantizers: 3,
        centroids_per_subquantizer: 16,
        training_iterations: 6,
        rerank_multiplier: 16,
        seed: 0x5eed,
        max_training_vectors: 32,
    };

    let (serial, serial_stats) =
        ProductQuantizer::build(&map, config.clone(), BuildParallelism::serial()).unwrap();
    assert_eq!(serial_stats.training_vectors, 32);
    assert!(matches!(
        ProductQuantizer::build_with_limits(
            &map,
            config.clone(),
            BuildParallelism::serial(),
            ProductQuantizationBuildLimits {
                max_training_bytes: Some(1),
                ..Default::default()
            },
        ),
        Err(prolly::Error::ProximityResourceLimitExceeded { .. })
    ));
    for threads in [2, 4] {
        let (parallel, stats) = ProductQuantizer::build(
            &map,
            config.clone(),
            BuildParallelism::new(threads).unwrap(),
        )
        .unwrap();
        assert_eq!(parallel.manifest_cid(), serial.manifest_cid());
        assert_eq!(parallel.quality(), serial.quality());
        assert_eq!(stats, serial_stats);
    }
    let loaded = ProductQuantizer::load(store.clone(), serial.manifest_cid().clone()).unwrap();
    assert_eq!(loaded.source_descriptor(), &map.tree().descriptor);
    assert_eq!(loaded.quality(), serial.quality());

    let query = [12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7];
    let mut exact_request = SearchRequest::exact(&query, 11);
    exact_request.filter = ProximityFilter::Prefix(b"vector-00");
    let exact = map.search(exact_request).unwrap();
    let mut request = SearchRequest::exact(&query, 11);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::ProductQuantized;
    request.filter = ProximityFilter::Prefix(b"vector-00");
    let result = loaded.search(&map, request).unwrap();
    assert!(result.stats.quantized_distance_evaluations > 0);
    assert_eq!(result.stats.reranked_candidates, 100);
    assert_eq!(
        result.stats.candidate_handles_peak,
        result.stats.reranked_candidates
    );
    assert!(result.stats.candidate_retained_bytes_peak > 0);
    assert_eq!(result.neighbors.len(), exact.neighbors.len());
    for (expected, actual) in exact.neighbors.iter().zip(&result.neighbors) {
        assert_eq!(expected.key, actual.key);
        assert_eq!(expected.distance.to_bits(), actual.distance.to_bits());
    }

    let exact_error = loaded
        .search(&map, SearchRequest::exact(&query, 3))
        .unwrap_err();
    assert!(matches!(
        exact_error,
        prolly::Error::InvalidProximitySearch { .. }
    ));

    let (mutated, _) = map
        .mutate_batch([ProximityMutation {
            key: b"vector-0001".to_vec(),
            value: None,
        }])
        .unwrap();
    let mut stale_request = SearchRequest::exact(&query, 3);
    stale_request.policy = SearchPolicy::FixedBudget;
    stale_request.options.backend = SearchBackend::ProductQuantized;
    assert!(matches!(
        loaded.search(&mutated, stale_request),
        Err(prolly::Error::InvalidProximitySearch { .. })
    ));
}

#[test]
fn product_quantization_reranks_all_canonical_metrics() {
    for metric in [
        DistanceMetric::L2Squared,
        DistanceMetric::Cosine,
        DistanceMetric::InnerProduct,
    ] {
        let store = Arc::new(MemStore::new());
        let mut map_config = ProximityConfig::new(5);
        map_config.metric = metric;
        map_config.hierarchy.log_chunk_size = 2;
        let map = ProximityMap::build(store, map_config, records(65, 5)).unwrap();
        let (pq, _) = ProductQuantizer::build(
            &map,
            ProductQuantizationConfig {
                subquantizers: 2,
                centroids_per_subquantizer: 8,
                training_iterations: 4,
                rerank_multiplier: 16,
                seed: 7,
                max_training_vectors: 65_536,
            },
            BuildParallelism::serial(),
        )
        .unwrap();
        let query = [5.1, 5.2, 5.3, 5.4, 5.5];
        let exact = map.search(SearchRequest::exact(&query, 5)).unwrap();
        let mut request = SearchRequest::exact(&query, 5);
        request.policy = SearchPolicy::FixedBudget;
        request.options.backend = SearchBackend::ProductQuantized;
        let accelerated = pq.search(&map, request).unwrap();
        for (expected, actual) in exact.neighbors.iter().zip(&accelerated.neighbors) {
            assert_eq!(expected.key, actual.key, "metric={metric:?}");
            assert_eq!(
                expected.distance.to_bits(),
                actual.distance.to_bits(),
                "metric={metric:?}"
            );
        }
    }
}
