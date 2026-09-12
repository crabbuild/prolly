use prolly::{
    walk_content_graph, AcceleratorCatalog, AcceleratorSet, BuildParallelism, ContentGraphLimits,
    ContentObjectKind, DistanceMetric, MemStore, ProximityConfig, ProximityFilter, ProximityMap,
    ProximityRecord, SearchBackend, SearchPolicy, SearchRequest, TurboQuantizationBuildLimits,
    TurboQuantizationConfig, TurboQuantizer, TypedContentRoot,
};
use std::sync::Arc;

fn records(count: usize, dimensions: usize) -> Vec<ProximityRecord> {
    (0..count)
        .map(|record| ProximityRecord {
            key: format!("vector-{record:04}").into_bytes(),
            vector: (0..dimensions)
                .map(|dimension| {
                    let base = ((record * 31 + dimension * 17) % 257) as f32 / 19.0;
                    if record == 0 {
                        0.0
                    } else {
                        base - 6.0
                    }
                })
                .collect(),
            value: record.to_le_bytes().to_vec(),
        })
        .collect()
}

#[test]
fn turboquant_is_canonical_bounded_verified_and_exhaustively_reranked() {
    for metric in [DistanceMetric::L2Squared, DistanceMetric::InnerProduct] {
        let store = Arc::new(MemStore::new());
        let mut map_config = ProximityConfig::new(128);
        map_config.metric = metric;
        let map = ProximityMap::build(store.clone(), map_config, records(193, 128)).unwrap();
        let config = TurboQuantizationConfig {
            bit_width: 4,
            rerank_multiplier: 256,
            seed: 0x5eed,
        };
        let (serial, serial_stats) =
            TurboQuantizer::build(&map, config.clone(), BuildParallelism::serial()).unwrap();
        assert_eq!(serial_stats.encoded_vectors, 193);
        assert_eq!(serial_stats.zero_vectors, 1);
        assert_eq!(serial_stats.encoded_output_bytes, 193 * (8 + 64));
        assert_eq!(serial.verify(&map).unwrap().quality, serial.quality());

        for threads in [2, 4] {
            let (parallel, stats) = TurboQuantizer::build(
                &map,
                config.clone(),
                BuildParallelism::new(threads).unwrap(),
            )
            .unwrap();
            assert_eq!(parallel.manifest_cid(), serial.manifest_cid());
            assert_eq!(parallel.quality(), serial.quality());
            assert_eq!(stats, serial_stats);
        }

        assert!(matches!(
            TurboQuantizer::build_with_limits(
                &map,
                config.clone(),
                BuildParallelism::serial(),
                TurboQuantizationBuildLimits {
                    max_encoded_output_bytes: Some(1),
                    ..Default::default()
                },
            ),
            Err(prolly::Error::ProximityResourceLimitExceeded { .. })
        ));

        let loaded = TurboQuantizer::load(store.clone(), serial.manifest_cid().clone()).unwrap();
        let query: Vec<_> = (0..128).map(|index| index as f32 / 23.0 - 2.0).collect();
        let mut exact_request = SearchRequest::exact(&query, 17);
        exact_request.filter = ProximityFilter::Prefix(b"vector-0");
        let exact = map.search(exact_request).unwrap();

        let mut accelerated_request = SearchRequest::exact(&query, 17);
        accelerated_request.policy = SearchPolicy::FixedBudget;
        accelerated_request.options.backend = SearchBackend::TurboQuantized;
        accelerated_request.filter = ProximityFilter::Prefix(b"vector-0");
        let accelerated = loaded.search(&map, accelerated_request).unwrap();
        assert_eq!(accelerated.neighbors, exact.neighbors);
        assert_eq!(accelerated.plan.backend, SearchBackend::TurboQuantized);
        assert_eq!(accelerated.stats.reranked_candidates, 193);
        assert!(accelerated.stats.quantized_distance_evaluations > 0);
    }
}

#[test]
fn turboquant_catalog_and_typed_graph_include_complete_closure() {
    let store = Arc::new(MemStore::new());
    let map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(33, 128)).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig::default(),
        BuildParallelism::serial(),
    )
    .unwrap();
    let manifest = index.manifest_cid().clone();
    let set = AcceleratorSet::empty()
        .with_turboquant(map.tree(), index)
        .unwrap();
    let catalog = AcceleratorCatalog::build(store.clone(), map.tree(), set).unwrap();
    let reopened =
        AcceleratorCatalog::load(store.clone(), catalog.manifest_cid().clone(), map.tree())
            .unwrap();
    assert_eq!(reopened.entries().len(), 1);
    let walk = walk_content_graph(
        &store,
        &[TypedContentRoot::new(
            ContentObjectKind::TurboQuantization,
            manifest,
        )],
        &ContentGraphLimits::default(),
    )
    .unwrap();
    assert_eq!(
        walk.objects_by_kind
            .get(&ContentObjectKind::TurboQuantization),
        Some(&1)
    );
    assert!(
        walk.objects_by_kind
            .get(&ContentObjectKind::OrderedNode)
            .copied()
            .unwrap_or_default()
            > 0
    );
}

#[test]
fn turboquant_rejects_unsupported_dimensions_and_stale_sources() {
    let store = Arc::new(MemStore::new());
    let unsupported =
        ProximityMap::build(store.clone(), ProximityConfig::new(7), records(17, 7)).unwrap();
    assert!(TurboQuantizer::build(
        &unsupported,
        TurboQuantizationConfig::default(),
        BuildParallelism::serial(),
    )
    .is_err());

    let map = ProximityMap::build(store.clone(), ProximityConfig::new(8), records(17, 8)).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig::default(),
        BuildParallelism::serial(),
    )
    .unwrap();
    let changed = ProximityMap::build(store, ProximityConfig::new(8), records(18, 8)).unwrap();
    let query = vec![1.0; 8];
    let mut request = SearchRequest::exact(&query, 1);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::TurboQuantized;
    assert!(index.search(&changed, request).is_err());
}

#[test]
fn turboquant_proof_replays_the_committed_plan_and_closure() {
    let store = Arc::new(MemStore::new());
    let map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(65, 128)).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig {
            rerank_multiplier: 128,
            ..TurboQuantizationConfig::default()
        },
        BuildParallelism::serial(),
    )
    .unwrap();
    let query = vec![0.25; 128];
    let mut request = SearchRequest::exact(&query, 7);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::TurboQuantized;
    let proof = index
        .prove_search(&map, request, &ContentGraphLimits::default())
        .unwrap();
    let verified = proof.verify(&ContentGraphLimits::default()).unwrap();
    assert_eq!(verified.result.plan.backend, SearchBackend::TurboQuantized);

    let mut incomplete = proof;
    incomplete.accelerator_objects.pop();
    assert!(incomplete.verify(&ContentGraphLimits::default()).is_err());
}

#[test]
fn turboquant_composite_shadows_base_and_merges_exact_delta() {
    use prolly::{
        CompositeAccelerator, CompositeAcceleratorConfig, CompositeBase, CompositeBuildLimits,
        CompositeBuildOutcome, ProximityMutation, SearchIo, SearchRuntime,
    };

    let store = Arc::new(MemStore::new());
    let base_map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(97, 128)).unwrap();
    let (base, _) = TurboQuantizer::build(
        &base_map,
        TurboQuantizationConfig {
            rerank_multiplier: 256,
            ..TurboQuantizationConfig::default()
        },
        BuildParallelism::serial(),
    )
    .unwrap();
    let replacement = vec![0.01; 128];
    let (current, _) = base_map
        .mutate_batch([
            ProximityMutation {
                key: b"vector-0001".to_vec(),
                value: Some((replacement, b"updated".to_vec())),
            },
            ProximityMutation {
                key: b"vector-0002".to_vec(),
                value: None,
            },
        ])
        .unwrap();
    let composite = match CompositeAccelerator::build(
        &base_map,
        &current,
        CompositeBase::TurboQuantized(base),
        CompositeAcceleratorConfig {
            max_delta_records: 10,
            max_shadow_records: 10,
            max_delta_ratio_ppm: 1_000_000,
            max_shadow_ratio_ppm: 1_000_000,
            base_overfetch_multiplier: 2,
        },
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, .. } => *accelerator,
        CompositeBuildOutcome::FullRebuildRequired { .. } => panic!("unexpected rebuild"),
    };
    let set = AcceleratorSet::empty()
        .with_composite(current.tree(), composite)
        .unwrap();
    let query = vec![0.0; 128];
    let exact = current.search(SearchRequest::exact(&query, 12)).unwrap();
    let mut request = SearchRequest::exact(&query, 12);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::Composite;
    let accelerated = current
        .search_with(
            &set,
            &SearchIo::new(store, Arc::new(SearchRuntime::default())),
            request,
        )
        .unwrap();
    assert_eq!(accelerated.neighbors, exact.neighbors);
}

#[cfg(feature = "async-store")]
#[test]
fn turboquant_async_build_load_search_verify_and_cancel_match_sync() {
    use prolly::{
        AsyncProximityMap, AsyncSearchControl, AsyncTurboQuantizer, CancellationToken,
        SearchCompletion, SyncStoreAsAsync,
    };
    use std::future::Future;
    use std::task::{Context, Poll};

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    block_on(async {
        let store = Arc::new(MemStore::new());
        let async_store = SyncStoreAsAsync::new(store.clone());
        let asynchronous = AsyncProximityMap::build(
            async_store.clone(),
            ProximityConfig::new(128),
            records(129, 128),
        )
        .await
        .unwrap();
        let config = TurboQuantizationConfig {
            rerank_multiplier: 256,
            ..TurboQuantizationConfig::default()
        };
        let (index, _) =
            AsyncTurboQuantizer::build(&asynchronous, config, BuildParallelism::new(2).unwrap())
                .await
                .unwrap();
        index
            .verify(&asynchronous, &ContentGraphLimits::default())
            .await
            .unwrap();
        let loaded = AsyncTurboQuantizer::load(&async_store, index.manifest_cid().clone())
            .await
            .unwrap();
        let query: Vec<_> = (0..128).map(|index| index as f32 / 29.0 - 1.0).collect();
        let sync = ProximityMap::load(store.clone(), asynchronous.tree().descriptor.clone())
            .unwrap()
            .search(SearchRequest::exact(&query, 13))
            .unwrap();
        let mut request = SearchRequest::exact(&query, 13);
        request.policy = SearchPolicy::FixedBudget;
        request.options.backend = SearchBackend::TurboQuantized;
        let result = loaded
            .search(
                &asynchronous,
                request.clone(),
                AsyncSearchControl::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.neighbors, sync.neighbors);
        assert_eq!(result.plan.backend, SearchBackend::TurboQuantized);

        let cancellation = CancellationToken::default();
        cancellation.cancel();
        let cancelled = loaded
            .search(
                &asynchronous,
                request,
                AsyncSearchControl {
                    cancellation: Some(cancellation),
                    ..AsyncSearchControl::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(cancelled.completion, SearchCompletion::Cancelled);
        assert!(cancelled.neighbors.is_empty());
    });
}
