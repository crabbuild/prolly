#![cfg(feature = "async-store")]

use prolly::{
    AcceleratorCatalog, AcceleratorSet, AsyncAcceleratorCatalog, AsyncAcceleratorSet,
    AsyncCompositeAccelerator, AsyncHnswIndex, AsyncIoConfig, AsyncProductQuantizer,
    AsyncProximityMap, AsyncSearchControl, AsyncStore, BatchOp, BuildParallelism,
    CancellationToken, CompositeAccelerator, CompositeAcceleratorConfig, CompositeBase,
    CompositeBuildLimits, CompositeBuildOutcome, HnswConfig, HnswIndex, MemStore, MemStoreError,
    ProductQuantizationConfig, ProductQuantizer, ProximityConfig, ProximityMap, ProximityMutation,
    ProximityRecord, ScalarQuantizationConfig, SearchBackend, SearchCompletion, SearchIo,
    SearchPolicy, SearchRequest, SearchRuntime, Store,
};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

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

#[derive(Clone)]
struct ReverseCompletionStore(Arc<MemStore>);

impl AsyncStore for ReverseCompletionStore {
    type Error = MemStoreError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Store::get(&self.0, key)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        Store::put(&self.0, key, value)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        Store::delete(&self.0, key)
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        Store::batch(&self.0, ops)
    }

    async fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let mut indexed: Vec<_> = keys.iter().enumerate().collect();
        indexed.reverse();
        let mut output = vec![None; keys.len()];
        for (index, key) in indexed {
            output[index] = Store::get(&self.0, key)?;
        }
        Ok(output)
    }

    fn read_parallelism(&self) -> usize {
        8
    }
}

fn records() -> Vec<ProximityRecord> {
    (0usize..256)
        .map(|index| ProximityRecord {
            key: format!("async-{index:04}").into_bytes(),
            vector: vec![index as f32 / 3.0, (index % 13) as f32, 1.0],
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

#[test]
fn async_search_io_shares_validated_cache_and_reports_actual_physical_bytes() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let sync = ProximityMap::build(store.clone(), ProximityConfig::new(3), records()).unwrap();
        let descriptor = sync.tree().descriptor.clone();
        let io = SearchIo::new(
            ReverseCompletionStore(store),
            Arc::new(SearchRuntime::default()),
        )
        .with_proximity_dimensions(3);
        let query = [21.25, 4.0, 1.0];

        let first_map = AsyncProximityMap::load(io.clone(), descriptor.clone())
            .await
            .unwrap();
        let first = first_map
            .search_with_runtime(
                SearchRequest::exact(&query, 12),
                AsyncSearchControl::default(),
            )
            .await
            .unwrap();
        assert!(first.stats.physical_bytes_read > 0);

        let second_map = AsyncProximityMap::load(io.clone(), descriptor)
            .await
            .unwrap();
        let second = second_map
            .search_with_runtime(
                SearchRequest::exact(&query, 12),
                AsyncSearchControl::default(),
            )
            .await
            .unwrap();
        assert_eq!(second.neighbors, first.neighbors);
        assert_eq!(second.stats.physical_bytes_read, 0);
    });
}

#[test]
fn async_completion_permutations_preserve_sync_logical_execution() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let mut config = ProximityConfig::new(3);
        config.hierarchy.log_chunk_size = 2;
        config.hierarchy.level_hash_seed = 29;
        let sync = ProximityMap::build(store.clone(), config, records()).unwrap();
        let descriptor = sync.tree().descriptor.clone();
        let query = [21.25, 4.0, 1.0];
        let request = SearchRequest::exact(&query, 12);
        let expected = sync.search(request.clone()).unwrap();
        let asynchronous = AsyncProximityMap::load(ReverseCompletionStore(store), descriptor)
            .await
            .unwrap();

        for io in [
            AsyncIoConfig {
                max_in_flight_reads: 1,
                prefetch_window: 1,
                max_buffered_bytes: 1,
            },
            AsyncIoConfig {
                max_in_flight_reads: 4,
                prefetch_window: 16,
                max_buffered_bytes: 1024 * 1024,
            },
        ] {
            let actual = asynchronous
                .search(
                    request.clone(),
                    AsyncSearchControl {
                        io,
                        ..AsyncSearchControl::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(actual.neighbors, expected.neighbors);
            assert_eq!(actual.completion, expected.completion);
            assert_eq!(actual.stats.nodes_read, expected.stats.nodes_read);
            assert_eq!(
                actual.stats.distance_evaluations,
                expected.stats.distance_evaluations
            );
            assert_eq!(actual.stats.committed_bytes, expected.stats.committed_bytes);
        }
    });
}

#[test]
fn async_scalar_quantized_routing_matches_sync_order_and_reranking() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let mut config = ProximityConfig::new(3);
        config.hierarchy.log_chunk_size = 2;
        config.hierarchy.level_hash_seed = 29;
        config.scalar_quantization = Some(ScalarQuantizationConfig { group_size: 2 });
        let sync = ProximityMap::build(store.clone(), config, records()).unwrap();
        let query = [21.25, 4.0, 1.0];
        let mut request = SearchRequest::exact(&query, 12);
        request.policy = SearchPolicy::FixedBudget;
        let expected = sync.search(request.clone()).unwrap();
        assert!(expected.stats.quantized_distance_evaluations > 0);

        let asynchronous = AsyncProximityMap::load(
            ReverseCompletionStore(store),
            sync.tree().descriptor.clone(),
        )
        .await
        .unwrap();
        let actual = asynchronous
            .search(request, AsyncSearchControl::default())
            .await
            .unwrap();
        assert_eq!(actual.neighbors, expected.neighbors);
        assert_eq!(actual.completion, expected.completion);
        assert_eq!(
            actual.stats.quantized_distance_evaluations,
            expected.stats.quantized_distance_evaluations
        );
        assert_eq!(
            actual.stats.distance_evaluations,
            expected.stats.distance_evaluations
        );
        assert_eq!(
            actual.stats.reranked_candidates,
            expected.stats.reranked_candidates
        );
        assert_eq!(actual.stats.committed_bytes, expected.stats.committed_bytes);
    });
}

#[test]
fn cancellation_and_deadline_are_explicit_partial_completions() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(store.clone(), ProximityConfig::new(3), records()).unwrap();
        let asynchronous =
            AsyncProximityMap::load(ReverseCompletionStore(store), map.tree().descriptor.clone())
                .await
                .unwrap();
        let query = [1.0, 2.0, 3.0];
        let cancellation = CancellationToken::default();
        cancellation.cancel();
        let cancelled = asynchronous
            .search(
                SearchRequest::exact(&query, 5),
                AsyncSearchControl {
                    cancellation: Some(cancellation),
                    ..AsyncSearchControl::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(cancelled.completion, SearchCompletion::Cancelled);
        assert!(cancelled.neighbors.is_empty());

        let expired = asynchronous
            .search(
                SearchRequest::exact(&query, 5),
                AsyncSearchControl {
                    deadline: Some(Instant::now()),
                    ..AsyncSearchControl::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(expired.completion, SearchCompletion::DeadlineExceeded);
        assert!(expired.neighbors.is_empty());
    });
}

#[test]
fn async_only_hnsw_and_pq_use_the_sync_plan_and_logical_execution() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(store.clone(), ProximityConfig::new(3), records()).unwrap();
        let (hnsw, _) = HnswIndex::build(&map, HnswConfig::default()).unwrap();
        let pq_config = ProductQuantizationConfig {
            subquantizers: 3,
            centroids_per_subquantizer: 16,
            training_iterations: 4,
            max_training_vectors: 128,
            ..ProductQuantizationConfig::default()
        };
        let (pq, _) =
            ProductQuantizer::build(&map, pq_config, BuildParallelism::new(2).unwrap()).unwrap();
        let hnsw_manifest = hnsw.manifest_cid().clone();
        let pq_manifest = pq.manifest_cid().clone();
        let sync_accelerators = AcceleratorSet::empty()
            .with_hnsw(map.tree(), hnsw)
            .unwrap()
            .with_pq(map.tree(), pq)
            .unwrap();

        let async_store = ReverseCompletionStore(store.clone());
        let async_hnsw = AsyncHnswIndex::load(&async_store, hnsw_manifest)
            .await
            .unwrap();
        let async_pq = AsyncProductQuantizer::load(&async_store, pq_manifest)
            .await
            .unwrap();
        let async_accelerators = AsyncAcceleratorSet::empty()
            .with_hnsw(map.tree(), async_hnsw)
            .unwrap()
            .with_pq(map.tree(), async_pq)
            .unwrap();
        let io = SearchIo::new(async_store, Arc::new(SearchRuntime::default()))
            .with_proximity_dimensions(3);
        let async_map = AsyncProximityMap::load(io, map.tree().descriptor.clone())
            .await
            .unwrap();
        let sync_io = SearchIo::new(store, Arc::new(SearchRuntime::default()));
        let query = [21.25, 4.0, 1.0];

        for backend in [SearchBackend::Hnsw, SearchBackend::ProductQuantized] {
            let mut request = SearchRequest::exact(&query, 12);
            request.policy = SearchPolicy::FixedBudget;
            request.options.backend = backend;
            let expected = map
                .search_with(&sync_accelerators, &sync_io, request.clone())
                .unwrap();
            let actual = async_map
                .search_with_accelerators(
                    &async_accelerators,
                    request,
                    AsyncSearchControl::default(),
                )
                .await
                .unwrap();
            assert_eq!(actual.plan, expected.plan);
            assert_eq!(actual.neighbors, expected.neighbors);
            assert_eq!(actual.completion, expected.completion);
            assert_eq!(actual.stats.nodes_read, expected.stats.nodes_read);
            assert_eq!(
                actual.stats.distance_evaluations,
                expected.stats.distance_evaluations
            );
            assert_eq!(
                actual.stats.quantized_distance_evaluations,
                expected.stats.quantized_distance_evaluations
            );
            assert_eq!(actual.stats.committed_bytes, expected.stats.committed_bytes);
        }
    });
}

#[test]
fn async_only_composite_matches_sync_plan_results_and_logical_work() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let base = ProximityMap::build(store.clone(), ProximityConfig::new(3), records()).unwrap();
        let (current, _) = base
            .mutate_batch([
                ProximityMutation {
                    key: b"async-0001".to_vec(),
                    value: None,
                },
                ProximityMutation {
                    key: b"async-0021".to_vec(),
                    value: Some((vec![900.0, 1.0, 1.0], b"updated".to_vec())),
                },
                ProximityMutation {
                    key: b"async-0256".to_vec(),
                    value: Some((vec![85.25, 4.0, 1.0], b"inserted".to_vec())),
                },
            ])
            .unwrap();
        let (base_hnsw, _) = HnswIndex::build(&base, HnswConfig::default()).unwrap();
        let composite = match CompositeAccelerator::build(
            &base,
            &current,
            CompositeBase::Hnsw(base_hnsw),
            CompositeAcceleratorConfig::default(),
            CompositeBuildLimits::default(),
        )
        .unwrap()
        {
            CompositeBuildOutcome::Composite { accelerator, .. } => accelerator,
            CompositeBuildOutcome::FullRebuildRequired { reasons, .. } => {
                panic!("unexpected rebuild: {reasons:?}")
            }
        };
        let manifest = composite.manifest_cid().clone();
        let sync_set = AcceleratorSet::empty()
            .with_composite(current.tree(), *composite)
            .unwrap();
        let query = [85.25, 4.0, 1.0];
        let mut request = SearchRequest::exact(&query, 10);
        request.policy = SearchPolicy::FixedBudget;
        request.options.backend = SearchBackend::Composite;
        let expected = current
            .search_with(
                &sync_set,
                &SearchIo::new(store.clone(), Arc::new(SearchRuntime::default())),
                request.clone(),
            )
            .unwrap();
        let catalog = AcceleratorCatalog::build(store.clone(), current.tree(), sync_set).unwrap();
        let catalog_manifest = catalog.manifest_cid().clone();

        let async_store = ReverseCompletionStore(store);
        let async_composite = AsyncCompositeAccelerator::load(&async_store, manifest)
            .await
            .unwrap();
        assert_eq!(
            async_composite.current_source_descriptor(),
            &current.tree().descriptor
        );
        let async_catalog =
            AsyncAcceleratorCatalog::load(&async_store, catalog_manifest, current.tree())
                .await
                .unwrap();
        assert_eq!(async_catalog.typed_root(), catalog.typed_root());
        let async_set = async_catalog.into_accelerators();
        let io = SearchIo::new(async_store, Arc::new(SearchRuntime::default()))
            .with_proximity_dimensions(3);
        let async_map = AsyncProximityMap::load(io, current.tree().descriptor.clone())
            .await
            .unwrap();
        let actual = async_map
            .search_with_accelerators(&async_set, request, AsyncSearchControl::default())
            .await
            .unwrap();
        assert_eq!(actual.plan, expected.plan);
        assert_eq!(actual.neighbors, expected.neighbors);
        assert_eq!(actual.completion, expected.completion);
        assert_eq!(actual.stats.nodes_read, expected.stats.nodes_read);
        assert_eq!(actual.stats.committed_bytes, expected.stats.committed_bytes);
        assert_eq!(
            actual.stats.distance_evaluations,
            expected.stats.distance_evaluations
        );
        assert_eq!(
            actual.stats.quantized_distance_evaluations,
            expected.stats.quantized_distance_evaluations
        );
    });
}

#[test]
fn async_only_pq_composite_matches_sync_execution() {
    block_on(async {
        let store = Arc::new(MemStore::new());
        let base = ProximityMap::build(store.clone(), ProximityConfig::new(3), records()).unwrap();
        let (current, _) = base
            .mutate_batch([
                ProximityMutation {
                    key: b"async-0002".to_vec(),
                    value: None,
                },
                ProximityMutation {
                    key: b"async-0042".to_vec(),
                    value: Some((vec![42.0, 99.0, 1.0], b"pq-updated".to_vec())),
                },
                ProximityMutation {
                    key: b"async-0256".to_vec(),
                    value: Some((vec![64.0, 0.0, 1.0], b"pq-inserted".to_vec())),
                },
            ])
            .unwrap();
        let (base_pq, _) = ProductQuantizer::build(
            &base,
            ProductQuantizationConfig {
                subquantizers: 1,
                centroids_per_subquantizer: 8,
                training_iterations: 3,
                rerank_multiplier: 16,
                seed: 23,
                max_training_vectors: 256,
            },
            BuildParallelism::new(3).unwrap(),
        )
        .unwrap();
        let composite = match CompositeAccelerator::build(
            &base,
            &current,
            CompositeBase::ProductQuantized(base_pq),
            CompositeAcceleratorConfig::default(),
            CompositeBuildLimits::default(),
        )
        .unwrap()
        {
            CompositeBuildOutcome::Composite { accelerator, .. } => accelerator,
            CompositeBuildOutcome::FullRebuildRequired { reasons, .. } => {
                panic!("unexpected rebuild: {reasons:?}")
            }
        };
        let manifest = composite.manifest_cid().clone();
        let sync_set = AcceleratorSet::empty()
            .with_composite(current.tree(), *composite)
            .unwrap();
        let query = [64.0, 0.0, 1.0];
        let mut request = SearchRequest::exact(&query, 8);
        request.policy = SearchPolicy::FixedBudget;
        request.options.backend = SearchBackend::Composite;
        let expected = current
            .search_with(
                &sync_set,
                &SearchIo::new(store.clone(), Arc::new(SearchRuntime::default())),
                request.clone(),
            )
            .unwrap();

        let async_store = ReverseCompletionStore(store);
        let async_composite = AsyncCompositeAccelerator::load(&async_store, manifest)
            .await
            .unwrap();
        let async_set = AsyncAcceleratorSet::empty()
            .with_composite(current.tree(), async_composite)
            .unwrap();
        let async_map = AsyncProximityMap::load(
            SearchIo::new(async_store, Arc::new(SearchRuntime::default()))
                .with_proximity_dimensions(3),
            current.tree().descriptor.clone(),
        )
        .await
        .unwrap();
        let actual = async_map
            .search_with_accelerators(&async_set, request, AsyncSearchControl::default())
            .await
            .unwrap();
        assert_eq!(actual.plan, expected.plan);
        assert_eq!(actual.neighbors, expected.neighbors);
        assert_eq!(actual.completion, expected.completion);
        assert_eq!(actual.stats.nodes_read, expected.stats.nodes_read);
        assert_eq!(actual.stats.committed_bytes, expected.stats.committed_bytes);
        assert_eq!(
            actual.stats.distance_evaluations,
            expected.stats.distance_evaluations
        );
        assert_eq!(
            actual.stats.quantized_distance_evaluations,
            expected.stats.quantized_distance_evaluations
        );
    });
}
