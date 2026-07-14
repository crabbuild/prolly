#![cfg(feature = "async-store")]

use prolly::{
    AsyncIoConfig, AsyncProximityMap, AsyncSearchControl, AsyncStore, BatchOp, CancellationToken,
    MemStore, MemStoreError, ProximityConfig, ProximityMap, ProximityRecord,
    ScalarQuantizationConfig, SearchCompletion, SearchPolicy, SearchRequest, Store,
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
