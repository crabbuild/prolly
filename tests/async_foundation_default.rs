use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

use prolly::{
    AsyncProlly, AsyncStore, BatchOp, Config, ExecutionConfig, MemStore, MemStoreError, Prolly,
    ProllyEngine, Store, SyncStoreAsAsync,
};

#[derive(Clone)]
struct DefaultAsyncStore(Arc<MemStore>);

impl AsyncStore for DefaultAsyncStore {
    type Error = MemStoreError;

    async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        panic!("read-only engine traversal must use retained shared bytes")
    }

    async fn get_shared(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>, Self::Error> {
        Store::get_shared(self.0.as_ref(), key)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        Store::put(self.0.as_ref(), key, value)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        Store::delete(self.0.as_ref(), key)
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        Store::batch(self.0.as_ref(), ops)
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = futures_util::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[test]
fn async_prolly_is_available_without_cargo_features_or_a_runtime() {
    let store = Arc::new(MemStore::new());
    let config = Config::default();
    let sync = Prolly::new(store.clone(), config.clone());
    let tree = sync
        .put(&sync.create(), b"key".to_vec(), b"value".to_vec())
        .unwrap();
    let _ready_adapter_remains_available = SyncStoreAsAsync::new(store.clone());
    let asynchronous = AsyncProlly::new(DefaultAsyncStore(store), config);

    assert_eq!(
        block_on(asynchronous.get(&tree, b"key")).unwrap(),
        Some(b"value".to_vec())
    );
}

#[test]
fn public_engine_is_the_direct_async_core() {
    let store = Arc::new(MemStore::new());
    let writer = Prolly::new(store.clone(), Config::default());
    let tree = writer
        .put(&writer.create(), b"key".to_vec(), b"value".to_vec())
        .unwrap();
    let engine = ProllyEngine::with_execution_config(
        DefaultAsyncStore(store),
        Config::default(),
        ExecutionConfig::default(),
    );

    assert_eq!(
        block_on(engine.get(&tree, b"key")).unwrap(),
        Some(b"value".to_vec())
    );
}
