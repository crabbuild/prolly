mod common;

use common::configured_prolly;
use prolly::Conflict;

fn collect_conflicts(
    prolly: &prolly::Prolly<prolly::MemStore>,
    base: &prolly::Tree,
    left: &prolly::Tree,
    right: &prolly::Tree,
) -> Vec<Conflict> {
    prolly
        .stream_conflicts(base, left, right)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn stream_conflicts_yields_value_value_and_delete_update_conflicts() {
    let prolly = configured_prolly();

    let mut base = prolly.create();
    base = prolly
        .put(&base, b"k1".to_vec(), b"base-1".to_vec())
        .unwrap();
    base = prolly
        .put(&base, b"k2".to_vec(), b"base-2".to_vec())
        .unwrap();
    base = prolly
        .put(&base, b"k3".to_vec(), b"base-3".to_vec())
        .unwrap();

    let mut left = base.clone();
    left = prolly
        .put(&left, b"k1".to_vec(), b"left-1".to_vec())
        .unwrap();
    left = prolly.delete(&left, b"k2").unwrap();

    let mut right = base.clone();
    right = prolly
        .put(&right, b"k1".to_vec(), b"right-1".to_vec())
        .unwrap();
    right = prolly
        .put(&right, b"k2".to_vec(), b"right-2".to_vec())
        .unwrap();
    right = prolly
        .put(&right, b"k3".to_vec(), b"right-3".to_vec())
        .unwrap();

    let conflicts = collect_conflicts(&prolly, &base, &left, &right);

    assert_eq!(conflicts.len(), 2);
    assert_eq!(conflicts[0].key, b"k1".to_vec());
    assert_eq!(conflicts[0].base, Some(b"base-1".to_vec()));
    assert_eq!(conflicts[0].left, Some(b"left-1".to_vec()));
    assert_eq!(conflicts[0].right, Some(b"right-1".to_vec()));

    assert_eq!(conflicts[1].key, b"k2".to_vec());
    assert_eq!(conflicts[1].base, Some(b"base-2".to_vec()));
    assert_eq!(conflicts[1].left, None);
    assert_eq!(conflicts[1].right, Some(b"right-2".to_vec()));
}

#[test]
fn stream_conflicts_preserves_absent_base_for_add_add_conflicts() {
    let prolly = configured_prolly();

    let base = prolly.create();
    let left = prolly.put(&base, b"k".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    let conflicts = collect_conflicts(&prolly, &base, &left, &right);

    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].key, b"k".to_vec());
    assert_eq!(conflicts[0].base, None);
    assert_eq!(conflicts[0].left, Some(b"left".to_vec()));
    assert_eq!(conflicts[0].right, Some(b"right".to_vec()));
}

#[test]
fn stream_conflicts_treats_empty_value_as_present() {
    let prolly = configured_prolly();

    let base = prolly
        .put(&prolly.create(), b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let left = prolly.put(&base, b"k".to_vec(), Vec::new()).unwrap();
    let right = prolly.delete(&base, b"k").unwrap();

    let conflicts = collect_conflicts(&prolly, &base, &left, &right);

    assert_eq!(conflicts.len(), 1);
    assert_eq!(conflicts[0].base, Some(b"base".to_vec()));
    assert_eq!(conflicts[0].left, Some(Vec::new()));
    assert_eq!(conflicts[0].right, None);
}

#[test]
fn stream_conflicts_skips_non_conflicting_right_changes() {
    let prolly = configured_prolly();

    let mut base = prolly.create();
    base = prolly
        .put(&base, b"changed-by-right".to_vec(), b"base".to_vec())
        .unwrap();
    base = prolly
        .put(&base, b"same-change".to_vec(), b"base".to_vec())
        .unwrap();
    base = prolly
        .put(&base, b"left-only".to_vec(), b"base".to_vec())
        .unwrap();

    let mut left = base.clone();
    left = prolly
        .put(&left, b"same-change".to_vec(), b"shared".to_vec())
        .unwrap();
    left = prolly
        .put(&left, b"left-only".to_vec(), b"left".to_vec())
        .unwrap();

    let mut right = base.clone();
    right = prolly
        .put(&right, b"changed-by-right".to_vec(), b"right".to_vec())
        .unwrap();
    right = prolly
        .put(&right, b"same-change".to_vec(), b"shared".to_vec())
        .unwrap();

    let conflicts = collect_conflicts(&prolly, &base, &left, &right);

    assert!(conflicts.is_empty());
}

#[cfg(feature = "async-store")]
mod async_tests {
    use futures_util::StreamExt;
    use prolly::{AsyncProlly, Config, MemStore, Prolly, SyncStoreAsAsync};
    use std::{
        future::Future,
        sync::Arc,
        task::{Context, Poll},
    };

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut future = Box::pin(future);

        loop {
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    fn async_pair() -> (
        Prolly<Arc<MemStore>>,
        AsyncProlly<SyncStoreAsAsync<Arc<MemStore>>>,
    ) {
        let store = Arc::new(MemStore::new());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(2)
            .build();
        (
            Prolly::new(store.clone(), config.clone()),
            AsyncProlly::new(SyncStoreAsAsync::new(store), config),
        )
    }

    #[test]
    fn async_stream_conflicts_next_yields_delete_aware_conflict() {
        let (sync_prolly, async_prolly) = async_pair();

        let base = sync_prolly
            .put(&sync_prolly.create(), b"k".to_vec(), b"base".to_vec())
            .unwrap();
        let left = sync_prolly.delete(&base, b"k").unwrap();
        let right = sync_prolly
            .put(&base, b"k".to_vec(), b"right".to_vec())
            .unwrap();

        let mut conflicts = async_prolly.stream_conflicts(&base, &left, &right);
        let conflict = block_on(conflicts.next()).unwrap().unwrap();

        assert_eq!(conflict.key, b"k".to_vec());
        assert_eq!(conflict.base, Some(b"base".to_vec()));
        assert_eq!(conflict.left, None);
        assert_eq!(conflict.right, Some(b"right".to_vec()));
        assert!(block_on(conflicts.next()).is_none());
    }

    #[test]
    fn async_stream_conflicts_collect_skips_clean_changes() {
        let (sync_prolly, async_prolly) = async_pair();

        let mut base = sync_prolly.create();
        base = sync_prolly
            .put(&base, b"conflict".to_vec(), b"base".to_vec())
            .unwrap();
        base = sync_prolly
            .put(&base, b"same-change".to_vec(), b"base".to_vec())
            .unwrap();
        base = sync_prolly
            .put(&base, b"right-only".to_vec(), b"base".to_vec())
            .unwrap();

        let mut left = base.clone();
        left = sync_prolly
            .put(&left, b"conflict".to_vec(), b"left".to_vec())
            .unwrap();
        left = sync_prolly
            .put(&left, b"same-change".to_vec(), b"shared".to_vec())
            .unwrap();

        let mut right = base.clone();
        right = sync_prolly
            .put(&right, b"conflict".to_vec(), b"right".to_vec())
            .unwrap();
        right = sync_prolly
            .put(&right, b"same-change".to_vec(), b"shared".to_vec())
            .unwrap();
        right = sync_prolly
            .put(&right, b"right-only".to_vec(), b"right".to_vec())
            .unwrap();

        let conflicts = block_on(
            async_prolly
                .stream_conflicts(&base, &left, &right)
                .collect(),
        )
        .unwrap();

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].key, b"conflict".to_vec());
        assert_eq!(conflicts[0].left, Some(b"left".to_vec()));
        assert_eq!(conflicts[0].right, Some(b"right".to_vec()));
    }

    #[test]
    fn async_stream_conflicts_into_stream_yields_absent_base_conflict() {
        let (sync_prolly, async_prolly) = async_pair();

        let base = sync_prolly.create();
        let left = sync_prolly
            .put(&base, b"k".to_vec(), b"left".to_vec())
            .unwrap();
        let right = sync_prolly
            .put(&base, b"k".to_vec(), b"right".to_vec())
            .unwrap();

        let stream = async_prolly
            .stream_conflicts(&base, &left, &right)
            .into_stream();
        let conflicts = block_on(stream.collect::<Vec<_>>())
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].key, b"k".to_vec());
        assert_eq!(conflicts[0].base, None);
        assert_eq!(conflicts[0].left, Some(b"left".to_vec()));
        assert_eq!(conflicts[0].right, Some(b"right".to_vec()));
    }
}
