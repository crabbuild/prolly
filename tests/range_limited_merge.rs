mod common;

use common::configured_prolly;
use prolly::{resolver, Error, Resolution};

#[test]
fn merge_prefix_applies_only_right_changes_inside_prefix() {
    let prolly = configured_prolly();

    let mut base = prolly.create();
    base = prolly
        .put(&base, b"tenant/a/name".to_vec(), b"base-a".to_vec())
        .unwrap();
    base = prolly
        .put(&base, b"tenant/a/status".to_vec(), b"active".to_vec())
        .unwrap();
    base = prolly
        .put(&base, b"tenant/b/name".to_vec(), b"base-b".to_vec())
        .unwrap();
    base = prolly
        .put(&base, b"global".to_vec(), b"base-global".to_vec())
        .unwrap();

    let mut left = base.clone();
    left = prolly
        .put(&left, b"tenant/b/name".to_vec(), b"left-b".to_vec())
        .unwrap();
    left = prolly
        .put(&left, b"global".to_vec(), b"left-global".to_vec())
        .unwrap();

    let mut right = base.clone();
    right = prolly
        .put(&right, b"tenant/a/name".to_vec(), b"right-a".to_vec())
        .unwrap();
    right = prolly.delete(&right, b"tenant/a/status").unwrap();
    right = prolly
        .put(&right, b"tenant/b/name".to_vec(), b"right-b".to_vec())
        .unwrap();
    right = prolly
        .put(&right, b"global".to_vec(), b"right-global".to_vec())
        .unwrap();

    let merged = prolly
        .merge_prefix(&base, &left, &right, b"tenant/a/", None)
        .unwrap();

    assert_eq!(
        prolly.get(&merged, b"tenant/a/name").unwrap(),
        Some(b"right-a".to_vec())
    );
    assert_eq!(prolly.get(&merged, b"tenant/a/status").unwrap(), None);
    assert_eq!(
        prolly.get(&merged, b"tenant/b/name").unwrap(),
        Some(b"left-b".to_vec())
    );
    assert_eq!(
        prolly.get(&merged, b"global").unwrap(),
        Some(b"left-global".to_vec())
    );
}

#[test]
fn merge_range_detects_and_resolves_conflicts_only_inside_range() {
    let prolly = configured_prolly();

    let mut base = prolly.create();
    base = prolly
        .put(&base, b"doc/1/title".to_vec(), b"base-title".to_vec())
        .unwrap();
    base = prolly
        .put(&base, b"doc/2/title".to_vec(), b"base-outside".to_vec())
        .unwrap();

    let mut left = base.clone();
    left = prolly
        .put(&left, b"doc/1/title".to_vec(), b"left-title".to_vec())
        .unwrap();
    left = prolly
        .put(&left, b"doc/2/title".to_vec(), b"left-outside".to_vec())
        .unwrap();

    let mut right = base.clone();
    right = prolly
        .put(&right, b"doc/1/title".to_vec(), b"right-title".to_vec())
        .unwrap();
    right = prolly
        .put(&right, b"doc/2/title".to_vec(), b"right-outside".to_vec())
        .unwrap();

    assert!(matches!(
        prolly.merge_prefix(&base, &left, &right, b"doc/1/", None),
        Err(Error::Conflict(_))
    ));

    let resolved = prolly
        .merge_prefix(
            &base,
            &left,
            &right,
            b"doc/1/",
            Some(Box::new(resolver::prefer_right)),
        )
        .unwrap();

    assert_eq!(
        prolly.get(&resolved, b"doc/1/title").unwrap(),
        Some(b"right-title".to_vec())
    );
    assert_eq!(
        prolly.get(&resolved, b"doc/2/title").unwrap(),
        Some(b"left-outside".to_vec())
    );
}

#[test]
fn merge_range_allows_custom_value_resolution() {
    let prolly = configured_prolly();

    let base = prolly
        .put(&prolly.create(), b"doc/1/body".to_vec(), b"base".to_vec())
        .unwrap();
    let left = prolly
        .put(&base, b"doc/1/body".to_vec(), b"left".to_vec())
        .unwrap();
    let right = prolly
        .put(&base, b"doc/1/body".to_vec(), b"right".to_vec())
        .unwrap();

    let merged = prolly
        .merge_prefix(
            &base,
            &left,
            &right,
            b"doc/1/",
            Some(Box::new(|conflict| {
                let mut value = conflict.left.clone().unwrap();
                value.extend_from_slice(b"+");
                value.extend_from_slice(conflict.right.as_ref().unwrap());
                Resolution::value(value)
            })),
        )
        .unwrap();

    assert_eq!(
        prolly.get(&merged, b"doc/1/body").unwrap(),
        Some(b"left+right".to_vec())
    );
}

#[test]
fn merge_range_empty_or_reversed_range_is_noop() {
    let prolly = configured_prolly();

    let base = prolly
        .put(&prolly.create(), b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let left = prolly.put(&base, b"k".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();

    let empty = prolly
        .merge_range(&base, &left, &right, b"k", Some(b"k"), None)
        .unwrap();
    let reversed = prolly
        .merge_range(&base, &left, &right, b"z", Some(b"a"), None)
        .unwrap();

    assert_eq!(empty.root, left.root);
    assert_eq!(reversed.root, left.root);
}

#[cfg(feature = "async-store")]
mod async_tests {
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

    #[test]
    fn async_merge_prefix_matches_sync_merge_prefix() {
        let store = Arc::new(MemStore::new());
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(4)
            .chunking_factor(2)
            .build();
        let sync_prolly = Prolly::new(store.clone(), config.clone());
        let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), config);

        let mut base = sync_prolly.create();
        base = sync_prolly
            .put(&base, b"team/a/name".to_vec(), b"base-a".to_vec())
            .unwrap();
        base = sync_prolly
            .put(&base, b"team/b/name".to_vec(), b"base-b".to_vec())
            .unwrap();

        let left = sync_prolly
            .put(&base, b"team/b/name".to_vec(), b"left-b".to_vec())
            .unwrap();
        let right = sync_prolly
            .put(&base, b"team/a/name".to_vec(), b"right-a".to_vec())
            .unwrap();

        let merged =
            block_on(async_prolly.merge_prefix(&base, &left, &right, b"team/a/", None)).unwrap();

        assert_eq!(
            block_on(async_prolly.get(&merged, b"team/a/name")).unwrap(),
            Some(b"right-a".to_vec())
        );
        assert_eq!(
            block_on(async_prolly.get(&merged, b"team/b/name")).unwrap(),
            Some(b"left-b".to_vec())
        );
    }
}
