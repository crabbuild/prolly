use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

use prolly::{AsyncProlly, Config, MemStore, NodeLayoutSpec, Prolly, SyncStoreAsAsync};

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
fn input_tree_format_is_authoritative_for_sync_and_async_reads() {
    let store = Arc::new(MemStore::new());
    let tree_config = Config::builder().node_layout(NodeLayoutSpec::Plain).build();
    let writer = Prolly::new(store.clone(), tree_config);
    let tree = writer
        .put(&writer.create(), b"key".to_vec(), b"value".to_vec())
        .unwrap();

    let sync = Prolly::new(store.clone(), Config::default());
    let asynchronous = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());

    assert_eq!(sync.get(&tree, b"key").unwrap(), Some(b"value".to_vec()));
    assert_eq!(
        block_on(asynchronous.get(&tree, b"key")).unwrap(),
        Some(b"value".to_vec())
    );
}

#[test]
fn input_tree_format_is_authoritative_for_sync_and_async_writes() {
    let store = Arc::new(MemStore::new());
    let tree_config = Config::builder().node_layout(NodeLayoutSpec::Plain).build();
    let writer = Prolly::new(store.clone(), tree_config.clone());
    let base = writer
        .put(&writer.create(), b"a".to_vec(), b"1".to_vec())
        .unwrap();
    let expected = writer
        .batch(
            &writer.create(),
            vec![
                prolly::Mutation::Upsert {
                    key: b"a".to_vec(),
                    val: b"2".to_vec(),
                },
                prolly::Mutation::Upsert {
                    key: b"b".to_vec(),
                    val: b"3".to_vec(),
                },
            ],
        )
        .unwrap();

    let sync = Prolly::new(store.clone(), Config::default());
    let asynchronous = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());
    let sync_tree = sync
        .batch(
            &base,
            vec![
                prolly::Mutation::Upsert {
                    key: b"a".to_vec(),
                    val: b"2".to_vec(),
                },
                prolly::Mutation::Upsert {
                    key: b"b".to_vec(),
                    val: b"3".to_vec(),
                },
            ],
        )
        .unwrap();
    let async_tree = block_on(asynchronous.batch(
        &base,
        vec![
            prolly::Mutation::Upsert {
                key: b"a".to_vec(),
                val: b"2".to_vec(),
            },
            prolly::Mutation::Upsert {
                key: b"b".to_vec(),
                val: b"3".to_vec(),
            },
        ],
    ))
    .unwrap();

    assert_eq!(sync_tree.root, expected.root);
    assert_eq!(async_tree.root, expected.root);
    assert_eq!(sync_tree.config.format, tree_config.format);
    assert_eq!(async_tree.config.format, tree_config.format);
}

#[test]
fn sync_and_async_reads_preserve_order_duplicates_and_missing_values() {
    let store = Arc::new(MemStore::new());
    let writer = Prolly::new(store.clone(), Config::default());
    let tree = writer
        .batch(
            &writer.create(),
            vec![
                prolly::Mutation::Upsert {
                    key: b"a".to_vec(),
                    val: b"1".to_vec(),
                },
                prolly::Mutation::Upsert {
                    key: b"b".to_vec(),
                    val: b"2".to_vec(),
                },
            ],
        )
        .unwrap();
    let keys = [
        b"b".to_vec(),
        b"missing".to_vec(),
        b"a".to_vec(),
        b"b".to_vec(),
    ];
    let sync = Prolly::new(store.clone(), Config::default());
    let asynchronous = AsyncProlly::new(SyncStoreAsAsync::new(store), Config::default());

    assert_eq!(
        sync.get_many(&tree, &keys).unwrap(),
        block_on(asynchronous.get_many(&tree, &keys)).unwrap()
    );
}
