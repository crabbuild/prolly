mod common;

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

use common::{assert_tree_invariants, load_node};
use prolly::{
    AsyncProlly, BatchBuilder, Cid, Config, Error, MemStore, Mutation, Prolly, Store,
    SyncStoreAsAsync,
};

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
fn sync_mutation_rejects_valid_node_bytes_stored_under_wrong_cid() {
    let store = Arc::new(MemStore::new());
    let config = Config::default();
    let writer = Prolly::new(store.clone(), config.clone());
    let tree = writer
        .put(&writer.create(), b"key".to_vec(), b"value".to_vec())
        .unwrap();
    let expected = tree.root.clone().unwrap();
    let correct_bytes = store.get(expected.as_bytes()).unwrap().unwrap();

    let mut wrong_node = load_node(&store, &expected);
    wrong_node.vals[0] = b"different-value".to_vec();
    let wrong_bytes = wrong_node.to_bytes();
    let actual = Cid::from_bytes(&wrong_bytes);
    assert_ne!(actual, expected);
    store.put(expected.as_bytes(), &wrong_bytes).unwrap();

    let reader = Prolly::new(store, config);
    let error = reader
        .put(&tree, b"another-key".to_vec(), b"another-value".to_vec())
        .unwrap_err();
    assert!(matches!(
        error,
        Error::CidMismatch {
            expected: found_expected,
            actual: found_actual,
        } if found_expected == expected && found_actual == actual
    ));

    reader
        .store()
        .put(expected.as_bytes(), &correct_bytes)
        .unwrap();
    assert_eq!(reader.get(&tree, b"key").unwrap(), Some(b"value".to_vec()));
}

#[test]
fn sync_read_rejects_correctly_addressed_node_with_wrong_tree_format() {
    let store = Arc::new(MemStore::new());
    let plain_config = Config::builder()
        .node_layout(prolly::NodeLayoutSpec::Plain)
        .build();
    let writer = Prolly::new(store.clone(), plain_config);
    let mut tree = writer
        .put(&writer.create(), b"key".to_vec(), b"value".to_vec())
        .unwrap();
    tree.config = Config::default();

    let reader = Prolly::new(store, Config::default());
    assert!(matches!(
        reader.get(&tree, b"key"),
        Err(Error::FormatMismatch { .. })
    ));
}

#[test]
fn tree_invariants_hold_after_mixed_updates_and_deletes() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(5)
        .chunking_factor(2)
        .hash_seed(11)
        .build();
    let prolly = Prolly::new(store.clone(), config.clone());
    let mut tree = prolly.create();

    for i in 0..80 {
        tree = prolly
            .put(
                &tree,
                format!("k{i:03}").into_bytes(),
                format!("v{i}").into_bytes(),
            )
            .unwrap();
    }
    for i in (0..80).step_by(3) {
        tree = prolly.delete(&tree, format!("k{i:03}").as_bytes()).unwrap();
    }
    for i in (1..80).step_by(5) {
        tree = prolly
            .put(
                &tree,
                format!("k{i:03}").into_bytes(),
                format!("updated-{i}").into_bytes(),
            )
            .unwrap();
    }

    let stats = prolly.collect_stats(&tree).unwrap();
    assert!(stats.num_nodes > 1);
    assert!(stats.num_leaves > 1);
    assert_tree_invariants(&store, &tree, &config);
}

#[test]
fn exact_max_chunk_size_is_valid_capacity_for_put_and_batch_build() {
    let put_store = Arc::new(MemStore::new());
    let batch_store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(u32::MAX)
        .hash_seed(17)
        .build();
    let entries = (0..4)
        .map(|i| {
            (
                format!("k{i:03}").into_bytes(),
                format!("v{i:03}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();

    let prolly = Prolly::new(put_store.clone(), config.clone());
    let mut put_tree = prolly.create();
    for (key, val) in &entries {
        put_tree = prolly.put(&put_tree, key.clone(), val.clone()).unwrap();
    }

    let mut builder = BatchBuilder::new(batch_store.clone(), config.clone());
    for (key, val) in &entries {
        builder.add(key.clone(), val.clone());
    }
    let batch_tree = builder.build().unwrap();

    let put_root = load_node(&put_store, put_tree.root.as_ref().unwrap());
    let batch_root = load_node(&batch_store, batch_tree.root.as_ref().unwrap());

    assert!(put_root.leaf);
    assert_eq!(put_root.len(), config.max_chunk_size());
    assert_eq!(put_root.to_bytes(), batch_root.to_bytes());
    assert_tree_invariants(&put_store, &put_tree, &config);
    assert_tree_invariants(&batch_store, &batch_tree, &config);
}

#[test]
fn diff_reports_iterator_errors_from_missing_child_nodes() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();
    let prolly = Prolly::new(store.clone(), config);
    let mut base = prolly.create();

    for i in 0..32 {
        base = prolly
            .put(
                &base,
                format!("k{i:02}").into_bytes(),
                format!("v{i:02}").into_bytes(),
            )
            .unwrap();
    }
    let other = prolly.put(&base, b"k99".to_vec(), b"new".to_vec()).unwrap();

    let root = load_node(&store, base.root.as_ref().unwrap());
    assert!(!root.leaf);
    assert!(root.vals.len() > 1);
    store.delete(root.vals.last().unwrap()).unwrap();
    prolly.clear_cache();

    assert!(matches!(
        prolly.diff(&base, &other),
        Err(Error::NotFound(_))
    ));
}

#[test]
fn sync_and_async_writers_produce_identical_reachable_nodes_across_patterns() {
    let config = Config::builder()
        .min_chunk_size(3)
        .max_chunk_size(8)
        .chunking_factor(4)
        .hash_seed(29)
        .build();
    let sync = Prolly::new(MemStore::new(), config.clone());
    let async_prolly = AsyncProlly::new(SyncStoreAsAsync::new(Arc::new(MemStore::new())), config);

    let mut random_entries = (0..96)
        .map(|index| {
            (
                format!("k{index:03}").into_bytes(),
                format!("value-{index:03}").into_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let mut state = 0x9e37_79b9_u32;
    for index in (1..random_entries.len()).rev() {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        random_entries.swap(index, state as usize % (index + 1));
    }

    let mut sync_tree = sync.build_from_entries(random_entries.clone()).unwrap();
    let mut async_tree = block_on(async_prolly.build_from_entries(random_entries)).unwrap();
    assert_eq!(sync_tree.root, async_tree.root);
    assert_eq!(
        sync.export_snapshot(&sync_tree).unwrap(),
        block_on(async_prolly.export_snapshot(&async_tree)).unwrap()
    );

    sync_tree = sync
        .put(&sync_tree, b"k999".to_vec(), b"append".to_vec())
        .unwrap();
    async_tree =
        block_on(async_prolly.put(&async_tree, b"k999".to_vec(), b"append".to_vec())).unwrap();

    let clustered = (40..46)
        .map(|index| Mutation::Upsert {
            key: format!("k{index:03}").into_bytes(),
            val: format!("clustered-{index:03}").into_bytes(),
        })
        .chain(std::iter::once(Mutation::Upsert {
            key: b"k010".to_vec(),
            val: b"random-update".to_vec(),
        }))
        .collect::<Vec<_>>();
    sync_tree = sync.batch(&sync_tree, clustered.clone()).unwrap();
    async_tree = block_on(async_prolly.batch(&async_tree, clustered)).unwrap();

    sync_tree = sync.delete(&sync_tree, b"k003").unwrap();
    async_tree = block_on(async_prolly.delete(&async_tree, b"k003")).unwrap();
    sync_tree = sync.delete_range(&sync_tree, b"k060", b"k070").unwrap();
    async_tree = block_on(async_prolly.delete_range(&async_tree, b"k060", b"k070")).unwrap();

    let sync_left = sync
        .put(&sync_tree, b"left".to_vec(), b"branch".to_vec())
        .unwrap();
    let sync_right = sync
        .put(&sync_tree, b"right".to_vec(), b"branch".to_vec())
        .unwrap();
    let async_left =
        block_on(async_prolly.put(&async_tree, b"left".to_vec(), b"branch".to_vec())).unwrap();
    let async_right =
        block_on(async_prolly.put(&async_tree, b"right".to_vec(), b"branch".to_vec())).unwrap();
    sync_tree = sync
        .merge(&sync_tree, &sync_left, &sync_right, None)
        .unwrap();
    async_tree =
        block_on(async_prolly.merge(&async_tree, &async_left, &async_right, None)).unwrap();

    assert_eq!(sync_tree.root, async_tree.root);
    let sync_reachable = sync.export_snapshot(&sync_tree).unwrap();
    let async_reachable = block_on(async_prolly.export_snapshot(&async_tree)).unwrap();
    assert_eq!(sync_reachable, async_reachable);
    assert!(sync_reachable.verify().unwrap().valid);
}
