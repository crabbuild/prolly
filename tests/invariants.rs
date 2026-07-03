mod common;

use std::sync::Arc;

use common::{assert_tree_invariants, load_node};
use prolly::{BatchBuilder, Config, Error, MemStore, Prolly, Store};

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
    assert_eq!(put_root.len(), config.max_chunk_size);
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
