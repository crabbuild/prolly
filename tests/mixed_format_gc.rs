use std::sync::Arc;

use prolly::{Config, MemStore, NodeLayoutSpec, Prolly};

#[test]
fn reachability_uses_each_input_trees_persisted_format() {
    let store = Arc::new(MemStore::new());
    let prefix = Prolly::new(store.clone(), Config::default());
    let plain_config = Config::builder().node_layout(NodeLayoutSpec::Plain).build();
    let plain = Prolly::new(store.clone(), plain_config);
    let prefix_tree = prefix
        .put(&prefix.create(), b"prefix".to_vec(), b"one".to_vec())
        .unwrap();
    let plain_tree = plain
        .put(&plain.create(), b"plain".to_vec(), b"two".to_vec())
        .unwrap();

    let inspector = Prolly::new(store, Config::default());
    let reachable = inspector
        .mark_reachable(&[plain_tree, prefix_tree])
        .unwrap();

    assert_eq!(reachable.live_nodes, 2);
    assert_eq!(reachable.leaf_nodes, 2);
}
