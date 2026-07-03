mod common;

use std::sync::Arc;

use prolly::{Cid, Config, Error, MemStore, Prolly, Store};

fn config() -> Config {
    Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(99)
        .build()
}

fn insert_range(
    prolly: &Prolly<Arc<MemStore>>,
    mut tree: prolly::Tree,
    range: std::ops::Range<u8>,
) -> prolly::Tree {
    for idx in range {
        tree = prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }
    tree
}

#[test]
fn copy_missing_nodes_makes_tree_readable_from_destination_store() {
    let source_store = Arc::new(MemStore::new());
    let destination_store = Arc::new(MemStore::new());
    let config = config();
    let source = Prolly::new(source_store, config.clone());
    let destination = Prolly::new(destination_store.clone(), config);

    let tree = insert_range(&source, source.create(), 0..24);
    let reachability = source.mark_reachable(std::slice::from_ref(&tree)).unwrap();

    let plan = source
        .plan_missing_nodes(&tree, &destination_store)
        .unwrap();
    assert_eq!(plan.required_nodes, reachability.live_nodes);
    assert_eq!(plan.required_bytes, reachability.live_bytes);
    assert_eq!(plan.required_cids(), reachability.cids());
    assert_eq!(plan.missing_nodes, plan.required_nodes);
    assert_eq!(plan.missing_cids(), plan.required_cids());
    assert!(plan.missing_bytes > 0);

    let copied = source
        .copy_missing_nodes(&tree, &destination_store)
        .unwrap();
    assert_eq!(copied.copied_nodes, plan.missing_nodes);
    assert_eq!(copied.copied_bytes, plan.missing_bytes);

    for idx in 0..24 {
        assert_eq!(
            destination
                .get(&tree, format!("k{idx:03}").as_bytes())
                .unwrap(),
            Some(format!("v{idx:03}").into_bytes())
        );
    }
    common::assert_tree_invariants(&destination_store, &tree, &tree.config);

    let second_plan = source
        .plan_missing_nodes(&tree, &destination_store)
        .unwrap();
    assert!(second_plan.is_empty());
    assert_eq!(second_plan.missing_bytes, 0);
}

#[test]
fn plan_missing_nodes_reuses_existing_destination_subtrees() {
    let source_store = Arc::new(MemStore::new());
    let destination_store = Arc::new(MemStore::new());
    let config = config();
    let source = Prolly::new(source_store, config);

    let base = insert_range(&source, source.create(), 0..24);
    source
        .copy_missing_nodes(&base, &destination_store)
        .unwrap();

    let updated = insert_range(&source, base, 24..48);
    let plan = source
        .plan_missing_nodes(&updated, &destination_store)
        .unwrap();

    assert!(plan.missing_nodes > 0);
    assert!(plan.missing_nodes < plan.required_nodes);
    assert!(plan.missing_bytes > 0);

    let copied = source
        .copy_missing_nodes(&updated, &destination_store)
        .unwrap();
    assert_eq!(copied.copied_nodes, plan.missing_nodes);
}

#[test]
fn plan_missing_nodes_rejects_corrupt_destination_bytes() {
    let source_store = Arc::new(MemStore::new());
    let destination_store = Arc::new(MemStore::new());
    let config = config();
    let source = Prolly::new(source_store, config);

    let tree = insert_range(&source, source.create(), 0..8);
    source
        .copy_missing_nodes(&tree, &destination_store)
        .unwrap();

    let root = tree.root.clone().unwrap();
    destination_store
        .put(root.as_bytes(), b"wrong bytes")
        .unwrap();

    let err = source
        .plan_missing_nodes(&tree, &destination_store)
        .unwrap_err();
    match err {
        Error::CidMismatch { expected, actual } => {
            assert_eq!(expected, root);
            assert_eq!(actual, Cid::from_bytes(b"wrong bytes"));
        }
        other => panic!("expected CidMismatch, got {other:?}"),
    }
}
