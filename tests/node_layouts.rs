use prolly::{
    chunking, BatchBuilder, BoundaryInput, Config, Error, MemStore, Node, NodeLayoutSpec,
    SortedBatchBuilder, Store, Tree, TreeFormat,
};
use std::sync::Arc;

fn key_only_format(layout: NodeLayoutSpec) -> TreeFormat {
    let mut format = TreeFormat::default();
    format.chunking.input = BoundaryInput::Key;
    format.node_layout = layout;
    format
}

fn leaf(format: TreeFormat) -> Node {
    Node::builder()
        .keys(vec![b"alpha".to_vec(), b"alphabet".to_vec()])
        .vals(vec![b"one".to_vec(), b"two".to_vec()])
        .leaf(true)
        .level(0)
        .tree_format(format)
        .build()
}

#[test]
fn built_in_layouts_round_trip_and_have_distinct_content_ids() {
    let prefix = leaf(key_only_format(NodeLayoutSpec::PrefixCompressed));
    let plain = leaf(key_only_format(NodeLayoutSpec::Plain));

    assert_eq!(Node::from_bytes(&prefix.to_bytes()).unwrap(), prefix);
    assert_eq!(Node::from_bytes(&plain.to_bytes()).unwrap(), plain);
    assert_ne!(prefix.to_bytes(), plain.to_bytes());
    assert_ne!(prefix.cid(), plain.cid());
}

#[test]
fn decoding_with_the_wrong_tree_format_is_rejected() {
    let node = leaf(key_only_format(NodeLayoutSpec::PrefixCompressed));
    let wrong = key_only_format(NodeLayoutSpec::Plain);

    assert!(matches!(
        Node::from_bytes_with_format(&node.to_bytes(), &wrong),
        Err(Error::FormatMismatch { .. })
    ));
}

#[test]
fn internal_nodes_require_one_nonzero_count_per_child() {
    let format = key_only_format(NodeLayoutSpec::PrefixCompressed);
    let node = Node::builder()
        .keys(vec![b"a".to_vec(), b"m".to_vec()])
        .vals(vec![vec![1; 32], vec![2; 32]])
        .child_counts(vec![4, 0])
        .leaf(false)
        .level(1)
        .tree_format(format)
        .build();

    assert!(matches!(node.validate(), Err(Error::InvalidNode)));
}

#[test]
fn leaf_nodes_reject_child_counts() {
    let mut node = leaf(key_only_format(NodeLayoutSpec::Plain));
    node.child_counts.push(2);

    assert!(matches!(node.validate(), Err(Error::InvalidNode)));
}

fn assert_reachable_nodes_fit(store: &Arc<MemStore>, tree: &Tree, hard_cap: usize) {
    let mut pending = tree.root.iter().cloned().collect::<Vec<_>>();
    while let Some(cid) = pending.pop() {
        let bytes = store.get(cid.as_bytes()).unwrap().unwrap();
        assert!(
            bytes.len() <= hard_cap,
            "node {cid:?} uses {} bytes above cap {hard_cap}",
            bytes.len()
        );
        let node = Node::from_bytes(&bytes).unwrap();
        if !node.leaf {
            pending.extend(node.vals.into_iter().map(|value| {
                let bytes: [u8; 32] = value.try_into().unwrap();
                prolly::Cid(bytes)
            }));
        }
    }
}

#[test]
fn every_layout_enforces_the_exact_serialized_hard_cap() {
    for layout in [
        NodeLayoutSpec::PrefixCompressed,
        NodeLayoutSpec::Plain,
        NodeLayoutSpec::OffsetTable,
    ] {
        for hard_cap in [192_u64, 255, 512] {
            let mut policy = chunking::entry_count_key_hash();
            policy.min = 4;
            policy.target = 128;
            policy.max = 16_384;
            policy.hard_max_node_bytes = hard_cap;
            let config = Config::builder()
                .chunking(policy)
                .node_layout(layout.clone())
                .build();
            let entries = (0..600)
                .map(|index| {
                    (
                        format!("shared-prefix-{index:06}").into_bytes(),
                        vec![index as u8; 17 + index % 37],
                    )
                })
                .collect::<Vec<_>>();

            let batch_store = Arc::new(MemStore::new());
            let mut batch = BatchBuilder::new(batch_store.clone(), config.clone());
            for (key, value) in entries.iter().rev() {
                batch.add(key.clone(), value.clone());
            }
            let batch_tree = batch.build().unwrap();
            assert_reachable_nodes_fit(&batch_store, &batch_tree, hard_cap as usize);

            let sorted_store = Arc::new(MemStore::new());
            let mut sorted = SortedBatchBuilder::new(sorted_store.clone(), config);
            for (key, value) in &entries {
                sorted.add(key.clone(), value.clone()).unwrap();
            }
            let sorted_tree = sorted.build().unwrap();
            assert_reachable_nodes_fit(&sorted_store, &sorted_tree, hard_cap as usize);
            assert_eq!(batch_tree.root, sorted_tree.root);
        }
    }
}
