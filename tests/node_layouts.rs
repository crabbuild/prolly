use prolly::{BoundaryInput, Error, Node, NodeLayoutSpec, TreeFormat};

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
