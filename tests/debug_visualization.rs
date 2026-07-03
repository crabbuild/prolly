use prolly::{Config, MemStore, Prolly};

fn small_config() -> Config {
    Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build()
}

fn build_tree(prolly: &Prolly<MemStore>, count: usize) -> prolly::Tree {
    let mut tree = prolly.create();
    for idx in 0..count {
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
fn debug_tree_renders_empty_tree() {
    let prolly = Prolly::new(MemStore::new(), small_config());
    let view = prolly.debug_tree(&prolly.create()).unwrap();

    assert!(view.levels.is_empty());
    assert_eq!(view.to_text(), "empty tree");
}

#[test]
fn debug_tree_groups_nodes_by_level_root_first() {
    let prolly = Prolly::new(MemStore::new(), small_config());
    let tree = build_tree(&prolly, 40);

    let view = prolly.debug_tree(&tree).unwrap();

    assert!(view.levels.len() > 1);
    assert!(view
        .levels
        .windows(2)
        .all(|pair| pair[0].level > pair[1].level));
    assert!(view
        .levels
        .first()
        .unwrap()
        .nodes
        .iter()
        .all(|node| !node.leaf));
    assert!(view
        .levels
        .last()
        .unwrap()
        .nodes
        .iter()
        .all(|node| node.leaf));
    assert!(view
        .levels
        .iter()
        .flat_map(|level| &level.nodes)
        .all(|node| node.entry_count <= node.max_entries && node.encoded_bytes > 0));

    let rendered = view.to_text();
    assert!(rendered.contains("level"));
    assert!(rendered.contains("fill="));
    assert!(rendered.contains("keys=\""));
}

#[test]
fn debug_compare_trees_highlights_shared_and_rewritten_subtrees() {
    let prolly = Prolly::new(MemStore::new(), small_config());
    let before = build_tree(&prolly, 80);
    let after = prolly
        .put(&before, b"k042".to_vec(), b"updated".to_vec())
        .unwrap();

    let comparison = prolly.debug_compare_trees(&before, &after).unwrap();

    assert!(comparison.shared_nodes > 0);
    assert!(comparison.left_only_nodes > 0);
    assert!(comparison.right_only_nodes > 0);
    assert!(comparison.shared_bytes > 0);
    assert!(comparison.left_only_bytes > 0);
    assert!(comparison.right_only_bytes > 0);
    assert!(comparison.levels.iter().any(|level| level.shared_nodes > 0));
    assert!(comparison
        .levels
        .iter()
        .flat_map(|level| &level.nodes)
        .any(|node| node.status == prolly::TreeDebugNodeStatus::Shared));

    let rendered = comparison.to_text();
    assert!(rendered.contains("shared="));
    assert!(rendered.contains("left_only="));
    assert!(rendered.contains("right_only="));
}

#[test]
fn debug_compare_empty_trees_is_empty() {
    let prolly = Prolly::new(MemStore::new(), small_config());
    let empty = prolly.create();

    let comparison = prolly.debug_compare_trees(&empty, &empty).unwrap();

    assert_eq!(comparison.shared_nodes, 0);
    assert_eq!(comparison.left_only_nodes, 0);
    assert_eq!(comparison.right_only_nodes, 0);
    assert_eq!(comparison.to_text(), "empty comparison");
}

#[cfg(feature = "async-store")]
mod async_tests {
    use super::small_config;
    use futures_util::task::noop_waker;
    use prolly::{AsyncProlly, MemStore, SyncStoreAsAsync};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match Pin::new(&mut future).poll(&mut cx) {
                Poll::Ready(output) => return output,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn async_debug_tree_and_compare_match_sync_adapter_path() {
        let store = Arc::new(MemStore::new());
        let prolly = AsyncProlly::new(SyncStoreAsAsync::new(store), small_config());
        let mut tree = prolly.create();

        for idx in 0..80 {
            tree = block_on(prolly.put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            ))
            .unwrap();
        }

        let updated = block_on(prolly.put(&tree, b"k042".to_vec(), b"updated".to_vec())).unwrap();
        let view = block_on(prolly.debug_tree(&updated)).unwrap();
        let comparison = block_on(prolly.debug_compare_trees(&tree, &updated)).unwrap();

        assert!(view.levels.len() > 1);
        assert!(comparison.shared_nodes > 0);
        assert!(comparison.right_only_nodes > 0);
        assert!(comparison.to_text().contains("right_only="));
    }
}
