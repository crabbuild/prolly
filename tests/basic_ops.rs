mod common;

use common::configured_prolly;
use prolly::{Config, MemStore, Prolly};

#[test]
fn put_get_delete_and_range_are_ordered() {
    let prolly = configured_prolly();
    let mut tree = prolly.create();

    tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
    tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
    tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();

    assert_eq!(prolly.get(&tree, b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(prolly.get(&tree, b"missing").unwrap(), None);

    let entries = prolly
        .range(&tree, b"a", Some(b"c"))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(
        entries,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec())
        ]
    );

    let tree = prolly.delete(&tree, b"b").unwrap();
    assert_eq!(prolly.get(&tree, b"b").unwrap(), None);
}

#[test]
fn updates_are_immutable_and_content_addressed() {
    let prolly = configured_prolly();
    let base = prolly.create();
    let first = prolly.put(&base, b"key".to_vec(), b"old".to_vec()).unwrap();
    let second = prolly
        .put(&first, b"key".to_vec(), b"new".to_vec())
        .unwrap();

    assert_eq!(prolly.get(&first, b"key").unwrap(), Some(b"old".to_vec()));
    assert_eq!(prolly.get(&second, b"key").unwrap(), Some(b"new".to_vec()));
    assert_ne!(first.root, second.root);
}

#[test]
fn same_content_builds_same_root() {
    let left = configured_prolly();
    let right = configured_prolly();

    let mut left_tree = left.create();
    let mut right_tree = right.create();
    for (key, value) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3"), (b"d", b"4")] {
        left_tree = left.put(&left_tree, key.to_vec(), value.to_vec()).unwrap();
        right_tree = right
            .put(&right_tree, key.to_vec(), value.to_vec())
            .unwrap();
    }

    assert_eq!(left_tree.root, right_tree.root);
}

#[test]
fn node_cache_can_be_observed_and_cleared() {
    let prolly = configured_prolly();
    let tree = prolly.create();
    let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();

    assert!(prolly.cache_len() > 0);
    assert_eq!(prolly.get(&tree, b"a").unwrap(), Some(b"1".to_vec()));

    prolly.clear_cache();
    assert_eq!(prolly.cache_len(), 0);
    assert_eq!(prolly.get(&tree, b"a").unwrap(), Some(b"1".to_vec()));
    assert!(prolly.cache_len() > 0);
}

#[test]
fn manager_metrics_track_cache_reads_writes_and_reset() {
    let prolly = configured_prolly();
    assert_eq!(prolly.metrics(), prolly::ProllyMetricsSnapshot::default());

    let tree = prolly.create();
    let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
    let write_metrics = prolly.metrics();

    assert!(write_metrics.nodes_written > 0);
    assert!(write_metrics.bytes_written > 0);
    assert!(write_metrics.store_batch_put_calls > 0);
    assert_eq!(
        write_metrics.store_batch_put_nodes,
        write_metrics.nodes_written
    );

    prolly.reset_metrics();
    prolly.clear_cache();

    assert_eq!(prolly.get(&tree, b"a").unwrap(), Some(b"1".to_vec()));
    let cold_metrics = prolly.metrics();
    assert!(cold_metrics.node_cache_misses > 0);
    assert!(cold_metrics.nodes_read > 0);
    assert!(cold_metrics.bytes_read > 0);

    assert_eq!(prolly.get(&tree, b"a").unwrap(), Some(b"1".to_vec()));
    let warm_metrics = prolly.metrics();
    assert!(warm_metrics.node_cache_hits > cold_metrics.node_cache_hits);
    assert_eq!(warm_metrics.nodes_read, cold_metrics.nodes_read);

    prolly.reset_metrics();
    assert_eq!(prolly.metrics(), prolly::ProllyMetricsSnapshot::default());
}

#[test]
fn bounded_node_cache_limits_entries_and_preserves_reads() {
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .node_cache_max_nodes(2)
        .build();
    let prolly = Prolly::new(MemStore::new(), config);
    let mut tree = prolly.create();

    for idx in 0..24 {
        tree = prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
        assert!(prolly.cache_len() <= 2);
    }

    assert!(prolly.metrics().node_cache_evictions > 0);
    prolly.reset_metrics();

    for idx in 0..24 {
        assert_eq!(
            prolly.get(&tree, format!("k{idx:03}").as_bytes()).unwrap(),
            Some(format!("v{idx:03}").into_bytes())
        );
        assert!(prolly.cache_len() <= 2);
    }

    let metrics = prolly.metrics();
    assert!(metrics.node_cache_misses > 0);
    assert!(metrics.node_cache_evictions > 0);
}

#[test]
fn pinned_root_survives_bounded_node_cache_eviction() {
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .node_cache_max_nodes(1)
        .build();
    let prolly = Prolly::new(MemStore::new(), config);
    let mut tree = prolly.create();

    for idx in 0..32 {
        tree = prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }
    assert!(prolly.collect_stats(&tree).unwrap().tree_height > 0);

    prolly.clear_cache();
    assert_eq!(prolly.pin_tree_root(&tree).unwrap(), 1);
    assert_eq!(prolly.cache_pinned_len(), 1);
    assert!(prolly.cache_pinned_bytes_len() > 0);
    assert_eq!(prolly.pin_tree_root(&tree).unwrap(), 0);

    for idx in 0..32 {
        assert_eq!(
            prolly.get(&tree, format!("k{idx:03}").as_bytes()).unwrap(),
            Some(format!("v{idx:03}").into_bytes())
        );
        assert_eq!(prolly.cache_pinned_len(), 1);
    }

    assert_eq!(prolly.unpin_all_cache_nodes(), 1);
    assert_eq!(prolly.cache_pinned_len(), 0);
    assert!(prolly.cache_len() <= 1);
}

#[test]
fn pinned_path_can_exceed_cache_limit_until_unpinned() {
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .node_cache_max_nodes(1)
        .build();
    let prolly = Prolly::new(MemStore::new(), config);
    let mut tree = prolly.create();

    for idx in 0..64 {
        tree = prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }
    assert!(prolly.collect_stats(&tree).unwrap().tree_height > 0);

    prolly.clear_cache();
    let pinned = prolly.pin_tree_path(&tree, b"k031").unwrap();
    assert!(pinned > 1, "multi-level tree should pin root and leaf path");
    assert_eq!(prolly.cache_pinned_len(), pinned);
    assert_eq!(prolly.cache_len(), pinned);
    assert!(prolly.cache_len() > 1);

    assert_eq!(prolly.unpin_all_cache_nodes(), pinned);
    assert_eq!(prolly.cache_pinned_len(), 0);
    assert!(prolly.cache_len() <= 1);
}

#[test]
fn byte_bounded_node_cache_limits_serialized_weight_and_preserves_reads() {
    const CACHE_BYTES: usize = 512;

    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .node_cache_max_bytes(CACHE_BYTES)
        .build();
    let prolly = Prolly::new(MemStore::new(), config);
    let mut tree = prolly.create();

    for idx in 0..48 {
        tree = prolly
            .put(
                &tree,
                format!("k{idx:03}").into_bytes(),
                format!("value-{idx:03}-payload").into_bytes(),
            )
            .unwrap();
        assert!(prolly.cache_bytes_len() <= CACHE_BYTES);
    }

    assert!(prolly.metrics().node_cache_evictions > 0);
    prolly.reset_metrics();

    for idx in 0..48 {
        assert_eq!(
            prolly.get(&tree, format!("k{idx:03}").as_bytes()).unwrap(),
            Some(format!("value-{idx:03}-payload").into_bytes())
        );
        assert!(prolly.cache_bytes_len() <= CACHE_BYTES);
    }

    let metrics = prolly.metrics();
    assert!(metrics.node_cache_misses > 0);
    assert!(metrics.node_cache_evictions > 0);
}

#[test]
fn zero_node_cache_max_disables_node_cache() {
    let config = Config::builder().node_cache_max_nodes(0).build();
    let prolly = Prolly::new(MemStore::new(), config);
    let tree = prolly.create();
    let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();

    assert_eq!(prolly.cache_len(), 0);
    prolly.reset_metrics();

    assert_eq!(prolly.get(&tree, b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(prolly.get(&tree, b"a").unwrap(), Some(b"1".to_vec()));

    let metrics = prolly.metrics();
    assert_eq!(prolly.cache_len(), 0);
    assert_eq!(metrics.node_cache_hits, 0);
    assert!(metrics.node_cache_misses >= 2);
    assert!(metrics.nodes_read >= 2);
}

#[test]
fn zero_node_cache_max_disables_pinning() {
    let config = Config::builder().node_cache_max_nodes(0).build();
    let prolly = Prolly::new(MemStore::new(), config);
    let tree = prolly.create();
    let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();

    assert_eq!(prolly.pin_tree_root(&tree).unwrap(), 0);
    assert_eq!(prolly.pin_tree_path(&tree, b"a").unwrap(), 0);
    assert_eq!(prolly.cache_len(), 0);
    assert_eq!(prolly.cache_pinned_len(), 0);
    assert_eq!(prolly.unpin_all_cache_nodes(), 0);
}

#[test]
fn zero_node_cache_byte_max_disables_node_cache() {
    let config = Config::builder().node_cache_max_bytes(0).build();
    let prolly = Prolly::new(MemStore::new(), config);
    let tree = prolly.create();
    let tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();

    assert_eq!(prolly.cache_len(), 0);
    assert_eq!(prolly.cache_bytes_len(), 0);
    prolly.reset_metrics();

    assert_eq!(prolly.get(&tree, b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(prolly.get(&tree, b"a").unwrap(), Some(b"1".to_vec()));

    let metrics = prolly.metrics();
    assert_eq!(prolly.cache_len(), 0);
    assert_eq!(prolly.cache_bytes_len(), 0);
    assert_eq!(metrics.node_cache_hits, 0);
    assert!(metrics.node_cache_misses >= 2);
    assert!(metrics.nodes_read >= 2);
}

#[test]
fn stats_diff_reports_growth_shrink_and_unchanged_trees() {
    let prolly = configured_prolly();
    let mut before = prolly.create();
    for idx in 0..8 {
        before = prolly
            .put(
                &before,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let mut after = before.clone();
    for idx in 8..16 {
        after = prolly
            .put(
                &after,
                format!("k{idx:03}").into_bytes(),
                format!("v{idx:03}").into_bytes(),
            )
            .unwrap();
    }

    let growth = prolly.stats_diff(&before, &after).unwrap();
    assert_eq!(growth.before, prolly.collect_stats(&before).unwrap());
    assert_eq!(growth.after, prolly.collect_stats(&after).unwrap());
    assert_eq!(growth.absolute.total_key_value_pairs_diff, 8);
    assert!(growth.absolute.num_nodes_diff >= 0);
    assert!(growth.absolute.total_tree_size_bytes_diff > 0);
    assert!(growth.percentage.total_key_value_pairs_pct > 0.0);

    let shrink = prolly.stats_diff(&after, &before).unwrap();
    assert_eq!(shrink.absolute.total_key_value_pairs_diff, -8);
    assert!(shrink.absolute.total_tree_size_bytes_diff < 0);
    assert!(shrink.percentage.total_key_value_pairs_pct < 0.0);

    let unchanged = prolly.stats_diff(&after, &after).unwrap();
    assert_eq!(unchanged.absolute.total_key_value_pairs_diff, 0);
    assert_eq!(unchanged.absolute.total_tree_size_bytes_diff, 0);
    assert_eq!(unchanged.percentage.total_key_value_pairs_pct, 0.0);
}
