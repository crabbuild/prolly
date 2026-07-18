use prolly::{
    chunking, BatchBuilder, BatchOp, Config, MemStore, MemStoreError, Mutation, Node,
    NodeLayoutSpec, Prolly, Store, Tree,
};
use std::fmt;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};

struct BatchReadMemStore {
    inner: Arc<MemStore>,
}

impl BatchReadMemStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemStore::new()),
        }
    }
}

impl Store for BatchReadMemStore {
    type Error = MemStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(ops)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        keys.iter().map(|key| self.inner.get(key)).collect()
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }
}

#[derive(Debug)]
enum FailingStoreError {
    Inner(MemStoreError),
    InjectedBatchPut,
}

impl fmt::Display for FailingStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inner(error) => error.fmt(formatter),
            Self::InjectedBatchPut => formatter.write_str("injected batch_put failure"),
        }
    }
}

impl std::error::Error for FailingStoreError {}

struct FailingBatchPutMemStore {
    inner: Arc<MemStore>,
    fail_next_batch_put: AtomicBool,
    successful_batch_puts: AtomicUsize,
}

impl FailingBatchPutMemStore {
    fn new() -> Self {
        Self {
            inner: Arc::new(MemStore::new()),
            fail_next_batch_put: AtomicBool::new(false),
            successful_batch_puts: AtomicUsize::new(0),
        }
    }

    fn fail_next_batch_put(&self) {
        self.fail_next_batch_put.store(true, Ordering::Relaxed);
    }
}

impl Store for FailingBatchPutMemStore {
    type Error = FailingStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key).map_err(FailingStoreError::Inner)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put(key, value).map_err(FailingStoreError::Inner)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key).map_err(FailingStoreError::Inner)
    }

    fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(ops).map_err(FailingStoreError::Inner)
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        if self.fail_next_batch_put.swap(false, Ordering::Relaxed) {
            return Err(FailingStoreError::InjectedBatchPut);
        }
        self.inner
            .batch_put(entries)
            .map_err(FailingStoreError::Inner)?;
        self.successful_batch_puts.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }
}

fn fixture<const N: usize>(keys: [&[u8]; N]) -> (Prolly<MemStore>, Tree) {
    let manager = Prolly::new(MemStore::new(), Config::default());
    let mut tree = manager.create();
    for key in keys {
        tree = manager.put(&tree, key.to_vec(), key.to_vec()).unwrap();
    }
    (manager, tree)
}

fn keys<S: Store>(manager: &Prolly<S>, tree: &Tree) -> Vec<Vec<u8>> {
    manager
        .range(tree, b"", None)
        .unwrap()
        .map(|entry| entry.unwrap().0)
        .collect()
}

fn bytes<const N: usize>(keys: [&str; N]) -> Vec<Vec<u8>> {
    keys.into_iter()
        .map(|key| key.as_bytes().to_vec())
        .collect()
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:020}").into_bytes()
}

fn value(index: usize) -> Vec<u8> {
    format!("value-{index:020}").into_bytes()
}

fn rebuild_without_range(store: Arc<BatchReadMemStore>, start: &[u8], end: &[u8]) -> Tree {
    let mut builder = BatchBuilder::new(store, Config::default());
    for index in 0..200_000 {
        let key = key(index);
        if key.as_slice() < start || key.as_slice() >= end {
            builder.add(key, value(index));
        }
    }
    builder.build().unwrap()
}

fn fixed_small_chunk_config(layout: NodeLayoutSpec) -> Config {
    let mut policy = chunking::entry_count_key_hash();
    policy.min = 1;
    policy.target = 2;
    policy.max = 4;
    Config::builder()
        .chunking(policy)
        .node_layout(layout)
        .build()
}

fn next_random(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1);
    *state
}

fn seeded_records(seed: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut state = seed;
    (0..2_000)
        .map(|index| {
            (
                format!("seeded-key-{index:06}").into_bytes(),
                format!("seeded-value-{:016x}", next_random(&mut state)).into_bytes(),
            )
        })
        .collect()
}

#[test]
fn delete_range_is_half_open_and_immutable() {
    let (manager, base) = fixture([b"a", b"b", b"c", b"d", b"e", b"f"]);
    let deleted = manager.delete_range(&base, b"b", b"e").unwrap();
    assert_eq!(keys(&manager, &base), bytes(["a", "b", "c", "d", "e", "f"]));
    assert_eq!(keys(&manager, &deleted), bytes(["a", "e", "f"]));
}

#[test]
fn empty_reversed_and_disjoint_ranges_are_write_free_noops() {
    let (manager, base) = fixture([b"a", b"b", b"c"]);
    for (start, end) in [
        (b"b".as_slice(), b"b".as_slice()),
        (b"z".as_slice(), b"a".as_slice()),
        (b"x".as_slice(), b"z".as_slice()),
    ] {
        manager.reset_metrics();
        let deleted = manager.delete_range(&base, start, end).unwrap();
        assert_eq!(deleted.root, base.root);
        assert_eq!(manager.metrics().nodes_written, 0);
    }
}

#[test]
fn range_bounds_may_extend_beyond_the_keyspace() {
    let (manager, base) = fixture([b"a", b"b", b"c", b"d", b"e", b"f"]);

    let deleted_all = manager.delete_range(&base, b"", b"\xff").unwrap();
    assert!(deleted_all.root.is_none());

    let deleted_from_before = manager.delete_range(&base, b"", b"c").unwrap();
    assert_eq!(
        keys(&manager, &deleted_from_before),
        bytes(["c", "d", "e", "f"])
    );

    let deleted_through_after = manager.delete_range(&base, b"d", b"\xff").unwrap();
    assert_eq!(
        keys(&manager, &deleted_through_after),
        bytes(["a", "b", "c"])
    );
}

#[test]
fn delete_range_rejects_a_mismatched_persisted_format() {
    let (manager, base) = fixture([b"a", b"b", b"c"]);
    let mut mismatched = base;
    mismatched.config = Config::builder().hash_seed(42).build();

    let result = manager.delete_range(&mismatched, b"a", b"z");
    assert!(matches!(result, Err(prolly::Error::FormatMismatch { .. })));
}

#[test]
fn clustered_range_delete_skips_covered_leaf_payloads() {
    let store = Arc::new(BatchReadMemStore::new());
    let manager = Prolly::new(store.clone(), Config::default());
    let mut builder = BatchBuilder::new(store.clone(), Config::default());
    for index in 0..200_000 {
        builder.add(key(index), value(index));
    }
    let base = builder.build().unwrap();
    let start = key(99_000);
    let end = key(101_000);

    manager.clear_cache();
    manager.reset_metrics();
    let (deleted, stats) = manager
        .delete_range_with_stats(&base, &start, &end)
        .unwrap();

    assert_eq!(
        deleted.root,
        rebuild_without_range(store, &start, &end).root
    );
    assert_eq!(stats.nodes_read, manager.metrics().nodes_read, "{stats:?}");
    assert!(stats.nodes_read <= 12, "{stats:?}");
    assert!(manager.metrics().store_batch_get_calls >= 1);
}

#[test]
fn randomized_range_deletes_match_clean_builds_across_builtin_layouts_and_policies() {
    let layouts = [
        NodeLayoutSpec::PrefixCompressed,
        NodeLayoutSpec::Plain,
        NodeLayoutSpec::OffsetTable,
    ];

    for seed in 0..50u64 {
        let policy = match seed % 4 {
            0 => chunking::entry_count_key_value_hash(),
            1 => chunking::entry_count_key_hash(),
            2 => chunking::logical_bytes_key_weibull(),
            _ => chunking::logical_bytes_rolling_hash(),
        };
        let mut state = seed.wrapping_add(1);
        let start_index = (next_random(&mut state) % 2_000) as usize;
        let end_index =
            start_index + 1 + (next_random(&mut state) % (2_000 - start_index) as u64) as usize;
        let start = format!("seeded-key-{start_index:06}").into_bytes();
        let end = format!("seeded-key-{end_index:06}").into_bytes();
        let records = seeded_records(seed);

        for layout in &layouts {
            let config = Config::builder()
                .chunking(policy.clone())
                .node_layout(layout.clone())
                .build();
            let store = Arc::new(MemStore::new());
            let manager = Prolly::new(store.clone(), config.clone());
            let mut base_builder = BatchBuilder::new(store, config.clone());
            for (key, value) in &records {
                base_builder.add(key.clone(), value.clone());
            }
            let base = base_builder.build().unwrap();
            let deleted = manager.delete_range(&base, &start, &end).unwrap();

            let expected_store = Arc::new(MemStore::new());
            let mut expected_builder = BatchBuilder::new(expected_store, config);
            for (key, value) in &records {
                if key.as_slice() < start.as_slice() || key.as_slice() >= end.as_slice() {
                    expected_builder.add(key.clone(), value.clone());
                }
            }
            assert_eq!(
                deleted.root,
                expected_builder.build().unwrap().root,
                "seed={seed}, layout={layout:?}, range={start_index}..{end_index}"
            );
        }
    }
}

#[test]
fn height_three_range_delete_falls_back_to_the_canonical_rebuild() {
    let config = fixed_small_chunk_config(NodeLayoutSpec::Plain);
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), config.clone());
    let mut base_builder = BatchBuilder::new(store.clone(), config.clone());
    for index in 0..2_000 {
        base_builder.add(key(index), value(index));
    }
    let base = base_builder.build().unwrap();
    let root = Node::from_bytes(
        &store
            .get(base.root.as_ref().unwrap().as_bytes())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(
        root.level >= 3,
        "test must force the height-2 splice fallback"
    );

    let start = key(600);
    let end = key(1_400);
    manager.clear_cache();
    manager.reset_metrics();
    let (deleted, stats) = manager
        .delete_range_with_stats(&base, &start, &end)
        .unwrap();
    let metrics = manager.metrics();
    assert_eq!(stats.nodes_read, metrics.nodes_read, "{stats:?}");
    assert_eq!(stats.bytes_read, metrics.bytes_read, "{stats:?}");

    let expected_store = Arc::new(MemStore::new());
    let mut expected_builder = BatchBuilder::new(expected_store, config);
    for index in 0..2_000 {
        if !(600..1_400).contains(&index) {
            expected_builder.add(key(index), value(index));
        }
    }
    assert_eq!(deleted.root, expected_builder.build().unwrap().root);
}

#[test]
fn range_delete_does_not_publish_when_batch_put_fails() {
    let config = fixed_small_chunk_config(NodeLayoutSpec::Plain);
    let store = Arc::new(FailingBatchPutMemStore::new());
    let manager = Prolly::new(store.clone(), config.clone());
    let mut builder = BatchBuilder::new(store.clone(), config);
    for index in 0..40 {
        builder.add(key(index), value(index));
    }
    let base = builder.build().unwrap();
    let source_root = base.root.clone();
    let successful_writes_before = store.successful_batch_puts.load(Ordering::Relaxed);
    let root = Node::from_bytes(
        &store
            .get(source_root.as_ref().unwrap().as_bytes())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(root.level, 2, "test must exercise the localized splice");

    store.fail_next_batch_put();
    let result = manager.delete_range(&base, &key(10), &key(30));

    assert!(matches!(result, Err(prolly::Error::Store(_))));
    assert_eq!(base.root, source_root);
    assert_eq!(keys(&manager, &base), (0..40).map(key).collect::<Vec<_>>());
    assert_eq!(
        store.successful_batch_puts.load(Ordering::Relaxed),
        successful_writes_before,
        "the failed operation must not publish a result root or rewritten nodes"
    );
}

#[test]
fn canonical_batch_does_not_return_a_root_when_persistence_fails() {
    let config = fixed_small_chunk_config(NodeLayoutSpec::PrefixCompressed);
    let store = Arc::new(FailingBatchPutMemStore::new());
    let manager = Prolly::new(store.clone(), config.clone());
    let mut builder = BatchBuilder::new(store.clone(), config);
    for index in 0..40 {
        builder.add(key(index), value(index));
    }
    let base = builder.build().unwrap();
    let successful_writes_before = store.successful_batch_puts.load(Ordering::Relaxed);

    store.fail_next_batch_put();
    let result = manager.batch(
        &base,
        vec![Mutation::Upsert {
            key: b"key-00000000000000000020a".to_vec(),
            val: b"inserted".to_vec(),
        }],
    );

    assert!(matches!(result, Err(prolly::Error::Store(_))));
    assert_eq!(keys(&manager, &base), (0..40).map(key).collect::<Vec<_>>());
    assert_eq!(
        store.successful_batch_puts.load(Ordering::Relaxed),
        successful_writes_before
    );
}

#[test]
fn sorted_builder_does_not_return_a_root_when_persistence_fails() {
    let config = fixed_small_chunk_config(NodeLayoutSpec::Plain);
    let store = Arc::new(FailingBatchPutMemStore::new());
    store.fail_next_batch_put();
    let mut builder = prolly::SortedBatchBuilder::new(store.clone(), config);
    for index in 0..40 {
        builder.add(key(index), value(index)).unwrap();
    }

    assert!(matches!(builder.build(), Err(prolly::Error::Store(_))));
    assert_eq!(store.successful_batch_puts.load(Ordering::Relaxed), 0);
}

#[test]
fn value_sensitive_height_two_root_promotion_falls_back_to_the_canonical_rebuild() {
    let mut policy = chunking::entry_count_key_value_hash();
    policy.min = 1;
    policy.target = 2;
    policy.max = 4;
    policy.hash_seed = 397;
    let config = Config::builder()
        .chunking(policy)
        .node_layout(NodeLayoutSpec::Plain)
        .build();
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), config.clone());
    let mut base_builder = BatchBuilder::new(store.clone(), config.clone());
    for index in 0..40 {
        base_builder.add(key(index), value(index));
    }
    let base = base_builder.build().unwrap();
    let base_root = Node::from_bytes(
        &store
            .get(base.root.as_ref().unwrap().as_bytes())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(base_root.level, 2, "regression must start height two");

    let expected_store = Arc::new(MemStore::new());
    let mut expected_builder = BatchBuilder::new(expected_store.clone(), config);
    for index in 0..40 {
        if !(13..23).contains(&index) {
            expected_builder.add(key(index), value(index));
        }
    }
    let expected = expected_builder.build().unwrap();
    let expected_root = Node::from_bytes(
        &expected_store
            .get(expected.root.as_ref().unwrap().as_bytes())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        expected_root.level, 3,
        "the clean oracle must promote when the root boundary observes changed internal CIDs"
    );

    manager.clear_cache();
    manager.reset_metrics();
    let deleted = manager.delete_range(&base, &key(13), &key(23)).unwrap();

    assert_eq!(deleted.root, expected.root);
    assert!(
        manager.metrics().store_batch_get_calls >= 1,
        "the valid height-2 tree must reach localized internal-window routing before falling back"
    );
}

#[test]
fn localized_point_deletes_fall_back_when_the_canonical_root_promotes() {
    let mut policy = chunking::entry_count_key_value_hash();
    policy.min = 1;
    policy.target = 2;
    policy.max = 4;
    policy.hash_seed = 397;
    let config = Config::builder()
        .chunking(policy)
        .node_layout(NodeLayoutSpec::Plain)
        .build();
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), config.clone());
    let mut base_builder = BatchBuilder::new(store.clone(), config.clone());
    for index in 0..40 {
        base_builder.add(key(index), value(index));
    }
    let base = base_builder.build().unwrap();
    let base_root = Node::from_bytes(
        &store
            .get(base.root.as_ref().unwrap().as_bytes())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(base_root.level, 2, "regression must start height two");

    let expected_store = Arc::new(MemStore::new());
    let mut expected_builder = BatchBuilder::new(expected_store.clone(), config);
    for index in 0..40 {
        if !(13..23).contains(&index) {
            expected_builder.add(key(index), value(index));
        }
    }
    let expected = expected_builder.build().unwrap();
    let expected_root = Node::from_bytes(
        &expected_store
            .get(expected.root.as_ref().unwrap().as_bytes())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        expected_root.level, 3,
        "the clean oracle must promote when changed internal CIDs affect root boundaries"
    );

    let (actual, _) = manager
        .batch_with_write_stats(
            &base,
            (13..23)
                .map(|index| Mutation::Delete { key: key(index) })
                .collect(),
        )
        .unwrap();

    assert_eq!(actual.root, expected.root);
}

#[test]
fn synchronous_range_stats_report_existing_deletes_and_manager_io_for_all_paths() {
    let (manager, leaf_root) = fixture([b"a", b"b", b"c", b"d", b"e", b"f"]);
    manager.clear_cache();
    manager.reset_metrics();
    let before = manager.metrics();
    let (deleted_leaf, leaf_stats) = manager
        .delete_range_with_stats(&leaf_root, b"b", b"e")
        .unwrap();
    let after = manager.metrics();
    assert_eq!(leaf_stats.input_mutations, 3, "{leaf_stats:?}");
    assert_eq!(leaf_stats.effective_mutations, 3, "{leaf_stats:?}");
    assert_eq!(leaf_stats.nodes_read, after.nodes_read - before.nodes_read);
    assert_eq!(leaf_stats.bytes_read, after.bytes_read - before.bytes_read);
    assert_eq!(
        leaf_stats.nodes_written,
        after.nodes_written - before.nodes_written
    );
    assert_eq!(
        leaf_stats.bytes_written,
        after.bytes_written - before.bytes_written
    );
    assert!(leaf_stats.nodes_written > 0, "{leaf_stats:?}");
    assert_eq!(keys(&manager, &deleted_leaf), bytes(["a", "e", "f"]));

    let config = fixed_small_chunk_config(NodeLayoutSpec::Plain);
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), config.clone());
    let mut builder = BatchBuilder::new(store, config);
    for index in 0..40 {
        builder.add(key(index), value(index));
    }
    let height_two = builder.build().unwrap();
    let root = Node::from_bytes(
        &manager
            .store()
            .get(height_two.root.as_ref().unwrap().as_bytes())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(root.level, 2, "test must exercise the localized splice");

    manager.clear_cache();
    manager.reset_metrics();
    let before = manager.metrics();
    let (_, localized_stats) = manager
        .delete_range_with_stats(&height_two, &key(10), &key(30))
        .unwrap();
    let after = manager.metrics();
    assert_eq!(localized_stats.input_mutations, 20, "{localized_stats:?}");
    assert_eq!(
        localized_stats.effective_mutations, 20,
        "{localized_stats:?}"
    );
    assert_eq!(
        localized_stats.nodes_read,
        after.nodes_read - before.nodes_read,
        "{localized_stats:?}"
    );
    assert_eq!(
        localized_stats.bytes_read,
        after.bytes_read - before.bytes_read,
        "{localized_stats:?}"
    );
    assert_eq!(
        localized_stats.nodes_written,
        after.nodes_written - before.nodes_written,
        "{localized_stats:?}"
    );
    assert_eq!(
        localized_stats.bytes_written,
        after.bytes_written - before.bytes_written,
        "{localized_stats:?}"
    );
    assert!(localized_stats.nodes_written > 0, "{localized_stats:?}");

    let config = fixed_small_chunk_config(NodeLayoutSpec::Plain);
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), config.clone());
    let mut builder = BatchBuilder::new(store, config);
    for index in 0..2_000 {
        builder.add(key(index), value(index));
    }
    let height_three = builder.build().unwrap();
    let root = Node::from_bytes(
        &manager
            .store()
            .get(height_three.root.as_ref().unwrap().as_bytes())
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert!(root.level >= 3, "test must exercise streaming fallback");

    manager.clear_cache();
    manager.reset_metrics();
    let before = manager.metrics();
    let (_, fallback_stats) = manager
        .delete_range_with_stats(&height_three, &key(600), &key(1_400))
        .unwrap();
    let after = manager.metrics();
    assert_eq!(fallback_stats.input_mutations, 800, "{fallback_stats:?}");
    assert_eq!(
        fallback_stats.effective_mutations, 800,
        "{fallback_stats:?}"
    );
    assert_eq!(
        fallback_stats.nodes_read,
        after.nodes_read - before.nodes_read,
        "{fallback_stats:?}"
    );
    assert_eq!(
        fallback_stats.bytes_read,
        after.bytes_read - before.bytes_read,
        "{fallback_stats:?}"
    );
    assert_eq!(
        fallback_stats.nodes_written,
        after.nodes_written - before.nodes_written,
        "{fallback_stats:?}"
    );
    assert_eq!(
        fallback_stats.bytes_written,
        after.bytes_written - before.bytes_written,
        "{fallback_stats:?}"
    );
    assert!(fallback_stats.nodes_written > 0, "{fallback_stats:?}");
}

#[test]
fn localized_range_stats_match_warm_cache_metric_deltas() {
    let config = fixed_small_chunk_config(NodeLayoutSpec::Plain);
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), config.clone());
    let mut builder = BatchBuilder::new(store, config);
    for index in 0..40 {
        builder.add(key(index), value(index));
    }
    let base = builder.build().unwrap();

    manager.clear_cache();
    let first = manager.delete_range(&base, &key(10), &key(30)).unwrap();
    assert_ne!(first.root, base.root);

    manager.reset_metrics();
    let (_, stats) = manager
        .delete_range_with_stats(&base, &key(10), &key(30))
        .unwrap();
    let metrics = manager.metrics();

    assert_eq!(stats.nodes_read, metrics.nodes_read, "{stats:?}");
    assert_eq!(stats.bytes_read, metrics.bytes_read, "{stats:?}");
}
