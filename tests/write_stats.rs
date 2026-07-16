use prolly::{
    BatchBuilder, BatchOp, BoundaryInput, BoundaryRule, ChunkMeasure, Config, HashAlgorithm,
    MemStore, MemStoreError, Mutation, Node, Prolly, Store, Tree,
};
use std::sync::Arc;

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

const SCATTERED_RECORDS: usize = 100_000;
const SCATTERED_UPDATES: usize = 1_000;

fn config() -> Config {
    let mut chunking = prolly::chunking::entry_count_key_hash();
    chunking.measure = ChunkMeasure::EntryCount;
    chunking.input = BoundaryInput::Key;
    chunking.hash = HashAlgorithm::XxHash64;
    chunking.rule = BoundaryRule::HashThreshold { factor: 8 };
    chunking.min = 4;
    chunking.target = 8;
    chunking.max = 32;
    Config::builder().chunking(chunking).build()
}

fn records() -> Vec<Mutation> {
    (0..2_000)
        .map(|index| Mutation::Upsert {
            key: format!("key-{index:06}").into_bytes(),
            val: format!("value-{index:06}").into_bytes(),
        })
        .collect()
}

fn leaf_ranges(store: &Arc<MemStore>, tree: &Tree) -> Vec<(Vec<u8>, Vec<u8>)> {
    fn visit(store: &Arc<MemStore>, cid: &prolly::Cid, ranges: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        let bytes = store.get(cid.as_bytes()).unwrap().unwrap();
        let node = Node::from_bytes(&bytes).unwrap();
        if node.leaf {
            ranges.push((
                node.keys.first().unwrap().clone(),
                node.keys.last().unwrap().clone(),
            ));
        } else {
            for value in node.vals {
                let bytes: [u8; 32] = value.try_into().unwrap();
                visit(store, &prolly::Cid(bytes), ranges);
            }
        }
    }

    let mut ranges = Vec::new();
    if let Some(root) = &tree.root {
        visit(store, root, &mut ranges);
    }
    ranges
}

fn assert_nodes_fit_hard_cap(store: &Arc<MemStore>, tree: &Tree, hard_cap: usize) {
    fn visit(store: &Arc<MemStore>, cid: &prolly::Cid, hard_cap: usize) {
        let bytes = store.get(cid.as_bytes()).unwrap().unwrap();
        assert!(
            bytes.len() <= hard_cap,
            "node {cid:?} uses {} bytes, above the {hard_cap}-byte hard cap",
            bytes.len()
        );
        let node = Node::from_bytes(&bytes).unwrap();
        if !node.leaf {
            for value in node.vals {
                let child: [u8; 32] = value.try_into().unwrap();
                visit(store, &prolly::Cid(child), hard_cap);
            }
        }
    }

    if let Some(root) = &tree.root {
        visit(store, root, hard_cap);
    }
}

fn leaf_cids(store: &Arc<MemStore>, tree: &Tree) -> Vec<prolly::Cid> {
    fn visit(store: &Arc<MemStore>, cid: &prolly::Cid, leaves: &mut Vec<prolly::Cid>) {
        let bytes = store.get(cid.as_bytes()).unwrap().unwrap();
        let node = Node::from_bytes(&bytes).unwrap();
        if node.leaf {
            leaves.push(cid.clone());
        } else {
            for value in node.vals {
                let child: [u8; 32] = value.try_into().unwrap();
                visit(store, &prolly::Cid(child), leaves);
            }
        }
    }

    let mut leaves = Vec::new();
    if let Some(root) = &tree.root {
        visit(store, root, &mut leaves);
    }
    leaves
}

#[test]
fn middle_value_update_resynchronizes_without_streaming_the_tree() {
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), config());
    let tree = manager.batch(&manager.create(), records()).unwrap();
    let before_ranges = leaf_ranges(&store, &tree);

    let (updated, stats) = manager
        .batch_with_write_stats(
            &tree,
            vec![Mutation::Upsert {
                key: b"key-001000".to_vec(),
                val: b"changed".to_vec(),
            }],
        )
        .unwrap();

    assert!(stats.used_key_stable_fast_path, "{stats:?}");
    assert_eq!(stats.entries_streamed, 8, "{stats:?}");
    assert!(stats.nodes_written <= 4, "{stats:?}");
    assert_eq!(leaf_ranges(&store, &updated), before_ranges);
    assert_eq!(
        manager.get(&updated, b"key-001000").unwrap(),
        Some(b"changed".to_vec())
    );

    let mut rebuilt = BatchBuilder::new(store, config());
    for index in 0..2_000 {
        rebuilt.add(
            format!("key-{index:06}").into_bytes(),
            if index == 1_000 {
                b"changed".to_vec()
            } else {
                format!("value-{index:06}").into_bytes()
            },
        );
    }
    assert_eq!(updated.root, rebuilt.build().unwrap().root);
}

#[test]
fn duplicate_mutations_are_last_write_wins_before_streaming() {
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store, config());
    let tree = manager.batch(&manager.create(), records()).unwrap();
    let (updated, stats) = manager
        .batch_with_write_stats(
            &tree,
            vec![
                Mutation::Upsert {
                    key: b"key-000500".to_vec(),
                    val: b"first".to_vec(),
                },
                Mutation::Delete {
                    key: b"key-000500".to_vec(),
                },
                Mutation::Upsert {
                    key: b"key-000500".to_vec(),
                    val: b"last".to_vec(),
                },
            ],
        )
        .unwrap();

    assert_eq!(stats.effective_mutations, 1);
    assert_eq!(
        manager.get(&updated, b"key-000500").unwrap(),
        Some(b"last".to_vec())
    );
}

#[test]
fn sequential_value_updates_match_a_full_canonical_rebuild() {
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), config());
    let mut tree = manager.batch(&manager.create(), records()).unwrap();

    let mut changed = std::collections::BTreeSet::new();
    for index in (0..2_000).step_by(19) {
        changed.insert(index);
        tree = manager
            .put(
                &tree,
                format!("key-{index:06}").into_bytes(),
                format!("changed-{index:06}").into_bytes(),
            )
            .unwrap();
        let mut checkpoint = BatchBuilder::new(store.clone(), config());
        for item in 0..2_000 {
            checkpoint.add(
                format!("key-{item:06}").into_bytes(),
                if changed.contains(&item) {
                    format!("changed-{item:06}").into_bytes()
                } else {
                    format!("value-{item:06}").into_bytes()
                },
            );
        }
        assert_eq!(
            tree.root,
            checkpoint.build().unwrap().root,
            "first divergence after key {index}"
        );
    }

    let mut rebuilt = BatchBuilder::new(store, config());
    for index in 0..2_000 {
        rebuilt.add(
            format!("key-{index:06}").into_bytes(),
            if index % 19 == 0 {
                format!("changed-{index:06}").into_bytes()
            } else {
                format!("value-{index:06}").into_bytes()
            },
        );
    }
    assert_eq!(tree.root, rebuilt.build().unwrap().root);
}

#[test]
fn scattered_value_updates_use_batched_canonical_rewrite() {
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), Config::default());
    let mut base = BatchBuilder::new(store.clone(), Config::default());
    for index in 0..SCATTERED_RECORDS {
        base.add(
            format!("key-{index:020}").into_bytes(),
            format!("value-{index:020}-00").into_bytes(),
        );
    }
    let base = base.build().unwrap();
    let changed = (0..SCATTERED_UPDATES)
        .map(|offset| (offset * 7_919) % SCATTERED_RECORDS)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(changed.len(), SCATTERED_UPDATES);

    let (updated, stats) = manager
        .batch_with_write_stats(
            &base,
            changed
                .iter()
                .map(|index| Mutation::Upsert {
                    key: format!("key-{index:020}").into_bytes(),
                    val: format!("value-{index:020}-01").into_bytes(),
                })
                .collect(),
        )
        .unwrap();

    assert!(stats.used_key_stable_fast_path, "{stats:?}");
    assert!(stats.used_batched_value_update_path, "{stats:?}");

    let mut rebuilt = BatchBuilder::new(store, Config::default());
    for index in 0..SCATTERED_RECORDS {
        rebuilt.add(
            format!("key-{index:020}").into_bytes(),
            format!(
                "value-{index:020}-{}",
                if changed.contains(&index) { "01" } else { "00" }
            )
            .into_bytes(),
        );
    }
    assert_eq!(updated.root, rebuilt.build().unwrap().root);
}

#[test]
fn batched_value_update_path_rejects_structural_and_growing_edits() {
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), Config::default());
    let mut builder = BatchBuilder::new(store, Config::default());
    for index in 0..20_000 {
        builder.add(
            format!("key-{index:020}").into_bytes(),
            format!("value-{index:020}-00").into_bytes(),
        );
    }
    let base = builder.build().unwrap();

    let growing = (0..300)
        .map(|index| Mutation::Upsert {
            key: format!("key-{:020}", index * 61).into_bytes(),
            val: vec![b'x'; 128],
        })
        .collect();
    let (_, growing_stats) = manager.batch_with_write_stats(&base, growing).unwrap();
    assert!(growing_stats.used_key_stable_fast_path);
    assert!(!growing_stats.used_batched_value_update_path);

    let mut insertion = (0..299)
        .map(|index| Mutation::Upsert {
            key: format!("key-{:020}", index * 61).into_bytes(),
            val: format!("value-{:020}-01", index * 61).into_bytes(),
        })
        .collect::<Vec<_>>();
    insertion.push(Mutation::Upsert {
        key: b"key-new".to_vec(),
        val: b"inserted".to_vec(),
    });
    let (inserted, insertion_stats) = manager.batch_with_write_stats(&base, insertion).unwrap();
    assert!(!insertion_stats.used_batched_value_update_path);
    assert_eq!(
        manager.get(&inserted, b"key-new").unwrap(),
        Some(b"inserted".to_vec())
    );

    let mut deletion = (0..299)
        .map(|index| Mutation::Upsert {
            key: format!("key-{:020}", index * 61).into_bytes(),
            val: format!("value-{:020}-01", index * 61).into_bytes(),
        })
        .collect::<Vec<_>>();
    deletion.push(Mutation::Delete {
        key: b"key-000000000000019999".to_vec(),
    });
    let (deleted, deletion_stats) = manager.batch_with_write_stats(&base, deletion).unwrap();
    assert!(!deletion_stats.used_batched_value_update_path);
    assert_eq!(
        manager.get(&deleted, b"key-000000000000019999").unwrap(),
        None
    );
}

#[test]
fn clustered_batch_delete_batches_internal_frontier_reads() {
    const RECORDS: usize = 200_000;
    const DELETES: usize = 2_000;
    let store = Arc::new(BatchReadMemStore::new());
    let manager = Prolly::new(store.clone(), Config::default());
    let mut builder = BatchBuilder::new(store.clone(), Config::default());
    for index in 0..RECORDS {
        builder.add(
            format!("key-{index:020}").into_bytes(),
            format!("value-{index:020}-00").into_bytes(),
        );
    }
    let base = builder.build().unwrap();
    let delete_start = (RECORDS - DELETES) / 2;

    manager.clear_cache();
    manager.reset_metrics();
    let (deleted, stats) = manager
        .batch_with_write_stats(
            &base,
            (delete_start..delete_start + DELETES)
                .map(|index| Mutation::Delete {
                    key: format!("key-{index:020}").into_bytes(),
                })
                .collect(),
        )
        .unwrap();
    let metrics = manager.metrics();

    assert!(
        metrics.store_batch_get_calls > 0,
        "internal frontiers should use ordered batch reads: metrics={metrics:?}, stats={stats:?}"
    );
    assert!(
        metrics.store_batch_get_calls >= 2,
        "the affected leaf window should also use an ordered batch read: metrics={metrics:?}, stats={stats:?}"
    );
    assert!(
        metrics.store_get_calls <= stats.resync_distance_nodes + 2,
        "point reads should be limited to local leaf replay and root checks: metrics={metrics:?}, stats={stats:?}"
    );
    assert!(
        stats.nodes_read <= stats.resync_distance_nodes + 8,
        "clustered deletes should not hydrate the full internal frontier: metrics={metrics:?}, stats={stats:?}"
    );

    let mut rebuilt = BatchBuilder::new(store, Config::default());
    for index in 0..RECORDS {
        if !(delete_start..delete_start + DELETES).contains(&index) {
            rebuilt.add(
                format!("key-{index:020}").into_bytes(),
                format!("value-{index:020}-00").into_bytes(),
            );
        }
    }
    assert_eq!(deleted.root, rebuilt.build().unwrap().root);
}

#[test]
fn shrinking_a_cap_split_leaf_converges_to_the_canonical_root() {
    const HARD_CAP: u64 = 300;
    let mut chunking = prolly::chunking::entry_count_key_hash();
    chunking.min = 8;
    chunking.target = 16;
    chunking.max = 32;
    chunking.rule = BoundaryRule::HashThreshold { factor: u32::MAX };
    chunking.hard_max_node_bytes = HARD_CAP;
    let capped = Config::builder().chunking(chunking).build();

    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store.clone(), capped.clone());
    let mut initial = BatchBuilder::new(store.clone(), capped.clone());
    for index in 0..64 {
        initial.add(format!("key-{index:04}").into_bytes(), vec![b'x'; 100]);
    }
    let initial = initial.build().unwrap();
    assert_nodes_fit_hard_cap(&store, &initial, HARD_CAP as usize);

    let (updated, stats) = manager
        .batch_with_write_stats(
            &initial,
            vec![Mutation::Upsert {
                key: b"key-0020".to_vec(),
                val: b"small".to_vec(),
            }],
        )
        .unwrap();

    let mut rebuilt = BatchBuilder::new(store.clone(), capped);
    for index in 0..64 {
        rebuilt.add(
            format!("key-{index:04}").into_bytes(),
            if index == 20 {
                b"small".to_vec()
            } else {
                vec![b'x'; 100]
            },
        );
    }
    let rebuilt = rebuilt.build().unwrap();

    assert!(
        !stats.used_key_stable_fast_path,
        "a cap-derived boundary must be rechunked: {stats:?}"
    );
    assert_eq!(
        leaf_cids(&store, &updated),
        leaf_cids(&store, &rebuilt),
        "leaf chunking diverged: {stats:?}"
    );
    assert_eq!(
        updated.root, rebuilt.root,
        "internal chunking diverged: {stats:?}"
    );
    assert_nodes_fit_hard_cap(&store, &updated, HARD_CAP as usize);
}
