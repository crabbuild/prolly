use prolly::{
    chunking, BatchBuilder, BatchOp, BatchWriter, Config, Error, MemStore, Mutation,
    NodeLayoutSpec, ParallelConfig, Prolly, Store,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

static PARALLEL_TELEMETRY_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug)]
struct PublicationStoreError(String);

impl std::fmt::Display for PublicationStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for PublicationStoreError {}

#[derive(Default)]
struct PublicationStore {
    inner: MemStore,
    batch_put_calls: AtomicUsize,
    fail_batch_put: AtomicBool,
}

impl PublicationStore {
    fn map_error(error: prolly::MemStoreError) -> PublicationStoreError {
        PublicationStoreError(error.to_string())
    }
}

impl Store for PublicationStore {
    type Error = PublicationStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key).map_err(Self::map_error)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.inner.put(key, value).map_err(Self::map_error)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key).map_err(Self::map_error)
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        self.inner.batch(ops).map_err(Self::map_error)
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        self.inner.batch_get(keys).map_err(Self::map_error)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.inner.batch_get_ordered(keys).map_err(Self::map_error)
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        self.inner
            .batch_get_ordered_unique(keys)
            .map_err(Self::map_error)
    }

    fn prefers_batch_reads(&self) -> bool {
        true
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.batch_put_calls.fetch_add(1, Ordering::Relaxed);
        if self.fail_batch_put.load(Ordering::Acquire) {
            return Err(PublicationStoreError(
                "injected atomic publication failure".to_owned(),
            ));
        }
        self.inner.batch_put(entries).map_err(Self::map_error)
    }
}

fn records() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..180)
        .map(|index| {
            (
                format!("key-{index:04}").into_bytes(),
                format!("value-{index:04}").into_bytes(),
            )
        })
        .collect()
}

fn numbered_records(count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|index| (format!("key-{index:06}").into_bytes(), vec![b'x'; 32]))
        .collect()
}

fn build_records(config: Config, records: &[(Vec<u8>, Vec<u8>)]) -> prolly::Tree {
    let mut builder = BatchBuilder::new(Arc::new(MemStore::new()), config);
    for (key, value) in records {
        builder.add(key.clone(), value.clone());
    }
    builder.build().unwrap()
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[test]
fn randomized_mixed_api_histories_match_bulk_for_every_policy() {
    for mut policy in [
        chunking::entry_count_key_hash(),
        chunking::entry_count_key_value_hash(),
        chunking::logical_bytes_key_weibull(),
        chunking::logical_bytes_rolling_hash(),
    ] {
        match policy.measure {
            prolly::ChunkMeasure::EntryCount => {
                policy.min = 2;
                policy.target = 8;
                policy.max = 16;
                policy.hard_max_node_bytes = 1_024;
            }
            _ => {
                policy.min = 96;
                policy.target = 256;
                policy.max = 768;
                policy.hard_max_node_bytes = 1_024;
            }
        }

        for seed in 0..32_u64 {
            let config = Config::builder()
                .chunking(policy.clone())
                .node_layout(NodeLayoutSpec::PrefixCompressed)
                .build();
            let manager = Prolly::new(Arc::new(MemStore::new()), config.clone());
            let writer = BatchWriter::new();
            let parallel = ParallelConfig::new(4, 1);
            let mut tree = manager.create();
            let mut expected = BTreeMap::new();
            let mut state = seed.wrapping_add(1).wrapping_mul(0x9e37_79b9_7f4a_7c15);

            for step in 0..192 {
                let random = next_random(&mut state);
                let key_index = random as usize % 96;
                let key = format!("key-{key_index:04}").into_bytes();
                let mutation = if random % 5 == 0 {
                    expected.remove(&key);
                    Mutation::Delete { key }
                } else {
                    let value_len = 8 + ((random >> 8) as usize % 73);
                    let value = vec![(random >> 24) as u8; value_len];
                    expected.insert(key.clone(), value.clone());
                    Mutation::Upsert { key, val: value }
                };

                tree = match step % 6 {
                    0 => match mutation {
                        Mutation::Upsert { key, val } => manager.put(&tree, key, val).unwrap(),
                        Mutation::Delete { key } => manager.delete(&tree, &key).unwrap(),
                    },
                    1 => manager.batch(&tree, vec![mutation]).unwrap(),
                    2 => {
                        manager
                            .batch_with_stats(&tree, vec![mutation])
                            .unwrap()
                            .tree
                    }
                    3 => {
                        writer
                            .apply_batch_with_stats(&manager, &tree, vec![mutation])
                            .unwrap()
                            .tree
                    }
                    4 => manager
                        .parallel_batch(&tree, vec![mutation], &parallel)
                        .unwrap(),
                    _ => manager.append_batch(&tree, vec![mutation]).unwrap(),
                };
            }

            let mut bulk = BatchBuilder::new(Arc::new(MemStore::new()), config);
            for (key, value) in expected {
                bulk.add(key, value);
            }
            assert_eq!(
                tree.root,
                bulk.build().unwrap().root,
                "policy={:?}, seed={seed}",
                policy.rule
            );
        }
    }
}

#[test]
fn direct_batch_writer_stats_matches_canonical_root_for_every_policy() {
    for policy in [
        chunking::entry_count_key_hash(),
        chunking::entry_count_key_value_hash(),
        chunking::logical_bytes_key_weibull(),
        chunking::logical_bytes_rolling_hash(),
    ] {
        let config = Config::builder().chunking(policy).build();
        let base_records = numbered_records(5_000);
        let store = Arc::new(MemStore::new());
        let mut base_builder = BatchBuilder::new(store.clone(), config.clone());
        for (key, value) in &base_records {
            base_builder.add(key.clone(), value.clone());
        }
        let base = base_builder.build().unwrap();
        let manager = Prolly::new(store, config.clone());
        let inserted = (b"key-002500a".to_vec(), vec![b'y'; 32]);
        let actual = BatchWriter::new()
            .apply_batch_with_stats(
                &manager,
                &base,
                vec![Mutation::Upsert {
                    key: inserted.0.clone(),
                    val: inserted.1.clone(),
                }],
            )
            .unwrap()
            .tree;

        let mut final_records = base_records;
        final_records.push(inserted);
        final_records.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let expected = build_records(config, &final_records);

        assert_eq!(actual.root, expected.root, "policy diverged");
    }
}

#[test]
fn every_stats_and_parallel_writer_returns_the_canonical_root() {
    for policy in [
        chunking::entry_count_key_hash(),
        chunking::entry_count_key_value_hash(),
        chunking::logical_bytes_key_weibull(),
        chunking::logical_bytes_rolling_hash(),
    ] {
        let config = Config::builder().chunking(policy).build();
        let base_records = numbered_records(5_000);
        let store = Arc::new(MemStore::new());
        let mut base_builder = BatchBuilder::new(store.clone(), config.clone());
        for (key, value) in &base_records {
            base_builder.add(key.clone(), value.clone());
        }
        let base = base_builder.build().unwrap();
        let manager = Prolly::new(store, config.clone());
        let mutations = vec![Mutation::Upsert {
            key: b"key-002500a".to_vec(),
            val: vec![b'y'; 32],
        }];

        let mut final_records = base_records;
        final_records.push((b"key-002500a".to_vec(), vec![b'y'; 32]));
        final_records.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        let expected = build_records(config, &final_records).root;

        let roots = [
            BatchWriter::new()
                .apply_batch(&manager, &base, mutations.clone())
                .unwrap()
                .root,
            BatchWriter::new()
                .apply_batch_with_stats(&manager, &base, mutations.clone())
                .unwrap()
                .tree
                .root,
            manager.batch(&base, mutations.clone()).unwrap().root,
            manager
                .batch_with_stats(&base, mutations.clone())
                .unwrap()
                .tree
                .root,
            manager
                .batch_with_write_stats(&base, mutations.clone())
                .unwrap()
                .0
                .root,
            manager
                .parallel_batch(&base, mutations.clone(), &ParallelConfig::new(4, 1))
                .unwrap()
                .root,
            manager
                .parallel_batch_with_stats(&base, mutations.clone(), &ParallelConfig::new(4, 1))
                .unwrap()
                .tree
                .root,
        ];

        assert!(roots.iter().all(|root| root == &expected));
    }
}

#[test]
fn mutation_rejects_a_tree_whose_declared_format_differs_from_its_root() {
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store, Config::default());
    let mut tree = manager
        .put(&manager.create(), b"key".to_vec(), b"value".to_vec())
        .unwrap();
    tree.config.format.node_layout = NodeLayoutSpec::Plain;

    assert!(matches!(
        manager.put(&tree, b"key".to_vec(), b"changed".to_vec()),
        Err(Error::FormatMismatch { .. })
    ));
}

#[test]
fn mutation_histories_converge_to_the_bulk_root() {
    for policy in [
        chunking::entry_count_key_value_hash(),
        chunking::entry_count_key_hash(),
        chunking::logical_bytes_key_weibull(),
        chunking::logical_bytes_rolling_hash(),
    ] {
        let config = Config::builder()
            .chunking(policy)
            .node_layout(NodeLayoutSpec::PrefixCompressed)
            .build();
        let records = records();

        let bulk_store = Arc::new(MemStore::new());
        let mut bulk = BatchBuilder::new(bulk_store, config.clone());
        for (key, value) in &records {
            bulk.add(key.clone(), value.clone());
        }
        let expected = bulk.build().unwrap().root;

        let ascending = Prolly::new(Arc::new(MemStore::new()), config.clone());
        let mut ascending_tree = ascending.create();
        for (key, value) in &records {
            ascending_tree = ascending
                .put(&ascending_tree, key.clone(), value.clone())
                .unwrap();
        }

        let descending = Prolly::new(Arc::new(MemStore::new()), config.clone());
        let mut descending_tree = descending.create();
        for (key, value) in records.iter().rev() {
            descending_tree = descending
                .put(&descending_tree, key.clone(), value.clone())
                .unwrap();
        }

        let batched = Prolly::new(Arc::new(MemStore::new()), config);
        let batch_tree = batched
            .batch(
                &batched.create(),
                records
                    .iter()
                    .map(|(key, val)| Mutation::Upsert {
                        key: key.clone(),
                        val: val.clone(),
                    })
                    .collect(),
            )
            .unwrap();

        assert_eq!(ascending_tree.root, expected);
        assert_eq!(descending_tree.root, expected);
        assert_eq!(batch_tree.root, expected);
    }
}

#[test]
fn delete_then_reinsert_returns_to_the_same_root() {
    let config = Config::builder()
        .chunking(chunking::entry_count_key_hash())
        .build();
    let manager = Prolly::new(Arc::new(MemStore::new()), config);
    let original = manager
        .batch(
            &manager.create(),
            records()
                .into_iter()
                .map(|(key, val)| Mutation::Upsert { key, val })
                .collect(),
        )
        .unwrap();
    let removed = manager.delete(&original, b"key-0090").unwrap();
    let restored = manager
        .put(&removed, b"key-0090".to_vec(), b"value-0090".to_vec())
        .unwrap();

    assert_eq!(restored.root, original.root);
}

#[test]
fn disjoint_merge_converges_to_the_direct_canonical_root() {
    let config = Config::builder()
        .chunking(chunking::entry_count_key_hash())
        .build();
    let manager = Prolly::new(Arc::new(MemStore::new()), config);
    let base = manager
        .batch(
            &manager.create(),
            records()
                .into_iter()
                .map(|(key, val)| Mutation::Upsert { key, val })
                .collect(),
        )
        .unwrap();
    let left = manager.put(&base, b"left".to_vec(), b"L".to_vec()).unwrap();
    let right = manager
        .put(&base, b"right".to_vec(), b"R".to_vec())
        .unwrap();
    let merged = manager.merge(&base, &left, &right, None).unwrap();
    let direct = manager
        .batch(
            &base,
            vec![
                Mutation::Upsert {
                    key: b"left".to_vec(),
                    val: b"L".to_vec(),
                },
                Mutation::Upsert {
                    key: b"right".to_vec(),
                    val: b"R".to_vec(),
                },
            ],
        )
        .unwrap();

    assert_eq!(merged.root, direct.root);
}

#[test]
fn chained_append_batches_match_the_bulk_root() {
    for policy in [
        chunking::entry_count_key_value_hash(),
        chunking::entry_count_key_hash(),
        chunking::logical_bytes_key_weibull(),
        chunking::logical_bytes_rolling_hash(),
    ] {
        let config = Config::builder()
            .chunking(policy)
            .node_layout(NodeLayoutSpec::Plain)
            .build();
        let all = records();
        let manager = Prolly::new(Arc::new(MemStore::new()), config.clone());
        let mut tree = manager
            .batch(
                &manager.create(),
                all[..60]
                    .iter()
                    .map(|(key, val)| Mutation::Upsert {
                        key: key.clone(),
                        val: val.clone(),
                    })
                    .collect(),
            )
            .unwrap();
        for batch in all[60..].chunks(17) {
            tree = manager
                .append_batch(
                    &tree,
                    batch
                        .iter()
                        .map(|(key, val)| Mutation::Upsert {
                            key: key.clone(),
                            val: val.clone(),
                        })
                        .collect(),
                )
                .unwrap();
        }

        let mut bulk = BatchBuilder::new(Arc::new(MemStore::new()), config);
        for (key, value) in &all {
            bulk.add(key.clone(), value.clone());
        }
        assert_eq!(tree.root, bulk.build().unwrap().root);
    }
}

#[test]
fn mixed_structural_batch_matches_the_bulk_root() {
    let config = Config::builder()
        .chunking(chunking::entry_count_key_hash())
        .node_layout(NodeLayoutSpec::Plain)
        .build();
    let manager = Prolly::new(Arc::new(MemStore::new()), config.clone());
    let original = records();
    let base = manager
        .batch(
            &manager.create(),
            original
                .iter()
                .map(|(key, val)| Mutation::Upsert {
                    key: key.clone(),
                    val: val.clone(),
                })
                .collect(),
        )
        .unwrap();
    let mutations = (0..90)
        .map(|index| match index % 5 {
            0 => Mutation::Delete {
                key: format!("key-{:04}", index * 2).into_bytes(),
            },
            1 | 2 => Mutation::Upsert {
                key: format!("key-{:04}", index * 2).into_bytes(),
                val: format!("changed-{index:04}").into_bytes(),
            },
            _ => Mutation::Upsert {
                key: format!("key-new-{index:04}").into_bytes(),
                val: format!("inserted-{index:04}").into_bytes(),
            },
        })
        .collect::<Vec<_>>();
    let actual = manager.batch(&base, mutations.clone()).unwrap();

    let mut expected_entries = original.into_iter().collect::<BTreeMap<_, _>>();
    for mutation in mutations {
        match mutation {
            Mutation::Upsert { key, val } => {
                expected_entries.insert(key, val);
            }
            Mutation::Delete { key } => {
                expected_entries.remove(&key);
            }
        }
    }
    let mut bulk = BatchBuilder::new(Arc::new(MemStore::new()), config);
    for (key, value) in expected_entries {
        bulk.add(key, value);
    }
    assert_eq!(actual.root, bulk.build().unwrap().root);
}

#[test]
fn parallel_batch_matches_the_canonical_batch_root() {
    let config = Config::builder()
        .chunking(chunking::entry_count_key_hash())
        .node_layout(NodeLayoutSpec::Plain)
        .build();
    let manager = Prolly::new(Arc::new(MemStore::new()), config.clone());
    let original = records();
    let base = manager
        .batch(
            &manager.create(),
            original
                .iter()
                .cloned()
                .map(|(key, val)| Mutation::Upsert { key, val })
                .collect(),
        )
        .unwrap();
    let mutations = (0..90)
        .map(|index| match index % 3 {
            0 => Mutation::Delete {
                key: format!("key-{:04}", index * 2).into_bytes(),
            },
            1 => Mutation::Upsert {
                key: format!("key-{:04}", index * 2).into_bytes(),
                val: format!("changed-{index:04}").into_bytes(),
            },
            _ => Mutation::Upsert {
                key: format!("key-new-{index:04}").into_bytes(),
                val: format!("inserted-{index:04}").into_bytes(),
            },
        })
        .collect::<Vec<_>>();

    let canonical = manager.batch(&base, mutations.clone()).unwrap();
    for width in [1, 2, 4, 8, 0] {
        let parallel = manager
            .parallel_batch(&base, mutations.clone(), &ParallelConfig::new(width, 1))
            .unwrap();
        assert_eq!(parallel.root, canonical.root, "width={width}");
    }

    let mut expected_entries = original.into_iter().collect::<BTreeMap<_, _>>();
    for mutation in mutations {
        match mutation {
            Mutation::Upsert { key, val } => {
                expected_entries.insert(key, val);
            }
            Mutation::Delete { key } => {
                expected_entries.remove(&key);
            }
        }
    }
    let mut bulk = BatchBuilder::new(Arc::new(MemStore::new()), config);
    for (key, value) in expected_entries {
        bulk.add(key, value);
    }
    assert_eq!(canonical.root, bulk.build().unwrap().root);
}

#[test]
fn parallel_structural_islands_match_canonical_roots_for_every_policy_and_width() {
    let _telemetry_lock = PARALLEL_TELEMETRY_TEST_LOCK.lock().unwrap();
    rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap()
        .install(|| {
            let original = (0..4_096)
                .map(|index| {
                    (
                        format!("k{index:05}").into_bytes(),
                        vec![(index % 251) as u8; 32],
                    )
                })
                .collect::<Vec<_>>();
            let mut saw_parallel_distant_islands = false;
            let mut saw_adjacent_admission_bypass = false;

            for (policy_index, mut policy) in [
                chunking::entry_count_key_hash(),
                chunking::entry_count_key_value_hash(),
                chunking::logical_bytes_key_weibull(),
                chunking::logical_bytes_rolling_hash(),
            ]
            .into_iter()
            .enumerate()
            {
                match policy.measure {
                    prolly::ChunkMeasure::EntryCount => {
                        policy.min = 2;
                        policy.target = 4;
                        policy.max = 8;
                        policy.hard_max_node_bytes = 2_048;
                    }
                    _ => {
                        policy.min = 96;
                        policy.target = 192;
                        policy.max = 384;
                        policy.hard_max_node_bytes = 1_024;
                    }
                }
                let config = Config::builder()
                    .chunking(policy.clone())
                    .node_layout(NodeLayoutSpec::PrefixCompressed)
                    .build();
                let manager = Prolly::new(Arc::new(MemStore::new()), config.clone());
                let base = manager
                    .batch(
                        &manager.create(),
                        original
                            .iter()
                            .cloned()
                            .map(|(key, val)| Mutation::Upsert { key, val })
                            .collect(),
                    )
                    .unwrap();
                let fixtures = [
                    (
                        "distant-inserts",
                        vec![
                            Mutation::Upsert {
                                key: b"k00256a".to_vec(),
                                val: vec![b'a'; 32],
                            },
                            Mutation::Upsert {
                                key: b"k03500a".to_vec(),
                                val: vec![b'b'; 32],
                            },
                        ],
                    ),
                    (
                        "adjacent-inserts",
                        vec![
                            Mutation::Upsert {
                                key: b"k01000a".to_vec(),
                                val: vec![b'c'; 32],
                            },
                            Mutation::Upsert {
                                key: b"k01016a".to_vec(),
                                val: vec![b'd'; 32],
                            },
                        ],
                    ),
                    (
                        "distant-deletes",
                        vec![
                            Mutation::Delete {
                                key: b"k00500".to_vec(),
                            },
                            Mutation::Delete {
                                key: b"k03000".to_vec(),
                            },
                        ],
                    ),
                    (
                        "distant-mixed",
                        vec![
                            Mutation::Delete {
                                key: b"k00750".to_vec(),
                            },
                            Mutation::Upsert {
                                key: b"k02000a".to_vec(),
                                val: vec![b'e'; 32],
                            },
                            Mutation::Upsert {
                                key: b"k03200".to_vec(),
                                val: vec![b'f'; 32],
                            },
                        ],
                    ),
                ];

                for (fixture_name, mutations) in fixtures {
                    let sequential = manager
                        .parallel_batch_with_stats(
                            &base,
                            mutations.clone(),
                            &ParallelConfig::sequential(),
                        )
                        .unwrap();
                    let automatic = manager.batch(&base, mutations.clone()).unwrap();
                    assert_eq!(
                        automatic.root, sequential.tree.root,
                        "policy={:?}, fixture={fixture_name}",
                        policy.rule
                    );

                    let mut widest = None;
                    for width in [1, 2, 4, 8, 0] {
                        let result = manager
                            .parallel_batch_with_stats(
                                &base,
                                mutations.clone(),
                                &ParallelConfig::new(width, 1),
                            )
                            .unwrap();
                        assert_eq!(
                            result.tree.root, sequential.tree.root,
                            "policy={:?}, fixture={fixture_name}, width={width}",
                            policy.rule
                        );
                        if fixture_name == "distant-inserts"
                            && width == 4
                            && result.stats.structural_islands > 1
                        {
                            assert!(result.stats.parallel_tasks > 1);
                            saw_parallel_distant_islands = true;
                        }
                        if policy_index == 0
                            && fixture_name == "adjacent-inserts"
                            && width == 4
                            && result.stats.structural_islands == 0
                            && result.stats.parallel_tasks == 0
                        {
                            saw_adjacent_admission_bypass = true;
                        }
                        if width == 0 {
                            widest = Some(result.tree);
                        }
                    }

                    let mut expected = original.iter().cloned().collect::<BTreeMap<_, _>>();
                    for mutation in mutations {
                        match mutation {
                            Mutation::Upsert { key, val } => {
                                expected.insert(key, val);
                            }
                            Mutation::Delete { key } => {
                                expected.remove(&key);
                            }
                        }
                    }
                    let expected = expected.into_iter().collect::<Vec<_>>();
                    assert_eq!(
                        sequential.tree.root,
                        build_records(config.clone(), &expected).root,
                        "policy={:?}, fixture={fixture_name}, fresh build",
                        policy.rule
                    );
                    assert_eq!(
                        manager.export_snapshot(&sequential.tree).unwrap(),
                        manager.export_snapshot(&widest.unwrap()).unwrap(),
                    );
                }
            }

            assert!(saw_parallel_distant_islands);
            assert!(saw_adjacent_admission_bypass);
        });
}

#[test]
fn failed_structural_proof_falls_back_before_one_atomic_publication() {
    let _telemetry_lock = PARALLEL_TELEMETRY_TEST_LOCK.lock().unwrap();
    rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap()
        .install(|| {
            let config = Config::builder()
                .min_chunk_size(2)
                .max_chunk_size(4)
                .chunking_factor(u32::MAX)
                .node_layout(NodeLayoutSpec::PrefixCompressed)
                .build();
            let records = (0..4_096)
                .map(|index| {
                    (
                        format!("k{index:05}").into_bytes(),
                        vec![(index % 251) as u8; 32],
                    )
                })
                .collect::<Vec<_>>();
            let store = Arc::new(PublicationStore::default());
            let mut builder = BatchBuilder::new(store.clone(), config.clone());
            for (key, value) in &records {
                builder.add(key.clone(), value.clone());
            }
            let base = builder.build().unwrap();
            store.batch_put_calls.store(0, Ordering::Relaxed);
            let manager = Prolly::new(store.clone(), config.clone());
            let mutations = (0..10)
                .map(|island| Mutation::Upsert {
                    key: format!("k{:05}a", 256 + island * 350).into_bytes(),
                    val: vec![b'a' + island as u8; 32],
                })
                .collect::<Vec<_>>();

            let result = manager
                .parallel_batch_with_stats(&base, mutations.clone(), &ParallelConfig::new(2, 1))
                .unwrap();
            if result.stats.structural_islands > 0 {
                assert_eq!(result.stats.structural_islands, 10);
                assert_eq!(result.stats.coalesced_islands, 0);
                assert_eq!(result.stats.parallel_tasks, 2);
            }
            assert_eq!(store.batch_put_calls.load(Ordering::Relaxed), 1);

            let mut expected = records.into_iter().collect::<BTreeMap<_, _>>();
            for mutation in mutations {
                match mutation {
                    Mutation::Upsert { key, val } => {
                        expected.insert(key, val);
                    }
                    Mutation::Delete { key } => {
                        expected.remove(&key);
                    }
                }
            }
            let expected = expected.into_iter().collect::<Vec<_>>();
            assert_eq!(result.tree.root, build_records(config, &expected).root);
        });
}

#[test]
fn configured_executor_returns_no_root_when_atomic_publication_fails() {
    rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap()
        .install(|| {
            let config = Config::builder()
                .chunking(chunking::entry_count_key_hash())
                .build();
            let records = numbered_records(5_000);
            let store = Arc::new(PublicationStore::default());
            let mut builder = BatchBuilder::new(store.clone(), config.clone());
            for (key, value) in &records {
                builder.add(key.clone(), value.clone());
            }
            let base = builder.build().unwrap();
            let manager = Prolly::new(store.clone(), config);
            let mutations = (0..256)
                .map(|index| Mutation::Upsert {
                    key: format!("key-{:06}", index * 16 + 1).into_bytes(),
                    val: vec![b'z'; 32],
                })
                .collect::<Vec<_>>();

            store.batch_put_calls.store(0, Ordering::Relaxed);
            store.fail_batch_put.store(true, Ordering::Release);
            let error = manager
                .parallel_batch_with_stats(&base, mutations.clone(), &ParallelConfig::new(4, 1))
                .unwrap_err();
            assert!(error
                .to_string()
                .contains("injected atomic publication failure"));
            assert_eq!(store.batch_put_calls.load(Ordering::Relaxed), 1);

            store.fail_batch_put.store(false, Ordering::Release);
            let recovered = manager
                .parallel_batch(&base, mutations.clone(), &ParallelConfig::new(4, 1))
                .unwrap();
            let sequential = manager
                .parallel_batch(&base, mutations, &ParallelConfig::sequential())
                .unwrap();
            assert_eq!(recovered.root, sequential.root);
        });
}

#[test]
fn concurrent_configured_callers_match_sequential_roots() {
    let _telemetry_lock = PARALLEL_TELEMETRY_TEST_LOCK.lock().unwrap();
    let config = Config::builder()
        .chunking(chunking::entry_count_key_hash())
        .build();
    let records = numbered_records(10_000);
    let store = Arc::new(MemStore::new());
    let mut builder = BatchBuilder::new(store.clone(), config.clone());
    for (key, value) in &records {
        builder.add(key.clone(), value.clone());
    }
    let base = builder.build().unwrap();
    let manager = Arc::new(Prolly::new(store, config));
    let workloads = (0..8)
        .map(|caller| {
            (0..64)
                .map(|index| Mutation::Upsert {
                    key: format!("key-{:06}", index * 137).into_bytes(),
                    val: vec![b'a' + caller as u8; 32],
                })
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let expected = workloads
        .iter()
        .map(|mutations| {
            manager
                .parallel_batch(&base, mutations.clone(), &ParallelConfig::sequential())
                .unwrap()
                .root
        })
        .collect::<Vec<_>>();
    let barrier = Arc::new(std::sync::Barrier::new(workloads.len()));

    std::thread::scope(|scope| {
        let handles = workloads
            .into_iter()
            .zip(expected)
            .map(|(mutations, expected)| {
                let manager = manager.clone();
                let base = base.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    let result = manager
                        .parallel_batch_with_stats(&base, mutations, &ParallelConfig::default())
                        .unwrap();
                    assert_eq!(result.tree.root, expected);
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap();
        }
    });
}
