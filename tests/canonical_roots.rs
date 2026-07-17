use prolly::{
    chunking, BatchBuilder, BatchWriter, Config, Error, MemStore, Mutation, NodeLayoutSpec,
    ParallelConfig, Prolly,
};
use std::collections::BTreeMap;
use std::sync::Arc;

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
    let parallel = manager
        .parallel_batch(&base, mutations, &ParallelConfig::new(0, 1))
        .unwrap();

    assert_eq!(parallel.root, canonical.root);
}
