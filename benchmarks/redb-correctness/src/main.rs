use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{
    Cid, Config, Diff, Error, MemStore, Mutation, Node, Prolly, RangeCursor, Resolution, Store,
    TransactionalStore, Tree,
};
use prolly_store_redb::{Durability, RedbStore, RedbStoreConfig, RedbStoreOptions};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

const ROOT: &[u8] = b"correctness/main";
const EPOCHS: usize = 32;
const MUTATIONS_PER_EPOCH: usize = 256;
const KEY_SPACE: usize = 4_096;
const SEEDS: [u64; 4] = [
    0x8a5c_39e7_d124_6fb1,
    0x1357_9bdf_2468_ace0,
    0xfedc_ba98_7654_3210,
    0x0ddc_0ffe_e15e_beef,
];

fn main() {
    let versioned_path = temp_path("versioned");
    let diff_merge_path = temp_path("diff-merge");
    remove_db(&versioned_path);
    remove_db(&diff_merge_path);

    for (index, seed) in SEEDS.into_iter().enumerate() {
        let random_path = temp_path(&format!("random-{index}"));
        remove_db(&random_path);
        randomized_reopen_differential(&random_path, seed);
        inspect_storage(&random_path);
        remove_db(&random_path);
    }
    let legacy_path = temp_path("legacy-engine");
    remove_db(&legacy_path);
    legacy_engine_migration(&legacy_path);
    remove_db(&legacy_path);
    advanced_diff_and_merge(&diff_merge_path);
    version_history_and_merge(&versioned_path);
    versioned_transaction_and_concurrency(&versioned_path);
    locking_characteristic(&temp_path("locking"));

    remove_db(&versioned_path);
    remove_db(&diff_merge_path);
    println!(
        "CORRECT,seeds={},epochs_per_seed={EPOCHS},mutations={},key_space={KEY_SPACE}",
        SEEDS.len(),
        SEEDS.len() * EPOCHS * MUTATIONS_PER_EPOCH
    );
}

fn advanced_diff_and_merge(path: &Path) {
    let config = Config::builder()
        .min_chunk_size(16)
        .max_chunk_size(64)
        .chunking_factor(32)
        .build();
    let mut base_model = BTreeMap::new();
    let mut target_model = BTreeMap::new();
    let (base, target) = {
        let store = Arc::new(open_store(path));
        let prolly = Prolly::new(store, config.clone());
        let mutations = (0..1_024)
            .map(|id| {
                let key = format!("diff/{id:05}").into_bytes();
                let value = format!("base/{id:05}").into_bytes();
                base_model.insert(key.clone(), value.clone());
                Mutation::Upsert { key, val: value }
            })
            .collect();
        let base = prolly.batch(&prolly.create(), mutations).unwrap();
        target_model.clone_from(&base_model);
        let mut mutations = Vec::new();
        for id in (0..1_024).step_by(11) {
            let key = format!("diff/{id:05}").into_bytes();
            let value = format!("changed/{id:05}").into_bytes();
            target_model.insert(key.clone(), value.clone());
            mutations.push(Mutation::Upsert { key, val: value });
        }
        for id in (3..1_024).step_by(17) {
            let key = format!("diff/{id:05}").into_bytes();
            target_model.remove(&key);
            mutations.push(Mutation::Delete { key });
        }
        for id in 1_024..1_124 {
            let key = format!("diff/{id:05}").into_bytes();
            let value = format!("added/{id:05}").into_bytes();
            target_model.insert(key.clone(), value.clone());
            mutations.push(Mutation::Upsert { key, val: value });
        }
        let target = prolly.batch(&base, mutations).unwrap();
        prolly.publish_named_root(b"diff/base", &base).unwrap();
        prolly.publish_named_root(b"diff/target", &target).unwrap();
        (base, target)
    };

    let store = Arc::new(open_store(path));
    let prolly = Prolly::new(store.clone(), config.clone());
    let reopened_base = prolly.load_named_root(b"diff/base").unwrap().unwrap();
    let reopened_target = prolly.load_named_root(b"diff/target").unwrap().unwrap();
    assert_eq!(reopened_base, base);
    assert_eq!(reopened_target, target);
    let expected = model_diff(&base_model, &target_model);
    let eager = prolly.diff(&base, &target).unwrap();
    assert_eq!(eager, expected);
    let streamed = prolly
        .stream_diff(&base, &target)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(streamed, expected);
    let ranged = prolly
        .range_diff(&base, &target, b"diff/00200", Some(b"diff/00800"))
        .unwrap();
    let expected_range = expected
        .iter()
        .filter(|diff| diff.key() >= b"diff/00200" && diff.key() < b"diff/00800")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(ranged, expected_range);

    let mut cursor = RangeCursor::start();
    let mut paged = Vec::new();
    loop {
        let page = prolly.diff_page(&base, &target, &cursor, None, 19).unwrap();
        paged.extend(page.diffs);
        match page.next_cursor {
            Some(next) => cursor = next,
            None => break,
        }
    }
    assert_eq!(paged, expected);

    let left_mutations = vec![
        Mutation::Upsert {
            key: b"merge/left".to_vec(),
            val: b"left".to_vec(),
        },
        Mutation::Upsert {
            key: b"diff/00010".to_vec(),
            val: b"left-change".to_vec(),
        },
    ];
    let right_mutations = vec![
        Mutation::Upsert {
            key: b"merge/right".to_vec(),
            val: b"right".to_vec(),
        },
        Mutation::Upsert {
            key: b"diff/00900".to_vec(),
            val: b"right-change".to_vec(),
        },
    ];
    let left = prolly.batch(&base, left_mutations.clone()).unwrap();
    let right = prolly.batch(&base, right_mutations.clone()).unwrap();
    let merged = prolly.merge(&base, &left, &right, None).unwrap();
    let mut direct_mutations = left_mutations;
    direct_mutations.extend(right_mutations);
    let direct = prolly.batch(&base, direct_mutations).unwrap();
    assert_eq!(merged.root, direct.root);

    let conflict_base = prolly
        .put(&base, b"conflict/key".to_vec(), b"base".to_vec())
        .unwrap();
    let conflict_left = prolly
        .put(&conflict_base, b"conflict/key".to_vec(), b"left".to_vec())
        .unwrap();
    let conflict_right = prolly
        .put(&conflict_base, b"conflict/key".to_vec(), b"right".to_vec())
        .unwrap();
    assert!(matches!(
        prolly.merge(&conflict_base, &conflict_left, &conflict_right, None),
        Err(Error::Conflict(_))
    ));
    let resolved = prolly
        .merge(
            &conflict_base,
            &conflict_left,
            &conflict_right,
            Some(Box::new(|conflict| {
                let mut value = conflict.left.clone().unwrap();
                value.extend_from_slice(b"+");
                value.extend_from_slice(conflict.right.as_ref().unwrap());
                Resolution::value(value)
            })),
        )
        .unwrap();
    assert_eq!(
        prolly.get(&resolved, b"conflict/key").unwrap(),
        Some(b"left+right".to_vec())
    );
    let delete_branch = prolly.delete(&conflict_base, b"conflict/key").unwrap();
    let delete_wins = prolly
        .merge(
            &conflict_base,
            &delete_branch,
            &conflict_right,
            Some(Box::new(|_| Resolution::delete())),
        )
        .unwrap();
    assert_eq!(prolly.get(&delete_wins, b"conflict/key").unwrap(), None);
    assert_tree_invariants(store.as_ref(), &merged, &config);
    assert_tree_invariants(store.as_ref(), &resolved, &config);
    assert_tree_invariants(store.as_ref(), &delete_wins, &config);
    println!(
        "DIFF_MERGE,diffs={},range_diffs={},pages={},canonical_merge=true,conflicts=true",
        expected.len(),
        expected_range.len(),
        expected.len().div_ceil(19)
    );
}

fn version_history_and_merge(path: &Path) {
    {
        let store = Arc::new(open_store(path));
        let prolly = Prolly::new(store, Config::default());
        let map = prolly.versioned_map(b"history");
        let empty = map.apply_at_millis(Vec::new(), 1_000).unwrap();
        let first = map
            .apply_at_millis(
                vec![Mutation::Upsert {
                    key: b"doc/1".to_vec(),
                    val: b"draft".to_vec(),
                }],
                2_000,
            )
            .unwrap();
        let second = map
            .apply_at_millis(
                vec![
                    Mutation::Upsert {
                        key: b"doc/1".to_vec(),
                        val: b"published".to_vec(),
                    },
                    Mutation::Upsert {
                        key: b"doc/2".to_vec(),
                        val: b"new".to_vec(),
                    },
                ],
                3_000,
            )
            .unwrap();
        assert_eq!(
            map.get_at(&first.id, b"doc/1").unwrap(),
            Some(b"draft".to_vec())
        );
        assert_eq!(
            map.diff(&first.id, &second.id).unwrap(),
            vec![
                Diff::Changed {
                    key: b"doc/1".to_vec(),
                    old: b"draft".to_vec(),
                    new: b"published".to_vec(),
                },
                Diff::Added {
                    key: b"doc/2".to_vec(),
                    val: b"new".to_vec(),
                },
            ]
        );
        let snapshot = map.snapshot_at(&first.id).unwrap().unwrap();
        assert_eq!(snapshot.get(b"doc/1").unwrap(), Some(b"draft".to_vec()));
        let stale = map
            .apply_if(
                Some(&first.id),
                vec![Mutation::Upsert {
                    key: b"doc/1".to_vec(),
                    val: b"stale".to_vec(),
                }],
            )
            .unwrap();
        assert!(stale.is_conflict());
        assert_eq!(map.versions().unwrap().len(), 3);
        map.rollback_to(&first.id).unwrap();
        assert_eq!(map.get(b"doc/1").unwrap(), Some(b"draft".to_vec()));
        assert_eq!(map.get(b"doc/2").unwrap(), None);
        assert!(map.version(&empty.id).unwrap().is_some());

        let merge_map = prolly.versioned_map(b"version-merge");
        let base = merge_map.put(b"choice", b"base").unwrap();
        let head = merge_map.put(b"choice", b"head").unwrap();
        merge_map.rollback_to(&base.id).unwrap();
        let candidate = merge_map.put(b"choice", b"candidate").unwrap();
        merge_map.rollback_to(&head.id).unwrap();
        let prepared = merge_map.prepare_merge(&base.id, &candidate.id).unwrap();
        assert_eq!(
            prepared
                .stream_conflicts()
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .len(),
            1
        );
        assert!(prepared
            .publish(Some(Box::new(|conflict| {
                Resolution::value(conflict.right.clone().unwrap())
            })))
            .unwrap()
            .is_applied());
        assert_eq!(
            merge_map.get(b"choice").unwrap(),
            Some(b"candidate".to_vec())
        );

        let stale_merge = merge_map.prepare_merge(&base.id, &candidate.id).unwrap();
        merge_map.put(b"concurrent", b"advance").unwrap();
        assert!(stale_merge
            .publish(Some(Box::new(|conflict| {
                Resolution::value(conflict.right.clone().unwrap())
            })))
            .unwrap()
            .is_conflict());

        let pruning = prolly.versioned_map(b"pruning");
        let first = pruning.put(b"value", b"one").unwrap();
        let middle = pruning.put(b"value", b"two").unwrap();
        let newest = pruning.put(b"value", b"three").unwrap();
        pruning.rollback_to(&first.id).unwrap();
        let pruned = pruning.prune_versions(1).unwrap();
        assert_eq!(pruned.removed, vec![middle.id]);
        assert!(pruned.retained.contains(&first.id));
        assert!(pruned.retained.contains(&newest.id));
        let other = prolly.versioned_map(b"other-map");
        other.put(b"survivor", vec![7; 4_096]).unwrap();
        assert!(pruning.plan_gc().unwrap().reclaimable_nodes > 0);
        pruning.sweep_gc().unwrap();
        assert_eq!(pruning.get(b"value").unwrap(), Some(b"one".to_vec()));
        assert_eq!(
            pruning.get_at(&newest.id, b"value").unwrap(),
            Some(b"three".to_vec())
        );
        assert_eq!(other.get(b"survivor").unwrap(), Some(vec![7; 4_096]));
        assert_eq!(pruning.verify_catalog().unwrap().version_count, 2);
    }

    let store = Arc::new(open_store(path));
    let prolly = Prolly::new(store, Config::default());
    let history = prolly.versioned_map(b"history");
    assert_eq!(history.get(b"doc/1").unwrap(), Some(b"draft".to_vec()));
    assert_eq!(history.versions().unwrap().len(), 3);
    let merge_map = prolly.versioned_map(b"version-merge");
    assert_eq!(
        merge_map.get(b"choice").unwrap(),
        Some(b"candidate".to_vec())
    );
    assert_eq!(
        merge_map.get(b"concurrent").unwrap(),
        Some(b"advance".to_vec())
    );
    let pruning = prolly.versioned_map(b"pruning");
    assert_eq!(pruning.verify_catalog().unwrap().version_count, 2);
    println!("VERSION_HISTORY,history=3,stale_cas=true,merge_cas=true,pruned_catalog=2");
}

fn model_diff(base: &BTreeMap<Vec<u8>, Vec<u8>>, target: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<Diff> {
    let mut keys = base
        .keys()
        .chain(target.keys())
        .cloned()
        .collect::<Vec<_>>();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter_map(|key| match (base.get(&key), target.get(&key)) {
            (None, Some(value)) => Some(Diff::Added {
                key,
                val: value.clone(),
            }),
            (Some(value), None) => Some(Diff::Removed {
                key,
                val: value.clone(),
            }),
            (Some(old), Some(new)) if old != new => Some(Diff::Changed {
                key,
                old: old.clone(),
                new: new.clone(),
            }),
            _ => None,
        })
        .collect()
}

fn legacy_engine_migration(path: &Path) {
    const NODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_nodes");

    let config = Config::builder()
        .min_chunk_size(32)
        .max_chunk_size(128)
        .chunking_factor(64)
        .build();
    let mut model = BTreeMap::new();
    {
        let store = Arc::new(open_store(path));
        let prolly = Prolly::new(store, config.clone());
        let mutations = (0..2_000)
            .map(|id| {
                let key = format!("legacy/{id:05}").into_bytes();
                let mut value = format!("legacy-value-{id:05};").into_bytes();
                value.resize(128, (id % 251) as u8);
                model.insert(key.clone(), value.clone());
                Mutation::Upsert { key, val: value }
            })
            .collect();
        let tree = prolly.batch(&prolly.create(), mutations).unwrap();
        prolly.publish_named_root(ROOT, &tree).unwrap();
    }

    let database = Database::open(path).unwrap();
    let transaction = database.begin_read().unwrap();
    let nodes = transaction.open_table(NODES).unwrap();
    let mut decoded_nodes = Vec::new();
    for entry in nodes.iter().unwrap() {
        let (key, value) = entry.unwrap();
        let envelope = value.value().strip_prefix(b"PRN1").unwrap();
        let (encoding, stored) = envelope.split_first().unwrap();
        let decoded = match *encoding {
            0 => stored.to_vec(),
            1 => lz4_flex::decompress_size_prepended(stored).unwrap(),
            other => panic!("unexpected encoding {other}"),
        };
        decoded_nodes.push((key.value().to_vec(), decoded));
    }
    drop(nodes);
    drop(transaction);
    let transaction = database.begin_write().unwrap();
    {
        let mut nodes = transaction.open_table(NODES).unwrap();
        for (key, value) in &decoded_nodes {
            nodes.insert(key.as_slice(), value.as_slice()).unwrap();
        }
    }
    transaction.commit().unwrap();
    drop(database);

    let store = Arc::new(open_store(path));
    let prolly = Prolly::new(store.clone(), config.clone());
    let legacy_tree = prolly.load_named_root(ROOT).unwrap().unwrap();
    assert_eq!(
        collect_entries(&prolly, &legacy_tree),
        model_entries(&model)
    );
    assert_tree_invariants(store.as_ref(), &legacy_tree, &config);

    let mut mutations = Vec::new();
    for id in (0..2_000).step_by(7) {
        let key = format!("legacy/{id:05}").into_bytes();
        model.remove(&key);
        mutations.push(Mutation::Delete { key });
    }
    for id in 2_000..2_200 {
        let key = format!("current/{id:05}").into_bytes();
        let value = format!("current-value-{id:05}").into_bytes();
        model.insert(key.clone(), value.clone());
        mutations.push(Mutation::Upsert { key, val: value });
    }
    let mixed_tree = prolly.batch(&legacy_tree, mutations).unwrap();
    prolly.publish_named_root(ROOT, &mixed_tree).unwrap();
    assert_eq!(collect_entries(&prolly, &mixed_tree), model_entries(&model));
    assert_tree_invariants(store.as_ref(), &mixed_tree, &config);
    let sweep = prolly
        .sweep_store_gc(std::slice::from_ref(&mixed_tree))
        .unwrap();
    assert!(sweep.deleted_nodes > 0);
    drop(prolly);
    let mut store = Arc::try_unwrap(store).unwrap();
    store.compact().unwrap();
    drop(store);

    let store = Arc::new(open_store(path));
    let prolly = Prolly::new(store.clone(), config);
    let reopened = prolly.load_named_root(ROOT).unwrap().unwrap();
    assert_eq!(reopened, mixed_tree);
    assert_eq!(collect_entries(&prolly, &reopened), model_entries(&model));
    assert_tree_invariants(store.as_ref(), &reopened, &reopened.config);
    println!(
        "LEGACY_ENGINE,migrated_nodes={},entries={},gc_deleted={}",
        decoded_nodes.len(),
        model.len(),
        sweep.deleted_nodes
    );
}

fn randomized_reopen_differential(path: &Path, seed: u64) {
    let config = Config::builder()
        .min_chunk_size(32)
        .max_chunk_size(128)
        .chunking_factor(64)
        .build();
    let memory = Prolly::new(MemStore::new(), config.clone());
    let mut memory_tree = memory.create();
    let mut redb_tree = memory.create();
    let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    let mut random = seed;

    for epoch in 0..EPOCHS {
        let mut touched = BTreeMap::<Vec<u8>, Mutation>::new();
        for ordinal in 0..MUTATIONS_PER_EPOCH {
            let id = (next_random(&mut random) as usize) % KEY_SPACE;
            let key = format!("key/{id:05}").into_bytes();
            let mutation = if next_random(&mut random).is_multiple_of(5) {
                Mutation::Delete { key: key.clone() }
            } else {
                let salt = next_random(&mut random);
                let mut value =
                    format!("epoch={epoch};ordinal={ordinal};salt={salt:016x};").into_bytes();
                value.resize(96 + (salt as usize % 64), (id % 251) as u8);
                Mutation::Upsert {
                    key: key.clone(),
                    val: value,
                }
            };
            touched.insert(key, mutation);
        }
        let mutations = touched.into_values().collect::<Vec<_>>();
        for mutation in &mutations {
            match mutation {
                Mutation::Upsert { key, val } => {
                    model.insert(key.clone(), val.clone());
                }
                Mutation::Delete { key } => {
                    model.remove(key);
                }
            }
        }

        memory_tree = memory.batch(&memory_tree, mutations.clone()).unwrap();
        let store = Arc::new(open_store(path));
        let redb = Prolly::new(store.clone(), config.clone());
        let loaded = redb.load_named_root(ROOT).unwrap();
        if epoch == 0 {
            assert!(loaded.is_none());
        } else {
            assert_eq!(loaded.as_ref(), Some(&redb_tree));
            redb_tree = loaded.unwrap();
        }
        redb_tree = redb.batch(&redb_tree, mutations).unwrap();
        redb.publish_named_root(ROOT, &redb_tree).unwrap();

        assert_eq!(redb_tree.root, memory_tree.root, "epoch {epoch} root CID");
        assert_eq!(collect_entries(&redb, &redb_tree), model_entries(&model));
        assert_tree_invariants(store.as_ref(), &redb_tree, &config);
        for probe in 0..64 {
            let id = (next_random(&mut random) as usize) % KEY_SPACE;
            let key = format!("key/{id:05}").into_bytes();
            assert_eq!(
                redb.get(&redb_tree, &key).unwrap(),
                model.get(&key).cloned()
            );
            assert_eq!(
                redb.get(&redb_tree, &key).unwrap(),
                memory.get(&memory_tree, &key).unwrap()
            );
            assert!(probe < 64);
        }
    }

    let store = Arc::new(open_store(path));
    let redb = Prolly::new(store.clone(), config.clone());
    let base = redb.load_named_root(ROOT).unwrap().unwrap();
    assert_eq!(base, redb_tree);

    let left = redb
        .batch(
            &base,
            vec![
                Mutation::Upsert {
                    key: b"branch/left".to_vec(),
                    val: b"left-value".to_vec(),
                },
                Mutation::Delete {
                    key: b"key/00001".to_vec(),
                },
            ],
        )
        .unwrap();
    let right = redb
        .batch(
            &base,
            vec![Mutation::Upsert {
                key: b"branch/right".to_vec(),
                val: b"right-value".to_vec(),
            }],
        )
        .unwrap();
    let diffs = redb.diff(&base, &left).unwrap();
    assert!(diffs.iter().any(|diff| {
        matches!(diff, Diff::Added { key, val } if key == b"branch/left" && val == b"left-value")
    }));
    let merged = redb.merge(&base, &left, &right, None).unwrap();
    assert_eq!(
        redb.get(&merged, b"branch/left").unwrap(),
        Some(b"left-value".to_vec())
    );
    assert_eq!(
        redb.get(&merged, b"branch/right").unwrap(),
        Some(b"right-value".to_vec())
    );
    assert_tree_invariants(store.as_ref(), &merged, &config);
    redb.publish_named_root(ROOT, &merged).unwrap();

    let plan = redb.plan_store_gc(std::slice::from_ref(&merged)).unwrap();
    assert!(plan.reclaimable_nodes > 0);
    let sweep = redb.sweep_store_gc(std::slice::from_ref(&merged)).unwrap();
    assert_eq!(sweep.deleted_nodes, plan.reclaimable_nodes);
    assert_tree_invariants(store.as_ref(), &merged, &config);
    drop(redb);

    let mut store = Arc::try_unwrap(store).expect("exclusive store after manager drop");
    let before = std::fs::metadata(path).unwrap().len();
    let compacted = store.compact().unwrap();
    drop(store);
    let after = std::fs::metadata(path).unwrap().len();
    assert!(after <= before);
    println!("COMPACTION,seed={seed:016x},compacted={compacted},before={before},after={after}");

    let store = Arc::new(open_store(path));
    let redb = Prolly::new(store.clone(), config);
    let reopened = redb.load_named_root(ROOT).unwrap().unwrap();
    assert_eq!(reopened, merged);
    assert_tree_invariants(store.as_ref(), &reopened, &reopened.config);
}

fn inspect_storage(path: &Path) {
    const NODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_nodes");

    let database = Database::open(path).unwrap();
    let transaction = database.begin_read().unwrap();
    let nodes = transaction.open_table(NODES).unwrap();
    let mut raw_nodes = 0_u64;
    let mut compressed_nodes = 0_u64;
    let mut stored_bytes = 0_u64;
    let mut decoded_bytes = 0_u64;
    for entry in nodes.iter().unwrap() {
        let (key, value) = entry.unwrap();
        let envelope = value.value().strip_prefix(b"PRN1").unwrap();
        let (encoding, stored) = envelope.split_first().unwrap();
        let decoded = match *encoding {
            0 => {
                raw_nodes += 1;
                stored.to_vec()
            }
            1 => {
                compressed_nodes += 1;
                lz4_flex::decompress_size_prepended(stored).unwrap()
            }
            other => panic!("unexpected encoding {other}"),
        };
        stored_bytes += stored.len() as u64;
        decoded_bytes += decoded.len() as u64;
        assert_eq!(Cid::from_bytes(&decoded).as_bytes(), key.value());
        Node::from_bytes(&decoded).unwrap().validate().unwrap();
    }
    assert!(raw_nodes > 0);
    assert!(compressed_nodes > 0);
    println!(
        "STORAGE,raw_nodes={raw_nodes},compressed_nodes={compressed_nodes},stored_bytes={stored_bytes},decoded_bytes={decoded_bytes}"
    );
}

fn locking_characteristic(path: &Path) {
    remove_db(path);
    let first = open_store(path);
    assert!(RedbStore::open(path).is_err());
    drop(first);
    let reopened = open_store(path);
    drop(reopened);
    remove_db(path);
}

fn versioned_transaction_and_concurrency(path: &Path) {
    {
        let store = Arc::new(open_store(path));
        assert!(store.supports_transactions());
        let prolly = Arc::new(Prolly::new(store, Config::default()));
        prolly
            .versioned_maps_transaction(|maps| {
                maps.put(b"users", b"user/1", b"Ada")?;
                maps.put(b"users", b"user/1/status", b"active")?;
                maps.put(b"by_email", b"ada@example.com", b"user/1")?;
                Ok(())
            })
            .unwrap();

        let failed = prolly.versioned_maps_transaction(|maps| {
            maps.put(b"users", b"user/2", b"Grace")?;
            maps.put(b"by_email", b"grace@example.com", b"user/2")?;
            Err::<(), _>(prolly::Error::InvalidVersionedMap(
                "intentional rollback".to_string(),
            ))
        });
        assert!(failed.is_err());
        assert_eq!(prolly.versioned_map(b"users").get(b"user/2").unwrap(), None);
        assert_eq!(
            prolly
                .versioned_map(b"by_email")
                .get(b"grace@example.com")
                .unwrap(),
            None
        );

        let concurrent = prolly.versioned_map(b"concurrent");
        concurrent.initialize().unwrap();
        let barrier = Arc::new(Barrier::new(9));
        let handles = (0..8)
            .map(|id| {
                let prolly = prolly.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    prolly
                        .versioned_map(b"concurrent")
                        .put(
                            format!("worker/{id}").as_bytes(),
                            format!("value/{id}").as_bytes(),
                        )
                        .unwrap();
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }
    }

    let store = Arc::new(open_store(path));
    let prolly = Prolly::new(store, Config::default());
    assert_eq!(
        prolly.versioned_map(b"users").get(b"user/1").unwrap(),
        Some(b"Ada".to_vec())
    );
    assert_eq!(
        prolly
            .versioned_map(b"by_email")
            .get(b"ada@example.com")
            .unwrap(),
        Some(b"user/1".to_vec())
    );
    for id in 0..8 {
        assert_eq!(
            prolly
                .versioned_map(b"concurrent")
                .get(format!("worker/{id}").as_bytes())
                .unwrap(),
            Some(format!("value/{id}").into_bytes())
        );
    }
    assert_eq!(
        prolly
            .versioned_map(b"concurrent")
            .versions()
            .unwrap()
            .len(),
        9
    );
}

fn open_store(path: &Path) -> RedbStore {
    RedbStore::open_with_options(
        path,
        RedbStoreOptions {
            database: RedbStoreConfig {
                cache_size_bytes: 64 * 1024 * 1024,
                durability: Durability::Immediate,
            },
            node_read_cache_size_bytes: 32 * 1024 * 1024,
            compress_nodes: true,
        },
    )
    .unwrap()
}

fn collect_entries<S: Store>(prolly: &Prolly<S>, tree: &Tree) -> Vec<(Vec<u8>, Vec<u8>)> {
    prolly
        .range(tree, &[], None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn model_entries(model: &BTreeMap<Vec<u8>, Vec<u8>>) -> Vec<(Vec<u8>, Vec<u8>)> {
    model
        .iter()
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

fn assert_tree_invariants<S: Store>(store: &S, tree: &Tree, config: &Config)
where
    S::Error: std::fmt::Debug,
{
    if let Some(root) = &tree.root {
        let (_, first_key) = assert_node_invariants(store, root, None, config);
        assert!(first_key.is_some());
    }
}

fn assert_node_invariants<S: Store>(
    store: &S,
    cid: &Cid,
    expected_level: Option<u8>,
    config: &Config,
) -> (usize, Option<Vec<u8>>)
where
    S::Error: std::fmt::Debug,
{
    let bytes = store.get(cid.as_bytes()).unwrap().unwrap();
    assert_eq!(Cid::from_bytes(&bytes), cid.clone());
    let node = Node::from_bytes(&bytes).unwrap();
    node.validate().unwrap();
    assert_eq!(node.keys.len(), node.vals.len());
    assert!(node.keys.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(node.len() <= config.max_chunk_size());
    if let Some(level) = expected_level {
        assert_eq!(node.level, level);
    }
    if node.leaf {
        return (1, node.keys.first().cloned());
    }

    let mut total = 1;
    for (key, child) in node.keys.iter().zip(&node.vals) {
        let child_cid = Cid(child.as_slice().try_into().unwrap());
        let (child_count, first_key) =
            assert_node_invariants(store, &child_cid, Some(node.level - 1), config);
        assert_eq!(Some(key), first_key.as_ref());
        total += child_count;
    }
    (total, node.keys.first().cloned())
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

fn temp_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "prolly-redb-correctness-{label}-{}-{nonce}.redb",
        std::process::id()
    ))
}

fn remove_db(path: &Path) {
    let _ = std::fs::remove_file(path);
}
