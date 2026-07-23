use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{
    Cid, Config, ManifestStore, ManifestUpdate, NodePublication, NodePublicationHint,
    NodeStoreScan, Prolly, PublicationOrigin, RootCondition, RootManifest, RootWrite, Store,
    TransactionNodeWrite, TransactionUpdate, TransactionalStore,
};
use prolly_store_redb::{Durability, RedbStore, RedbStoreConfig, RedbStoreOptions};
use redb::{Database, ReadableDatabase, TableDefinition};

#[test]
fn redb_store_satisfies_store_contract() {
    with_store("store-contract", |store| {
        prolly_store_test::assert_store_contract(store);
    });
}

#[test]
fn redb_store_satisfies_manifest_store_contract() {
    with_store("manifest-contract", |store| {
        prolly_store_test::assert_manifest_store_contract(store);
    });
}

#[test]
fn redb_store_satisfies_node_store_scan_contract() {
    let path = temp_db_path("scan-contract");
    remove_db(&path);
    prolly_store_test::assert_node_store_scan_contract(RedbStore::open(&path).unwrap());
    remove_db(&path);
}

#[test]
fn redb_store_supports_strict_indexed_maps() {
    let path = temp_db_path("indexed-map");
    remove_db(&path);
    prolly_store_test::assert_indexed_map_contract(RedbStore::open(&path).unwrap());
    remove_db(&path);
}

#[test]
fn redb_store_persists_named_root_across_reopen() {
    let path = temp_db_path("root-reopen");
    remove_db(&path);
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();

    let tree = {
        let prolly = Prolly::new(RedbStore::open(&path).unwrap(), config.clone());
        let tree = prolly
            .put(
                &prolly.create(),
                b"project/name".to_vec(),
                b"CrabDB".to_vec(),
            )
            .unwrap();
        prolly.publish_named_root(b"main", &tree).unwrap();
        tree
    };

    {
        let prolly = Prolly::new(RedbStore::open(&path).unwrap(), config);
        let loaded = prolly.load_named_root(b"main").unwrap().unwrap();
        assert_eq!(loaded, tree);
        assert_eq!(
            prolly.get(&loaded, b"project/name").unwrap(),
            Some(b"CrabDB".to_vec())
        );
    }
    remove_db(&path);
}

#[test]
fn redb_store_persists_distinct_composite_hints() {
    let path = temp_db_path("hints");
    remove_db(&path);
    {
        let store = RedbStore::open(&path).unwrap();
        assert!(store.supports_hints());
        assert!(!store.prefers_rightmost_path_hints());
        store.put_hint(b"a", b"bc", b"first").unwrap();
        store.put_hint(b"ab", b"c", b"second").unwrap();
    }
    {
        let store = RedbStore::open(&path).unwrap();
        assert_eq!(
            store.get_hint(b"a", b"bc").unwrap(),
            Some(b"first".to_vec())
        );
        assert_eq!(
            store.get_hint(b"ab", b"c").unwrap(),
            Some(b"second".to_vec())
        );
    }
    remove_db(&path);
}

#[test]
fn redb_store_publishes_nodes_and_hint_together() {
    with_store("publication", |store| {
        let node = b"published-node";
        let cid = Cid::from_bytes(node);
        let entries = [(cid.as_bytes(), node.as_slice())];
        store
            .publish_nodes(NodePublication::with_hint(
                &entries,
                NodePublicationHint::new(b"tree", b"rightmost", b"path"),
                PublicationOrigin::PointUpsert,
            ))
            .unwrap();
        assert_eq!(store.get(cid.as_bytes()).unwrap(), Some(node.to_vec()));
        assert_eq!(
            store.get_hint(b"tree", b"rightmost").unwrap(),
            Some(b"path".to_vec())
        );
    });
}

#[test]
fn redb_store_accepts_custom_cache_and_durability() {
    let path = temp_db_path("configured");
    remove_db(&path);
    let store = RedbStore::open_with_config(
        &path,
        RedbStoreConfig {
            cache_size_bytes: 8 * 1024 * 1024,
            durability: Durability::None,
        },
    )
    .unwrap();
    store.put(b"configured", b"value").unwrap();
    assert_eq!(store.get(b"configured").unwrap(), Some(b"value".to_vec()));
    drop(store);
    remove_db(&path);
}

#[test]
fn redb_store_retains_native_shared_reads() {
    let path = temp_db_path("shared-reads");
    remove_db(&path);
    let store = RedbStore::open_with_options(
        &path,
        RedbStoreOptions {
            database: RedbStoreConfig {
                cache_size_bytes: 8 * 1024 * 1024,
                durability: Durability::None,
            },
            node_read_cache_size_bytes: 1024 * 1024,
            compress_nodes: true,
        },
    )
    .unwrap();
    store.put(b"shared", b"immutable-value").unwrap();

    assert!(store.has_native_shared_reads());
    let first = store.get_shared(b"shared").unwrap().unwrap();
    let second = store.get_shared(b"shared").unwrap().unwrap();
    assert!(Arc::ptr_eq(&first, &second));
    store.put(b"shared", b"replacement").unwrap();
    let replacement = store.get_shared(b"shared").unwrap().unwrap();
    assert_eq!(replacement.as_ref(), b"replacement");
    assert!(!Arc::ptr_eq(&first, &replacement));
    drop(store);
    remove_db(&path);
}

#[test]
fn redb_store_compresses_large_nodes_transparently() {
    const NODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_nodes");

    let path = temp_db_path("compressed-nodes");
    remove_db(&path);
    let key = Cid::from_bytes(b"compressible-node");
    let value = vec![0x5a; 64 * 1024];
    {
        let store = RedbStore::open(&path).unwrap();
        store.put(key.as_bytes(), &value).unwrap();
        assert_eq!(store.get(key.as_bytes()).unwrap(), Some(value.clone()));
    }

    let database = Database::open(&path).unwrap();
    let transaction = database.begin_read().unwrap();
    let table = transaction.open_table(NODES).unwrap();
    let stored = table.get(key.as_bytes()).unwrap().unwrap();
    let envelope = stored.value().strip_prefix(b"PRN1").unwrap();
    let (encoding, bytes) = envelope.split_first().unwrap();
    assert_eq!(*encoding, 1);
    assert!(bytes.len() < value.len() / 4);
    drop(stored);
    drop(table);
    drop(transaction);
    drop(database);
    remove_db(&path);
}

#[test]
fn redb_store_can_disable_cache_and_compression() {
    const NODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_nodes");

    let path = temp_db_path("uncompressed-nodes");
    remove_db(&path);
    let key = Cid::from_bytes(b"uncompressed-node");
    let value = vec![0x5a; 16 * 1024];
    {
        let store = RedbStore::open_with_options(
            &path,
            RedbStoreOptions {
                database: RedbStoreConfig::default(),
                node_read_cache_size_bytes: 0,
                compress_nodes: false,
            },
        )
        .unwrap();
        assert!(!store.has_native_shared_reads());
        store.put(key.as_bytes(), &value).unwrap();
        assert_eq!(store.get(key.as_bytes()).unwrap(), Some(value.clone()));
    }

    let database = Database::open(&path).unwrap();
    let transaction = database.begin_read().unwrap();
    let table = transaction.open_table(NODES).unwrap();
    let stored = table.get(key.as_bytes()).unwrap().unwrap();
    let envelope = stored.value().strip_prefix(b"PRN1").unwrap();
    let (encoding, bytes) = envelope.split_first().unwrap();
    assert_eq!(*encoding, 0);
    assert_eq!(bytes, value);
    drop(stored);
    drop(table);
    drop(transaction);
    drop(database);
    remove_db(&path);
}

#[test]
fn redb_store_refreshes_retained_read_snapshot_after_writes() {
    let path = temp_db_path("retained-read-refresh");
    remove_db(&path);
    let store = RedbStore::open_with_options(
        &path,
        RedbStoreOptions {
            database: RedbStoreConfig::default(),
            node_read_cache_size_bytes: 0,
            compress_nodes: false,
        },
    )
    .unwrap();

    store.put(b"key", b"before").unwrap();
    assert_eq!(
        store.get(b"key").unwrap().as_deref(),
        Some(b"before".as_slice())
    );
    store.put(b"key", b"after").unwrap();
    assert_eq!(
        store.get(b"key").unwrap().as_deref(),
        Some(b"after".as_slice())
    );
    store.delete(b"key").unwrap();
    assert_eq!(store.get(b"key").unwrap(), None);

    drop(store);
    remove_db(&path);
}

#[test]
fn redb_store_reads_legacy_raw_node_table() {
    const NODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_nodes");

    let path = temp_db_path("legacy-nodes");
    remove_db(&path);
    let key = Cid::from_bytes(b"legacy-node");
    let current_key = Cid::from_bytes(b"current-node");
    {
        let database = Database::create(&path).unwrap();
        let transaction = database.begin_write().unwrap();
        {
            let mut table = transaction.open_table(NODES).unwrap();
            table
                .insert(key.as_bytes(), b"legacy-value".as_slice())
                .unwrap();
        }
        transaction.commit().unwrap();
    }

    let store = RedbStore::open(&path).unwrap();
    assert_eq!(
        store.get(key.as_bytes()).unwrap(),
        Some(b"legacy-value".to_vec())
    );
    store.put(current_key.as_bytes(), b"current-value").unwrap();
    let mut expected = vec![key.clone(), current_key];
    expected.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
    assert_eq!(store.list_node_cids().unwrap(), expected);

    store.put(key.as_bytes(), b"migrated-value").unwrap();
    assert_eq!(
        store.get(key.as_bytes()).unwrap(),
        Some(b"migrated-value".to_vec())
    );
    assert_eq!(store.list_node_cids().unwrap(), expected);
    drop(store);

    let database = Database::open(&path).unwrap();
    let transaction = database.begin_read().unwrap();
    let table = transaction.open_table(NODES).unwrap();
    let migrated = table.get(key.as_bytes()).unwrap().unwrap();
    assert!(migrated.value().starts_with(b"PRN1"));
    drop(migrated);
    drop(table);
    drop(transaction);
    drop(database);
    remove_db(&path);
}

#[test]
fn redb_store_reads_and_migrates_v2_encoded_nodes() {
    const NODES_V2: TableDefinition<&[u8], (u8, &[u8])> = TableDefinition::new("prolly_nodes_v2");
    const NODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_nodes");

    let path = temp_db_path("v2-nodes");
    remove_db(&path);
    let key = Cid::from_bytes(b"v2-node");
    {
        let database = Database::create(&path).unwrap();
        let transaction = database.begin_write().unwrap();
        {
            let mut table = transaction.open_table(NODES_V2).unwrap();
            table
                .insert(key.as_bytes(), (0, b"v2-value".as_slice()))
                .unwrap();
        }
        transaction.commit().unwrap();
    }

    let store = RedbStore::open(&path).unwrap();
    assert_eq!(
        store.get(key.as_bytes()).unwrap(),
        Some(b"v2-value".to_vec())
    );
    assert_eq!(store.list_node_cids().unwrap(), vec![key.clone()]);
    store.put(key.as_bytes(), b"default-value").unwrap();
    assert_eq!(
        store.get(key.as_bytes()).unwrap(),
        Some(b"default-value".to_vec())
    );
    drop(store);

    let database = Database::open(&path).unwrap();
    let transaction = database.begin_read().unwrap();
    let v2 = transaction.open_table(NODES_V2).unwrap();
    let nodes = transaction.open_table(NODES).unwrap();
    assert!(v2.get(key.as_bytes()).unwrap().is_none());
    assert!(nodes.get(key.as_bytes()).unwrap().is_some());
    drop(nodes);
    drop(v2);
    drop(transaction);
    drop(database);
    remove_db(&path);
}

#[test]
fn redb_store_compaction_preserves_data() {
    let path = temp_db_path("compaction");
    remove_db(&path);
    let key = Cid::from_bytes(b"retained-node");
    let mut store = RedbStore::open(&path).unwrap();
    store.put(key.as_bytes(), b"retained-value").unwrap();
    for index in 0_u32..256 {
        store
            .put(&index.to_be_bytes(), &vec![index as u8; 16 * 1024])
            .unwrap();
    }
    for index in 0_u32..256 {
        store.delete(&index.to_be_bytes()).unwrap();
    }

    let _compacted = store.compact().unwrap();
    assert_eq!(
        store.get(key.as_bytes()).unwrap(),
        Some(b"retained-value".to_vec())
    );
    drop(store);

    let store = RedbStore::open(&path).unwrap();
    assert_eq!(
        store.get(key.as_bytes()).unwrap(),
        Some(b"retained-value".to_vec())
    );
    drop(store);
    remove_db(&path);
}

#[test]
fn redb_store_serializes_competing_root_cas() {
    let path = temp_db_path("concurrent-cas");
    remove_db(&path);
    let store = Arc::new(RedbStore::open(&path).unwrap());
    let barrier = Arc::new(Barrier::new(3));
    let first = RootManifest::new(Some(Cid::from_bytes(b"first")), Config::default());
    let second = RootManifest::new(Some(Cid::from_bytes(b"second")), Config::default());

    let handles = [first.clone(), second.clone()].map(|manifest| {
        let store = Arc::clone(&store);
        let barrier = Arc::clone(&barrier);
        std::thread::spawn(move || {
            barrier.wait();
            store
                .compare_and_swap_root(b"main", None, Some(&manifest))
                .unwrap()
        })
    });
    barrier.wait();
    let results = handles.map(|handle| handle.join().unwrap());

    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, ManifestUpdate::Applied))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, ManifestUpdate::Conflict { .. }))
            .count(),
        1
    );
    assert!(matches!(
        store.get_root(b"main").unwrap(),
        Some(current) if current == first || current == second
    ));
    drop(store);
    remove_db(&path);
}

#[test]
fn redb_store_conflict_aborts_all_staged_writes() {
    with_store("transaction-conflict", |store| {
        let current = RootManifest::new(Some(Cid::from_bytes(b"current")), Config::default());
        let stale = RootManifest::new(Some(Cid::from_bytes(b"stale")), Config::default());
        let replacement =
            RootManifest::new(Some(Cid::from_bytes(b"replacement")), Config::default());
        store.put_root(b"main", &current).unwrap();

        let update = store
            .commit_transaction(
                &[TransactionNodeWrite::Upsert {
                    key: b"uncommitted-node".to_vec(),
                    value: b"uncommitted-value".to_vec(),
                }],
                &[RootCondition::new(b"main".to_vec(), Some(stale))],
                &[RootWrite::Put {
                    name: b"main".to_vec(),
                    manifest: replacement,
                }],
            )
            .unwrap();

        assert!(matches!(update, TransactionUpdate::Conflict(_)));
        assert_eq!(store.get(b"uncommitted-node").unwrap(), None);
        assert_eq!(store.get_root(b"main").unwrap(), Some(current));
    });
}

#[test]
fn redb_store_commits_nodes_and_roots_atomically() {
    with_store("transaction-success", |store| {
        let manifest = RootManifest::new(
            Some(Cid::from_bytes(b"transaction-root")),
            Config::default(),
        );
        let update = store
            .commit_transaction(
                &[TransactionNodeWrite::Upsert {
                    key: b"transaction-node".to_vec(),
                    value: b"transaction-value".to_vec(),
                }],
                &[RootCondition::new(b"main".to_vec(), None)],
                &[RootWrite::Put {
                    name: b"main".to_vec(),
                    manifest: manifest.clone(),
                }],
            )
            .unwrap();

        assert_eq!(
            update,
            TransactionUpdate::Applied {
                nodes_written: 1,
                roots_written: 1,
            }
        );
        assert_eq!(
            store.get(b"transaction-node").unwrap(),
            Some(b"transaction-value".to_vec())
        );
        assert_eq!(store.get_root(b"main").unwrap(), Some(manifest));
    });
}

#[test]
fn redb_store_rejects_malformed_node_keys_during_scan() {
    with_store("malformed-node-key", |store| {
        store.put(b"not-a-cid", b"value").unwrap();
        let error = store.list_node_cids().unwrap_err();
        assert!(error.to_string().contains("invalid CID length"));
    });
}

fn with_store(label: &str, test: impl FnOnce(&RedbStore)) {
    let path = temp_db_path(label);
    remove_db(&path);
    {
        let store = RedbStore::open(&path).unwrap();
        test(&store);
    }
    remove_db(&path);
}

fn temp_db_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "prolly-redb-{label}-{}-{nanos}.redb",
        std::process::id()
    ))
}

fn remove_db(path: &Path) {
    let _ = std::fs::remove_file(path);
}
