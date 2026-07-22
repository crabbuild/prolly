use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{
    Cid, Config, ManifestStore, ManifestUpdate, NodePublication, NodePublicationHint,
    NodeStoreScan, Prolly, PublicationOrigin, RootCondition, RootManifest, RootWrite, Store,
    TransactionNodeWrite, TransactionUpdate, TransactionalStore,
};
use prolly_store_redb::{Durability, RedbStore, RedbStoreConfig};

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
