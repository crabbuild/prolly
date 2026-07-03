#![cfg(feature = "rocksdb")]

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{Config, Prolly, RocksDBStore};

#[test]
fn rocksdb_store_satisfies_store_contract() {
    let path = temp_db_path("contract");
    remove_rocksdb_dir(&path);

    {
        let store = RocksDBStore::open(&path).unwrap();
        common::assert_store_contract(&store);
    }

    remove_rocksdb_dir(&path);
}

#[test]
fn rocksdb_store_satisfies_manifest_store_contract() {
    let path = temp_db_path("manifest-contract");
    remove_rocksdb_dir(&path);

    {
        let store = RocksDBStore::open(&path).unwrap();
        common::assert_manifest_store_contract(&store);
    }

    remove_rocksdb_dir(&path);
}

#[test]
fn rocksdb_store_satisfies_node_store_scan_contract() {
    let path = temp_db_path("scan-contract");
    remove_rocksdb_dir(&path);

    {
        let store = RocksDBStore::open(&path).unwrap();
        common::assert_node_store_scan_contract(store);
    }

    remove_rocksdb_dir(&path);
}

#[test]
fn rocksdb_store_persists_named_root_across_reopen() {
    let path = temp_db_path("root-manifest");
    remove_rocksdb_dir(&path);

    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();

    let tree = {
        let store = RocksDBStore::open(&path).unwrap();
        let prolly = Prolly::new(store, config.clone());
        let tree = prolly.create();
        let tree = prolly
            .put(&tree, b"project/name".to_vec(), b"crabdb".to_vec())
            .unwrap();

        prolly.publish_named_root(b"main", &tree).unwrap();
        tree
    };

    {
        let store = RocksDBStore::open(&path).unwrap();
        let prolly = Prolly::new(store, config);
        let loaded = prolly.load_named_root(b"main").unwrap().unwrap();
        assert_eq!(loaded, tree);
        assert_eq!(
            prolly.get(&loaded, b"project/name").unwrap(),
            Some(b"crabdb".to_vec())
        );
    }

    remove_rocksdb_dir(&path);
}

fn temp_db_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crabdb-prolly-rocksdb-{label}-{}-{nanos}",
        std::process::id()
    ))
}

fn remove_rocksdb_dir(path: &Path) {
    let _ = fs::remove_dir_all(path);
}
