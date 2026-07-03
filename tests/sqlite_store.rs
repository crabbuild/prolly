#![cfg(feature = "sqlite")]

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{Config, Prolly, SqliteStore};

#[test]
fn sqlite_store_is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<SqliteStore>();
}

#[test]
fn sqlite_store_supports_store_contract_operations() {
    let store = SqliteStore::open_in_memory().unwrap();
    common::assert_store_contract(&store);
}

#[test]
fn sqlite_store_supports_manifest_contract_operations() {
    let store = SqliteStore::open_in_memory().unwrap();
    common::assert_manifest_store_contract(&store);
}

#[test]
fn sqlite_store_supports_node_scan_contract_operations() {
    let store = SqliteStore::open_in_memory().unwrap();
    common::assert_node_store_scan_contract(store);
}

#[test]
fn sqlite_store_persists_prolly_tree_nodes_across_reopen() {
    let path = temp_db_path("persist");
    remove_sqlite_files(&path);

    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();

    let tree = {
        let store = SqliteStore::open(&path).unwrap();
        let prolly = Prolly::new(store, config.clone());
        let mut tree = prolly.create();

        for i in 0..40 {
            tree = prolly
                .put(
                    &tree,
                    format!("k{i:02}").into_bytes(),
                    format!("v{i:02}").into_bytes(),
                )
                .unwrap();
        }

        tree
    };

    {
        let store = SqliteStore::open(&path).unwrap();
        let prolly = Prolly::new(store, config);

        assert_eq!(prolly.get(&tree, b"k00").unwrap(), Some(b"v00".to_vec()));
        assert_eq!(prolly.get(&tree, b"k17").unwrap(), Some(b"v17".to_vec()));
        assert_eq!(prolly.get(&tree, b"k39").unwrap(), Some(b"v39".to_vec()));
        assert_eq!(prolly.get(&tree, b"missing").unwrap(), None);
    }

    remove_sqlite_files(&path);
}

#[test]
fn sqlite_store_persists_named_root_across_reopen() {
    let path = temp_db_path("root-manifest");
    remove_sqlite_files(&path);

    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();

    let tree = {
        let store = Arc::new(SqliteStore::open(&path).unwrap());
        let prolly = Prolly::new(store.clone(), config.clone());
        let tree = prolly.create();
        let tree = prolly
            .put(&tree, b"project/name".to_vec(), b"crabdb".to_vec())
            .unwrap();

        prolly.publish_named_root(b"main", &tree).unwrap();
        tree
    };

    {
        let store = Arc::new(SqliteStore::open(&path).unwrap());
        let prolly = Prolly::new(store, config.clone());
        let loaded = prolly.load_named_root(b"main").unwrap().unwrap();
        assert_eq!(loaded, tree);

        assert_eq!(
            prolly.get(&loaded, b"project/name").unwrap(),
            Some(b"crabdb".to_vec())
        );
    }

    remove_sqlite_files(&path);
}

fn temp_db_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crabdb-prolly-{label}-{}-{nanos}.db",
        std::process::id()
    ))
}

fn remove_sqlite_files(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("db-wal"));
    let _ = fs::remove_file(path.with_extension("db-shm"));
}
