mod common;

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::{
    Cid, Config, FileNodeStore, FileNodeStoreError, ManifestStoreScan, NamedRootUpdate,
    NodeStoreScan, Prolly, Store,
};

fn temp_store_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    let path = std::env::temp_dir().join(format!(
        "prolly-file-node-store-{name}-{}-{nanos}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    path
}

fn config() -> Config {
    Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .hash_seed(177)
        .build()
}

#[test]
fn file_node_store_persists_tree_roots_and_nodes_separately() {
    let path = temp_store_dir("persist");
    let store = Arc::new(FileNodeStore::open(&path).unwrap());
    let prolly = Prolly::new(store.clone(), config());
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
    prolly
        .publish_named_root_at_millis(b"main", &tree, 1_000)
        .unwrap();

    assert!(store.node_namespace_dir().exists());
    assert!(store.root_namespace_dir().exists());
    assert!(!store.list_node_cids().unwrap().is_empty());
    assert_eq!(store.list_roots().unwrap().len(), 1);

    let reopened = Arc::new(FileNodeStore::open(&path).unwrap());
    let reopened_prolly = Prolly::new(reopened.clone(), tree.config.clone());
    let loaded = reopened_prolly.load_named_root(b"main").unwrap().unwrap();

    assert_eq!(loaded, tree);
    assert_eq!(
        reopened_prolly.get(&loaded, b"k031").unwrap(),
        Some(b"v031".to_vec())
    );
    common::assert_tree_invariants(&reopened, &loaded, &loaded.config);

    let _ = fs::remove_dir_all(path);
}

#[test]
fn file_node_store_rejects_and_detects_cid_mismatches() {
    let path = temp_store_dir("cid");
    let store = FileNodeStore::open(&path).unwrap();
    let expected = Cid::from_bytes(b"expected bytes");
    let wrong = b"wrong bytes";

    let err = store.put(expected.as_bytes(), wrong).unwrap_err();
    match err {
        FileNodeStoreError::CidMismatch {
            expected: err_expected,
            actual,
            ..
        } => {
            assert_eq!(err_expected, expected);
            assert_eq!(actual, Cid::from_bytes(wrong));
        }
        other => panic!("expected CidMismatch, got {other:?}"),
    }

    let bytes = b"valid node bytes";
    let cid = Cid::from_bytes(bytes);
    store.put(cid.as_bytes(), bytes).unwrap();
    fs::write(store.path_for_cid(&cid), b"corrupt").unwrap();

    let err = store.get(cid.as_bytes()).unwrap_err();
    assert!(matches!(err, FileNodeStoreError::CidMismatch { .. }));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn file_node_store_named_root_cas_and_gc_scan_work() {
    let path = temp_store_dir("manifest");
    let store = Arc::new(FileNodeStore::open(&path).unwrap());

    common::assert_manifest_store_contract(store.as_ref());
    common::assert_node_store_scan_contract(store.clone());

    let store = Arc::new(FileNodeStore::open(&path).unwrap());
    let prolly = Prolly::new(store.clone(), config());
    let empty = prolly.create();
    let first = prolly.put(&empty, b"k".to_vec(), b"1".to_vec()).unwrap();
    let second = prolly.put(&first, b"k".to_vec(), b"2".to_vec()).unwrap();

    assert!(prolly
        .compare_and_swap_named_root(b"main", None, Some(&first))
        .unwrap()
        .is_applied());
    match prolly
        .compare_and_swap_named_root(b"main", None, Some(&second))
        .unwrap()
    {
        NamedRootUpdate::Conflict { current } => assert_eq!(current, Some(first.clone())),
        other => panic!("expected stale CAS conflict, got {other:?}"),
    }
    assert_eq!(prolly.load_named_root(b"main").unwrap(), Some(first));

    let _ = fs::remove_dir_all(path);
}

#[test]
fn file_node_store_rejects_non_cid_node_keys() {
    let path = temp_store_dir("invalid-key");
    let store = FileNodeStore::open(&path).unwrap();
    let err = store.get(b"not-a-cid").unwrap_err();
    assert!(matches!(err, FileNodeStoreError::InvalidKey { .. }));
    let _ = fs::remove_dir_all(path);
}
