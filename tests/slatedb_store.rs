#![cfg(feature = "slatedb")]

mod common;

use std::sync::Arc;

use prolly::{Config, Prolly, SlateDbStore};
use slatedb::object_store::ObjectStore;

#[test]
fn slatedb_store_satisfies_store_contract() {
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let path = format!("contract-{}", std::process::id());
    let store = SlateDbStore::open(path, object_store).unwrap();

    common::assert_store_contract(&store);
}

#[test]
fn slatedb_store_satisfies_manifest_store_contract() {
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let path = format!("manifest-contract-{}", std::process::id());
    let store = SlateDbStore::open(path, object_store).unwrap();

    common::assert_manifest_store_contract(&store);
}

#[test]
fn slatedb_store_satisfies_node_scan_contract() {
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let path = format!("scan-contract-{}", std::process::id());
    let store = SlateDbStore::open(path, object_store).unwrap();

    common::assert_node_store_scan_contract(store);
}

#[test]
fn slatedb_store_persists_named_root_across_reopen() {
    let object_store: Arc<dyn ObjectStore> =
        Arc::new(slatedb::object_store::memory::InMemory::new());
    let path = format!("manifest-reopen-{}", std::process::id());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();

    let tree = {
        let store = SlateDbStore::open(path.clone(), object_store.clone()).unwrap();
        let prolly = Prolly::new(store, config.clone());
        let tree = prolly.create();
        let tree = prolly
            .put(&tree, b"project/name".to_vec(), b"crabdb".to_vec())
            .unwrap();
        prolly.publish_named_root(b"main", &tree).unwrap();
        tree
    };

    let store = SlateDbStore::open(path, object_store).unwrap();
    let prolly = Prolly::new(store, config);
    let loaded = prolly.load_named_root(b"main").unwrap().unwrap();
    assert_eq!(loaded, tree);
    assert_eq!(
        prolly.get(&loaded, b"project/name").unwrap(),
        Some(b"crabdb".to_vec())
    );
}
