#![cfg(feature = "pglite")]

mod common;

use prolly::{Config, PgliteStore, Prolly};

#[test]
fn pglite_store_satisfies_store_contract_when_enabled() {
    if std::env::var("PROLLY_PGLITE_TEST").ok().as_deref() != Some("1") {
        return;
    }

    let store = PgliteStore::open_in_memory().unwrap();
    common::assert_store_contract(&store);
}

#[test]
fn pglite_store_satisfies_manifest_contract_when_enabled() {
    if std::env::var("PROLLY_PGLITE_TEST").ok().as_deref() != Some("1") {
        return;
    }

    let store = PgliteStore::open_in_memory().unwrap();
    common::assert_manifest_store_contract(&store);
}

#[test]
fn pglite_store_satisfies_node_scan_contract_when_enabled() {
    if std::env::var("PROLLY_PGLITE_TEST").ok().as_deref() != Some("1") {
        return;
    }

    let store = PgliteStore::open_in_memory().unwrap();
    common::assert_node_store_scan_contract(store);
}

#[test]
fn pglite_store_persists_named_root_across_reopen_when_enabled() {
    if std::env::var("PROLLY_PGLITE_TEST").ok().as_deref() != Some("1") {
        return;
    }

    let path = std::env::temp_dir().join(format!(
        "crabdb-pglite-root-manifest-test-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&path);

    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();

    let tree = {
        let store = PgliteStore::open(path.to_string_lossy().to_string()).unwrap();
        let prolly = Prolly::new(store, config.clone());
        let tree = prolly.create();
        let tree = prolly
            .put(&tree, b"project/name".to_vec(), b"crabdb".to_vec())
            .unwrap();

        prolly.publish_named_root(b"main", &tree).unwrap();
        tree
    };

    {
        let store = PgliteStore::open(path.to_string_lossy().to_string()).unwrap();
        let prolly = Prolly::new(store, config);
        let loaded = prolly.load_named_root(b"main").unwrap().unwrap();
        assert_eq!(loaded, tree);
        assert_eq!(
            prolly.get(&loaded, b"project/name").unwrap(),
            Some(b"crabdb".to_vec())
        );
    }

    let _ = std::fs::remove_dir_all(path);
}
