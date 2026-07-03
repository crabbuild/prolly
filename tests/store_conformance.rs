mod common;

use std::sync::Arc;

use prolly::{Config, Error, MemStore, NamedRootRetention, NamedRootUpdate, Prolly};

#[test]
fn memstore_satisfies_store_contract() {
    common::assert_store_contract(&MemStore::new());
}

#[test]
fn memstore_satisfies_manifest_store_contract() {
    common::assert_manifest_store_contract(&MemStore::new());
}

#[test]
fn memstore_satisfies_node_store_scan_contract() {
    common::assert_node_store_scan_contract(MemStore::new());
}

#[test]
fn arc_store_adapter_satisfies_store_contract() {
    let store = Arc::new(MemStore::new());
    common::assert_store_contract(&store);
}

#[test]
fn arc_manifest_adapter_satisfies_manifest_store_contract() {
    let store = Arc::new(MemStore::new());
    common::assert_manifest_store_contract(&store);
}

#[test]
fn arc_node_store_scan_adapter_satisfies_scan_contract() {
    let store = Arc::new(MemStore::new());
    common::assert_node_store_scan_contract(store);
}

#[test]
fn prolly_named_root_helpers_publish_load_cas_and_delete() {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, Config::default());

    let empty = prolly.create();
    let first = prolly
        .put(&empty, b"project/name".to_vec(), b"crabdb".to_vec())
        .unwrap();
    let second = prolly
        .put(&first, b"project/name".to_vec(), b"prolly-map".to_vec())
        .unwrap();

    assert_eq!(prolly.load_named_root(b"main").unwrap(), None);

    prolly
        .publish_named_root_at_millis(b"main", &first, 100)
        .unwrap();
    assert_eq!(
        prolly.load_named_root(b"main").unwrap(),
        Some(first.clone())
    );

    let conflict = prolly
        .compare_and_swap_named_root_at_millis(b"main", Some(&empty), Some(&second), 150)
        .unwrap();
    assert_eq!(
        conflict,
        NamedRootUpdate::Conflict {
            current: Some(first.clone())
        }
    );
    assert_eq!(
        prolly.load_named_root(b"main").unwrap(),
        Some(first.clone())
    );

    assert!(prolly
        .compare_and_swap_named_root_at_millis(b"main", Some(&first), Some(&second), 200)
        .unwrap()
        .is_applied());
    assert_eq!(
        prolly.load_named_root(b"main").unwrap(),
        Some(second.clone())
    );
    let manifest = prolly
        .list_named_root_manifests()
        .unwrap()
        .into_iter()
        .find(|root| root.name == b"main")
        .unwrap()
        .manifest;
    assert_eq!(manifest.created_at_millis, Some(100));
    assert_eq!(manifest.updated_at_millis, Some(200));

    assert!(prolly
        .compare_and_swap_named_root_at_millis(b"main", Some(&second), None, 250)
        .unwrap()
        .is_applied());
    assert_eq!(prolly.load_named_root(b"main").unwrap(), None);

    prolly.publish_named_root(b"main", &first).unwrap();
    prolly.delete_named_root(b"main").unwrap();
    assert_eq!(prolly.load_named_root(b"main").unwrap(), None);
}

#[test]
fn prolly_named_root_retention_helpers_select_roots() {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, Config::default());

    let empty = prolly.create();
    let first = prolly.put(&empty, b"k".to_vec(), b"1".to_vec()).unwrap();
    let second = prolly.put(&first, b"k".to_vec(), b"2".to_vec()).unwrap();
    let third = prolly.put(&second, b"k".to_vec(), b"3".to_vec()).unwrap();
    let other = prolly
        .put(&empty, b"other".to_vec(), b"value".to_vec())
        .unwrap();

    prolly
        .publish_named_root_at_millis(b"checkpoint/0001", &first, 100)
        .unwrap();
    prolly
        .publish_named_root_at_millis(b"checkpoint/0002", &second, 200)
        .unwrap();
    prolly
        .publish_named_root_at_millis(b"checkpoint/0003", &third, 300)
        .unwrap();
    prolly
        .publish_named_root_at_millis(b"main", &third, 350)
        .unwrap();
    prolly
        .publish_named_root_at_millis(b"other/0001", &other, 400)
        .unwrap();

    let all_names = prolly
        .list_named_roots()
        .unwrap()
        .into_iter()
        .map(|root| root.name)
        .collect::<Vec<_>>();
    assert_eq!(
        all_names,
        vec![
            b"checkpoint/0001".to_vec(),
            b"checkpoint/0002".to_vec(),
            b"checkpoint/0003".to_vec(),
            b"main".to_vec(),
            b"other/0001".to_vec()
        ]
    );

    let exact = prolly
        .load_named_roots(vec![
            b"checkpoint/0002".as_slice(),
            b"missing".as_slice(),
            b"checkpoint/0002".as_slice(),
        ])
        .unwrap();
    assert_eq!(
        exact
            .roots
            .iter()
            .map(|root| root.name.clone())
            .collect::<Vec<_>>(),
        vec![b"checkpoint/0002".to_vec()]
    );
    assert_eq!(exact.missing_names, vec![b"missing".to_vec()]);
    assert!(!exact.is_complete());

    let prefixed = prolly
        .load_retained_named_roots(&NamedRootRetention::prefix(b"checkpoint/"))
        .unwrap();
    assert_eq!(
        prefixed
            .roots
            .iter()
            .map(|root| root.name.clone())
            .collect::<Vec<_>>(),
        vec![
            b"checkpoint/0001".to_vec(),
            b"checkpoint/0002".to_vec(),
            b"checkpoint/0003".to_vec()
        ]
    );

    let newest = prolly
        .load_retained_named_roots(&NamedRootRetention::newest_by_name(b"checkpoint/", 2))
        .unwrap();
    assert_eq!(
        newest
            .roots
            .iter()
            .map(|root| root.name.clone())
            .collect::<Vec<_>>(),
        vec![b"checkpoint/0002".to_vec(), b"checkpoint/0003".to_vec()]
    );

    let none = prolly
        .load_retained_named_roots(&NamedRootRetention::newest_by_name(b"checkpoint/", 0))
        .unwrap();
    assert!(none.roots.is_empty());
    assert!(none.is_complete());

    let recent = prolly
        .load_retained_named_roots(&NamedRootRetention::updated_since(b"checkpoint/", 250))
        .unwrap();
    assert_eq!(
        recent
            .roots
            .iter()
            .map(|root| root.name.clone())
            .collect::<Vec<_>>(),
        vec![b"checkpoint/0003".to_vec()]
    );

    let recent_window = prolly
        .load_retained_named_roots(&NamedRootRetention::updated_within_millis(
            b"checkpoint/",
            350,
            100,
        ))
        .unwrap();
    assert_eq!(
        recent_window
            .roots
            .iter()
            .map(|root| root.name.clone())
            .collect::<Vec<_>>(),
        vec![b"checkpoint/0003".to_vec()]
    );
}

#[test]
fn store_gc_can_retain_named_roots_updated_since_cutoff() {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, Config::default());

    let empty = prolly.create();
    let old = prolly.put(&empty, b"k".to_vec(), b"old".to_vec()).unwrap();
    let new = prolly.put(&old, b"k".to_vec(), b"new".to_vec()).unwrap();

    prolly
        .publish_named_root_at_millis(b"checkpoint/0001", &old, 100)
        .unwrap();
    prolly
        .publish_named_root_at_millis(b"checkpoint/0002", &new, 200)
        .unwrap();

    let missing = prolly
        .plan_store_gc_for_retention(&NamedRootRetention::exact(vec![b"checkpoint/missing"]))
        .unwrap_err();
    assert!(matches!(
        missing,
        Error::MissingNamedRoots { names } if names == vec![b"checkpoint/missing".to_vec()]
    ));

    let retention = NamedRootRetention::updated_since(b"checkpoint/", 150);
    let plan = prolly.plan_store_gc_for_retention(&retention).unwrap();
    assert!(plan.reclaimable_nodes > 0);
    assert_eq!(plan.missing_candidates, 0);

    let sweep = prolly.sweep_store_gc_for_retention(&retention).unwrap();
    assert_eq!(sweep.deleted_nodes, plan.reclaimable_nodes);
    assert_eq!(prolly.get(&new, b"k").unwrap(), Some(b"new".to_vec()));
    assert!(prolly.get(&old, b"k").is_err());
}
