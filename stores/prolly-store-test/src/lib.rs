//! Shared conformance assertions for workspace store adapters.

use prolly::{
    BatchOp, Cid, Config, ManifestStore, ManifestStoreScan, ManifestUpdate, NodeStoreScan, Prolly,
    RootManifest, Store,
};

pub fn assert_store_contract<S>(store: &S)
where
    S: Store,
    S::Error: std::fmt::Debug,
{
    assert_eq!(store.get(b"missing").unwrap(), None);
    store.batch(&[]).unwrap();
    store.batch_put(&[]).unwrap();

    store.put(b"alpha", b"1").unwrap();
    store.put(b"beta", b"2").unwrap();
    let keys: Vec<&[u8]> = vec![b"beta", b"missing", b"alpha", b"beta"];
    assert_eq!(
        store.batch_get_ordered(&keys).unwrap(),
        vec![
            Some(b"2".to_vec()),
            None,
            Some(b"1".to_vec()),
            Some(b"2".to_vec()),
        ]
    );

    let found = store.batch_get(&keys).unwrap();
    assert_eq!(found.get(b"alpha".as_slice()), Some(&b"1".to_vec()));
    assert_eq!(found.get(b"beta".as_slice()), Some(&b"2".to_vec()));

    store
        .batch(&[
            BatchOp::Upsert {
                key: b"alpha",
                value: b"updated",
            },
            BatchOp::Delete { key: b"beta" },
        ])
        .unwrap();
    assert_eq!(store.get(b"alpha").unwrap(), Some(b"updated".to_vec()));
    assert_eq!(store.get(b"beta").unwrap(), None);
}

pub fn assert_manifest_store_contract<S>(store: &S)
where
    S: ManifestStore + ManifestStoreScan,
    S::Error: std::fmt::Debug,
{
    let config = Config::default();
    let first = RootManifest::new(Some(Cid::from_bytes(b"first")), config.clone());
    let second = RootManifest::new(Some(Cid::from_bytes(b"second")), config);

    assert_eq!(store.get_root(b"main").unwrap(), None);
    assert!(store
        .compare_and_swap_root(b"main", None, Some(&first))
        .unwrap()
        .is_applied());
    assert_eq!(
        store
            .compare_and_swap_root(b"main", None, Some(&second))
            .unwrap(),
        ManifestUpdate::Conflict {
            current: Some(first.clone()),
        }
    );
    assert!(store
        .compare_and_swap_root(b"main", Some(&first), Some(&second))
        .unwrap()
        .is_applied());

    store.put_root(b"zeta", &first).unwrap();
    let roots = store.list_roots().unwrap();
    assert_eq!(
        roots.into_iter().map(|root| root.name).collect::<Vec<_>>(),
        vec![b"main".to_vec(), b"zeta".to_vec()],
    );
}

pub fn assert_node_store_scan_contract<S>(store: S)
where
    S: Store + ManifestStore + NodeStoreScan,
    <S as Store>::Error: std::fmt::Debug,
    <S as ManifestStore>::Error: std::fmt::Debug,
    <S as NodeStoreScan>::Error: std::fmt::Debug,
{
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();
    store.put_hint(b"scan", b"rightmost", b"hint").unwrap();
    let prolly = Prolly::new(store, config);
    let base = prolly
        .put(&prolly.create(), b"k".to_vec(), b"old".to_vec())
        .unwrap();
    let updated = prolly.put(&base, b"k".to_vec(), b"new".to_vec()).unwrap();

    let plan = prolly
        .plan_store_gc(std::slice::from_ref(&updated))
        .unwrap();
    assert!(plan.reclaimable_nodes > 0);
    let sweep = prolly
        .sweep_store_gc(std::slice::from_ref(&updated))
        .unwrap();
    assert_eq!(sweep.deleted_nodes, plan.reclaimable_nodes);
    assert_eq!(prolly.get(&updated, b"k").unwrap(), Some(b"new".to_vec()));
}
