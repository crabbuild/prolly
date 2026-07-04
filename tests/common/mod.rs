#![allow(dead_code)]

#[cfg(feature = "async-store")]
use prolly::{AsyncManifestStore, AsyncManifestStoreScan, AsyncStore};
use prolly::{
    BatchOp, Cid, Config, Diff, ManifestStore, ManifestStoreScan, ManifestUpdate, MemStore, Node,
    NodeStoreScan, Prolly, RootManifest, Store, Tree,
};

pub fn configured_prolly() -> Prolly<MemStore> {
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();
    Prolly::new(MemStore::new(), config)
}

pub fn entries<S: Store>(prolly: &Prolly<S>, tree: &Tree) -> Vec<(Vec<u8>, Vec<u8>)> {
    prolly
        .range(tree, &[], None)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

pub fn canonical_diffs(mut diffs: Vec<Diff>) -> Vec<Diff> {
    diffs.sort_by(|left, right| {
        let (left_key, left_kind) = diff_sort_parts(left);
        let (right_key, right_kind) = diff_sort_parts(right);
        left_key
            .cmp(right_key)
            .then_with(|| left_kind.cmp(&right_kind))
    });
    diffs
}

pub fn assert_store_contract<S>(store: &S)
where
    S: Store,
    S::Error: std::fmt::Debug,
{
    assert_eq!(store.get(b"missing").unwrap(), None);
    assert!(store.batch_get(&[]).unwrap().is_empty());
    assert_eq!(
        store.batch_get_ordered(&[]).unwrap(),
        Vec::<Option<Vec<u8>>>::new()
    );
    assert_eq!(
        store.batch_get_ordered_unique(&[]).unwrap(),
        Vec::<Option<Vec<u8>>>::new()
    );
    store.batch(&[]).unwrap();
    store.batch_put(&[]).unwrap();
    store.delete(b"missing").unwrap();

    store.put(b"alpha", b"1").unwrap();
    store.put(b"beta", b"2").unwrap();
    assert_eq!(store.get(b"alpha").unwrap(), Some(b"1".to_vec()));

    let ordered_keys: Vec<&[u8]> = vec![b"beta", b"missing", b"alpha", b"beta"];
    assert_eq!(
        store.batch_get_ordered(&ordered_keys).unwrap(),
        vec![
            Some(b"2".to_vec()),
            None,
            Some(b"1".to_vec()),
            Some(b"2".to_vec())
        ]
    );

    let unique_keys: Vec<&[u8]> = vec![b"alpha", b"missing", b"beta"];
    assert_eq!(
        store.batch_get_ordered_unique(&unique_keys).unwrap(),
        vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())]
    );

    let found = store.batch_get(&ordered_keys).unwrap();
    assert_eq!(found.get(b"alpha".as_slice()), Some(&b"1".to_vec()));
    assert_eq!(found.get(b"beta".as_slice()), Some(&b"2".to_vec()));
    assert!(!found.contains_key(b"missing".as_slice()));

    store
        .batch(&[
            BatchOp::Upsert {
                key: b"alpha",
                value: b"old",
            },
            BatchOp::Upsert {
                key: b"alpha",
                value: b"updated",
            },
            BatchOp::Delete { key: b"beta" },
            BatchOp::Delete { key: b"missing" },
            BatchOp::Upsert {
                key: b"gamma",
                value: b"3",
            },
        ])
        .unwrap();
    assert_eq!(store.get(b"alpha").unwrap(), Some(b"updated".to_vec()));
    assert_eq!(store.get(b"beta").unwrap(), None);
    assert_eq!(store.get(b"gamma").unwrap(), Some(b"3".to_vec()));

    store
        .batch_put(&[(b"alpha".as_slice(), b"new".as_slice()), (b"delta", b"4")])
        .unwrap();
    assert_eq!(store.get(b"alpha").unwrap(), Some(b"new".to_vec()));
    assert_eq!(store.get(b"delta").unwrap(), Some(b"4".to_vec()));

    let supports_hints = store.supports_hints();
    store.put_hint(b"ns", b"root", b"hint-v1").unwrap();
    if supports_hints {
        assert_eq!(
            store.get_hint(b"ns", b"root").unwrap(),
            Some(b"hint-v1".to_vec())
        );
    } else {
        assert_eq!(store.get_hint(b"ns", b"root").unwrap(), None);
    }

    store
        .batch_put_with_hint(
            &[(b"node-1".as_slice(), b"node-bytes".as_slice())],
            b"ns",
            b"root",
            b"hint-v2",
        )
        .unwrap();
    assert_eq!(store.get(b"node-1").unwrap(), Some(b"node-bytes".to_vec()));
    if supports_hints {
        assert_eq!(
            store.get_hint(b"ns", b"root").unwrap(),
            Some(b"hint-v2".to_vec())
        );
        assert_eq!(store.get_hint(b"other-ns", b"root").unwrap(), None);
    } else {
        assert_eq!(store.get_hint(b"ns", b"root").unwrap(), None);
    }
}

pub fn assert_manifest_store_contract<S>(store: &S)
where
    S: ManifestStore + ManifestStoreScan,
    S::Error: std::fmt::Debug,
{
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(8)
        .chunking_factor(4)
        .hash_seed(42)
        .build();
    let main_v1 = RootManifest::new(Some(Cid::from_bytes(b"main-v1")), config.clone());
    let main_v2 = RootManifest::new(Some(Cid::from_bytes(b"main-v2")), config.clone());
    let empty = RootManifest::new(None, config);

    assert_eq!(store.get_root(b"main").unwrap(), None);
    store.delete_root(b"main").unwrap();

    store.put_root(b"main", &main_v1).unwrap();
    assert_eq!(store.get_root(b"main").unwrap(), Some(main_v1.clone()));

    store.put_root(b"main", &empty).unwrap();
    assert_eq!(store.get_root(b"main").unwrap(), Some(empty.clone()));

    store.delete_root(b"main").unwrap();
    assert_eq!(store.get_root(b"main").unwrap(), None);

    assert!(store
        .compare_and_swap_root(b"main", None, Some(&main_v1))
        .unwrap()
        .is_applied());
    assert_eq!(store.get_root(b"main").unwrap(), Some(main_v1.clone()));

    let stale_create = store
        .compare_and_swap_root(b"main", None, Some(&main_v2))
        .unwrap();
    assert_eq!(
        stale_create,
        ManifestUpdate::Conflict {
            current: Some(main_v1.clone())
        }
    );

    assert!(store
        .compare_and_swap_root(b"main", Some(&main_v1), Some(&main_v2))
        .unwrap()
        .is_applied());
    assert_eq!(store.get_root(b"main").unwrap(), Some(main_v2.clone()));

    let stale_delete = store
        .compare_and_swap_root(b"main", Some(&main_v1), None)
        .unwrap();
    assert_eq!(
        stale_delete,
        ManifestUpdate::Conflict {
            current: Some(main_v2.clone())
        }
    );

    assert!(store
        .compare_and_swap_root(b"main", Some(&main_v2), None)
        .unwrap()
        .is_applied());
    assert_eq!(store.get_root(b"main").unwrap(), None);

    store.put_root(b"zeta", &main_v1).unwrap();
    store.put_root(b"alpha", &main_v2).unwrap();
    let listed = store.list_roots().unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|root| root.name.clone())
            .collect::<Vec<_>>(),
        vec![b"alpha".to_vec(), b"zeta".to_vec()]
    );
    assert_eq!(listed[0].manifest, main_v2);
    assert_eq!(listed[1].manifest, main_v1);

    store.delete_root(b"alpha").unwrap();
    store.delete_root(b"zeta").unwrap();
    assert!(store.list_roots().unwrap().is_empty());
}

#[cfg(feature = "async-store")]
pub async fn assert_async_manifest_store_contract<S>(store: &S)
where
    S: AsyncManifestStore + AsyncManifestStoreScan,
    S::Error: std::fmt::Debug,
{
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(8)
        .chunking_factor(4)
        .hash_seed(42)
        .build();
    let main_v1 = RootManifest::new(Some(Cid::from_bytes(b"main-v1")), config.clone());
    let main_v2 = RootManifest::new(Some(Cid::from_bytes(b"main-v2")), config.clone());
    let empty = RootManifest::new(None, config);

    assert_eq!(store.get_root(b"main").await.unwrap(), None);
    store.delete_root(b"main").await.unwrap();

    store.put_root(b"main", &main_v1).await.unwrap();
    assert_eq!(
        store.get_root(b"main").await.unwrap(),
        Some(main_v1.clone())
    );

    store.put_root(b"main", &empty).await.unwrap();
    assert_eq!(store.get_root(b"main").await.unwrap(), Some(empty.clone()));

    store.delete_root(b"main").await.unwrap();
    assert_eq!(store.get_root(b"main").await.unwrap(), None);

    assert!(store
        .compare_and_swap_root(b"main", None, Some(&main_v1))
        .await
        .unwrap()
        .is_applied());
    assert_eq!(
        store.get_root(b"main").await.unwrap(),
        Some(main_v1.clone())
    );

    let stale_create = store
        .compare_and_swap_root(b"main", None, Some(&main_v2))
        .await
        .unwrap();
    assert_eq!(
        stale_create,
        ManifestUpdate::Conflict {
            current: Some(main_v1.clone())
        }
    );

    assert!(store
        .compare_and_swap_root(b"main", Some(&main_v1), Some(&main_v2))
        .await
        .unwrap()
        .is_applied());
    assert_eq!(
        store.get_root(b"main").await.unwrap(),
        Some(main_v2.clone())
    );

    let stale_delete = store
        .compare_and_swap_root(b"main", Some(&main_v1), None)
        .await
        .unwrap();
    assert_eq!(
        stale_delete,
        ManifestUpdate::Conflict {
            current: Some(main_v2.clone())
        }
    );

    assert!(store
        .compare_and_swap_root(b"main", Some(&main_v2), None)
        .await
        .unwrap()
        .is_applied());
    assert_eq!(store.get_root(b"main").await.unwrap(), None);

    store.put_root(b"zeta", &main_v1).await.unwrap();
    store.put_root(b"alpha", &main_v2).await.unwrap();
    let listed = store.list_roots().await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|root| root.name.clone())
            .collect::<Vec<_>>(),
        vec![b"alpha".to_vec(), b"zeta".to_vec()]
    );
    assert_eq!(listed[0].manifest, main_v2);
    assert_eq!(listed[1].manifest, main_v1);

    store.delete_root(b"alpha").await.unwrap();
    store.delete_root(b"zeta").await.unwrap();
    assert!(store.list_roots().await.unwrap().is_empty());
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
    store
        .put_root(
            b"metadata-root",
            &RootManifest::new(Some(Cid::from_bytes(b"not-a-node")), config.clone()),
        )
        .unwrap();

    let prolly = Prolly::new(store, config);
    let base = prolly.create();
    let base = prolly.put(&base, b"k".to_vec(), b"old".to_vec()).unwrap();
    let updated = prolly.put(&base, b"k".to_vec(), b"new".to_vec()).unwrap();

    let all_reachable = prolly
        .mark_reachable(&[base.clone(), updated.clone()])
        .unwrap();
    let plan = prolly
        .plan_store_gc(std::slice::from_ref(&updated))
        .unwrap();

    assert_eq!(plan.candidate_nodes, all_reachable.live_nodes);
    assert_eq!(plan.missing_candidates, 0);
    assert!(plan.reclaimable_nodes > 0);
    assert!(plan.reclaimable_bytes > 0);
    for cid in plan.reclaimable_cids() {
        assert!(!plan.reachability.contains(cid));
    }

    let sweep = prolly
        .sweep_store_gc(std::slice::from_ref(&updated))
        .unwrap();
    assert_eq!(sweep.deleted_nodes, plan.reclaimable_nodes);
    assert_eq!(sweep.deleted_bytes, plan.reclaimable_bytes);
    assert_eq!(prolly.get(&updated, b"k").unwrap(), Some(b"new".to_vec()));
    assert!(prolly.get(&base, b"k").is_err());
}

#[cfg(feature = "async-store")]
pub async fn assert_async_store_contract<S>(store: &S)
where
    S: AsyncStore,
    S::Error: std::fmt::Debug,
{
    assert_eq!(store.get(b"missing").await.unwrap(), None);
    assert!(store.batch_get(&[]).await.unwrap().is_empty());
    assert_eq!(
        store.batch_get_ordered(&[]).await.unwrap(),
        Vec::<Option<Vec<u8>>>::new()
    );
    assert_eq!(
        store.batch_get_ordered_unique(&[]).await.unwrap(),
        Vec::<Option<Vec<u8>>>::new()
    );
    store.batch(&[]).await.unwrap();
    store.batch_put(&[]).await.unwrap();
    store.delete(b"missing").await.unwrap();

    store.put(b"alpha", b"1").await.unwrap();
    store.put(b"beta", b"2").await.unwrap();
    assert_eq!(store.get(b"alpha").await.unwrap(), Some(b"1".to_vec()));

    let ordered_keys: Vec<&[u8]> = vec![b"beta", b"missing", b"alpha", b"beta"];
    assert_eq!(
        store.batch_get_ordered(&ordered_keys).await.unwrap(),
        vec![
            Some(b"2".to_vec()),
            None,
            Some(b"1".to_vec()),
            Some(b"2".to_vec())
        ]
    );

    let unique_keys: Vec<&[u8]> = vec![b"alpha", b"missing", b"beta"];
    assert_eq!(
        store.batch_get_ordered_unique(&unique_keys).await.unwrap(),
        vec![Some(b"1".to_vec()), None, Some(b"2".to_vec())]
    );

    let found = store.batch_get(&ordered_keys).await.unwrap();
    assert_eq!(found.get(b"alpha".as_slice()), Some(&b"1".to_vec()));
    assert_eq!(found.get(b"beta".as_slice()), Some(&b"2".to_vec()));
    assert!(!found.contains_key(b"missing".as_slice()));

    store
        .batch(&[
            BatchOp::Upsert {
                key: b"alpha",
                value: b"old",
            },
            BatchOp::Upsert {
                key: b"alpha",
                value: b"updated",
            },
            BatchOp::Delete { key: b"beta" },
            BatchOp::Delete { key: b"missing" },
            BatchOp::Upsert {
                key: b"gamma",
                value: b"3",
            },
        ])
        .await
        .unwrap();
    assert_eq!(
        store.get(b"alpha").await.unwrap(),
        Some(b"updated".to_vec())
    );
    assert_eq!(store.get(b"beta").await.unwrap(), None);
    assert_eq!(store.get(b"gamma").await.unwrap(), Some(b"3".to_vec()));

    store
        .batch_put(&[(b"alpha".as_slice(), b"new".as_slice()), (b"delta", b"4")])
        .await
        .unwrap();
    assert_eq!(store.get(b"alpha").await.unwrap(), Some(b"new".to_vec()));
    assert_eq!(store.get(b"delta").await.unwrap(), Some(b"4".to_vec()));

    let supports_hints = store.supports_hints();
    store.put_hint(b"ns", b"root", b"hint-v1").await.unwrap();
    if supports_hints {
        assert_eq!(
            store.get_hint(b"ns", b"root").await.unwrap(),
            Some(b"hint-v1".to_vec())
        );
    } else {
        assert_eq!(store.get_hint(b"ns", b"root").await.unwrap(), None);
    }

    store
        .batch_put_with_hint(
            &[(b"node-1".as_slice(), b"node-bytes".as_slice())],
            b"ns",
            b"root",
            b"hint-v2",
        )
        .await
        .unwrap();
    assert_eq!(
        store.get(b"node-1").await.unwrap(),
        Some(b"node-bytes".to_vec())
    );
    if supports_hints {
        assert_eq!(
            store.get_hint(b"ns", b"root").await.unwrap(),
            Some(b"hint-v2".to_vec())
        );
        assert_eq!(store.get_hint(b"other-ns", b"root").await.unwrap(), None);
    } else {
        assert_eq!(store.get_hint(b"ns", b"root").await.unwrap(), None);
    }
}

pub fn load_node<S>(store: &S, cid: &Cid) -> Node
where
    S: Store,
    S::Error: std::fmt::Debug,
{
    let bytes = store.get(cid.as_bytes()).unwrap().unwrap();
    Node::from_bytes(&bytes).unwrap()
}

pub fn assert_tree_invariants<S>(store: &S, tree: &Tree, config: &Config)
where
    S: Store,
    S::Error: std::fmt::Debug,
{
    if let Some(root) = &tree.root {
        let (_, first_key) = assert_node_invariants(store, root, None, config);
        assert!(first_key.is_some());
    }
}

fn diff_sort_parts(diff: &Diff) -> (&[u8], u8) {
    match diff {
        Diff::Added { key, .. } => (key, 0),
        Diff::Changed { key, .. } => (key, 1),
        Diff::Removed { key, .. } => (key, 2),
    }
}

fn assert_node_invariants<S>(
    store: &S,
    cid: &Cid,
    expected_level: Option<u8>,
    config: &Config,
) -> (usize, Option<Vec<u8>>)
where
    S: Store,
    S::Error: std::fmt::Debug,
{
    let node = load_node(store, cid);

    assert_eq!(node.keys.len(), node.vals.len());
    assert!(node.keys.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(node.len() <= config.max_chunk_size);
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
