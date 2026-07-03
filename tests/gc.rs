mod common;

use std::sync::Arc;

use prolly::{Cid, Config, MemStore, Prolly, Store};

#[test]
fn mark_reachable_deduplicates_roots_and_shared_nodes() {
    let prolly = common::configured_prolly();
    let empty = prolly.create();
    let tree = prolly.put(&empty, b"k1".to_vec(), b"v1".to_vec()).unwrap();
    let tree = prolly.put(&tree, b"k2".to_vec(), b"v2".to_vec()).unwrap();

    let reachable = prolly
        .mark_reachable(&[empty, tree.clone(), tree.clone()])
        .unwrap();

    assert!(reachable.live_nodes > 0);
    assert_eq!(reachable.live_nodes, reachable.cids().len());
    assert_eq!(
        reachable.live_nodes,
        reachable.leaf_nodes + reachable.internal_nodes
    );
    assert!(reachable.live_bytes > 0);
    assert!(reachable.contains(tree.root.as_ref().unwrap()));
}

#[test]
fn plan_gc_reports_reclaimable_and_missing_candidates_without_deleting() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();
    let prolly = Prolly::new(store.clone(), config);

    let base = prolly.create();
    let base = prolly.put(&base, b"k".to_vec(), b"old".to_vec()).unwrap();
    let updated = prolly.put(&base, b"k".to_vec(), b"new".to_vec()).unwrap();

    let mut candidates = prolly
        .mark_reachable(&[base.clone(), updated.clone()])
        .unwrap();
    let missing = Cid::from_bytes(b"missing-gc-candidate");
    candidates.live_cids.push(missing);

    let plan = prolly
        .plan_gc(std::slice::from_ref(&updated), &candidates.live_cids)
        .unwrap();

    assert!(plan.reclaimable_nodes > 0);
    assert!(plan.reclaimable_bytes > 0);
    assert_eq!(plan.missing_candidates, 1);
    assert_eq!(
        plan.candidate_nodes,
        plan.retained_candidate_nodes() + plan.reclaimable_nodes + plan.missing_candidates
    );
    for cid in plan.reclaimable_cids() {
        assert!(!plan.reachability.contains(cid));
        assert!(store.get(cid.as_bytes()).unwrap().is_some());
    }
}

#[test]
fn sweep_gc_deletes_only_unreachable_candidates_and_clears_manager_cache() {
    let store = Arc::new(MemStore::new());
    let config = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build();
    let prolly = Prolly::new(store.clone(), config);

    let base = prolly.create();
    let base = prolly.put(&base, b"k".to_vec(), b"old".to_vec()).unwrap();
    let updated = prolly.put(&base, b"k".to_vec(), b"new".to_vec()).unwrap();

    let candidates = prolly
        .mark_reachable(&[base.clone(), updated.clone()])
        .unwrap()
        .into_cids();
    assert!(prolly.cache_len() > 0);

    let sweep = prolly
        .sweep_gc(std::slice::from_ref(&updated), &candidates)
        .unwrap();

    assert!(sweep.deleted_nodes > 0);
    assert_eq!(sweep.deleted_nodes, sweep.plan.reclaimable_nodes);
    assert_eq!(sweep.deleted_bytes, sweep.plan.reclaimable_bytes);
    assert_eq!(prolly.cache_len(), 0);
    for cid in sweep.plan.reclaimable_cids() {
        assert_eq!(store.get(cid.as_bytes()).unwrap(), None);
    }

    assert_eq!(prolly.get(&updated, b"k").unwrap(), Some(b"new".to_vec()));
    assert!(prolly.get(&base, b"k").is_err());
}
