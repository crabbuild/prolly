use prolly::{chunking, Config, MemStore, Mutation, Prolly};
use std::sync::Arc;

fn tree() -> (Prolly<Arc<MemStore>>, prolly::Tree) {
    let mut policy = chunking::entry_count_key_hash();
    policy.min = 2;
    policy.target = 4;
    policy.max = 8;
    policy.rule = prolly::BoundaryRule::HashThreshold { factor: 4 };
    let manager = Prolly::new(
        Arc::new(MemStore::new()),
        Config::builder().chunking(policy).build(),
    );
    let tree = manager
        .batch(
            &manager.create(),
            (0..100)
                .map(|index| Mutation::Upsert {
                    key: format!("k{index:03}").into_bytes(),
                    val: format!("v{index:03}").into_bytes(),
                })
                .collect(),
        )
        .unwrap();
    (manager, tree)
}

#[test]
fn len_rank_and_select_use_persisted_subtree_counts() {
    let (manager, tree) = tree();

    assert_eq!(manager.len(&tree).unwrap(), 100);
    assert_eq!(manager.rank(&tree, b"k000").unwrap(), 0);
    assert_eq!(manager.rank(&tree, b"k050").unwrap(), 50);
    assert_eq!(manager.rank(&tree, b"k050x").unwrap(), 51);
    assert_eq!(manager.rank(&tree, b"z").unwrap(), 100);
    assert_eq!(
        manager.select(&tree, 50).unwrap(),
        Some((b"k050".to_vec(), b"v050".to_vec()))
    );
    assert_eq!(manager.select(&tree, 100).unwrap(), None);
}

#[test]
fn cardinalities_follow_canonical_updates() {
    let (manager, tree) = tree();
    let deleted = manager.delete(&tree, b"k050").unwrap();
    assert_eq!(manager.len(&deleted).unwrap(), 99);
    assert_eq!(manager.rank(&deleted, b"k051").unwrap(), 50);
    assert_eq!(
        manager.select(&deleted, 50).unwrap(),
        Some((b"k051".to_vec(), b"v051".to_vec()))
    );
}
