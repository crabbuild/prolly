use prolly::{Config, Error, MemStore, Mutation, Prolly, StructuralEdit};
use std::sync::Arc;

#[test]
fn diff_patch_round_trips_the_target_root_and_bytes() {
    let manager = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let base = manager
        .batch(
            &manager.create(),
            vec![
                Mutation::Upsert {
                    key: b"a".to_vec(),
                    val: b"1".to_vec(),
                },
                Mutation::Upsert {
                    key: b"b".to_vec(),
                    val: b"2".to_vec(),
                },
            ],
        )
        .unwrap();
    let target = manager
        .batch(
            &base,
            vec![
                Mutation::Upsert {
                    key: b"b".to_vec(),
                    val: b"changed".to_vec(),
                },
                Mutation::Upsert {
                    key: b"c".to_vec(),
                    val: b"3".to_vec(),
                },
            ],
        )
        .unwrap();
    let patch = manager.diff_patch(&base, &target).unwrap();
    let bytes = serde_cbor::to_vec(&patch).unwrap();
    let decoded = serde_cbor::from_slice(&bytes).unwrap();

    assert_eq!(patch, decoded);
    assert_eq!(
        manager.apply_patch(&base, &patch).unwrap().root,
        target.root
    );
}

#[test]
fn patch_rejects_wrong_base_and_unverified_subtrees() {
    let manager = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let base = manager.create();
    let target = manager.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
    let mut patch = manager.diff_patch(&base, &target).unwrap();
    assert!(matches!(
        manager.apply_patch(&target, &patch),
        Err(Error::PatchBaseMismatch)
    ));

    patch.edits = vec![StructuralEdit::Subtree {
        start_exclusive: None,
        end_inclusive: b"z".to_vec(),
        level: 1,
        cid: target.root.clone(),
        logical_count: 1,
    }];
    assert!(matches!(
        manager.apply_patch(&base, &patch),
        Err(Error::InvalidStructuralPatch(_))
    ));
}
