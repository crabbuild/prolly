use std::sync::Arc;

use prolly::{
    snapshot_id_from_name, snapshot_root_name, Config, MemStore, Prolly, SnapshotNamespace,
};

#[test]
fn snapshot_names_are_deterministic_and_reversible() {
    let branch = SnapshotNamespace::branch();
    let tag = SnapshotNamespace::tag();
    let checkpoint = SnapshotNamespace::checkpoint();
    let custom = SnapshotNamespace::custom(b"workspace/current/".to_vec());

    assert_eq!(
        snapshot_root_name(&branch, b"main"),
        b"refs/heads/main".to_vec()
    );
    assert_eq!(
        snapshot_root_name(&tag, b"v1.0.0"),
        b"refs/tags/v1.0.0".to_vec()
    );
    assert_eq!(
        snapshot_root_name(&checkpoint, b"run-42"),
        b"refs/checkpoints/run-42".to_vec()
    );
    assert_eq!(
        snapshot_root_name(&custom, b"latest"),
        b"workspace/current/latest".to_vec()
    );

    assert_eq!(
        snapshot_id_from_name(&branch, b"refs/heads/main"),
        Some(b"main".to_vec())
    );
    assert_eq!(snapshot_id_from_name(&branch, b"refs/tags/main"), None);
}

#[test]
fn snapshot_manager_publishes_lists_loads_and_deletes_branch_roots() {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, Config::default());
    let empty = prolly.create();
    let main = prolly
        .put(&empty, b"doc/title".to_vec(), b"main".to_vec())
        .unwrap();
    let feature = prolly
        .put(&main, b"doc/body".to_vec(), b"feature".to_vec())
        .unwrap();

    let branches = prolly.branch_snapshots();
    let tags = prolly.tag_snapshots();

    branches.publish_at_millis(b"main", &main, 100).unwrap();
    branches
        .publish_at_millis(b"feature/docs", &feature, 200)
        .unwrap();
    tags.publish_at_millis(b"v1", &main, 300).unwrap();

    assert_eq!(branches.load(b"main").unwrap(), Some(main.clone()));
    assert_eq!(tags.load(b"v1").unwrap(), Some(main.clone()));

    let listed = branches.list().unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|snapshot| snapshot.id.clone())
            .collect::<Vec<_>>(),
        vec![b"feature/docs".to_vec(), b"main".to_vec()]
    );
    assert_eq!(listed[0].name, b"refs/heads/feature/docs".to_vec());
    assert_eq!(listed[0].created_at_millis, Some(200));
    assert_eq!(listed[0].updated_at_millis, Some(200));

    let selection = branches
        .load_many([b"main".as_slice(), b"missing".as_slice()])
        .unwrap();
    assert_eq!(selection.snapshots.len(), 1);
    assert_eq!(selection.snapshots[0].id, b"main".to_vec());
    assert_eq!(selection.missing_ids, vec![b"missing".to_vec()]);

    branches.delete(b"feature/docs").unwrap();
    assert!(branches.load(b"feature/docs").unwrap().is_none());
    assert_eq!(tags.list().unwrap().len(), 1);
}

#[test]
fn snapshot_manager_compare_and_swap_preserves_conflict_state() {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, Config::default());
    let base = prolly
        .put(&prolly.create(), b"k".to_vec(), b"base".to_vec())
        .unwrap();
    let left = prolly.put(&base, b"k".to_vec(), b"left".to_vec()).unwrap();
    let right = prolly.put(&base, b"k".to_vec(), b"right".to_vec()).unwrap();
    let branches = prolly.branch_snapshots();

    let create = branches
        .compare_and_swap(b"main", None, Some(&base))
        .unwrap();
    assert!(create.is_applied());

    let conflict = branches
        .compare_and_swap(b"main", Some(&left), Some(&right))
        .unwrap();
    assert!(conflict.is_conflict());
    assert_eq!(conflict.current(), Some(&base));
    assert_eq!(branches.load(b"main").unwrap(), Some(base.clone()));

    let update = branches
        .compare_and_swap_at_millis(b"main", Some(&base), Some(&right), 500)
        .unwrap();
    assert!(update.is_applied());
    let listed = branches.list().unwrap();
    assert_eq!(listed[0].updated_at_millis, Some(500));
    assert_eq!(branches.load(b"main").unwrap(), Some(right));
}
