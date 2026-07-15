use prolly::{Config, MemStore, Prolly, Tree};

fn fixture<const N: usize>(keys: [&[u8]; N]) -> (Prolly<MemStore>, Tree) {
    let manager = Prolly::new(MemStore::new(), Config::default());
    let mut tree = manager.create();
    for key in keys {
        tree = manager.put(&tree, key.to_vec(), key.to_vec()).unwrap();
    }
    (manager, tree)
}

fn keys(manager: &Prolly<MemStore>, tree: &Tree) -> Vec<Vec<u8>> {
    manager
        .range(tree, b"", None)
        .unwrap()
        .map(|entry| entry.unwrap().0)
        .collect()
}

fn bytes<const N: usize>(keys: [&str; N]) -> Vec<Vec<u8>> {
    keys.into_iter()
        .map(|key| key.as_bytes().to_vec())
        .collect()
}

#[test]
fn delete_range_is_half_open_and_immutable() {
    let (manager, base) = fixture([b"a", b"b", b"c", b"d", b"e", b"f"]);
    let deleted = manager.delete_range(&base, b"b", b"e").unwrap();
    assert_eq!(keys(&manager, &base), bytes(["a", "b", "c", "d", "e", "f"]));
    assert_eq!(keys(&manager, &deleted), bytes(["a", "e", "f"]));
}

#[test]
fn empty_reversed_and_disjoint_ranges_are_write_free_noops() {
    let (manager, base) = fixture([b"a", b"b", b"c"]);
    for (start, end) in [
        (b"b".as_slice(), b"b".as_slice()),
        (b"z".as_slice(), b"a".as_slice()),
        (b"x".as_slice(), b"z".as_slice()),
    ] {
        manager.reset_metrics();
        let deleted = manager.delete_range(&base, start, end).unwrap();
        assert_eq!(deleted.root, base.root);
        assert_eq!(manager.metrics().nodes_written, 0);
    }
}

#[test]
fn range_bounds_may_extend_beyond_the_keyspace() {
    let (manager, base) = fixture([b"a", b"b", b"c", b"d", b"e", b"f"]);

    let deleted_all = manager.delete_range(&base, b"", b"\xff").unwrap();
    assert!(deleted_all.root.is_none());

    let deleted_from_before = manager.delete_range(&base, b"", b"c").unwrap();
    assert_eq!(
        keys(&manager, &deleted_from_before),
        bytes(["c", "d", "e", "f"])
    );

    let deleted_through_after = manager.delete_range(&base, b"d", b"\xff").unwrap();
    assert_eq!(
        keys(&manager, &deleted_through_after),
        bytes(["a", "b", "c"])
    );
}

#[test]
fn delete_range_rejects_a_mismatched_persisted_format() {
    let (manager, base) = fixture([b"a", b"b", b"c"]);
    let mut mismatched = base;
    mismatched.config = Config::builder().hash_seed(42).build();

    let result = manager.delete_range(&mismatched, b"a", b"z");
    assert!(matches!(result, Err(prolly::Error::FormatMismatch { .. })));
}
