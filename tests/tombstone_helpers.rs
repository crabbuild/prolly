use prolly::{
    is_tombstone_value, tombstone_compaction, tombstone_upsert, Config, MemStore, Prolly, Tombstone,
};

#[test]
fn logical_tombstone_value_can_later_compact_to_physical_delete() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let tree = prolly.create();
    let tree = prolly
        .put(&tree, b"doc/1".to_vec(), b"live".to_vec())
        .unwrap();

    let tombstone = Tombstone::new(b"agent-a".to_vec(), 1_700_000_000_000)
        .with_causal_metadata("left-root", b"cid-left".to_vec())
        .with_causal_metadata("right-root", b"cid-right".to_vec());
    let tree = prolly
        .batch(&tree, vec![tombstone.to_upsert_mutation(b"doc/1").unwrap()])
        .unwrap();

    let stored = prolly.get(&tree, b"doc/1").unwrap().expect("stored value");
    assert!(is_tombstone_value(&stored));

    let decoded = Tombstone::from_stored_bytes(&stored).unwrap().unwrap();
    assert_eq!(decoded.actor, b"agent-a".to_vec());
    assert_eq!(decoded.timestamp_millis, 1_700_000_000_000);
    assert_eq!(
        decoded.causal_metadata("right-root"),
        Some(b"cid-right".as_slice())
    );

    let delete = tombstone_compaction(b"doc/1".to_vec(), &stored)
        .unwrap()
        .expect("tombstone should compact to delete");
    let compacted = prolly.batch(&tree, vec![delete]).unwrap();
    assert_eq!(prolly.get(&compacted, b"doc/1").unwrap(), None);
}

#[test]
fn tombstone_public_helpers_distinguish_live_values() {
    let live = b"live value";
    assert!(!is_tombstone_value(live));
    assert_eq!(Tombstone::from_stored_bytes(live).unwrap(), None);
    assert_eq!(
        tombstone_compaction(b"live-key".to_vec(), live).unwrap(),
        None
    );

    let mutation = tombstone_upsert(
        b"deleted-key".to_vec(),
        &Tombstone::new(b"replica-a".to_vec(), 123),
    )
    .unwrap();
    assert_eq!(mutation.key(), b"deleted-key");
    assert!(!mutation.is_delete());
}

#[test]
fn invalid_tombstone_envelope_is_not_silently_ignored() {
    let err = Tombstone::from_stored_bytes(b"PLTD").unwrap_err();
    assert!(err.to_string().contains("invalid tombstone"));
}
