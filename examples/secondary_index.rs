use prolly::{
    Config, Error, IndexProjection, MemStore, Prolly, SecondaryIndex, SecondaryIndexEntry,
    SecondaryIndexRegistry,
};
use std::sync::Arc;

fn status(value: &[u8]) -> Vec<u8> {
    value
        .split(|byte| *byte == b'|')
        .next()
        .unwrap_or_default()
        .to_vec()
}

fn name(value: &[u8]) -> Vec<u8> {
    value
        .splitn(2, |byte| *byte == b'|')
        .nth(1)
        .unwrap_or_default()
        .to_vec()
}

fn by_status(generation: u64) -> Result<SecondaryIndex, Error> {
    SecondaryIndex::non_unique(
        "by-status",
        generation,
        format!("example.by-status/v{generation}"),
        |_, value| Ok(vec![status(value)]),
    )
}

fn registry(status_generation: u64) -> Result<SecondaryIndexRegistry, Error> {
    let include = SecondaryIndex::builder("by-status-name", 1, "example.by-status-name/v1")
        .projection(IndexProjection::Include)
        .extract(|_, value| {
            Ok(vec![SecondaryIndexEntry::included(
                status(value),
                name(value),
            )])
        })?;
    let all = SecondaryIndex::builder("by-status-all", 1, "example.by-status-all/v1")
        .projection(IndexProjection::All)
        .extract_terms(|_, value| Ok(vec![status(value)]))?;
    SecondaryIndexRegistry::new()
        .register(by_status(status_generation)?)?
        .register(include)?
        .register(all)
}

fn main() -> Result<(), Error> {
    let engine = Prolly::new(Arc::new(MemStore::new()), Config::default());

    // Indexes may be added after the source map is populated.
    let raw_users = engine.versioned_map(b"users");
    raw_users.put(b"user-1", b"active|Ada")?;
    raw_users.put(b"user-2", b"invited|Grace")?;
    let users = engine.indexed_map(b"users", registry(1)?)?;
    users.ensure_index(b"by-status")?;
    users.ensure_index(b"by-status-name")?;
    users.ensure_index(b"by-status-all")?;

    // Source and every active index advance in one strict transaction.
    users.edit(|edit| {
        edit.put(b"user-2", b"active|Grace");
        edit.put(b"user-3", b"active|Lin");
    })?;
    let snapshot = users.snapshot()?;
    assert_eq!(
        snapshot.index(b"by-status")?.primary_keys(b"active")?,
        vec![b"user-1".to_vec(), b"user-2".to_vec(), b"user-3".to_vec()]
    );
    assert_eq!(
        snapshot.index(b"by-status-name")?.projected(b"active")?[0].1,
        Some(b"Ada".to_vec())
    );
    assert_eq!(
        snapshot.index(b"by-status-all")?.projected(b"active")?[0].1,
        Some(b"active|Ada".to_vec())
    );
    assert_eq!(snapshot.index(b"by-status")?.records(b"active")?.len(), 3);
    assert!(users
        .verify_index(b"by-status", &snapshot.id().source_version)?
        .is_valid());

    // A greater generation shadow-builds and atomically replaces the old one.
    users.replace_index(b"by-status", by_status(2)?)?;
    users.keep_last(2)?;
    let bundle = users.export_current()?;

    // Import verifies all nodes and records before atomically publishing roots.
    let replica_engine = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let replica = replica_engine.indexed_map(b"users", registry(2)?)?;
    replica.import_current(&bundle, None)?;
    assert_eq!(
        replica
            .snapshot()?
            .index(b"by-status")?
            .primary_keys(b"active")?
            .len(),
        3
    );

    println!(
        "verified IndexedMap with {} active indexes and {} bundled nodes",
        users.health()?.active_indexes.len(),
        bundle.node_count()
    );
    Ok(())
}
