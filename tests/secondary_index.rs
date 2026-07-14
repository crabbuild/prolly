use prolly::{
    Error, IndexProjection, SecondaryIndex, SecondaryIndexEntry, SecondaryIndexError,
    SecondaryIndexRegistry,
};

#[test]
fn secondary_index_registry_validates_definitions() {
    let by_status = SecondaryIndex::non_unique("by-status", 1, "app.users.by-status/v1", |_, _| {
        Ok(vec![b"active".to_vec()])
    })
    .unwrap();
    assert_eq!(by_status.projection(), IndexProjection::KeysOnly);

    let registry = SecondaryIndexRegistry::new()
        .register(by_status.clone())
        .unwrap();
    assert!(registry.get(b"by-status").is_some());
    assert!(registry.register(by_status).is_err());

    let invalid = SecondaryIndex::non_unique(
        "bad-generation",
        0,
        "app.users.bad-generation/v1",
        |_, _| Ok(Vec::new()),
    );
    assert!(invalid.is_err());
}

#[test]
fn include_entries_carry_projection_bytes() {
    let entry = SecondaryIndexEntry::included(b"active", b"Ada");
    assert_eq!(entry.term, b"active");
    assert_eq!(entry.projection, Some(b"Ada".to_vec()));

    let index = SecondaryIndex::builder("by-status", 1, "app.users.by-status/v2")
        .projection(IndexProjection::Include)
        .extract(|_, _| Ok(vec![SecondaryIndexEntry::included(b"active", b"Ada")]))
        .unwrap();
    assert_eq!(index.projection(), IndexProjection::Include);
}

#[test]
fn definitions_validate_projection_contracts() {
    let keys_only = SecondaryIndex::non_unique("by-status", 1, "app.users.by-status/v1", |_, _| {
        Ok(vec![b"active".to_vec()])
    })
    .unwrap();
    assert!(keys_only.extract(b"user-1", b"Ada").unwrap()[0]
        .projection
        .is_none());

    let include = SecondaryIndex::builder("by-name", 1, "app.users.by-name/v1")
        .projection(IndexProjection::Include)
        .extract(|_, _| Ok(vec![SecondaryIndexEntry::term(b"Ada")]))
        .unwrap();
    assert!(include.extract(b"user-1", b"Ada").is_err());

    let all = SecondaryIndex::builder("by-name-all", 1, "app.users.by-name-all/v1")
        .projection(IndexProjection::All)
        .extract_terms(|_, _| Ok(vec![b"Ada".to_vec()]))
        .unwrap();
    assert!(all.extract(b"user-1", b"Ada").unwrap()[0]
        .projection
        .is_none());

    let failed = SecondaryIndex::non_unique("failing", 1, "app.users.failing/v1", |_, _| {
        Err(SecondaryIndexError::new("invalid source value"))
    })
    .unwrap();
    assert!(matches!(
        failed.extract(b"user-1", b"bad"),
        Err(Error::IndexExtractionFailed { reason, .. }) if reason == "invalid source value"
    ));
}
