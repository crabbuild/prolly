use prolly::{
    catalog_map_id, control_record_key, control_root_name, index_map_id, ActiveIndexControl, Cid,
    Config, Error, IndexControl, IndexProjection, MemStore, Mutation, ParallelConfig, Prolly,
    SecondaryIndex, SecondaryIndexEntry, SecondaryIndexError, SecondaryIndexRegistry,
};
use std::sync::Arc;

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

fn install_control(prolly: &Prolly<Arc<MemStore>>, source_map_id: &[u8]) {
    let control = IndexControl {
        source_map_id: source_map_id.to_vec(),
        catalog_map_id: catalog_map_id(source_map_id),
        active: vec![ActiveIndexControl {
            name: b"by-status".to_vec(),
            fingerprint: Cid([7; 32]),
        }],
    };
    let tree = prolly
        .put(
            &prolly.create(),
            control_record_key(),
            control.to_bytes().unwrap(),
        )
        .unwrap();
    prolly
        .publish_named_root(&control_root_name(source_map_id), &tree)
        .unwrap();
}

fn assert_fenced<T>(result: Result<T, Error>) {
    assert!(matches!(
        result,
        Err(Error::IndexesRequireIndexedMap { map_id, active_indexes })
            if map_id == b"users" && active_indexes == vec![b"by-status".to_vec()]
    ));
}

#[test]
fn active_control_fences_public_raw_write_routes() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let users = prolly.versioned_map(b"users");
    let first = users.put(b"user-1", b"Ada").unwrap();
    let second = users.put(b"user-2", b"Grace").unwrap();
    let bundle = users.snapshot().unwrap().unwrap().export().unwrap();
    let backup = users.backup().unwrap();
    install_control(&prolly, b"users");

    assert_fenced(users.initialize());
    assert_fenced(users.put(b"user-3", b"Lin"));
    assert_fenced(users.apply_if(
        Some(&second.id),
        vec![Mutation::Delete {
            key: b"user-1".to_vec(),
        }],
    ));
    assert_fenced(users.edit(|edit| {
        edit.put(b"user-3", b"Lin");
    }));
    assert_fenced(users.append(vec![Mutation::Upsert {
        key: b"user-3".to_vec(),
        val: b"Lin".to_vec(),
    }]));
    assert_fenced(users.parallel_apply(
        vec![Mutation::Upsert {
            key: b"user-3".to_vec(),
            val: b"Lin".to_vec(),
        }],
        &ParallelConfig::default(),
    ));
    assert_fenced(users.rollback_to(&first.id));
    assert_fenced(
        users.rebuild_from_iter_if(Some(&second.id), [(b"user-3".to_vec(), b"Lin".to_vec())]),
    );
    assert_fenced(users.import_as_head(&bundle));
    assert_fenced(users.restore_backup(&backup));
    assert_fenced(users.keep_last(1));
    assert_fenced(prolly.versioned_maps_transaction(|maps| {
        maps.put(b"users", b"user-3", b"Lin")?;
        Ok(())
    }));

    let hidden_index_id = index_map_id(b"users", b"by-status", &Cid([7; 32]));
    assert!(matches!(
        prolly.versioned_map(&hidden_index_id).put(b"term", b"corrupt"),
        Err(Error::IndexesRequireIndexedMap { map_id, .. }) if map_id == hidden_index_id
    ));
    let hidden_catalog_id = catalog_map_id(b"users");
    assert!(matches!(
        prolly.versioned_map(&hidden_catalog_id).put(b"current", b"corrupt"),
        Err(Error::IndexesRequireIndexedMap { map_id, .. }) if map_id == hidden_catalog_id
    ));

    assert_eq!(users.head().unwrap().unwrap().id, second.id);
}

#[test]
fn indexed_map_open_accepts_an_existing_unindexed_source() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let source = prolly.versioned_map(b"users");
    let source_version = source.put(b"user-1", b"Ada").unwrap();
    let registry = SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-status", 1, "app.users.by-status/v1", |_, _| {
                Ok(vec![b"active".to_vec()])
            })
            .unwrap(),
        )
        .unwrap();

    let indexed = prolly.indexed_map(b"users", registry).unwrap();
    let health = indexed.health().unwrap();
    assert_eq!(health.source_map_id, b"users");
    assert_eq!(health.source_version, Some(source_version.id));
    assert_eq!(health.catalog_version, None);
    assert!(health.active_indexes.is_empty());
    assert!(health.supports_transactions);
}

#[test]
fn indexed_map_open_fails_closed_when_control_has_no_catalog() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    prolly.versioned_map(b"users").initialize().unwrap();
    install_control(&prolly, b"users");
    let registry = SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-status", 1, "app.users.by-status/v1", |_, _| {
                Ok(vec![b"active".to_vec()])
            })
            .unwrap(),
        )
        .unwrap();

    assert!(matches!(
        prolly.indexed_map(b"users", registry),
        Err(Error::InvalidVersionedMap(_))
    ));
}

#[test]
fn ensure_index_builds_a_populated_source_and_activates_atomically() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let source = prolly.versioned_map(b"users");
    source.put(b"user-1", b"active,red").unwrap();
    let source_version = source.put(b"user-2", b"inactive,blue").unwrap();
    let by_tag = SecondaryIndex::non_unique("by-tag", 1, "app.users.by-tag/v1", |_, value| {
        Ok(value
            .split(|byte| *byte == b',')
            .map(|term| term.to_vec())
            .collect())
    })
    .unwrap();
    let registry = SecondaryIndexRegistry::new().register(by_tag).unwrap();
    let indexed = prolly.indexed_map(b"users", registry.clone()).unwrap();

    let built = indexed.ensure_index(b"by-tag").unwrap();
    assert!(built.activated);
    assert_eq!(built.source_version, source_version.id);
    assert_eq!(built.entries, 4);
    assert_eq!(built.attempts, 1);

    let health = indexed.health().unwrap();
    assert_eq!(health.active_indexes.len(), 1);
    assert_eq!(health.active_indexes[0].name, b"by-tag");
    assert_eq!(health.active_indexes[0].index_version, built.index_version);
    let hidden = prolly.versioned_map(&health.active_indexes[0].index_map_id);
    let hidden_snapshot = hidden.snapshot().unwrap().unwrap();
    assert_eq!(
        hidden_snapshot
            .range(&[], None)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .len(),
        4
    );
    assert_eq!(
        hidden_snapshot
            .get(&prolly::physical_index_key(b"red", b"user-1").unwrap())
            .unwrap(),
        Some(Vec::new())
    );

    let idempotent = indexed.ensure_index(b"by-tag").unwrap();
    assert!(!idempotent.activated);
    assert_eq!(idempotent.index_version, built.index_version);
    assert!(matches!(
        source.put(b"user-3", b"active,green"),
        Err(Error::IndexesRequireIndexedMap { .. })
    ));

    assert!(matches!(
        prolly.indexed_map(b"users", SecondaryIndexRegistry::new()),
        Err(Error::IndexRuntimeDefinitionMissing { name, generation: 1 }) if name == b"by-tag"
    ));
    let mismatched = SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-tag", 1, "app.users.by-tag/v2", |_, _| Ok(Vec::new()))
                .unwrap(),
        )
        .unwrap();
    assert!(matches!(
        prolly.indexed_map(b"users", mismatched),
        Err(Error::IndexDefinitionMismatch { name, .. }) if name == b"by-tag"
    ));
}

#[test]
fn ensure_index_initializes_an_absent_source() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let registry = SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-tag", 1, "app.users.by-tag/v1", |_, _| Ok(Vec::new()))
                .unwrap(),
        )
        .unwrap();
    let indexed = prolly.indexed_map(b"users", registry).unwrap();
    let built = indexed.ensure_index(b"by-tag").unwrap();
    assert!(built.activated);
    assert_eq!(built.entries, 0);
    assert!(indexed.source().head().unwrap().is_some());
    assert_eq!(indexed.health().unwrap().active_indexes.len(), 1);
}

#[test]
fn ensure_index_builds_sparse_include_and_all_projections() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let source = prolly.versioned_map(b"users");
    source.put(b"user-1", b"Ada").unwrap();
    source.put(b"skip", b"ignored").unwrap();
    let include = SecondaryIndex::builder("by-name", 1, "app.users.by-name/v1")
        .projection(IndexProjection::Include)
        .extract(|key, value| {
            if key == b"skip" {
                Ok(Vec::new())
            } else {
                Ok(vec![SecondaryIndexEntry::included(value, b"display")])
            }
        })
        .unwrap();
    let all = SecondaryIndex::builder("by-name-all", 1, "app.users.by-name-all/v1")
        .projection(IndexProjection::All)
        .extract_terms(|key, value| {
            if key == b"skip" {
                Ok(Vec::new())
            } else {
                Ok(vec![value.to_vec()])
            }
        })
        .unwrap();
    let registry = SecondaryIndexRegistry::new()
        .register(include)
        .unwrap()
        .register(all)
        .unwrap();
    let indexed = prolly.indexed_map(b"users", registry).unwrap();
    indexed.ensure_index(b"by-name").unwrap();
    indexed.ensure_index(b"by-name-all").unwrap();

    let health = indexed.health().unwrap();
    for (name, expected) in [
        (
            b"by-name".as_slice(),
            prolly::IndexValue::Included(b"display".to_vec()),
        ),
        (
            b"by-name-all".as_slice(),
            prolly::IndexValue::FullSource(b"Ada".to_vec()),
        ),
    ] {
        let active = health
            .active_indexes
            .iter()
            .find(|active| active.name == name)
            .unwrap();
        let value = prolly
            .versioned_map(&active.index_map_id)
            .snapshot()
            .unwrap()
            .unwrap()
            .get(&prolly::physical_index_key(b"Ada", b"user-1").unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(
            prolly::IndexValue::from_bytes(&value, 1024).unwrap(),
            expected
        );
    }
}

#[test]
fn indexed_writes_maintain_keys_only_indexes_incrementally() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let source = prolly.versioned_map(b"users");
    source.put(b"user-1", b"red").unwrap();
    source.put(b"user-2", b"blue").unwrap();
    let registry = SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-tag", 1, "app.users.by-tag/v1", |_, value| {
                Ok(vec![value.to_vec()])
            })
            .unwrap(),
        )
        .unwrap();
    let indexed = prolly.indexed_map(b"users", registry).unwrap();
    indexed.ensure_index(b"by-tag").unwrap();

    let changed = indexed
        .edit(|edit| {
            edit.put(b"user-1", b"green");
            edit.put(b"user-1", b"yellow");
            edit.delete(b"user-2");
            edit.put(b"user-3", b"red");
        })
        .unwrap();
    assert_eq!(indexed.get(b"user-1").unwrap(), Some(b"yellow".to_vec()));
    assert_eq!(indexed.get(b"user-2").unwrap(), None);
    assert_eq!(indexed.get(b"user-3").unwrap(), Some(b"red".to_vec()));
    let health = indexed.health().unwrap();
    assert_eq!(health.source_version.as_ref(), Some(&changed.source.id));
    assert_eq!(
        health.catalog_version.as_ref(),
        Some(&changed.catalog.as_ref().unwrap().id)
    );
    let active = &health.active_indexes[0];
    let snapshot = prolly
        .versioned_map(&active.index_map_id)
        .snapshot()
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot
            .get(&prolly::physical_index_key(b"yellow", b"user-1").unwrap())
            .unwrap(),
        Some(Vec::new())
    );
    assert_eq!(
        snapshot
            .get(&prolly::physical_index_key(b"blue", b"user-2").unwrap())
            .unwrap(),
        None
    );
    assert_eq!(
        snapshot
            .get(&prolly::physical_index_key(b"red", b"user-3").unwrap())
            .unwrap(),
        Some(Vec::new())
    );

    let no_op = indexed.put(b"user-1", b"yellow").unwrap();
    assert_eq!(no_op.source.id, changed.source.id);
    assert_eq!(
        no_op.catalog.as_ref().unwrap().id,
        changed.catalog.as_ref().unwrap().id
    );
}

#[test]
fn indexed_writes_update_projections_and_abort_extractor_failures() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let source = prolly.versioned_map(b"users");
    source.put(b"user-1", b"active|Ada").unwrap();
    let include = SecondaryIndex::builder("by-status", 1, "app.users.by-status/v1")
        .projection(IndexProjection::Include)
        .extract(|_, value| {
            if value == b"bad" {
                return Err(SecondaryIndexError::new("bad record"));
            }
            let mut parts = value.splitn(2, |byte| *byte == b'|');
            let term = parts.next().unwrap();
            let projection = parts.next().unwrap_or_default();
            Ok(vec![SecondaryIndexEntry::included(term, projection)])
        })
        .unwrap();
    let registry = SecondaryIndexRegistry::new().register(include).unwrap();
    let indexed = prolly.indexed_map(b"users", registry).unwrap();
    indexed.ensure_index(b"by-status").unwrap();
    let before = indexed.health().unwrap();

    let changed = indexed.put(b"user-1", b"active|Ada Lovelace").unwrap();
    let active = &indexed.health().unwrap().active_indexes[0];
    let encoded = prolly
        .versioned_map(&active.index_map_id)
        .snapshot()
        .unwrap()
        .unwrap()
        .get(&prolly::physical_index_key(b"active", b"user-1").unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(
        prolly::IndexValue::from_bytes(&encoded, 1024).unwrap(),
        prolly::IndexValue::Included(b"Ada Lovelace".to_vec())
    );
    assert_ne!(active.index_version, before.active_indexes[0].index_version);

    let failed_source = changed.source.id.clone();
    let failed_catalog = changed.catalog.as_ref().unwrap().id.clone();
    assert!(matches!(
        indexed.put(b"user-2", b"bad"),
        Err(Error::IndexExtractionFailed { .. })
    ));
    let after_failure = indexed.health().unwrap();
    assert_eq!(after_failure.source_version, Some(failed_source));
    assert_eq!(after_failure.catalog_version, Some(failed_catalog));

    let stale = indexed
        .apply_if(
            before.source_version.as_ref(),
            vec![Mutation::Delete {
                key: b"user-1".to_vec(),
            }],
        )
        .unwrap();
    assert!(stale.is_conflict());

    let metrics = indexed.metrics();
    assert!(metrics.normalized_source_mutations >= 1);
    assert!(metrics.records_extracted >= 1);
    assert!(metrics.terms_emitted >= 2);
    assert!(metrics.physical_upserts >= 1);
    assert!(metrics.projected_bytes >= b"Ada Lovelace".len() as u64);
}

#[test]
fn concurrent_indexed_writers_retry_from_fresh_coordinated_heads() {
    let prolly = Arc::new(Prolly::new(Arc::new(MemStore::new()), Config::default()));
    let registry = SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-tag", 1, "app.users.by-tag/v1", |_, value| {
                Ok(vec![value.to_vec()])
            })
            .unwrap(),
        )
        .unwrap();
    prolly
        .indexed_map(b"users", registry.clone())
        .unwrap()
        .ensure_index(b"by-tag")
        .unwrap();

    std::thread::scope(|scope| {
        for (key, value) in [
            (b"user-1".as_slice(), b"red".as_slice()),
            (b"user-2".as_slice(), b"blue".as_slice()),
        ] {
            let prolly = prolly.clone();
            let registry = registry.clone();
            scope.spawn(move || {
                prolly
                    .indexed_map(b"users", registry)
                    .unwrap()
                    .put(key, value)
                    .unwrap();
            });
        }
    });

    let indexed = prolly.indexed_map(b"users", registry).unwrap();
    assert_eq!(indexed.get(b"user-1").unwrap(), Some(b"red".to_vec()));
    assert_eq!(indexed.get(b"user-2").unwrap(), Some(b"blue".to_vec()));
    let active = indexed.health().unwrap().active_indexes.remove(0);
    let snapshot = prolly
        .versioned_map(active.index_map_id)
        .snapshot()
        .unwrap()
        .unwrap();
    assert!(snapshot
        .get(&prolly::physical_index_key(b"red", b"user-1").unwrap())
        .unwrap()
        .is_some());
    assert!(snapshot
        .get(&prolly::physical_index_key(b"blue", b"user-2").unwrap())
        .unwrap()
        .is_some());
}

#[test]
fn indexed_snapshot_queries_are_exact_projected_paged_and_historical() {
    let prolly = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let source = prolly.versioned_map(b"users");
    source.put(b"user-2", b"active|Grace").unwrap();
    source.put(b"user-1", b"active|Ada").unwrap();
    source.put(b"user-3", b"archived|Lin").unwrap();
    source
        .put(b"user-4", vec![0, 0xff, b'|', b'B', b'y', b't', b'e'])
        .unwrap();

    let keys = SecondaryIndex::non_unique("by-status", 1, "app.users.by-status/v1", |_, value| {
        Ok(vec![value
            .split(|byte| *byte == b'|')
            .next()
            .unwrap()
            .to_vec()])
    })
    .unwrap();
    let include = SecondaryIndex::builder("by-status-name", 1, "app.users.by-status-name/v1")
        .projection(IndexProjection::Include)
        .extract(|_, value| {
            let mut fields = value.splitn(2, |byte| *byte == b'|');
            Ok(vec![SecondaryIndexEntry::included(
                fields.next().unwrap(),
                fields.next().unwrap_or_default(),
            )])
        })
        .unwrap();
    let all = SecondaryIndex::builder("by-status-all", 1, "app.users.by-status-all/v1")
        .projection(IndexProjection::All)
        .extract_terms(|_, value| {
            Ok(vec![value
                .split(|byte| *byte == b'|')
                .next()
                .unwrap()
                .to_vec()])
        })
        .unwrap();
    let registry = SecondaryIndexRegistry::new()
        .register(keys)
        .unwrap()
        .register(include)
        .unwrap()
        .register(all)
        .unwrap();
    let indexed = prolly.indexed_map(b"users", registry).unwrap();
    indexed.ensure_index(b"by-status").unwrap();
    indexed.ensure_index(b"by-status-name").unwrap();
    indexed.ensure_index(b"by-status-all").unwrap();

    let historical = indexed.snapshot().unwrap();
    let historical_id = historical.id().clone();
    let by_status = historical.index(b"by-status").unwrap();
    assert_eq!(
        by_status.primary_keys(b"active").unwrap(),
        vec![b"user-1".to_vec(), b"user-2".to_vec()]
    );
    assert_eq!(
        by_status.records(b"active").unwrap(),
        vec![
            (b"user-1".to_vec(), b"active|Ada".to_vec()),
            (b"user-2".to_vec(), b"active|Grace".to_vec()),
        ]
    );
    assert_eq!(
        historical
            .index(b"by-status-name")
            .unwrap()
            .projected(b"active")
            .unwrap(),
        vec![
            (b"user-1".to_vec(), Some(b"Ada".to_vec())),
            (b"user-2".to_vec(), Some(b"Grace".to_vec())),
        ]
    );
    assert_eq!(
        historical
            .index(b"by-status-all")
            .unwrap()
            .projected(b"active")
            .unwrap()[0]
            .1,
        Some(b"active|Ada".to_vec())
    );

    let first = by_status.exact_page(b"active", None, 1).unwrap();
    assert_eq!(first.matches[0].primary_key, b"user-1");
    let encoded_cursor = first.next_cursor.as_ref().unwrap().to_bytes().unwrap();
    let cursor = prolly::SecondaryIndexCursor::from_bytes(&encoded_cursor).unwrap();
    let second = by_status.exact_page(b"active", Some(&cursor), 1).unwrap();
    assert_eq!(second.matches[0].primary_key, b"user-2");
    assert!(second.next_cursor.is_none());
    assert!(matches!(
        by_status.prefix_page(b"act", Some(&cursor), 1),
        Err(Error::IndexCursorVersionMismatch { .. })
    ));

    let reverse = by_status.exact_reverse_page(b"active", None, 1).unwrap();
    assert_eq!(reverse.matches[0].primary_key, b"user-2");
    let prefix = by_status.prefix(b"arch").unwrap();
    assert_eq!(prefix[0].term, b"archived");
    assert_eq!(by_status.prefix(&[0]).unwrap()[0].term, vec![0, 0xff]);
    assert_eq!(
        by_status.range(&[0], Some(&[1])).unwrap()[0].primary_key,
        b"user-4"
    );
    let range = by_status.range(b"active", Some(b"archived")).unwrap();
    assert_eq!(range.len(), 2);

    indexed.put(b"user-1", b"disabled|Ada").unwrap();
    let current = indexed.snapshot().unwrap();
    assert!(matches!(
        current
            .index(b"by-status")
            .unwrap()
            .exact_page(b"active", Some(&cursor), 1),
        Err(Error::IndexCursorVersionMismatch { .. })
    ));
    assert_eq!(
        indexed
            .snapshot_by_id(&historical_id)
            .unwrap()
            .index(b"by-status")
            .unwrap()
            .primary_keys(b"active")
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        indexed
            .snapshot_at(&historical_id.source_version)
            .unwrap()
            .id()
            .source_version,
        historical_id.source_version
    );
}
