use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use prolly::{
    indexed_collection_root_name, Config, Error, IndexProjection, IndexedSnapshotBundle, MemStore,
    Mutation, MutationBudget, Prolly, QueryBudget, RetryAdvice, SecondaryIndex,
    SecondaryIndexEntry, SecondaryIndexRegistry, TransferBudget,
};

fn registry() -> SecondaryIndexRegistry {
    SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-value", 1, "test.by-value/1", |_, value| {
                Ok(vec![value.to_vec()])
            })
            .unwrap(),
        )
        .unwrap()
        .register(
            SecondaryIndex::builder("included", 1, "test.included/1")
                .projection(IndexProjection::Include)
                .extract(|_, value| Ok(vec![SecondaryIndexEntry::included(value, b"projection")]))
                .unwrap(),
        )
        .unwrap()
}

fn engine() -> Prolly<Arc<MemStore>> {
    Prolly::new(Arc::new(MemStore::new()), Config::default())
}

#[test]
fn hard_cutover_rejects_an_obsolete_versioned_source() {
    let engine = engine();
    engine
        .versioned_map(b"users")
        .put(b"legacy", b"value")
        .unwrap();
    assert!(matches!(
        engine.indexed_map(b"users", registry()),
        Err(Error::IndexFormatUnsupported)
    ));
}

#[test]
fn canonical_mutation_build_query_and_history_are_coherent() {
    let engine = engine();
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    let first = indexed.put(b"u1", b"active").unwrap();
    indexed.put(b"u2", b"active").unwrap();
    let build = indexed.ensure_index(b"by-value").unwrap();
    assert!(build.activated);

    let snapshot = indexed.snapshot().unwrap();
    assert_eq!(
        snapshot
            .index(b"by-value")
            .unwrap()
            .primary_keys(b"active")
            .unwrap(),
        vec![b"u1".to_vec(), b"u2".to_vec()]
    );
    assert!(indexed
        .snapshot_at(&first.source.id)
        .unwrap()
        .index(b"by-value")
        .is_err());

    let before = snapshot.id().clone();
    indexed.put(b"u1", b"inactive").unwrap();
    assert_eq!(
        indexed
            .snapshot_by_id(&before)
            .unwrap()
            .index(b"by-value")
            .unwrap()
            .primary_keys(b"active")
            .unwrap(),
        vec![b"u1".to_vec(), b"u2".to_vec()]
    );
    let current = indexed.snapshot().unwrap();
    assert_eq!(
        current
            .index(b"by-value")
            .unwrap()
            .primary_keys(b"active")
            .unwrap(),
        vec![b"u2".to_vec()]
    );
}

#[test]
fn raw_versioned_writes_are_fenced_after_canonical_initialization() {
    let engine = engine();
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    indexed.put(b"u1", b"active").unwrap();
    assert!(matches!(
        engine.versioned_map(b"users").put(b"u2", b"active"),
        Err(Error::IndexesRequireIndexedMap { .. })
    ));
}

#[test]
fn mutation_budget_failure_does_not_publish() {
    let engine = engine();
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    let before = indexed.health().unwrap().state_version;
    let budget = MutationBudget {
        max_input_records: 1,
        max_input_bytes: 4,
        max_derived_entries: 1,
        max_derived_bytes: 4,
        max_accounted_memory_bytes: 8,
        max_cas_attempts: 1,
        max_elapsed: Duration::from_secs(1),
    };
    assert!(matches!(
        indexed.apply_with_budget(
            vec![Mutation::Upsert {
                key: b"key".to_vec(),
                val: b"value".to_vec(),
            }],
            &budget,
        ),
        Err(Error::IndexResourceLimitExceeded { .. })
    ));
    assert_eq!(indexed.health().unwrap().state_version, before);
    assert_eq!(indexed.get(b"key").unwrap(), None);
}

#[test]
fn query_pages_are_bounded_and_cursors_are_snapshot_bound() {
    let engine = engine();
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    for id in 0..10 {
        indexed
            .put(format!("u{id:02}"), b"active".to_vec())
            .unwrap();
    }
    indexed.ensure_index(b"by-value").unwrap();
    let snapshot = indexed.snapshot().unwrap();
    let index = snapshot.index(b"by-value").unwrap();
    let first = index.exact_page(b"active", None, 3).unwrap();
    assert_eq!(first.matches.len(), 3);
    let cursor = first.next_cursor.unwrap();
    let second = index.exact_page(b"active", Some(&cursor), 3).unwrap();
    assert_eq!(second.matches.len(), 3);
    assert!(matches!(
        index.exact_page(b"active", None, usize::MAX),
        Err(Error::IndexResourceLimitExceeded { .. })
    ));

    indexed.put(b"u99", b"active").unwrap();
    let new_snapshot = indexed.snapshot().unwrap();
    assert!(matches!(
        new_snapshot
            .index(b"by-value")
            .unwrap()
            .exact_page(b"active", Some(&cursor), 3),
        Err(Error::IndexCursorVersionMismatch { .. })
    ));
    assert!(QueryBudget::default().validate().is_ok());
}

#[test]
fn query_budgets_reject_retained_pages_and_source_joins_before_return() {
    let engine = engine();
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    indexed.put(b"u1", vec![b'x'; 256]).unwrap();
    indexed.put(b"u2", vec![b'x'; 256]).unwrap();
    indexed.ensure_index(b"included").unwrap();
    let snapshot = indexed.snapshot().unwrap();
    let index = snapshot.index(b"included").unwrap();
    let tiny = QueryBudget {
        max_page_entries: 2,
        max_returned_entries: 2,
        max_returned_bytes: 64,
        max_scanned_entries: 4,
        max_source_fetches: 2,
        max_accounted_memory_bytes: 64,
        max_elapsed: Duration::from_secs(1),
    };
    let query = index.query(tiny).unwrap();
    assert!(matches!(
        query.exact_page(&vec![b'x'; 256], None, 1),
        Err(Error::IndexResourceLimitExceeded { .. })
    ));
    let source_limited = QueryBudget {
        max_source_fetches: 1,
        ..QueryBudget::default()
    };
    assert!(matches!(
        index
            .query(source_limited)
            .unwrap()
            .records(&vec![b'x'; 256]),
        Err(Error::IndexResourceLimitExceeded { .. })
    ));
}

#[test]
fn concurrent_writers_publish_complete_snapshots() {
    let engine = Arc::new(engine());
    engine
        .indexed_map(b"users", registry())
        .unwrap()
        .ensure_index(b"by-value")
        .unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let mut workers = Vec::new();
    for (key, value) in [
        (b"a".to_vec(), b"x".to_vec()),
        (b"b".to_vec(), b"y".to_vec()),
    ] {
        let engine = Arc::clone(&engine);
        let barrier = Arc::clone(&barrier);
        workers.push(thread::spawn(move || {
            let indexed = engine.indexed_map(b"users", registry()).unwrap();
            barrier.wait();
            indexed.put(key, value).unwrap();
        }));
    }
    barrier.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    let snapshot = indexed.snapshot().unwrap();
    assert_eq!(snapshot.source().get(b"a").unwrap(), Some(b"x".to_vec()));
    assert_eq!(snapshot.source().get(b"b").unwrap(), Some(b"y".to_vec()));
    assert_eq!(
        snapshot
            .index(b"by-value")
            .unwrap()
            .primary_keys(b"x")
            .unwrap(),
        vec![b"a".to_vec()]
    );
}

#[test]
fn extractor_failure_is_atomic() {
    let failing = SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("failing", 1, "test.failing/1", |key, value| {
                if value == b"bad" {
                    Err(prolly::SecondaryIndexError::new("rejected"))
                } else {
                    Ok(vec![key.to_vec()])
                }
            })
            .unwrap(),
        )
        .unwrap();
    let engine = engine();
    let indexed = engine.indexed_map(b"users", failing).unwrap();
    indexed.put(b"u1", b"good").unwrap();
    indexed.ensure_index(b"failing").unwrap();
    let before = indexed.health().unwrap().state_version;
    assert!(matches!(
        indexed.put(b"u1", b"bad"),
        Err(Error::IndexExtractionFailed { .. })
    ));
    assert_eq!(indexed.health().unwrap().state_version, before);
    assert_eq!(indexed.get(b"u1").unwrap(), Some(b"good".to_vec()));
}

#[test]
fn replacement_retention_and_verification_keep_exact_generations() {
    let engine = engine();
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    indexed.put(b"u1", b"active").unwrap();
    indexed.ensure_index(b"by-value").unwrap();
    let old = indexed.snapshot().unwrap().source_version().clone();
    let replacement = SecondaryIndex::non_unique("by-value", 2, "test.by-value/2", |key, _| {
        Ok(vec![key.to_vec()])
    })
    .unwrap();
    indexed.replace_index(b"by-value", replacement).unwrap();
    let verification = indexed
        .verify_index(b"by-value", indexed.snapshot().unwrap().source_version())
        .unwrap();
    assert!(verification.is_valid());
    let retained = indexed.keep_last(2).unwrap();
    assert!(retained.retained_source_versions.contains(&old));
}

#[test]
fn canonical_bundle_is_bounded_verified_and_atomically_imported() {
    let source_engine = engine();
    let source = source_engine.indexed_map(b"users", registry()).unwrap();
    source.put(b"u1", b"active").unwrap();
    source.ensure_index(b"included").unwrap();
    let bundle = source.export_current().unwrap();
    let bytes = bundle.to_bytes().unwrap();
    assert_eq!(
        IndexedSnapshotBundle::from_bytes(&bytes)
            .unwrap()
            .to_bytes()
            .unwrap(),
        bytes
    );
    let destination_engine = engine();
    let destination = destination_engine
        .indexed_map(b"users", registry())
        .unwrap();
    destination.import_current(&bundle, None).unwrap();
    assert_eq!(
        destination
            .snapshot()
            .unwrap()
            .index(b"included")
            .unwrap()
            .projected(b"active")
            .unwrap()[0]
            .1,
        Some(b"projection".to_vec())
    );

    let tiny = TransferBudget {
        max_encoded_bytes: 1,
        ..TransferBudget::default()
    };
    assert!(matches!(
        source.export_current_with_budget(&tiny),
        Err(Error::IndexResourceLimitExceeded { .. })
    ));
    assert!(matches!(
        IndexedSnapshotBundle::from_bytes_with_budget(&bytes, &tiny),
        Err(Error::IndexResourceLimitExceeded { .. })
    ));
    let mut corrupt = bytes;
    corrupt.push(0);
    assert!(IndexedSnapshotBundle::from_bytes(&corrupt).is_err());
}

#[test]
fn only_the_canonical_root_controls_visibility() {
    let engine = engine();
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    indexed.put(b"u1", b"active").unwrap();
    let root = indexed_collection_root_name(b"users").unwrap();
    assert!(engine.load_named_root(&root).unwrap().is_some());
    assert!(engine.versioned_map(b"users").head().unwrap().is_none());
}

#[test]
fn durable_pins_protect_history_until_released() {
    let engine = engine();
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    indexed.put(b"u1", b"first").unwrap();
    indexed.ensure_index(b"by-value").unwrap();
    let old = indexed.snapshot().unwrap().source_version().clone();
    let pin = indexed.pin_snapshot(b"reader-1", &old).unwrap();
    indexed.put(b"u1", b"second").unwrap();
    indexed.keep_last(1).unwrap();
    assert!(indexed.snapshot_at(&old).is_ok());
    pin.release().unwrap();
    indexed.keep_last(1).unwrap();
    assert!(indexed.snapshot_at(&old).is_err());
}

#[test]
fn index_errors_are_structured_and_redact_application_data() {
    let sentinel = b"secret-primary-key".to_vec();
    let error = Error::IndexExtractionFailed {
        name: b"secret-index-name".to_vec(),
        primary_key: sentinel.clone(),
        reason: "secret extractor detail".to_string(),
    };
    let rendered = error.to_string();
    assert!(!rendered.contains("secret"));
    assert!(!format!("{error:?}").contains("secret"));
    assert_eq!(error.retry_advice(), RetryAdvice::Never);
    assert!(error.index_code().is_some());
}
