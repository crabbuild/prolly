use std::sync::Arc;
use std::time::Duration;

use prolly::{
    Config, Error, MaintenanceBudget, MemStore, Prolly, SecondaryIndex, SecondaryIndexRegistry,
};

fn registry() -> SecondaryIndexRegistry {
    SecondaryIndexRegistry::new()
        .register(
            SecondaryIndex::non_unique("by-value", 1, "resources.by-value/1", |key, value| {
                Ok(vec![[value, key].concat()])
            })
            .unwrap(),
        )
        .unwrap()
}

fn populated() -> Prolly<Arc<MemStore>> {
    let engine = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    for value in 0..200 {
        indexed
            .put(format!("key-{value:04}"), format!("value-{value:04}"))
            .unwrap();
    }
    engine
}

#[test]
fn spillable_build_matches_the_canonical_in_memory_root() {
    let control = populated();
    let expected = control
        .indexed_map(b"users", registry())
        .unwrap()
        .ensure_index(b"by-value")
        .unwrap();

    let spilled = populated();
    let budget = MaintenanceBudget {
        max_source_entries: 1_000,
        max_derived_entries: 1_000,
        max_verification_findings: 10,
        max_accounted_memory_bytes: 512,
        max_spill_bytes: 1024 * 1024,
        max_spill_runs: 128,
        max_merge_fan_in: 4,
        max_cas_attempts: 2,
        max_elapsed: Duration::from_secs(10),
    };
    let actual = spilled
        .indexed_map(b"users", registry())
        .unwrap()
        .ensure_index_with_budget(b"by-value", &budget)
        .unwrap();
    assert_eq!(actual.index_version, expected.index_version);
    assert_eq!(actual.entries, expected.entries);
}

#[test]
fn spill_budget_exhaustion_leaves_the_collection_unchanged() {
    let engine = populated();
    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    let before = indexed.health().unwrap().state_version;
    let budget = MaintenanceBudget {
        max_source_entries: 1_000,
        max_derived_entries: 1_000,
        max_verification_findings: 10,
        max_accounted_memory_bytes: 128,
        max_spill_bytes: 1,
        max_spill_runs: 2,
        max_merge_fan_in: 2,
        max_cas_attempts: 1,
        max_elapsed: Duration::from_secs(10),
    };
    assert!(matches!(
        indexed.ensure_index_with_budget(b"by-value", &budget),
        Err(Error::IndexResourceLimitExceeded { .. })
    ));
    assert_eq!(indexed.health().unwrap().state_version, before);
    assert!(indexed.snapshot().unwrap().index(b"by-value").is_err());
}
