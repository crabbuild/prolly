use prolly_sqlite_pattern_bench::cli::parse_args;
use prolly_sqlite_pattern_bench::model::{
    change_count, key, mutation_ids, value, Pattern,
};

#[test]
fn record_widths_and_samples_are_exact() {
    assert_eq!(key(42).len(), 24);
    assert_eq!(value(42, 0).len(), 100);
    assert_eq!(value(42, 1).len(), 100);
    assert_ne!(value(42, 0), value(42, 1));
    assert_eq!(change_count(10_000), 100);
    assert_eq!(change_count(50_000), 500);
    assert_eq!(change_count(1_000_000), 10_000);
}

#[test]
fn patterns_are_deterministic_and_semantically_distinct() {
    let random = mutation_ids(Pattern::Random, 10_000, 100);
    assert_eq!(random, mutation_ids(Pattern::Random, 10_000, 100));
    assert_eq!(random.len(), 100);
    assert!(random.iter().all(|id| *id < 10_000));
    assert_eq!(
        mutation_ids(Pattern::Append, 10_000, 3),
        vec![10_000, 10_001, 10_002]
    );
    assert_eq!(
        mutation_ids(Pattern::Clustered, 10_000, 3),
        vec![4_998, 4_999, 5_000]
    );
}

#[test]
fn smoke_profile_can_be_selected() {
    let config = parse_args([
        "bench",
        "--profile",
        "smoke",
        "--output",
        "/tmp/sqlite-pattern-smoke",
    ])
    .unwrap();
    assert_eq!(config.sizes, vec![100]);
    assert_eq!(config.runs, 1);
    assert_eq!(config.explicit_operations, Some(10));
}
