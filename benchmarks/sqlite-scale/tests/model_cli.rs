use std::collections::BTreeSet;

use prolly_sqlite_scale_bench::cli::parse_args;
use prolly_sqlite_scale_bench::model::{
    change_count, key, merge_ids, mutation_ids, value, Operation, Pattern,
};

#[test]
fn full_profile_freezes_the_one_million_baseline() {
    let config = parse_args(["bench", "--profile", "full"]).unwrap();
    assert_eq!(
        config.output.to_string_lossy(),
        "performance-results/sqlite/baseline"
    );
    assert_eq!(config.sizes, vec![1_000_000]);
    assert_eq!(config.runs, 3);
    assert_eq!(config.changes, None);
    assert_eq!(config.read_samples, 10_000);
    assert_eq!(config.operations, Operation::ALL);
    assert_eq!(config.patterns, Pattern::ALL);
    assert_eq!(config.min_free_bytes, 3 * 1024 * 1024 * 1024);
    assert_eq!(change_count(1_000_000), 300_000);
}

#[test]
fn smoke_profile_and_filters_are_explicit() {
    let config = parse_args([
        "bench",
        "--profile",
        "smoke",
        "--output",
        "/tmp/sqlite-scale-smoke",
        "--sizes",
        "500,1000",
        "--runs",
        "2",
        "--operations",
        "get_cold,query,diff",
        "--patterns",
        "random,clustered",
        "--changes",
        "25",
        "--read-samples",
        "10",
        "--min-free-gb",
        "1",
    ])
    .unwrap();
    assert_eq!(config.sizes, vec![500, 1_000]);
    assert_eq!(config.runs, 2);
    assert_eq!(config.changes, Some(25));
    assert_eq!(config.read_samples, 10);
    assert_eq!(
        config.operations,
        vec![Operation::GetCold, Operation::Query, Operation::Diff]
    );
    assert_eq!(config.patterns, vec![Pattern::Random, Pattern::Clustered]);
    assert_eq!(config.min_free_bytes, 1024 * 1024 * 1024);
}

#[test]
fn merge_requires_an_even_total_change_count() {
    let error = parse_args([
        "bench",
        "--profile",
        "smoke",
        "--changes",
        "11",
        "--operations",
        "merge",
    ])
    .unwrap_err();
    assert!(error.contains("even"));
}

#[test]
fn record_widths_and_patterns_are_stable() {
    assert_eq!(key(42).len(), 24);
    assert_eq!(value(42, 0).len(), 100);
    assert_eq!(value(42, 1).len(), 100);
    assert_ne!(value(42, 0), value(42, 1));

    let random = mutation_ids(Pattern::Random, 10_000, 1_000, 7);
    assert_eq!(random, mutation_ids(Pattern::Random, 10_000, 1_000, 7));
    assert_eq!(random.len(), 1_000);
    assert!(random.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(random.iter().all(|id| *id < 10_000));
    assert_eq!(
        mutation_ids(Pattern::Append, 10_000, 3, 0),
        vec![10_000, 10_001, 10_002]
    );
    assert_eq!(
        mutation_ids(Pattern::Clustered, 10_000, 4, 0),
        vec![4_998, 4_999, 5_000, 5_001]
    );
}

#[test]
fn merge_total_is_split_into_disjoint_interleaved_branches() {
    for pattern in Pattern::ALL {
        let (left, right) = merge_ids(100_000, 1_000, pattern);
        assert_eq!(left.len(), 500);
        assert_eq!(right.len(), 500);
        let left_set = left.iter().copied().collect::<BTreeSet<_>>();
        let right_set = right.iter().copied().collect::<BTreeSet<_>>();
        assert!(left_set.is_disjoint(&right_set));
    }

    let (left, right) = merge_ids(100_000, 1_000, Pattern::Random);
    let left = left.into_iter().collect::<BTreeSet<_>>();
    let right = right.into_iter().collect::<BTreeSet<_>>();
    let combined = left.union(&right).copied().collect::<Vec<_>>();
    let transitions = combined
        .windows(2)
        .filter(|pair| left.contains(&pair[0]) != left.contains(&pair[1]))
        .count();
    assert!(transitions > 900, "only {transitions} branch transitions");
}
