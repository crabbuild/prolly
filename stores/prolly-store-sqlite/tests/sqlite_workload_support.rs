#[path = "../benches/sqlite_workload_support.rs"]
mod support;

#[test]
fn generated_sets_are_deterministic_unique_and_bounded() {
    assert_eq!(support::sample_count(1_000), 100);
    assert_eq!(support::sample_count(100_000), 1_000);
    assert_eq!(support::sample_count(10_000_000), 10_000);

    let random = support::random_indexes(100_000, 1_000, support::RANDOM_SEED);
    assert_eq!(random.len(), 1_000);
    assert!(random.windows(2).all(|pair| pair[0] < pair[1]));
    assert_eq!(
        random,
        support::random_indexes(100_000, 1_000, support::RANDOM_SEED)
    );

    let clustered = support::clustered_indexes(100_000, 1_000);
    assert_eq!(clustered.len(), 1_000);
    assert!(
        clustered
            .windows(2)
            .all(|pair| pair[1] == pair[0] + 1)
    );

    let right_edge = support::right_edge_indexes(100_000, 10_000);
    assert_eq!(right_edge.first(), Some(&90_000));
    assert_eq!(right_edge.last(), Some(&99_999));

    let shuffled = support::shuffled_ids(10_000, support::RANDOM_SEED);
    assert_eq!(shuffled.len(), 10_000);
    assert_ne!(shuffled, (0..10_000).collect::<Vec<_>>());
    let mut sorted = shuffled;
    sorted.sort_unstable();
    assert_eq!(sorted, (0..10_000).collect::<Vec<_>>());
}

#[test]
fn workload_and_profile_names_round_trip() {
    for name in ["full", "normal"] {
        assert_eq!(
            support::DurabilityProfile::parse(name).unwrap().as_str(),
            name
        );
    }

    for name in support::Workload::ALL_NAMES {
        assert_eq!(support::Workload::parse(name).unwrap().as_str(), *name);
    }

    assert!(support::DurabilityProfile::parse("off").is_err());
    assert!(support::Workload::parse("unknown").is_err());
}

#[test]
fn operation_counts_and_encoded_records_are_exact() {
    assert_eq!(support::merge_count(1_000), 50);
    assert_eq!(support::merge_count(1_000_000), 5_000);
    assert_eq!(support::key(42), b"key-00000000000000000042");
    assert_eq!(
        support::value(42, 7),
        b"value-00000000000000000042-07-payload"
    );
}

#[test]
fn csv_rows_match_the_declared_schema() {
    let row = support::CsvRow::example();
    let header = support::CsvRow::header();
    let encoded = row.to_csv();
    assert_eq!(header.split(',').count(), encoded.split(',').count());
    assert!(encoded.contains("current,full,1000,1,sorted_stream_build"));
    assert!(encoded.ends_with(",true,ok"));
}

#[test]
fn mutation_workloads_have_exact_indexes_and_cardinality() {
    use support::Workload;

    let append = support::mutation_indexes(Workload::AppendBatchUpserts, 1_000, 100);
    assert_eq!(append.first(), Some(&1_000));
    assert_eq!(append.last(), Some(&1_099));
    assert_eq!(
        support::expected_result_entries(Workload::AppendBatchUpserts, 1_000, 100),
        1_100
    );

    for workload in [
        Workload::RandomBatchUpdates,
        Workload::ClusteredBatchUpdates,
    ] {
        let indexes = support::mutation_indexes(workload, 1_000, 100);
        assert_eq!(indexes.len(), 100);
        assert!(indexes.iter().all(|id| *id < 1_000));
        assert_eq!(
            support::expected_result_entries(workload, 1_000, 100),
            1_000
        );
    }

    for workload in [
        Workload::RandomBatchDeletes,
        Workload::ClusteredBatchDeletes,
    ] {
        assert_eq!(
            support::expected_result_entries(workload, 1_000, 100),
            900
        );
    }
}
