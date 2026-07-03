use prolly::{Config, MemStore, Mutation, Prolly};

fn test_prolly() -> Prolly<MemStore> {
    Prolly::new(
        MemStore::new(),
        Config::builder()
            .min_chunk_size(1)
            .max_chunk_size(3)
            .chunking_factor(1_000_000)
            .build(),
    )
}

#[test]
fn batch_with_stats_reports_tree_work() {
    let prolly = test_prolly();
    let tree = prolly.create();

    let result = prolly
        .batch_with_stats(
            &tree,
            vec![
                Mutation::Upsert {
                    key: b"a".to_vec(),
                    val: b"1".to_vec(),
                },
                Mutation::Upsert {
                    key: b"b".to_vec(),
                    val: b"2".to_vec(),
                },
                Mutation::Upsert {
                    key: b"a".to_vec(),
                    val: b"11".to_vec(),
                },
            ],
        )
        .unwrap();

    assert_eq!(
        prolly.get(&result.tree, b"a").unwrap(),
        Some(b"11".to_vec())
    );
    assert_eq!(prolly.get(&result.tree, b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(result.stats.input_mutations, 3);
    assert_eq!(result.stats.effective_mutations, 2);
    assert!(!result.stats.preprocess_input_sorted);
    assert!(result.stats.affected_leaves > 0);
    assert!(result.stats.changed_leaves > 0);
    assert!(result.stats.written_nodes > 0);
    assert!(result.stats.written_bytes > 0);
}

#[test]
fn append_batch_with_stats_reports_fast_path() {
    let prolly = test_prolly();
    let tree = prolly
        .batch(
            &prolly.create(),
            vec![
                Mutation::Upsert {
                    key: b"a".to_vec(),
                    val: b"1".to_vec(),
                },
                Mutation::Upsert {
                    key: b"b".to_vec(),
                    val: b"2".to_vec(),
                },
            ],
        )
        .unwrap();

    let result = prolly
        .append_batch_with_stats(
            &tree,
            vec![
                Mutation::Upsert {
                    key: b"c".to_vec(),
                    val: b"3".to_vec(),
                },
                Mutation::Upsert {
                    key: b"d".to_vec(),
                    val: b"4".to_vec(),
                },
            ],
        )
        .unwrap();

    assert_eq!(prolly.get(&result.tree, b"d").unwrap(), Some(b"4".to_vec()));
    assert_eq!(result.stats.input_mutations, 2);
    assert_eq!(result.stats.effective_mutations, 2);
    assert!(result.stats.preprocess_input_sorted);
    assert!(result.stats.used_append_fast_path);
    assert!(result.stats.written_nodes > 0);
}

#[test]
fn append_batch_with_stats_reports_fallback_path() {
    let prolly = test_prolly();
    let tree = prolly
        .batch(
            &prolly.create(),
            vec![
                Mutation::Upsert {
                    key: b"a".to_vec(),
                    val: b"1".to_vec(),
                },
                Mutation::Upsert {
                    key: b"b".to_vec(),
                    val: b"2".to_vec(),
                },
            ],
        )
        .unwrap();

    let result = prolly
        .append_batch_with_stats(
            &tree,
            vec![Mutation::Upsert {
                key: b"a".to_vec(),
                val: b"11".to_vec(),
            }],
        )
        .unwrap();

    assert_eq!(
        prolly.get(&result.tree, b"a").unwrap(),
        Some(b"11".to_vec())
    );
    assert_eq!(result.stats.input_mutations, 1);
    assert_eq!(result.stats.effective_mutations, 1);
    assert!(!result.stats.used_append_fast_path);
    assert!(result.stats.changed_leaves > 0);
}
