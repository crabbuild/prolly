use prolly::{splice, BatchBuilder, Config, MemStore, Mutation, Prolly};
use std::collections::BTreeMap;
use std::sync::Arc;

fn config() -> Config {
    Config::builder()
        .min_chunk_size(4)
        .max_chunk_size(8)
        .chunking_factor(8)
        .hash_seed(41)
        .build()
}

fn clean_build(
    store: Arc<MemStore>,
    config: Config,
    records: &BTreeMap<Vec<u8>, Vec<u8>>,
) -> prolly::Tree {
    let mut builder = BatchBuilder::new(store, config);
    for (key, value) in records {
        builder.add(key.clone(), value.clone());
    }
    builder.build().unwrap()
}

#[test]
fn middle_splice_matches_clean_root_and_reuses_untouched_nodes() {
    let store = Arc::new(MemStore::new());
    let config = config();
    let prolly = Prolly::new(store.clone(), config.clone());
    let mut records = BTreeMap::new();
    for index in 0..256 {
        records.insert(
            format!("key-{index:04}").into_bytes(),
            format!("value-{index:04}").into_bytes(),
        );
    }
    let before = clean_build(store.clone(), config.clone(), &records);
    let key = b"key-0128".to_vec();
    let value = b"replacement".to_vec();
    records.insert(key.clone(), value.clone());

    let (after, stats) =
        splice(&prolly, &before, vec![Mutation::Upsert { key, val: value }]).unwrap();
    let clean = clean_build(store, config, &records);

    assert_eq!(after.root, clean.root);
    assert!(stats.nodes_reused > 0);
    assert!(stats.entries_scanned < records.len());
    assert!(stats.nodes_rebuilt < stats.nodes_reused);
    assert!(stats.nodes_written < stats.nodes_reused + stats.nodes_written);
}

#[test]
fn mixed_splice_history_matches_clean_builder_at_chunk_and_root_edges() {
    let store = Arc::new(MemStore::new());
    let config = config();
    let prolly = Prolly::new(store.clone(), config.clone());
    let mut records = BTreeMap::new();
    for index in 0..96 {
        records.insert(format!("k{index:04}").into_bytes(), vec![index as u8]);
    }
    let mut tree = clean_build(store.clone(), config.clone(), &records);
    let mut state = 0x94d0_49bb_1331_11ebu64;

    for step in 0..128 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let index = (state as usize) % 160;
        let key = format!("k{index:04}").into_bytes();
        let mutation = if state & 3 == 0 {
            records.remove(&key);
            Mutation::Delete { key }
        } else {
            let value = format!("v-{step}-{state:016x}").into_bytes();
            records.insert(key.clone(), value.clone());
            Mutation::Upsert { key, val: value }
        };
        tree = splice(&prolly, &tree, vec![mutation]).unwrap().0;
        let clean = clean_build(store.clone(), config.clone(), &records);
        assert_eq!(tree.root, clean.root, "step {step}");
    }
}

#[test]
fn randomized_multi_mutation_batches_match_the_clean_oracle() {
    run_randomized_histories(0..8, 128, 64);
}

#[test]
#[ignore = "extended 100-seed canonical splice stress suite"]
fn extended_hundred_seed_splice_oracle() {
    run_randomized_histories(0..100, 1_000, 500);
}

fn run_randomized_histories(seeds: std::ops::Range<u64>, initial_records: usize, batches: usize) {
    for seed in seeds {
        let store = Arc::new(MemStore::new());
        let config = config();
        let prolly = Prolly::new(store.clone(), config.clone());
        let mut records = BTreeMap::new();
        for index in 0..initial_records {
            records.insert(
                format!("s{seed:02}-k{index:04}").into_bytes(),
                vec![index as u8],
            );
        }
        let mut tree = clean_build(store.clone(), config.clone(), &records);
        let mut state = 0x517c_c1b7_2722_0a95u64 ^ seed;
        for batch_index in 0..batches {
            let mut by_key = BTreeMap::new();
            for mutation_index in 0..=batch_index % 5 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let index = (state as usize) % (initial_records + initial_records / 2);
                let key = format!("s{seed:02}-k{index:04}").into_bytes();
                let mutation = if state & 7 < 3 {
                    records.remove(&key);
                    Mutation::Delete { key: key.clone() }
                } else {
                    let value = format!("{batch_index}-{mutation_index}-{state:016x}").into_bytes();
                    records.insert(key.clone(), value.clone());
                    Mutation::Upsert {
                        key: key.clone(),
                        val: value,
                    }
                };
                by_key.insert(key, mutation);
            }
            tree = splice(&prolly, &tree, by_key.into_values().collect())
                .unwrap()
                .0;
            let clean = clean_build(store.clone(), config.clone(), &records);
            assert_eq!(tree.root, clean.root, "seed={seed} batch={batch_index}");
        }
    }
}

#[test]
fn empty_growth_collapse_and_noop_delete_are_canonical() {
    let store = Arc::new(MemStore::new());
    let config = config();
    let prolly = Prolly::new(store.clone(), config.clone());
    let empty = prolly.create();
    let (still_empty, stats) = splice(
        &prolly,
        &empty,
        vec![Mutation::Delete {
            key: b"absent".to_vec(),
        }],
    )
    .unwrap();
    assert_eq!(still_empty.root, None);
    assert!(!stats.root_changed);

    let inserts: Vec<_> = (0..48)
        .map(|index| Mutation::Upsert {
            key: format!("edge-{index:03}").into_bytes(),
            val: vec![index as u8],
        })
        .collect();
    let grown = splice(&prolly, &still_empty, inserts).unwrap().0;
    assert!(grown.root.is_some());
    let deletes: Vec<_> = (0..48)
        .map(|index| Mutation::Delete {
            key: format!("edge-{index:03}").into_bytes(),
        })
        .collect();
    let collapsed = splice(&prolly, &grown, deletes).unwrap().0;
    assert_eq!(collapsed.root, None);
}
