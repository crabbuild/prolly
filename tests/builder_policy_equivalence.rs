use prolly::{
    chunking, BatchBuilder, Config, MemStore, Mutation, NodeLayoutSpec, Prolly, SortedBatchBuilder,
};
use std::collections::BTreeSet;
use std::sync::Arc;

fn entries() -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..2_000)
        .map(|index| {
            (
                format!("key-{index:06}").into_bytes(),
                format!("value-{index:06}-{}", "x".repeat(index % 31)).into_bytes(),
            )
        })
        .collect()
}

#[test]
fn batch_and_sorted_builders_agree_for_every_built_in_policy_and_layout() {
    let policies = [
        chunking::entry_count_key_value_hash(),
        chunking::entry_count_key_hash(),
        chunking::logical_bytes_key_weibull(),
        chunking::logical_bytes_rolling_hash(),
    ];
    let layouts = [
        NodeLayoutSpec::PrefixCompressed,
        NodeLayoutSpec::Plain,
        NodeLayoutSpec::OffsetTable,
    ];

    for policy in policies {
        for layout in &layouts {
            let config = Config::builder()
                .chunking(policy.clone())
                .node_layout(layout.clone())
                .build();
            let mut batch = BatchBuilder::new(Arc::new(MemStore::new()), config.clone());
            let mut sorted = SortedBatchBuilder::new(Arc::new(MemStore::new()), config);
            for (key, value) in entries() {
                batch.add(key.clone(), value.clone());
                sorted.add(key, value).unwrap();
            }

            assert_eq!(batch.build().unwrap().root, sorted.build().unwrap().root);
        }
    }
}

#[test]
fn batched_value_updates_match_full_rebuild_for_every_built_in_layout() {
    let changed = (0..300)
        .map(|offset| (offset * 7_919) % 20_000)
        .collect::<BTreeSet<_>>();
    assert_eq!(changed.len(), 300);

    for layout in [
        NodeLayoutSpec::PrefixCompressed,
        NodeLayoutSpec::Plain,
        NodeLayoutSpec::OffsetTable,
    ] {
        let config = Config::builder()
            .chunking(chunking::entry_count_key_hash())
            .node_layout(layout)
            .build();
        let store = Arc::new(MemStore::new());
        let manager = Prolly::new(store.clone(), config.clone());
        let mut base = BatchBuilder::new(store.clone(), config.clone());
        for index in 0..20_000 {
            base.add(
                format!("key-{index:020}").into_bytes(),
                format!("value-{index:020}-00").into_bytes(),
            );
        }
        let base = base.build().unwrap();
        let (updated, stats) = manager
            .batch_with_write_stats(
                &base,
                changed
                    .iter()
                    .map(|index| Mutation::Upsert {
                        key: format!("key-{index:020}").into_bytes(),
                        val: format!("value-{index:020}-01").into_bytes(),
                    })
                    .collect(),
            )
            .unwrap();
        assert!(stats.used_batched_value_update_path, "{stats:?}");

        let mut rebuilt = BatchBuilder::new(store, config);
        for index in 0..20_000 {
            rebuilt.add(
                format!("key-{index:020}").into_bytes(),
                format!(
                    "value-{index:020}-{}",
                    if changed.contains(&index) { "01" } else { "00" }
                )
                .into_bytes(),
            );
        }
        assert_eq!(updated.root, rebuilt.build().unwrap().root);
    }
}
