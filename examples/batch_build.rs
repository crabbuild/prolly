use prolly::{BatchBuilder, Config, MemStore, Prolly};
use std::sync::Arc;

fn main() -> Result<(), prolly::Error> {
    let store = Arc::new(MemStore::new());
    let config = Config::default();

    let mut builder = BatchBuilder::new(store.clone(), config.clone());
    for index in (0..1_000).rev() {
        builder.add(
            format!("event:{index:04}").into_bytes(),
            format!("payload-{index}").into_bytes(),
        );
    }

    let tree = builder.build()?;
    let prolly = Prolly::new(store, config);

    assert_eq!(
        prolly.get(&tree, b"event:0042")?,
        Some(b"payload-42".to_vec())
    );

    let stats = prolly.collect_stats(&tree)?;
    println!(
        "built {} entries across {} nodes",
        stats.total_key_value_pairs, stats.num_nodes
    );

    Ok(())
}
