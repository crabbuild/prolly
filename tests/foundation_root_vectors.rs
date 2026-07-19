use prolly::{Config, MemStore, NodeLayoutSpec, SortedBatchBuilder};
use std::sync::Arc;

#[test]
fn built_in_layout_root_vectors_are_stable() {
    for (layout, expected) in [
        (
            NodeLayoutSpec::PrefixCompressed,
            "c49de633b03068d3340ceb2c304ead345cfe8d7cabd5da5f1de34706de9be141",
        ),
        (
            NodeLayoutSpec::Plain,
            "81f3c9d2b4797163c782151fad1739b84c08603fdceeba82623d349a5ac48cf3",
        ),
        (
            NodeLayoutSpec::OffsetTable,
            "ee33ab4ae864b31e6f01fcbec46151db626d8997c585f269bffafa60d1b5a936",
        ),
    ] {
        let config = Config::builder().node_layout(layout.clone()).build();
        let mut builder = SortedBatchBuilder::new(Arc::new(MemStore::new()), config);
        for index in 0..2_048usize {
            builder
                .add(
                    format!("key-{index:08}").into_bytes(),
                    format!("value-{index:08}").into_bytes(),
                )
                .unwrap();
        }
        let root = builder.build().unwrap().root.unwrap();
        let root = root
            .as_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(root, expected, "root changed for {layout:?}");
    }
}
