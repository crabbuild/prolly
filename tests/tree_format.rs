use prolly::{
    BoundaryInput, BoundaryRule, ChunkMeasure, ChunkingSpec, Config, Encoding, Error,
    HashAlgorithm, NodeLayoutSpec, TreeFormat,
};

fn entry_count_key_hash() -> ChunkingSpec {
    ChunkingSpec {
        measure: ChunkMeasure::EntryCount,
        input: BoundaryInput::Key,
        hash: HashAlgorithm::XxHash64,
        rule: BoundaryRule::HashThreshold { factor: 128 },
        min: 4,
        target: 128,
        max: 1_048_576,
        hash_seed: 0,
        level_salt: true,
        hard_max_node_bytes: 16 * 1024 * 1024,
    }
}

#[test]
fn runtime_tuning_does_not_change_persisted_format_identity() {
    let default = Config::default();
    let tuned = Config::builder()
        .node_cache_max_nodes(32)
        .node_cache_max_bytes(64 * 1024)
        .read_parallelism(8)
        .build();

    assert_eq!(
        default.format.digest().unwrap(),
        tuned.format.digest().unwrap()
    );
    assert_ne!(default.runtime, tuned.runtime);
}

#[test]
fn default_format_uses_prefix_compression() {
    assert_eq!(
        Config::default().format.node_layout,
        NodeLayoutSpec::PrefixCompressed
    );
}

#[test]
fn config_builder_selects_chunking_and_layout() {
    let chunking = entry_count_key_hash();
    let config = Config::builder()
        .chunking(chunking.clone())
        .node_layout(NodeLayoutSpec::Plain)
        .build();

    assert_eq!(config.format.chunking, chunking);
    assert_eq!(config.format.node_layout, NodeLayoutSpec::Plain);
}

#[test]
fn equal_tree_formats_have_equal_canonical_bytes_and_digests() {
    let left = TreeFormat {
        chunking: entry_count_key_hash(),
        node_layout: NodeLayoutSpec::PrefixCompressed,
        value_encoding: Encoding::Raw,
    };
    let right = left.clone();

    assert_eq!(
        left.canonical_bytes().unwrap(),
        right.canonical_bytes().unwrap()
    );
    assert_eq!(left.digest().unwrap(), right.digest().unwrap());
}

#[test]
fn node_layout_participates_in_tree_format_identity() {
    let prefix = TreeFormat {
        chunking: entry_count_key_hash(),
        node_layout: NodeLayoutSpec::PrefixCompressed,
        value_encoding: Encoding::Raw,
    };
    let plain = TreeFormat {
        node_layout: NodeLayoutSpec::Plain,
        ..prefix.clone()
    };

    assert_ne!(
        prefix.canonical_bytes().unwrap(),
        plain.canonical_bytes().unwrap()
    );
    assert_ne!(prefix.digest().unwrap(), plain.digest().unwrap());
}

#[test]
fn empty_custom_format_identifiers_are_rejected() {
    let format = TreeFormat {
        chunking: entry_count_key_hash(),
        node_layout: NodeLayoutSpec::Custom {
            id: String::new(),
            parameters: vec![],
        },
        value_encoding: Encoding::Raw,
    };

    assert!(matches!(format.validate(), Err(Error::InvalidFormat(_))));
}
