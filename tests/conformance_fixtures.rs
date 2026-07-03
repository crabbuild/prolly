use prolly::{
    is_boundary_config, Config, Encoding, MemStore, Node, Prolly, RootManifest, Store, ValueRef,
    VersionedValue,
};
use serde_json::Value;

const FIXTURES: &str = include_str!("../conformance/prolly-fixtures.v1.json");

#[test]
fn checked_in_conformance_fixtures_are_decodable() {
    let fixtures: Value = serde_json::from_str(FIXTURES).unwrap();
    assert_eq!(fixtures["schema"], "prolly-conformance-v1");

    for fixture in fixtures["node_fixtures"].as_array().unwrap() {
        let bytes = from_hex(fixture["bytes"].as_str().unwrap());
        let node = Node::from_bytes(&bytes).unwrap();
        assert_eq!(hex(node.cid().as_bytes()), fixture["cid"].as_str().unwrap());
        assert_eq!(hex(&node.to_bytes()), fixture["bytes"].as_str().unwrap());

        if let Some(legacy) = fixture["legacy_cbor_bytes"].as_str() {
            assert_eq!(Node::from_bytes(&from_hex(legacy)).unwrap(), node);
        }
    }

    for fixture in fixtures["boundary_fixtures"].as_array().unwrap() {
        let config = config_from_fixture(&fixture["config"]);
        let count = fixture["count"].as_u64().unwrap() as usize;
        let key = from_hex(fixture["key"].as_str().unwrap());
        let value = from_hex(fixture["value"].as_str().unwrap());
        assert_eq!(
            is_boundary_config(&config, count, &key, &value),
            fixture["is_boundary"].as_bool().unwrap()
        );
    }

    for fixture in fixtures["value_fixtures"].as_array().unwrap() {
        let bytes = from_hex(fixture["bytes"].as_str().unwrap());
        let value = VersionedValue::from_bytes(&bytes).unwrap();
        assert_eq!(value.schema, fixture["schema_name"].as_str().unwrap());
        assert_eq!(value.version, fixture["version"].as_u64().unwrap());
        assert_eq!(hex(&value.payload), fixture["payload"].as_str().unwrap());
    }

    for fixture in fixtures["blob_fixtures"].as_array().unwrap() {
        let bytes = from_hex(fixture["bytes"].as_str().unwrap());
        let value = ValueRef::from_bytes(&bytes).unwrap();
        assert_eq!(hex(&value.to_bytes()), fixture["bytes"].as_str().unwrap());
    }

    for fixture in fixtures["manifest_fixtures"].as_array().unwrap() {
        let bytes = from_hex(fixture["bytes"].as_str().unwrap());
        let manifest = RootManifest::from_bytes(&bytes).unwrap();
        assert_eq!(
            manifest.root.as_ref().map(|cid| hex(cid.as_bytes())),
            fixture["root"].as_str().map(str::to_owned)
        );
        assert_eq!(
            manifest.created_at_millis,
            fixture["created_at_millis"].as_u64()
        );
        assert_eq!(
            manifest.updated_at_millis,
            fixture["updated_at_millis"].as_u64()
        );
    }
}

#[test]
fn checked_in_tree_fixture_loads_through_rust_api() {
    let fixtures: Value = serde_json::from_str(FIXTURES).unwrap();
    let fixture = &fixtures["tree_fixtures"].as_array().unwrap()[0];
    let store = MemStore::new();
    load_store_fixture(&store, fixture);

    let config = config_from_fixture(&fixture["config"]);
    let prolly = Prolly::new(store, config.clone());
    let tree = prolly::Tree {
        root: fixture["root"].as_str().map(|root| cid_from_hex(root)),
        config,
    };

    for lookup in fixture["lookups"].as_array().unwrap() {
        let key = from_hex(lookup["key"].as_str().unwrap());
        let expected = lookup["value"].as_str().map(from_hex);
        assert_eq!(prolly.get(&tree, &key).unwrap(), expected);
    }

    for range in fixture["ranges"].as_array().unwrap() {
        let start = from_hex(range["start"].as_str().unwrap());
        let end = range["end"].as_str().map(from_hex);
        let expected = range["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                (
                    from_hex(entry["key"].as_str().unwrap()),
                    from_hex(entry["value"].as_str().unwrap()),
                )
            })
            .collect::<Vec<_>>();
        let actual = prolly
            .range(&tree, &start, end.as_deref())
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(actual, expected);
    }
}

fn load_store_fixture(store: &MemStore, fixture: &Value) {
    for entry in fixture["store"].as_array().unwrap() {
        store
            .put(
                &from_hex(entry["cid"].as_str().unwrap()),
                &from_hex(entry["bytes"].as_str().unwrap()),
            )
            .unwrap();
    }
}

fn config_from_fixture(value: &Value) -> Config {
    let mut builder = Config::builder()
        .min_chunk_size(value["min_chunk_size"].as_u64().unwrap() as usize)
        .max_chunk_size(value["max_chunk_size"].as_u64().unwrap() as usize)
        .chunking_factor(value["chunking_factor"].as_u64().unwrap() as u32)
        .hash_seed(value["hash_seed"].as_u64().unwrap())
        .encoding(encoding_from_fixture(&value["encoding"]));

    if let Some(max_nodes) = value["node_cache_max_nodes"].as_u64() {
        builder = builder.node_cache_max_nodes(max_nodes as usize);
    }
    if let Some(max_bytes) = value["node_cache_max_bytes"].as_u64() {
        builder = builder.node_cache_max_bytes(max_bytes as usize);
    }

    builder.build()
}

fn encoding_from_fixture(value: &Value) -> Encoding {
    match value["kind"].as_str().unwrap() {
        "raw" => Encoding::Raw,
        "cbor" => Encoding::Cbor,
        "json" => Encoding::Json,
        "custom" => Encoding::Custom(value["custom_name"].as_str().unwrap().to_string()),
        other => panic!("unknown encoding fixture kind {other}"),
    }
}

fn cid_from_hex(hex: &str) -> prolly::Cid {
    let bytes = from_hex(hex);
    let array: [u8; 32] = bytes.try_into().unwrap();
    prolly::Cid(array)
}

fn from_hex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let digits = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(digits, 16).unwrap()
        })
        .collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
