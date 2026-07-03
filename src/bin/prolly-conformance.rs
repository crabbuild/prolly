use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use prolly::{
    debug_key, decode_segments, encode_segment, i128_key, i64_key, is_boundary_config, prefix_end,
    timestamp_millis_key, u128_key, u64_key, BlobRef, Cid, Config, Diff, Encoding, MemStore, Node,
    NodeStoreScan, Prolly, RootManifest, Store, ValueRef, VersionedValue,
};
use serde::Serialize;

const LEGACY_CBOR_LEAF_FIXTURE_HEX: &str = concat!(
    "a9008396186318721861187418651873182f18701872186f186c186c1879182f187318721863182f",
    "1861182e1872187396186318721861187418651873182f18701872186f186c186c1879182f1873",
    "18721863182f1862182e1872187396186318721861187418651873182f18701872186f186c186c",
    "1879182f187318721863182f1863182e1872187301838718761861186c18751865182d1861",
    "8718761861186c18751865182d18628718761861186c18751865182d186302f503000410051",
    "902000619010007182a0800"
);

#[derive(Serialize)]
struct FixtureDocument {
    schema: &'static str,
    generated_by: &'static str,
    rust_package: &'static str,
    defaults: ConfigFixture,
    node_fixtures: Vec<NodeFixture>,
    boundary_fixtures: Vec<BoundaryFixture>,
    key_fixtures: KeyFixtures,
    tree_fixtures: Vec<TreeFixture>,
    diff_fixtures: Vec<DiffFixture>,
    value_fixtures: Vec<ValueFixture>,
    blob_fixtures: Vec<BlobFixture>,
    manifest_fixtures: Vec<ManifestFixture>,
}

#[derive(Clone, Serialize)]
struct ConfigFixture {
    min_chunk_size: usize,
    max_chunk_size: usize,
    chunking_factor: u32,
    hash_seed: u64,
    encoding: EncodingFixture,
    node_cache_max_nodes: Option<usize>,
    node_cache_max_bytes: Option<usize>,
}

#[derive(Clone, Serialize)]
struct EncodingFixture {
    kind: String,
    custom_name: Option<String>,
}

#[derive(Serialize)]
struct NodeShape {
    leaf: bool,
    level: u8,
    min_chunk_size: usize,
    max_chunk_size: usize,
    chunking_factor: u32,
    hash_seed: u64,
    encoding: EncodingFixture,
    keys: Vec<String>,
    vals: Vec<String>,
}

#[derive(Serialize)]
struct NodeFixture {
    name: &'static str,
    node: NodeShape,
    bytes: String,
    cid: String,
    legacy_cbor_bytes: Option<String>,
}

#[derive(Serialize)]
struct BoundaryFixture {
    name: &'static str,
    config: ConfigFixture,
    count: usize,
    key: String,
    value: String,
    is_boundary: bool,
}

#[derive(Serialize)]
struct KeyFixtures {
    prefix_end: Vec<PrefixEndFixture>,
    numeric: Vec<NumericKeyFixture>,
    segments: Vec<SegmentFixture>,
    debug: Vec<DebugKeyFixture>,
}

#[derive(Serialize)]
struct PrefixEndFixture {
    prefix: String,
    end: Option<String>,
}

#[derive(Serialize)]
struct NumericKeyFixture {
    kind: &'static str,
    value: String,
    encoded: String,
}

#[derive(Serialize)]
struct SegmentFixture {
    segments: Vec<String>,
    encoded: String,
    decoded: Vec<String>,
}

#[derive(Serialize)]
struct DebugKeyFixture {
    key: String,
    debug: String,
}

#[derive(Serialize)]
struct TreeFixture {
    name: &'static str,
    config: ConfigFixture,
    root: Option<String>,
    store: Vec<StoredNodeFixture>,
    entries: Vec<EntryFixture>,
    lookups: Vec<LookupFixture>,
    ranges: Vec<RangeFixture>,
}

#[derive(Serialize)]
struct StoredNodeFixture {
    cid: String,
    bytes: String,
}

#[derive(Serialize)]
struct EntryFixture {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct LookupFixture {
    key: String,
    value: Option<String>,
}

#[derive(Serialize)]
struct RangeFixture {
    start: String,
    end: Option<String>,
    entries: Vec<EntryFixture>,
}

#[derive(Serialize)]
struct DiffFixture {
    name: &'static str,
    config: ConfigFixture,
    base_root: Option<String>,
    other_root: Option<String>,
    store: Vec<StoredNodeFixture>,
    diffs: Vec<DiffEntryFixture>,
}

#[derive(Serialize)]
struct DiffEntryFixture {
    kind: &'static str,
    key: String,
    value: Option<String>,
    old: Option<String>,
    new: Option<String>,
}

#[derive(Serialize)]
struct ValueFixture {
    name: &'static str,
    schema_name: String,
    version: u64,
    encoding: EncodingFixture,
    payload: String,
    bytes: String,
}

#[derive(Serialize)]
struct BlobFixture {
    name: &'static str,
    kind: &'static str,
    value: Option<String>,
    blob: Option<String>,
    cid: Option<String>,
    len: Option<u64>,
    bytes: String,
}

#[derive(Serialize)]
struct ManifestFixture {
    name: &'static str,
    root: Option<String>,
    config: ConfigFixture,
    created_at_millis: Option<u64>,
    updated_at_millis: Option<u64>,
    bytes: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let document = fixture_document()?;
    let json = serde_json::to_string_pretty(&document)?;

    let mut args = env::args().skip(1);
    match (args.next().as_deref(), args.next()) {
        (None, None) => {
            println!("{json}");
        }
        (Some("--write"), Some(path)) => {
            fs::write(PathBuf::from(path), format!("{json}\n"))?;
        }
        _ => {
            eprintln!("usage: cargo run -p prolly-map --bin prolly-conformance -- [--write PATH]");
            std::process::exit(2);
        }
    }

    Ok(())
}

fn fixture_document() -> Result<FixtureDocument, Box<dyn std::error::Error>> {
    Ok(FixtureDocument {
        schema: "prolly-conformance-v1",
        generated_by: "cargo run -p prolly-map --bin prolly-conformance",
        rust_package: "prolly-map",
        defaults: config_fixture(&Config::default()),
        node_fixtures: node_fixtures(),
        boundary_fixtures: boundary_fixtures(),
        key_fixtures: key_fixtures(),
        tree_fixtures: vec![tree_fixture()?],
        diff_fixtures: vec![diff_fixture()?],
        value_fixtures: value_fixtures()?,
        blob_fixtures: blob_fixtures(),
        manifest_fixtures: vec![manifest_fixture()?],
    })
}

fn node_fixtures() -> Vec<NodeFixture> {
    let leaf = leaf_fixture_node();
    let internal = internal_fixture_node();
    let custom = custom_encoding_fixture_node();

    vec![
        node_fixture(
            "compact_leaf",
            leaf,
            Some(LEGACY_CBOR_LEAF_FIXTURE_HEX.to_string()),
        ),
        node_fixture("compact_internal", internal, None),
        node_fixture("compact_custom_encoding", custom, None),
    ]
}

fn leaf_fixture_node() -> Node {
    Node::builder()
        .keys(vec![
            b"crates/prolly/src/a.rs".to_vec(),
            b"crates/prolly/src/b.rs".to_vec(),
            b"crates/prolly/src/c.rs".to_vec(),
        ])
        .vals(vec![
            b"value-a".to_vec(),
            b"value-b".to_vec(),
            b"value-c".to_vec(),
        ])
        .leaf(true)
        .level(0)
        .min_chunk_size(16)
        .max_chunk_size(512)
        .chunking_factor(256)
        .hash_seed(42)
        .encoding(Encoding::Raw)
        .build()
}

fn internal_fixture_node() -> Node {
    let mut cid_a = [0u8; 32];
    cid_a[0] = 1;
    let mut cid_b = [0u8; 32];
    cid_b[0] = 2;
    Node::builder()
        .keys(vec![
            b"crates/prolly/src/a.rs".to_vec(),
            b"crates/prolly/src/b.rs".to_vec(),
            b"crates/prolly/src/c.rs".to_vec(),
        ])
        .vals(vec![
            cid_a.to_vec(),
            cid_b.to_vec(),
            b"legacy-child".to_vec(),
        ])
        .leaf(false)
        .level(2)
        .min_chunk_size(16)
        .max_chunk_size(512)
        .chunking_factor(256)
        .hash_seed(42)
        .encoding(Encoding::Raw)
        .build()
}

fn custom_encoding_fixture_node() -> Node {
    Node::builder()
        .keys(vec![b"a".to_vec(), b"b".to_vec()])
        .vals(vec![b"1".to_vec(), b"2".to_vec()])
        .leaf(true)
        .level(0)
        .min_chunk_size(2)
        .max_chunk_size(128)
        .chunking_factor(64)
        .hash_seed(42)
        .encoding(Encoding::Custom(
            "application/x-crabdb-node-test".to_string(),
        ))
        .build()
}

fn node_fixture(name: &'static str, node: Node, legacy_cbor_bytes: Option<String>) -> NodeFixture {
    let bytes = node.to_bytes();
    NodeFixture {
        name,
        node: node_shape(&node),
        cid: hex(node.cid().as_bytes()),
        bytes: hex(&bytes),
        legacy_cbor_bytes,
    }
}

fn boundary_fixtures() -> Vec<BoundaryFixture> {
    let below_min = Config::builder()
        .min_chunk_size(4)
        .max_chunk_size(10)
        .chunking_factor(128)
        .build();
    let at_max = Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(128)
        .build();
    let hash_config = Config::builder()
        .min_chunk_size(1)
        .max_chunk_size(1_000)
        .chunking_factor(8)
        .hash_seed(42)
        .build();
    let (boundary_key, boundary_value) = find_boundary_case(&hash_config, true);
    let (non_boundary_key, non_boundary_value) = find_boundary_case(&hash_config, false);

    vec![
        boundary_fixture("below_min", &below_min, 2, b"key", b"value"),
        boundary_fixture("at_max", &at_max, 4, b"key", b"value"),
        boundary_fixture(
            "hash_boundary",
            &hash_config,
            1,
            &boundary_key,
            &boundary_value,
        ),
        boundary_fixture(
            "hash_non_boundary",
            &hash_config,
            1,
            &non_boundary_key,
            &non_boundary_value,
        ),
    ]
}

fn find_boundary_case(config: &Config, expected: bool) -> (Vec<u8>, Vec<u8>) {
    for idx in 0..100_000u32 {
        let key = format!("k{idx:05}").into_bytes();
        let value = format!("v{idx:05}").into_bytes();
        if is_boundary_config(config, config.min_chunk_size, &key, &value) == expected {
            return (key, value);
        }
    }
    panic!("failed to find boundary fixture case");
}

fn boundary_fixture(
    name: &'static str,
    config: &Config,
    count: usize,
    key: &[u8],
    value: &[u8],
) -> BoundaryFixture {
    BoundaryFixture {
        name,
        config: config_fixture(config),
        count,
        key: hex(key),
        value: hex(value),
        is_boundary: is_boundary_config(config, count, key, value),
    }
}

fn key_fixtures() -> KeyFixtures {
    let segment_sets = vec![
        vec![b"tenant".to_vec(), vec![0, 1, 0xff], Vec::new()],
        vec![
            b"conversation".to_vec(),
            b"c42".to_vec(),
            u64_key(7).to_vec(),
        ],
    ];

    KeyFixtures {
        prefix_end: vec![
            prefix_end_fixture(b""),
            prefix_end_fixture(b"abc"),
            prefix_end_fixture(&[0x12, 0xff]),
            prefix_end_fixture(&[0xff, 0xff]),
        ],
        numeric: vec![
            numeric_fixture("u64", "0".to_string(), &u64_key(0)),
            numeric_fixture("u64", "9".to_string(), &u64_key(9)),
            numeric_fixture("u64", u64::MAX.to_string(), &u64_key(u64::MAX)),
            numeric_fixture("i64", i64::MIN.to_string(), &i64_key(i64::MIN)),
            numeric_fixture("i64", "-42".to_string(), &i64_key(-42)),
            numeric_fixture("i64", "-1".to_string(), &i64_key(-1)),
            numeric_fixture("i64", "0".to_string(), &i64_key(0)),
            numeric_fixture("i64", "42".to_string(), &i64_key(42)),
            numeric_fixture("i64", i64::MAX.to_string(), &i64_key(i64::MAX)),
            numeric_fixture(
                "u128",
                "340282366920938463463374607431768211455".to_string(),
                &u128_key(u128::MAX),
            ),
            numeric_fixture("i128", "-1".to_string(), &i128_key(-1)),
            numeric_fixture(
                "timestamp_millis",
                "123456789".to_string(),
                &timestamp_millis_key(123_456_789),
            ),
        ],
        segments: segment_sets
            .into_iter()
            .map(|segments| {
                let mut encoded = Vec::new();
                for segment in &segments {
                    encoded.extend(encode_segment(segment));
                }
                SegmentFixture {
                    segments: segments.iter().map(|segment| hex(segment)).collect(),
                    decoded: decode_segments(&encoded)
                        .expect("generated segment fixture decodes")
                        .iter()
                        .map(|segment| hex(segment))
                        .collect(),
                    encoded: hex(&encoded),
                }
            })
            .collect(),
        debug: vec![
            DebugKeyFixture {
                key: hex(b"a\n\0\\\""),
                debug: debug_key(b"a\n\0\\\""),
            },
            DebugKeyFixture {
                key: hex(&[0, 1, 0x7f, 0x80, 0xff]),
                debug: debug_key(&[0, 1, 0x7f, 0x80, 0xff]),
            },
        ],
    }
}

fn prefix_end_fixture(prefix: &[u8]) -> PrefixEndFixture {
    PrefixEndFixture {
        prefix: hex(prefix),
        end: prefix_end(prefix).map(|end| hex(&end)),
    }
}

fn numeric_fixture(kind: &'static str, value: String, encoded: &[u8]) -> NumericKeyFixture {
    NumericKeyFixture {
        kind,
        value,
        encoded: hex(encoded),
    }
}

fn tree_fixture() -> Result<TreeFixture, Box<dyn std::error::Error>> {
    let config = port_config();
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store.clone(), config.clone());
    let mut tree = prolly.create();
    for (key, value) in [
        (b"b".as_slice(), b"2".as_slice()),
        (b"a", b"1"),
        (b"c", b"3"),
        (b"d", b"4"),
        (b"e", b"5"),
        (b"f", b"6"),
    ] {
        tree = prolly.put(&tree, key.to_vec(), value.to_vec())?;
    }

    Ok(TreeFixture {
        name: "six_entries_multi_leaf",
        config: config_fixture(&config),
        root: tree.root.as_ref().map(cid_hex),
        store: stored_nodes(store.as_ref())?,
        entries: entries(&prolly, &tree)?,
        lookups: vec![
            lookup(&prolly, &tree, b"a")?,
            lookup(&prolly, &tree, b"d")?,
            lookup(&prolly, &tree, b"missing")?,
        ],
        ranges: vec![
            range_fixture(&prolly, &tree, b"", None)?,
            range_fixture(&prolly, &tree, b"b", Some(b"e"))?,
            range_fixture(&prolly, &tree, b"e", None)?,
        ],
    })
}

fn diff_fixture() -> Result<DiffFixture, Box<dyn std::error::Error>> {
    let config = port_config();
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store.clone(), config.clone());
    let mut base = prolly.create();
    for (key, value) in [
        (b"a".as_slice(), b"1".as_slice()),
        (b"b", b"2"),
        (b"c", b"3"),
    ] {
        base = prolly.put(&base, key.to_vec(), value.to_vec())?;
    }
    let other = prolly.put(&base, b"b".to_vec(), b"22".to_vec())?;
    let other = prolly.delete(&other, b"c")?;
    let other = prolly.put(&other, b"d".to_vec(), b"4".to_vec())?;
    let diffs = prolly
        .diff(&base, &other)?
        .into_iter()
        .map(diff_entry)
        .collect();

    Ok(DiffFixture {
        name: "added_removed_changed",
        config: config_fixture(&config),
        base_root: base.root.as_ref().map(cid_hex),
        other_root: other.root.as_ref().map(cid_hex),
        store: stored_nodes(store.as_ref())?,
        diffs,
    })
}

fn value_fixtures() -> Result<Vec<ValueFixture>, Box<dyn std::error::Error>> {
    let raw = VersionedValue::raw("memory.chunk", 2, b"payload");
    let json = VersionedValue::json(
        "memory.score",
        7,
        &serde_json::json!({"name":"chunk","score":9}),
    )?;
    let custom = VersionedValue::with_encoding(
        "memory.custom",
        1,
        Encoding::Custom("application/x-crabdb-test".to_string()),
        b"custom-payload",
    );

    Ok(vec![
        value_fixture("versioned_raw", &raw)?,
        value_fixture("versioned_json", &json)?,
        value_fixture("versioned_custom", &custom)?,
    ])
}

fn value_fixture(
    name: &'static str,
    value: &VersionedValue,
) -> Result<ValueFixture, Box<dyn std::error::Error>> {
    Ok(ValueFixture {
        name,
        schema_name: value.schema.clone(),
        version: value.version,
        encoding: encoding_fixture(&value.encoding),
        payload: hex(&value.payload),
        bytes: hex(&value.to_bytes()?),
    })
}

fn blob_fixtures() -> Vec<BlobFixture> {
    let inline = ValueRef::Inline(b"small-value".to_vec());
    let blob_bytes = b"large-payload-for-content-addressed-blob";
    let blob_ref = BlobRef::from_bytes(blob_bytes);
    let blob = ValueRef::Blob(blob_ref.clone());

    vec![
        BlobFixture {
            name: "inline",
            kind: "inline",
            value: Some(hex(b"small-value")),
            blob: None,
            cid: None,
            len: None,
            bytes: hex(&inline.to_bytes()),
        },
        BlobFixture {
            name: "blob",
            kind: "blob",
            value: None,
            blob: Some(hex(blob_bytes)),
            cid: Some(hex(blob_ref.cid.as_bytes())),
            len: Some(blob_ref.len),
            bytes: hex(&blob.to_bytes()),
        },
    ]
}

fn manifest_fixture() -> Result<ManifestFixture, Box<dyn std::error::Error>> {
    let config = port_config();
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, config.clone());
    let tree = prolly.put(&prolly.create(), b"name".to_vec(), b"CrabDB".to_vec())?;
    let manifest = RootManifest::from_tree_with_timestamps_millis(
        &tree,
        Some(1_700_000_000_000),
        Some(1_700_000_000_123),
    );
    Ok(ManifestFixture {
        name: "root_manifest",
        root: manifest.root.as_ref().map(cid_hex),
        config: config_fixture(&manifest.config),
        created_at_millis: manifest.created_at_millis,
        updated_at_millis: manifest.updated_at_millis,
        bytes: hex(&manifest.to_bytes()?),
    })
}

fn port_config() -> Config {
    Config::builder()
        .min_chunk_size(2)
        .max_chunk_size(4)
        .chunking_factor(2)
        .build()
}

fn entries<S: Store>(
    prolly: &Prolly<S>,
    tree: &prolly::Tree,
) -> Result<Vec<EntryFixture>, prolly::Error> {
    prolly
        .range(tree, &[], None)?
        .map(|entry| {
            entry.map(|(key, value)| EntryFixture {
                key: hex(&key),
                value: hex(&value),
            })
        })
        .collect()
}

fn lookup<S: Store>(
    prolly: &Prolly<S>,
    tree: &prolly::Tree,
    key: &[u8],
) -> Result<LookupFixture, prolly::Error> {
    Ok(LookupFixture {
        key: hex(key),
        value: prolly.get(tree, key)?.map(|value| hex(&value)),
    })
}

fn range_fixture<S: Store>(
    prolly: &Prolly<S>,
    tree: &prolly::Tree,
    start: &[u8],
    end: Option<&[u8]>,
) -> Result<RangeFixture, prolly::Error> {
    Ok(RangeFixture {
        start: hex(start),
        end: end.map(hex),
        entries: prolly
            .range(tree, start, end)?
            .map(|entry| {
                entry.map(|(key, value)| EntryFixture {
                    key: hex(&key),
                    value: hex(&value),
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn stored_nodes(store: &MemStore) -> Result<Vec<StoredNodeFixture>, Box<dyn std::error::Error>> {
    store
        .list_node_cids()?
        .into_iter()
        .map(|cid| {
            let bytes = store
                .get(cid.as_bytes())?
                .expect("scanned CID should be present in the same store");
            Ok(StoredNodeFixture {
                cid: hex(cid.as_bytes()),
                bytes: hex(&bytes),
            })
        })
        .collect()
}

fn diff_entry(diff: Diff) -> DiffEntryFixture {
    match diff {
        Diff::Added { key, val } => DiffEntryFixture {
            kind: "added",
            key: hex(&key),
            value: Some(hex(&val)),
            old: None,
            new: None,
        },
        Diff::Removed { key, val } => DiffEntryFixture {
            kind: "removed",
            key: hex(&key),
            value: Some(hex(&val)),
            old: None,
            new: None,
        },
        Diff::Changed { key, old, new } => DiffEntryFixture {
            kind: "changed",
            key: hex(&key),
            value: None,
            old: Some(hex(&old)),
            new: Some(hex(&new)),
        },
    }
}

fn node_shape(node: &Node) -> NodeShape {
    NodeShape {
        leaf: node.leaf,
        level: node.level,
        min_chunk_size: node.min_chunk_size,
        max_chunk_size: node.max_chunk_size,
        chunking_factor: node.chunking_factor,
        hash_seed: node.hash_seed,
        encoding: encoding_fixture(&node.encoding),
        keys: node.keys.iter().map(|key| hex(key)).collect(),
        vals: node.vals.iter().map(|value| hex(value)).collect(),
    }
}

fn config_fixture(config: &Config) -> ConfigFixture {
    ConfigFixture {
        min_chunk_size: config.min_chunk_size,
        max_chunk_size: config.max_chunk_size,
        chunking_factor: config.chunking_factor,
        hash_seed: config.hash_seed,
        encoding: encoding_fixture(&config.encoding),
        node_cache_max_nodes: config.node_cache_max_nodes,
        node_cache_max_bytes: config.node_cache_max_bytes,
    }
}

fn encoding_fixture(encoding: &Encoding) -> EncodingFixture {
    match encoding {
        Encoding::Raw => EncodingFixture {
            kind: "raw".to_string(),
            custom_name: None,
        },
        Encoding::Cbor => EncodingFixture {
            kind: "cbor".to_string(),
            custom_name: None,
        },
        Encoding::Json => EncodingFixture {
            kind: "json".to_string(),
            custom_name: None,
        },
        Encoding::Custom(name) => EncodingFixture {
            kind: "custom".to_string(),
            custom_name: Some(name.clone()),
        },
    }
}

fn cid_hex(cid: &Cid) -> String {
    hex(cid.as_bytes())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
