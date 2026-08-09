use std::collections::BTreeMap;

use prolly::Cid;
use prolly_dynamodb_core::{
    encode_item, encode_primary_key, parse_condition, parse_key_condition, parse_projection,
    parse_read_expressions, parse_update, AttributeValue, DatabaseFormatRecord, Error, Item,
    KeyAttribute, KeyKind, StoragePublicationMode, TableDescription, TableId, TableStatus,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct Case {
    name: String,
    item: Option<Item>,
    key: Option<Item>,
    encoded_hex: String,
}

#[derive(Deserialize)]
struct ValidationFixtures {
    fixture_version: u32,
    cases: Vec<ValidationCase>,
}

#[derive(Deserialize)]
struct ValidationCase {
    name: String,
    kind: String,
    expression: String,
    #[serde(default)]
    condition: Option<String>,
    names: BTreeMap<String, String>,
    values: BTreeMap<String, AttributeValue>,
    category: String,
    message: String,
}

#[derive(Deserialize)]
struct Fixtures {
    fixture_version: u32,
    item_codec: String,
    items: Vec<Case>,
    keys: Vec<Case>,
}

#[derive(Deserialize)]
struct DatabaseFormatFixture {
    fixture_version: u32,
    database_format_version: u32,
    logical_protocol_major: u16,
    logical_protocol_minor: u16,
    tree_format_input: String,
    publication_mode: String,
    large_value_inline_threshold: u64,
    minimum_reader_version: u32,
    minimum_writer_version: u32,
    encoded_hex: String,
}

#[test]
fn database_format_10_fixture_is_stable() {
    let fixture: DatabaseFormatFixture =
        serde_json::from_str(include_str!("fixtures/database-format-10.json")).unwrap();
    assert_eq!(fixture.fixture_version, 1);
    assert_eq!(fixture.publication_mode, "prepublish_immutable_nodes");

    let expected = DatabaseFormatRecord {
        format_version: fixture.database_format_version,
        logical_protocol_major: fixture.logical_protocol_major,
        logical_protocol_minor: fixture.logical_protocol_minor,
        item_codec_digest: Cid::from_bytes(b"DDBI-v1-canonical-cbor"),
        key_codec_digest: Cid::from_bytes(b"DDBK-v1-ordered-components"),
        catalog_codec_digest: Cid::from_bytes(
            b"DDBC-v5-schema-record-v1-index-descriptors-DDBX-v1-base-indexed-snapshot-pair-v1-base-schema-pair-v1-canonical-cbor",
        ),
        commit_codec_digest: Cid::from_bytes(
            b"DDBAudit-v7-commit-maintenance-import-fence-gc-index-reconfiguration-worker-lease-fence-checkpoint-canonical-cbor",
        ),
        tree_format_digest: Cid::from_bytes(fixture.tree_format_input.as_bytes()),
        publication_mode: StoragePublicationMode::PrepublishImmutableNodes,
        large_value_inline_threshold: fixture.large_value_inline_threshold,
        minimum_reader_version: fixture.minimum_reader_version,
        minimum_writer_version: fixture.minimum_writer_version,
    };

    assert_eq!(hex(&expected.encode()), fixture.encoded_hex);
    assert_eq!(
        DatabaseFormatRecord::decode(&unhex(&fixture.encoded_hex)).unwrap(),
        expected
    );
}

#[test]
fn database_format_11_fixture_is_stable() {
    let fixture: DatabaseFormatFixture =
        serde_json::from_str(include_str!("fixtures/database-format-11.json")).unwrap();
    assert_eq!(fixture.fixture_version, 1);
    assert_eq!(fixture.database_format_version, 11);
    assert_eq!(fixture.publication_mode, "prepublish_immutable_nodes");

    let expected = DatabaseFormatRecord {
        format_version: fixture.database_format_version,
        logical_protocol_major: fixture.logical_protocol_major,
        logical_protocol_minor: fixture.logical_protocol_minor,
        item_codec_digest: Cid::from_bytes(b"DDBI-v1-canonical-cbor"),
        key_codec_digest: Cid::from_bytes(b"DDBK-v1-ordered-components"),
        catalog_codec_digest: Cid::from_bytes(
            b"DDBC-v8-schema-record-v1-snapshot-catalog-v1-indexed-snapshot-manifest-v1-current-only-commit-catalog-v1-table-log-v1-append-only-blob-registry-v1-canonical-cbor",
        ),
        commit_codec_digest: Cid::from_bytes(
            b"DDBAudit-v7-commit-maintenance-import-fence-gc-index-reconfiguration-worker-lease-fence-checkpoint-canonical-cbor",
        ),
        tree_format_digest: Cid::from_bytes(fixture.tree_format_input.as_bytes()),
        publication_mode: StoragePublicationMode::PrepublishImmutableNodes,
        large_value_inline_threshold: fixture.large_value_inline_threshold,
        minimum_reader_version: fixture.minimum_reader_version,
        minimum_writer_version: fixture.minimum_writer_version,
    };

    assert_eq!(hex(&expected.encode()), fixture.encoded_hex);
    assert_eq!(
        DatabaseFormatRecord::decode(&unhex(&fixture.encoded_hex)).unwrap(),
        expected
    );
}

#[test]
fn database_format_12_fixture_is_stable() {
    let fixture: DatabaseFormatFixture =
        serde_json::from_str(include_str!("fixtures/database-format-12.json")).unwrap();
    assert_eq!(fixture.fixture_version, 1);
    assert_eq!(fixture.database_format_version, 12);
    assert_eq!(fixture.publication_mode, "prepublish_immutable_nodes");

    let expected = DatabaseFormatRecord {
        format_version: fixture.database_format_version,
        logical_protocol_major: fixture.logical_protocol_major,
        logical_protocol_minor: fixture.logical_protocol_minor,
        item_codec_digest: Cid::from_bytes(b"DDBI-v1-canonical-cbor"),
        key_codec_digest: Cid::from_bytes(b"DDBK-v1-ordered-components"),
        catalog_codec_digest: Cid::from_bytes(
            b"DDBC-v10-schema-record-v1-detached-snapshot-manifest-tree-v1-snapshot-locator-v2-current-only-snapshot-catalog-v1-indexed-snapshot-manifest-v1-current-only-commit-catalog-v1-table-log-v1-append-only-blob-registry-v1-canonical-cbor",
        ),
        commit_codec_digest: Cid::from_bytes(
            b"DDBAudit-v7-commit-maintenance-import-fence-gc-index-reconfiguration-worker-lease-fence-checkpoint-canonical-cbor",
        ),
        tree_format_digest: Cid::from_bytes(fixture.tree_format_input.as_bytes()),
        publication_mode: StoragePublicationMode::PrepublishImmutableNodes,
        large_value_inline_threshold: fixture.large_value_inline_threshold,
        minimum_reader_version: fixture.minimum_reader_version,
        minimum_writer_version: fixture.minimum_writer_version,
    };

    assert_eq!(hex(&expected.encode()), fixture.encoded_hex);
    assert_eq!(
        DatabaseFormatRecord::decode(&unhex(&fixture.encoded_hex)).unwrap(),
        expected
    );
}

#[test]
fn canonical_v1_fixtures_are_stable() {
    let fixtures: Fixtures =
        serde_json::from_str(include_str!("fixtures/canonical-v1.json")).unwrap();
    assert_eq!(fixtures.fixture_version, 1);
    assert_eq!(fixtures.item_codec, "DDBI-v1-canonical-cbor");

    for case in fixtures.items {
        let actual = encode_item(&case.item.expect("item case must contain item")).unwrap();
        assert_eq!(hex(&actual), case.encoded_hex, "item fixture {}", case.name);
    }

    let schema = TableDescription {
        name: "Orders".into(),
        id: TableId([7; 32]),
        partition_key: KeyAttribute {
            name: "account".into(),
            kind: KeyKind::String,
        },
        sort_key: Some(KeyAttribute {
            name: "sequence".into(),
            kind: KeyKind::Number,
        }),
        attribute_definitions: std::collections::BTreeMap::from([
            ("account".into(), KeyKind::String),
            ("sequence".into(), KeyKind::Number),
        ]),
        secondary_indexes: Vec::new(),
        status: TableStatus::Active,
        created_at_millis: 0,
    };
    let encoded = fixtures
        .keys
        .into_iter()
        .map(|case| {
            let actual = encode_primary_key(&schema, &case.key.unwrap()).unwrap();
            assert_eq!(hex(&actual), case.encoded_hex, "key fixture {}", case.name);
            actual
        })
        .collect::<Vec<_>>();
    assert!(encoded.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn validation_categories_messages_and_precedence_are_stable() {
    let fixtures: ValidationFixtures =
        serde_json::from_str(include_str!("fixtures/validation-v1.json")).unwrap();
    assert_eq!(fixtures.fixture_version, 1);
    for case in fixtures.cases {
        let result = match case.kind.as_str() {
            "projection" => parse_projection(&case.expression, &case.names).map(|_| ()),
            "condition" => parse_condition(&case.expression, &case.names, &case.values).map(|_| ()),
            "key_condition" => {
                parse_key_condition(&case.expression, &case.names, &case.values).map(|_| ())
            }
            "read_none" => {
                parse_read_expressions(None, None, None, &case.names, &case.values).map(|_| ())
            }
            "update" => parse_update(
                &case.expression,
                case.condition.as_deref(),
                &case.names,
                &case.values,
            )
            .map(|_| ()),
            other => panic!("unknown validation fixture kind {other:?}"),
        };
        let error = result.expect_err(&case.name);
        let (category, message) = match error {
            Error::Validation(message) => ("validation", message),
            Error::Unsupported(message) => ("unsupported", message),
            other => panic!("unexpected error category for {}: {other:?}", case.name),
        };
        assert_eq!(category, case.category, "fixture {}", case.name);
        assert_eq!(message, case.message, "fixture {}", case.name);
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(&mut output, "{byte:02x}").unwrap();
    }
    output
}

fn unhex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0, "hex fixture length must be even");
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let pair = std::str::from_utf8(pair).unwrap();
            u8::from_str_radix(pair, 16).unwrap()
        })
        .collect()
}
