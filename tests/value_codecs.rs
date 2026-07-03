use prolly::{
    decode_cbor, decode_json, encode_cbor, encode_json, CborCodec, Config, Encoding, JsonCodec,
    MemStore, Prolly, ValueCodec, VersionedCborCodec, VersionedJsonCodec, VersionedValue,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct MemoryRecord {
    source: String,
    content: String,
    confidence: u8,
}

#[test]
fn json_and_cbor_helpers_are_public_crate_root_api() {
    let record = MemoryRecord {
        source: "conversation/c1".to_string(),
        content: "User prefers concise answers.".to_string(),
        confidence: 92,
    };

    let json = encode_json(&record).unwrap();
    assert_eq!(decode_json::<MemoryRecord>(&json).unwrap(), record);

    let cbor = encode_cbor(&record).unwrap();
    assert_eq!(decode_cbor::<MemoryRecord>(&cbor).unwrap(), record);
}

#[test]
fn value_codec_trait_round_trips_json_and_cbor_values() {
    let record = MemoryRecord {
        source: "conversation/c2".to_string(),
        content: "Codec objects can be passed around with schema owners.".to_string(),
        confidence: 91,
    };

    let json = JsonCodec;
    let json_bytes = json.encode(&record).unwrap();
    assert_eq!(json.decode::<MemoryRecord>(&json_bytes).unwrap(), record);

    let cbor = CborCodec;
    let cbor_bytes = cbor.encode(&record).unwrap();
    assert_eq!(cbor.decode::<MemoryRecord>(&cbor_bytes).unwrap(), record);
}

#[test]
fn versioned_value_envelope_can_be_stored_in_a_tree() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let tree = prolly.create();
    let record = MemoryRecord {
        source: "doc/readme".to_string(),
        content: "Prolly roots make snapshots reproducible.".to_string(),
        confidence: 100,
    };

    let value = VersionedValue::json("ai.memory.record", 1, &record)
        .unwrap()
        .to_bytes()
        .unwrap();
    let tree = prolly
        .put(&tree, b"memory/record/1".to_vec(), value)
        .unwrap();

    let stored = prolly
        .get(&tree, b"memory/record/1")
        .unwrap()
        .expect("value exists");
    let envelope = VersionedValue::from_bytes(&stored).unwrap();

    envelope.require_schema("ai.memory.record", 1).unwrap();
    assert_eq!(envelope.encoding, Encoding::Json);
    assert_eq!(envelope.decode_json::<MemoryRecord>().unwrap(), record);
    assert!(!envelope.matches_schema("ai.memory.record", 2));
}

#[test]
fn versioned_codecs_validate_schema_and_version_when_decoding() {
    let prolly = Prolly::new(MemStore::new(), Config::default());
    let tree = prolly.create();
    let codec = VersionedJsonCodec::new("ai.memory.record", 2);
    let record = MemoryRecord {
        source: "agent/run/1".to_string(),
        content: "Schema-checked codecs guard old roots.".to_string(),
        confidence: 97,
    };

    assert_eq!(codec.schema(), "ai.memory.record");
    assert_eq!(codec.version(), 2);

    let bytes = codec.encode(&record).unwrap();
    let tree = prolly
        .put(&tree, b"memory/record/2".to_vec(), bytes)
        .unwrap();
    let stored = prolly
        .get(&tree, b"memory/record/2")
        .unwrap()
        .expect("value exists");

    assert_eq!(codec.decode::<MemoryRecord>(&stored).unwrap(), record);

    let older_codec = VersionedJsonCodec::new("ai.memory.record", 1);
    let decoded: Result<MemoryRecord, _> = older_codec.decode(&stored);
    assert!(decoded.is_err());
}

#[test]
fn versioned_cbor_codec_round_trips_and_reports_expected_schema() {
    let codec = VersionedCborCodec::new("rag.chunk", 5);
    let record = MemoryRecord {
        source: "rag/index/current".to_string(),
        content: "Chunk metadata encoded with CBOR.".to_string(),
        confidence: 86,
    };

    assert_eq!(codec.schema(), "rag.chunk");
    assert_eq!(codec.version(), 5);

    let bytes = codec.encode(&record).unwrap();
    assert_eq!(codec.decode::<MemoryRecord>(&bytes).unwrap(), record);
}

#[test]
fn versioned_value_supports_cbor_and_custom_payloads() {
    let record = MemoryRecord {
        source: "rag/index/root".to_string(),
        content: "Chunk metadata".to_string(),
        confidence: 88,
    };

    let cbor = VersionedValue::cbor("rag.chunk", 3, &record).unwrap();
    assert_eq!(cbor.decode_cbor::<MemoryRecord>().unwrap(), record);

    let custom = VersionedValue::with_encoding(
        "vector.embedding",
        7,
        Encoding::Custom("f32-le-1536".to_string()),
        vec![1, 2, 3, 4],
    );
    let decoded = VersionedValue::from_bytes(&custom.to_bytes().unwrap()).unwrap();
    assert_eq!(decoded, custom);
}
