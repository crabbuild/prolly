use serde::{de::DeserializeOwned, Serialize};
use serde_cbor::Value;
use sha2::{Digest, Sha256};

use crate::{Error, ErrorCode, Result};

/// Encode the Prolly S3  wire profile.
///
/// Rust field and variant names are first replaced with their stable numeric
/// indices by serde's packed representation. The intermediate CBOR value is
/// then serialized again so every map, including caller-provided metadata,
/// uses deterministic CBOR key ordering. Persisted protocol types intentionally
/// contain no floating-point values, tags, or negative integers.
pub fn encode_canonical<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let packed = serde_cbor::ser::to_vec_packed(value)
        .map_err(|error| Error::serialization(format!("canonical encode failed: {error}")))?;
    let value: Value = serde_cbor::from_slice(&packed)
        .map_err(|error| Error::serialization(format!("canonical value decode failed: {error}")))?;
    validate_wire_value(&value)?;
    serde_cbor::to_vec(&value)
        .map_err(|error| Error::serialization(format!("canonical value encode failed: {error}")))
}

/// Decode and reject alternate encodings of the same value.
pub fn decode_canonical<T>(bytes: &[u8]) -> Result<T>
where
    T: DeserializeOwned + Serialize,
{
    let value: T = serde_cbor::from_slice(bytes).map_err(|error| {
        Error::new(
            ErrorCode::CorruptCommit,
            format!("canonical decode failed: {error}"),
        )
    })?;
    let encoded = encode_canonical(&value)?;
    if encoded != bytes {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "noncanonical CBOR encoding",
        ));
    }
    Ok(value)
}

fn validate_wire_value(value: &Value) -> Result<()> {
    match value {
        Value::Integer(value) if *value < 0 => Err(Error::serialization(
            "Prolly S3 forbids negative CBOR integers",
        )),
        Value::Float(_) => Err(Error::serialization(
            "Prolly S3 forbids CBOR floating-point values",
        )),
        Value::Tag(_, _) => Err(Error::serialization("Prolly S3 forbids CBOR semantic tags")),
        Value::Array(values) => {
            for value in values {
                validate_wire_value(value)?;
            }
            Ok(())
        }
        Value::Map(values) => {
            for (key, value) in values {
                validate_wire_value(key)?;
                validate_wire_value(value)?;
            }
            Ok(())
        }
        Value::__Hidden => Err(Error::serialization(
            "Prolly S3 encountered an unsupported CBOR value",
        )),
        _ => Ok(()),
    }
}

pub(crate) fn domain_hash(domain: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u32).to_be_bytes());
    hasher.update(domain);
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    hasher.finalize().into()
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde::Serialize;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, serde::Deserialize)]
    struct Fixture {
        version: u16,
        metadata: BTreeMap<String, Vec<u8>>,
    }

    #[test]
    fn canonical_maps_use_cbor_key_order_not_rust_string_order() {
        let fixture = Fixture {
            version: 1,
            metadata: BTreeMap::from([("aa".to_string(), vec![2]), ("z".to_string(), vec![1])]),
        };
        let encoded = encode_canonical(&fixture).unwrap();
        let decoded: Value = serde_cbor::from_slice(&encoded).unwrap();
        let Value::Map(root) = decoded else {
            panic!("fixture must encode as a map");
        };
        let Value::Map(metadata) = root.get(&Value::Integer(1)).unwrap() else {
            panic!("metadata must encode as a map");
        };
        assert_eq!(
            metadata.keys().collect::<Vec<_>>(),
            vec![&Value::Text("z".into()), &Value::Text("aa".into())]
        );
        assert_eq!(decode_canonical::<Fixture>(&encoded).unwrap(), fixture);
    }

    #[test]
    fn decoder_rejects_a_different_map_order() {
        // {0: 1, 1: {"aa": [2], "z": [1]}} with the inner text keys in
        // noncanonical order ("z"has the shorter encoded key).
        let noncanonical = hex::decode("a2000101a26261618102617a8101").unwrap();
        assert_eq!(
            decode_canonical::<Fixture>(&noncanonical).unwrap_err().code,
            ErrorCode::CorruptCommit
        );
    }
}
