use super::super::error::Error;
use super::codec::{put_varint, Reader};
use super::vector::{canonicalize, decode_components, encode_components};

const MAGIC: &[u8; 4] = b"PRVR";
const VERSION: u8 = 1;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct StoredRecord {
    pub(crate) vector: Vec<f32>,
    pub(crate) value: Vec<u8>,
}

impl StoredRecord {
    pub(crate) fn new(vector: &[f32], value: Vec<u8>, dimensions: u32) -> Result<Self, Error> {
        Ok(Self {
            vector: canonicalize(vector, dimensions)?,
            value,
        })
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + self.vector.len() * 4 + self.value.len());
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        put_varint(self.vector.len() as u64, &mut bytes);
        encode_components(&self.vector, &mut bytes);
        put_varint(self.value.len() as u64, &mut bytes);
        bytes.extend_from_slice(&self.value);
        bytes
    }

    pub(crate) fn decode(bytes: &[u8], dimensions: u32) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "record");
        reader.exact(MAGIC)?;
        if reader.u8()? != VERSION {
            return Err(Error::InvalidProximityObject {
                kind: "record",
                reason: "unsupported version".to_owned(),
            });
        }
        let encoded_dimensions = reader.varint()?;
        if encoded_dimensions != u64::from(dimensions) {
            return Err(Error::InvalidProximityObject {
                kind: "record",
                reason: "dimension mismatch".to_owned(),
            });
        }
        let vector_bytes = usize::try_from(dimensions)
            .ok()
            .and_then(|value| value.checked_mul(4))
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "record",
                reason: "vector length overflow".to_owned(),
            })?;
        let vector = decode_components(reader.take(vector_bytes)?, dimensions)?;
        let value_len = reader.usize()?;
        let value = reader.take(value_len)?.to_vec();
        reader.finish()?;
        Ok(Self { vector, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::cid::Cid;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn record_round_trip_is_canonical_and_rejects_trailing_bytes() {
        let record = StoredRecord::new(&[-0.0, 2.5], b"value".to_vec(), 2).unwrap();
        let bytes = record.encode();
        assert_eq!(StoredRecord::decode(&bytes, 2).unwrap(), record);
        let mut trailing = bytes;
        trailing.push(0);
        assert!(StoredRecord::decode(&trailing, 2).is_err());
    }

    #[test]
    fn record_matches_checked_in_golden_object() {
        let record = StoredRecord::new(&[-0.0, 2.5], b"value".to_vec(), 2).unwrap();
        let bytes = record.encode();
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../conformance/proximity-fixtures.v1.json"
        ))
        .unwrap();
        let object = &fixture["objects"]["record"];

        assert_eq!(hex(&bytes), object["bytes"]);
        assert_eq!(hex(Cid::from_bytes(&bytes).as_bytes()), object["cid"]);
        assert_eq!(StoredRecord::decode(&bytes, 2).unwrap(), record);
    }
}
