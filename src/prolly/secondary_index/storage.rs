use serde::de::{DeserializeOwned, Error as DeError, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

use super::super::cid::Cid;
use super::super::error::Error;
use super::super::key::{
    decode_segments, encode_segment, encode_segment_prefix, prefix_end, KeyBuilder,
};
use super::super::versioned_map::{MapVersionId, VERSIONED_MAP_ROOT_PREFIX};
use super::definition::{IndexProjection, SecondaryIndex};

pub const SECONDARY_INDEX_FORMAT_VERSION: u32 = 1;
pub const INDEX_PHYSICAL_LAYOUT_VERSION: u32 = 1;

const DESCRIPTOR_MAGIC: &[u8; 4] = b"PSID";
const DESCRIPTOR_FINGERPRINT_MAGIC: &[u8; 4] = b"PSIF";
const CHECKPOINT_MAGIC: &[u8; 4] = b"PSIP";
const HEAD_MAGIC: &[u8; 4] = b"PSIH";
const CONTROL_MAGIC: &[u8; 4] = b"PSIO";
const INDEX_VALUE_MAGIC: &[u8; 4] = b"PSIV";
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
const NON_UNIQUE_MODE_TAG: u8 = 0;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ByteString(Vec<u8>);

impl Serialize for ByteString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bytes(&self.0)
    }
}

struct ByteStringVisitor;

impl<'de> Visitor<'de> for ByteStringVisitor {
    type Value = ByteString;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a CBOR byte string")
    }

    fn visit_bytes<E>(self, value: &[u8]) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(ByteString(value.to_vec()))
    }

    fn visit_byte_buf<E>(self, value: Vec<u8>) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(ByteString(value))
    }
}

impl<'de> Deserialize<'de> for ByteString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_byte_buf(ByteStringVisitor)
    }
}

fn encode_record<T: Serialize>(magic: &[u8; 4], wire: &T) -> Result<Vec<u8>, Error> {
    let payload = serde_cbor::to_vec(wire).map_err(|error| Error::Serialize(error.to_string()))?;
    let total =
        8usize
            .checked_add(payload.len())
            .ok_or_else(|| Error::IndexResourceLimitExceeded {
                resource: "record_bytes",
                limit: MAX_RECORD_BYTES,
                actual: usize::MAX,
            })?;
    if total > MAX_RECORD_BYTES {
        return Err(Error::IndexResourceLimitExceeded {
            resource: "record_bytes",
            limit: MAX_RECORD_BYTES,
            actual: total,
        });
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(magic);
    bytes.extend_from_slice(&SECONDARY_INDEX_FORMAT_VERSION.to_be_bytes());
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

fn decode_record<T: DeserializeOwned>(bytes: &[u8], magic: &[u8; 4]) -> Result<T, Error> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(Error::IndexResourceLimitExceeded {
            resource: "record_bytes",
            limit: MAX_RECORD_BYTES,
            actual: bytes.len(),
        });
    }
    if bytes.len() < 8 || &bytes[..4] != magic {
        return Err(Error::Deserialize(
            "invalid secondary-index record magic".to_string(),
        ));
    }
    let version = u32::from_be_bytes(bytes[4..8].try_into().expect("fixed header length"));
    if version != SECONDARY_INDEX_FORMAT_VERSION {
        return Err(Error::Deserialize(format!(
            "unsupported secondary-index record version {version}"
        )));
    }
    let mut decoder = serde_cbor::Deserializer::from_slice(&bytes[8..]);
    let wire =
        T::deserialize(&mut decoder).map_err(|error| Error::Deserialize(error.to_string()))?;
    decoder
        .end()
        .map_err(|error| Error::Deserialize(error.to_string()))?;
    Ok(wire)
}

fn cid_from_bytes(bytes: Vec<u8>, field: &str) -> Result<Cid, Error> {
    let array: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        Error::Deserialize(format!(
            "{field} must contain 32 bytes, got {}",
            bytes.len()
        ))
    })?;
    Ok(Cid(array))
}

fn projection_tag(projection: IndexProjection) -> u8 {
    match projection {
        IndexProjection::KeysOnly => 0,
        IndexProjection::Include => 1,
        IndexProjection::All => 2,
    }
}

fn projection_from_tag(tag: u8) -> Result<IndexProjection, Error> {
    match tag {
        0 => Ok(IndexProjection::KeysOnly),
        1 => Ok(IndexProjection::Include),
        2 => Ok(IndexProjection::All),
        _ => Err(Error::Deserialize(format!(
            "unknown index projection tag {tag}"
        ))),
    }
}

#[derive(Serialize)]
struct DescriptorFingerprintWire(ByteString, ByteString, u64, String, u8, u8, u32);

#[derive(Serialize, Deserialize)]
struct DescriptorWire(
    u32,
    ByteString,
    ByteString,
    u64,
    String,
    ByteString,
    u8,
    u32,
);

/// Canonical persisted semantic definition for one index generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexDescriptor {
    pub format_version: u32,
    pub source_map_id: Vec<u8>,
    pub name: Vec<u8>,
    pub generation: u64,
    pub extractor_id: String,
    pub fingerprint: Cid,
    pub projection: IndexProjection,
    pub physical_layout_version: u32,
}

impl SecondaryIndexDescriptor {
    pub fn from_runtime(
        source_map_id: impl AsRef<[u8]>,
        index: &SecondaryIndex,
    ) -> Result<Self, Error> {
        let mut descriptor = Self {
            format_version: SECONDARY_INDEX_FORMAT_VERSION,
            source_map_id: source_map_id.as_ref().to_vec(),
            name: index.name().to_vec(),
            generation: index.generation(),
            extractor_id: index.extractor_id().to_string(),
            fingerprint: Cid([0; 32]),
            projection: index.projection(),
            physical_layout_version: INDEX_PHYSICAL_LAYOUT_VERSION,
        };
        descriptor.fingerprint = descriptor_fingerprint(&descriptor)?;
        Ok(descriptor)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        encode_record(
            DESCRIPTOR_MAGIC,
            &DescriptorWire(
                self.format_version,
                ByteString(self.source_map_id.clone()),
                ByteString(self.name.clone()),
                self.generation,
                self.extractor_id.clone(),
                ByteString(self.fingerprint.as_bytes().to_vec()),
                projection_tag(self.projection),
                self.physical_layout_version,
            ),
        )
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let DescriptorWire(
            format_version,
            source_map_id,
            name,
            generation,
            extractor_id,
            fingerprint,
            projection,
            physical_layout_version,
        ) = decode_record(bytes, DESCRIPTOR_MAGIC)?;
        let descriptor = Self {
            format_version,
            source_map_id: source_map_id.0,
            name: name.0,
            generation,
            extractor_id,
            fingerprint: cid_from_bytes(fingerprint.0, "descriptor fingerprint")?,
            projection: projection_from_tag(projection)?,
            physical_layout_version,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.format_version != SECONDARY_INDEX_FORMAT_VERSION
            || self.physical_layout_version != INDEX_PHYSICAL_LAYOUT_VERSION
            || self.source_map_id.is_empty()
            || self.name.is_empty()
            || self.generation == 0
            || self.extractor_id.is_empty()
        {
            return Err(Error::InvalidIndexDefinition {
                reason: "persisted descriptor contains invalid required fields".to_string(),
            });
        }
        let expected = descriptor_fingerprint(self)?;
        if expected != self.fingerprint {
            return Err(Error::IndexDefinitionMismatch {
                name: self.name.clone(),
                persisted: self.fingerprint.clone(),
                runtime: expected,
            });
        }
        Ok(())
    }
}

/// Hash the canonical semantic descriptor envelope (excluding its fingerprint field).
pub fn descriptor_fingerprint(descriptor: &SecondaryIndexDescriptor) -> Result<Cid, Error> {
    let bytes = encode_record(
        DESCRIPTOR_FINGERPRINT_MAGIC,
        &DescriptorFingerprintWire(
            ByteString(descriptor.source_map_id.clone()),
            ByteString(descriptor.name.clone()),
            descriptor.generation,
            descriptor.extractor_id.clone(),
            NON_UNIQUE_MODE_TAG,
            projection_tag(descriptor.projection),
            descriptor.physical_layout_version,
        ),
    )?;
    Ok(Cid::from_bytes(&bytes))
}

#[derive(Serialize, Deserialize)]
struct CheckpointWire(
    ByteString,
    ByteString,
    ByteString,
    u64,
    ByteString,
    ByteString,
    ByteString,
);

/// Exact hidden-index version selected for one source version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexCheckpoint {
    pub source_map_id: Vec<u8>,
    pub source_version: MapVersionId,
    pub index_name: Vec<u8>,
    pub generation: u64,
    pub definition_fingerprint: Cid,
    pub index_map_id: Vec<u8>,
    pub index_version: MapVersionId,
}

impl IndexCheckpoint {
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        encode_record(
            CHECKPOINT_MAGIC,
            &CheckpointWire(
                ByteString(self.source_map_id.clone()),
                ByteString(self.source_version.as_cid().as_bytes().to_vec()),
                ByteString(self.index_name.clone()),
                self.generation,
                ByteString(self.definition_fingerprint.as_bytes().to_vec()),
                ByteString(self.index_map_id.clone()),
                ByteString(self.index_version.as_cid().as_bytes().to_vec()),
            ),
        )
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let CheckpointWire(
            source_map_id,
            source_version,
            index_name,
            generation,
            fingerprint,
            index_map_id,
            index_version,
        ) = decode_record(bytes, CHECKPOINT_MAGIC)?;
        let checkpoint = Self {
            source_map_id: source_map_id.0,
            source_version: MapVersionId::from_cid(cid_from_bytes(
                source_version.0,
                "source version",
            )?),
            index_name: index_name.0,
            generation,
            definition_fingerprint: cid_from_bytes(fingerprint.0, "definition fingerprint")?,
            index_map_id: index_map_id.0,
            index_version: MapVersionId::from_cid(cid_from_bytes(
                index_version.0,
                "index version",
            )?),
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.source_map_id.is_empty()
            || self.index_name.is_empty()
            || self.generation == 0
            || self.index_map_id
                != index_map_id(
                    &self.source_map_id,
                    &self.index_name,
                    &self.definition_fingerprint,
                )
        {
            return Err(Error::Deserialize(
                "checkpoint contains invalid required fields or index ownership".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct IndexedHeadWire(ByteString, Vec<ByteString>);

/// Canonical current selection of one source version and all active index checkpoints.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedHeadRecord {
    pub source_version: MapVersionId,
    pub indexes: Vec<IndexCheckpoint>,
}

impl IndexedHeadRecord {
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let checkpoints = self
            .indexes
            .iter()
            .map(IndexCheckpoint::to_bytes)
            .collect::<Result<Vec<_>, _>>()?;
        encode_record(
            HEAD_MAGIC,
            &IndexedHeadWire(
                ByteString(self.source_version.as_cid().as_bytes().to_vec()),
                checkpoints.into_iter().map(ByteString).collect(),
            ),
        )
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let IndexedHeadWire(source_version, checkpoints) = decode_record(bytes, HEAD_MAGIC)?;
        let indexes = checkpoints
            .into_iter()
            .map(|bytes| IndexCheckpoint::from_bytes(&bytes.0))
            .collect::<Result<Vec<_>, _>>()?;
        let record = Self {
            source_version: MapVersionId::from_cid(cid_from_bytes(
                source_version.0,
                "source version",
            )?),
            indexes,
        };
        record.validate()?;
        Ok(record)
    }

    fn validate(&self) -> Result<(), Error> {
        validate_checkpoint_order(&self.indexes)?;
        if self
            .indexes
            .iter()
            .any(|checkpoint| checkpoint.source_version != self.source_version)
        {
            return Err(Error::Deserialize(
                "active checkpoint source version does not match indexed head".to_string(),
            ));
        }
        for checkpoint in &self.indexes {
            checkpoint.validate()?;
        }
        Ok(())
    }
}

fn validate_checkpoint_order(checkpoints: &[IndexCheckpoint]) -> Result<(), Error> {
    if checkpoints
        .windows(2)
        .any(|pair| pair[0].index_name >= pair[1].index_name)
    {
        return Err(Error::Deserialize(
            "active checkpoints must be strictly sorted by index name".to_string(),
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveIndexControl {
    pub name: Vec<u8>,
    pub fingerprint: Cid,
}

#[derive(Serialize, Deserialize)]
struct ActiveControlWire(ByteString, ByteString);

#[derive(Serialize, Deserialize)]
struct ControlWire(ByteString, ByteString, Vec<ActiveControlWire>);

/// Small deterministic root that fences all raw source-map mutations.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexControl {
    pub source_map_id: Vec<u8>,
    pub catalog_map_id: Vec<u8>,
    pub active: Vec<ActiveIndexControl>,
}

impl IndexControl {
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        encode_record(
            CONTROL_MAGIC,
            &ControlWire(
                ByteString(self.source_map_id.clone()),
                ByteString(self.catalog_map_id.clone()),
                self.active
                    .iter()
                    .map(|entry| {
                        ActiveControlWire(
                            ByteString(entry.name.clone()),
                            ByteString(entry.fingerprint.as_bytes().to_vec()),
                        )
                    })
                    .collect(),
            ),
        )
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let ControlWire(source_map_id, catalog_map_id, active) =
            decode_record(bytes, CONTROL_MAGIC)?;
        let control = Self {
            source_map_id: source_map_id.0,
            catalog_map_id: catalog_map_id.0,
            active: active
                .into_iter()
                .map(|ActiveControlWire(name, fingerprint)| {
                    Ok(ActiveIndexControl {
                        name: name.0,
                        fingerprint: cid_from_bytes(fingerprint.0, "control fingerprint")?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?,
        };
        control.validate()?;
        Ok(control)
    }

    pub fn fingerprint(&self) -> Result<Cid, Error> {
        Ok(Cid::from_bytes(&self.to_bytes()?))
    }

    fn validate(&self) -> Result<(), Error> {
        if self.source_map_id.is_empty()
            || self.catalog_map_id != catalog_map_id(&self.source_map_id)
            || self.active.is_empty()
        {
            return Err(Error::Deserialize(
                "invalid secondary-index control record".to_string(),
            ));
        }
        if self
            .active
            .windows(2)
            .any(|pair| pair[0].name >= pair[1].name)
            || self.active.iter().any(|entry| entry.name.is_empty())
        {
            return Err(Error::Deserialize(
                "active control entries must be non-empty and strictly sorted".to_string(),
            ));
        }
        Ok(())
    }
}

/// Canonical physical value stored in a secondary-index tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexValue {
    KeysOnly,
    Included(Vec<u8>),
    FullSource(Vec<u8>),
}

impl IndexValue {
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let (tag, payload) = match self {
            Self::KeysOnly => return Ok(Vec::new()),
            Self::Included(payload) => (1, payload),
            Self::FullSource(payload) => (2, payload),
        };
        let payload_len =
            u32::try_from(payload.len()).map_err(|_| Error::IndexResourceLimitExceeded {
                resource: "projection_bytes",
                limit: u32::MAX as usize,
                actual: payload.len(),
            })?;
        let mut bytes = Vec::with_capacity(13usize.saturating_add(payload.len()));
        bytes.extend_from_slice(INDEX_VALUE_MAGIC);
        bytes.extend_from_slice(&SECONDARY_INDEX_FORMAT_VERSION.to_be_bytes());
        bytes.push(tag);
        bytes.extend_from_slice(&payload_len.to_be_bytes());
        bytes.extend_from_slice(payload);
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8], max_payload_bytes: usize) -> Result<Self, Error> {
        if bytes.is_empty() {
            return Ok(Self::KeysOnly);
        }
        if bytes.len() < 13 || &bytes[..4] != INDEX_VALUE_MAGIC {
            return Err(Error::Deserialize(
                "invalid secondary-index value envelope".to_string(),
            ));
        }
        let version = u32::from_be_bytes(bytes[4..8].try_into().expect("fixed header length"));
        if version != SECONDARY_INDEX_FORMAT_VERSION {
            return Err(Error::Deserialize(format!(
                "unsupported secondary-index value version {version}"
            )));
        }
        let payload_len =
            u32::from_be_bytes(bytes[9..13].try_into().expect("fixed value header length"))
                as usize;
        if payload_len > max_payload_bytes {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "projection_bytes",
                limit: max_payload_bytes,
                actual: payload_len,
            });
        }
        if bytes.len() != 13usize.saturating_add(payload_len) {
            return Err(Error::Deserialize(
                "secondary-index value length mismatch or trailing bytes".to_string(),
            ));
        }
        let payload = bytes[13..].to_vec();
        match bytes[8] {
            1 => Ok(Self::Included(payload)),
            2 => Ok(Self::FullSource(payload)),
            tag => Err(Error::Deserialize(format!(
                "unknown secondary-index value tag {tag}"
            ))),
        }
    }
}

pub fn catalog_map_id(source_map_id: impl AsRef<[u8]>) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("system")
        .push_str("secondary-index-catalog")
        .push_segment(source_map_id)
        .finish()
}

pub fn index_map_id(
    source_map_id: impl AsRef<[u8]>,
    index_name: impl AsRef<[u8]>,
    fingerprint: &Cid,
) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("system")
        .push_str("secondary-index")
        .push_segment(source_map_id)
        .push_segment(index_name)
        .push_segment(fingerprint.as_bytes())
        .finish()
}

pub fn control_root_name(source_map_id: impl AsRef<[u8]>) -> Vec<u8> {
    let source_map_id = source_map_id.as_ref();
    let mut name = VERSIONED_MAP_ROOT_PREFIX.to_vec();
    append_hex(&mut name, source_map_id);
    name.extend_from_slice(b"/secondary-index-control");
    name
}

pub fn control_record_key() -> Vec<u8> {
    KeyBuilder::new().push_str("control").finish()
}

fn append_hex(output: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.reserve(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize]);
        output.push(HEX[(byte & 0x0f) as usize]);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DecodedPhysicalIndexKey {
    pub term: Vec<u8>,
    pub primary_key: Vec<u8>,
}

pub fn physical_index_key(term: &[u8], primary_key: &[u8]) -> Result<Vec<u8>, Error> {
    let capacity = term
        .len()
        .checked_mul(2)
        .and_then(|size| size.checked_add(primary_key.len().saturating_mul(2)))
        .and_then(|size| size.checked_add(4))
        .ok_or(Error::IndexResourceLimitExceeded {
            resource: "physical_key_bytes",
            limit: usize::MAX,
            actual: usize::MAX,
        })?;
    let mut key = Vec::with_capacity(capacity);
    key.extend_from_slice(&encode_segment(term));
    key.extend_from_slice(&encode_segment(primary_key));
    Ok(key)
}

pub fn decode_physical_index_key(key: &[u8]) -> Result<DecodedPhysicalIndexKey, Error> {
    let segments = decode_segments(key).map_err(|error| Error::Deserialize(error.to_string()))?;
    let [term, primary_key]: [Vec<u8>; 2] =
        segments.try_into().map_err(|segments: Vec<Vec<u8>>| {
            Error::Deserialize(format!(
                "physical secondary-index key must contain two segments, got {}",
                segments.len()
            ))
        })?;
    Ok(DecodedPhysicalIndexKey { term, primary_key })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TermBounds {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

pub fn term_bounds_exact(term: &[u8]) -> TermBounds {
    let start = encode_segment(term);
    let end = prefix_end(&start);
    TermBounds { start, end }
}

pub fn term_bounds_prefix(prefix: &[u8]) -> TermBounds {
    let start = encode_segment_prefix(prefix);
    let end = prefix_end(&start);
    TermBounds { start, end }
}

pub fn term_bounds_range(start_term: &[u8], end_term: Option<&[u8]>) -> Result<TermBounds, Error> {
    if end_term.is_some_and(|end| start_term > end) {
        return Err(Error::InvalidIndexDefinition {
            reason: "secondary-index term range start exceeds end".to_string(),
        });
    }
    Ok(TermBounds {
        start: encode_segment(start_term),
        end: end_term.map(encode_segment),
    })
}

pub fn catalog_format_key() -> Vec<u8> {
    KeyBuilder::new().push_str("format").finish()
}

pub fn catalog_current_key() -> Vec<u8> {
    KeyBuilder::new().push_str("current").finish()
}

pub fn catalog_descriptor_key(name: &[u8], generation: u64) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("definitions")
        .push_segment(name)
        .push_u64(generation)
        .finish()
}

pub fn catalog_checkpoint_key(
    source_version: &MapVersionId,
    name: &[u8],
    generation: u64,
) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("checkpoints")
        .push_segment(source_version.as_cid().as_bytes())
        .push_segment(name)
        .push_u64(generation)
        .finish()
}

/// Prefix containing every source-version checkpoint record.
pub fn catalog_checkpoints_prefix() -> Vec<u8> {
    KeyBuilder::new().push_str("checkpoints").finish()
}

pub fn catalog_retired_key(name: &[u8], generation: u64) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("retired")
        .push_segment(name)
        .push_u64(generation)
        .finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::cid::Cid;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn physical_keys_round_trip_arbitrary_bytes() {
        let key = physical_index_key(&[0, b'a'], &[b'u', 0, 0xff]).unwrap();
        assert_eq!(hex(&key), "00ff6100007500ffff0000");
        let decoded = decode_physical_index_key(&key).unwrap();
        assert_eq!(decoded.term, vec![0, b'a']);
        assert_eq!(decoded.primary_key, vec![b'u', 0, 0xff]);
        assert!(decode_physical_index_key(&[key, vec![1, 0, 0]].concat()).is_err());
    }

    #[test]
    fn projection_values_are_versioned_and_canonical() {
        assert_eq!(IndexValue::KeysOnly.to_bytes().unwrap(), Vec::<u8>::new());
        let encoded = IndexValue::Included(b"Ada".to_vec()).to_bytes().unwrap();
        assert_eq!(hex(&encoded), "50534956000000010100000003416461");
        assert_eq!(
            IndexValue::from_bytes(&encoded, 1024).unwrap(),
            IndexValue::Included(b"Ada".to_vec())
        );
        assert!(IndexValue::from_bytes(&[encoded, vec![0]].concat(), 1024).is_err());
        assert!(
            IndexValue::from_bytes(&IndexValue::FullSource(vec![7; 5]).to_bytes().unwrap(), 4)
                .is_err()
        );
    }

    #[test]
    fn hidden_ids_and_term_bounds_are_segment_safe() {
        let fingerprint = Cid([7; 32]);
        let source = b"users\0prod";
        let catalog = catalog_map_id(source);
        let index = index_map_id(source, b"by\0tag", &fingerprint);
        assert_ne!(catalog, index);
        assert_eq!(
            crate::prolly::key::decode_segments(&catalog).unwrap()[2],
            source
        );
        assert_eq!(
            crate::prolly::key::decode_segments(&index).unwrap()[3],
            b"by\0tag"
        );

        let exact = term_bounds_exact(b"a\0");
        assert!(physical_index_key(b"a\0", b"pk").unwrap() >= exact.start);
        assert!(physical_index_key(b"a\0x", b"pk").unwrap() >= exact.end.unwrap());
        let prefix = term_bounds_prefix(b"a\0");
        assert!(physical_index_key(b"a\0x", b"pk").unwrap() < prefix.end.unwrap());
    }

    #[test]
    fn control_record_has_fixed_canonical_bytes() {
        let control = IndexControl {
            source_map_id: b"u".to_vec(),
            catalog_map_id: catalog_map_id(b"u"),
            active: vec![ActiveIndexControl {
                name: b"i".to_vec(),
                fingerprint: Cid([0; 32]),
            }],
        };
        let bytes = control.to_bytes().unwrap();
        assert_eq!(
            hex(&bytes),
            "5053494f00000001834175582473797374656d00007365636f6e646172792d696e6465782d636174616c6f6700007500008182416958200000000000000000000000000000000000000000000000000000000000000000"
        );
        assert_eq!(IndexControl::from_bytes(&bytes).unwrap(), control);
        assert!(IndexControl::from_bytes(&[bytes, vec![0]].concat()).is_err());
    }

    #[test]
    fn descriptor_fingerprint_and_bytes_are_canonical() {
        let runtime =
            SecondaryIndex::non_unique("i", 1, "x/v1", |_, _| Ok(Vec::<Vec<u8>>::new())).unwrap();
        let descriptor = SecondaryIndexDescriptor::from_runtime(b"u", &runtime).unwrap();
        assert_eq!(
            descriptor.fingerprint,
            descriptor_fingerprint(&descriptor).unwrap()
        );
        let bytes = descriptor.to_bytes().unwrap();
        assert_eq!(
            hex(&bytes),
            "50534944000000018801417541690164782f76315820000a70517a0f9edfd0338318cb726a310dff9f0e7df0324eebf441f365ded3730001"
        );
        assert_eq!(
            SecondaryIndexDescriptor::from_bytes(&bytes).unwrap(),
            descriptor
        );
        assert!(SecondaryIndexDescriptor::from_bytes(&[bytes, vec![0]].concat()).is_err());
    }

    #[test]
    fn checkpoints_and_heads_round_trip_and_require_sorted_names() {
        let checkpoint = IndexCheckpoint {
            source_map_id: b"u".to_vec(),
            source_version: MapVersionId::from_cid(Cid([1; 32])),
            index_name: b"i".to_vec(),
            generation: 1,
            definition_fingerprint: Cid([2; 32]),
            index_map_id: index_map_id(b"u", b"i", &Cid([2; 32])),
            index_version: MapVersionId::from_cid(Cid([3; 32])),
        };
        let bytes = checkpoint.to_bytes().unwrap();
        assert_eq!(IndexCheckpoint::from_bytes(&bytes).unwrap(), checkpoint);
        let mut invalid_checkpoint = checkpoint.clone();
        invalid_checkpoint.generation = 0;
        assert!(invalid_checkpoint.to_bytes().is_err());

        let head = IndexedHeadRecord {
            source_version: checkpoint.source_version.clone(),
            indexes: vec![checkpoint.clone()],
        };
        let bytes = head.to_bytes().unwrap();
        assert_eq!(IndexedHeadRecord::from_bytes(&bytes).unwrap(), head);

        let mismatched = IndexedHeadRecord {
            source_version: MapVersionId::from_cid(Cid([8; 32])),
            indexes: vec![checkpoint.clone()],
        };
        assert!(mismatched.to_bytes().is_err());

        let mut duplicate = checkpoint;
        duplicate.generation = 2;
        let invalid = IndexedHeadRecord {
            source_version: duplicate.source_version.clone(),
            indexes: vec![duplicate.clone(), duplicate],
        };
        assert!(invalid.to_bytes().is_err());
    }

    #[test]
    fn catalog_keys_are_unambiguous_segments() {
        let version = MapVersionId::from_cid(Cid([9; 32]));
        assert_eq!(
            decode_segments(&catalog_format_key()).unwrap(),
            vec![b"format".to_vec()]
        );
        assert_eq!(
            decode_segments(&catalog_current_key()).unwrap(),
            vec![b"current".to_vec()]
        );
        assert_eq!(
            decode_segments(&catalog_descriptor_key(b"i\0", 7)).unwrap()[1],
            b"i\0"
        );
        assert_eq!(
            decode_segments(&catalog_checkpoint_key(&version, b"i", 7)).unwrap()[0],
            b"checkpoints"
        );
        assert_eq!(
            decode_segments(&catalog_retired_key(b"i", 7)).unwrap()[0],
            b"retired"
        );
    }
}
