//! Logical deletion value helpers.
//!
//! A tombstone is an ordinary value envelope that represents a delete without
//! physically removing the key immediately. This is useful for local-first sync,
//! peer replication, audit trails, and deferred compaction.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::error::{Error, Mutation};

const TOMBSTONE_MAGIC: &[u8; 4] = b"PLTD";
const TOMBSTONE_WIRE_VERSION: u8 = 1;
const HEADER_LEN: usize = 4 + 1 + 8 + 4 + 4;
const TIMESTAMP_OFFSET: usize = 5;
const ACTOR_LEN_OFFSET: usize = 13;
const METADATA_COUNT_OFFSET: usize = 17;
const METADATA_ENTRY_HEADER_LEN: usize = 4 + 8;

/// Logical deletion value with causal metadata.
///
/// The actor identifies who or what performed the delete. The timestamp is a
/// caller-provided Unix millisecond value. Causal metadata is sorted by key when
/// encoded, making the byte representation deterministic.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tombstone {
    /// Actor that created the tombstone.
    pub actor: Vec<u8>,
    /// Unix timestamp in milliseconds supplied by the caller.
    pub timestamp_millis: u64,
    /// Application-defined causal metadata.
    pub causal_metadata: BTreeMap<String, Vec<u8>>,
}

impl Tombstone {
    /// Create a tombstone with no causal metadata.
    pub fn new(actor: impl Into<Vec<u8>>, timestamp_millis: u64) -> Self {
        Self {
            actor: actor.into(),
            timestamp_millis,
            causal_metadata: BTreeMap::new(),
        }
    }

    /// Add causal metadata and return the tombstone for chaining.
    pub fn with_causal_metadata(
        mut self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Self {
        self.insert_causal_metadata(key, value);
        self
    }

    /// Insert or replace one causal metadata entry.
    pub fn insert_causal_metadata(
        &mut self,
        key: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> &mut Self {
        self.causal_metadata.insert(key.into(), value.into());
        self
    }

    /// Borrow one causal metadata value.
    pub fn causal_metadata(&self, key: &str) -> Option<&[u8]> {
        self.causal_metadata.get(key).map(Vec::as_slice)
    }

    /// Encode this tombstone as deterministic bytes suitable for a leaf value.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let actor_len = checked_u32_len(self.actor.len(), "actor")?;
        let metadata_count = checked_u32_len(self.causal_metadata.len(), "causal metadata count")?;

        let mut out = Vec::with_capacity(
            HEADER_LEN
                .saturating_add(self.actor.len())
                .saturating_add(metadata_encoded_len(&self.causal_metadata)),
        );
        out.extend_from_slice(TOMBSTONE_MAGIC);
        out.push(TOMBSTONE_WIRE_VERSION);
        out.extend_from_slice(&self.timestamp_millis.to_be_bytes());
        out.extend_from_slice(&actor_len.to_be_bytes());
        out.extend_from_slice(&metadata_count.to_be_bytes());
        out.extend_from_slice(&self.actor);

        for (key, value) in &self.causal_metadata {
            let key_bytes = key.as_bytes();
            let key_len = checked_u32_len(key_bytes.len(), "causal metadata key")?;
            let value_len = checked_u64_len(value.len(), "causal metadata value")?;
            out.extend_from_slice(&key_len.to_be_bytes());
            out.extend_from_slice(&value_len.to_be_bytes());
            out.extend_from_slice(key_bytes);
            out.extend_from_slice(value);
        }

        Ok(out)
    }

    /// Decode a tombstone envelope.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        decode_tombstone(bytes)
    }

    /// Decode stored bytes, returning `Ok(None)` for non-tombstone values.
    ///
    /// Values that start with the tombstone magic but contain an invalid
    /// tombstone envelope return an error.
    pub fn from_stored_bytes(bytes: &[u8]) -> Result<Option<Self>, Error> {
        if is_tombstone_value(bytes) {
            Self::from_bytes(bytes).map(Some)
        } else {
            Ok(None)
        }
    }

    /// Return true if bytes start with the tombstone magic prefix.
    pub fn is_encoded(bytes: &[u8]) -> bool {
        is_tombstone_value(bytes)
    }

    /// Build an upsert mutation that stores this tombstone at `key`.
    pub fn to_upsert_mutation(&self, key: impl Into<Vec<u8>>) -> Result<Mutation, Error> {
        tombstone_upsert(key, self)
    }
}

/// Return true if bytes start with the tombstone magic prefix.
pub fn is_tombstone_value(bytes: &[u8]) -> bool {
    bytes.starts_with(TOMBSTONE_MAGIC)
}

/// Build an upsert mutation that stores `tombstone` at `key`.
pub fn tombstone_upsert(key: impl Into<Vec<u8>>, tombstone: &Tombstone) -> Result<Mutation, Error> {
    Ok(Mutation::Upsert {
        key: key.into(),
        val: tombstone.to_bytes()?,
    })
}

/// Return a physical delete mutation when `stored_value` is a tombstone.
///
/// This is the small compaction primitive: scan a retained range, detect
/// tombstone values, then batch the returned delete mutations once the
/// application knows the tombstone no longer needs to be replicated.
pub fn tombstone_compaction(
    key: impl Into<Vec<u8>>,
    stored_value: &[u8],
) -> Result<Option<Mutation>, Error> {
    match Tombstone::from_stored_bytes(stored_value)? {
        Some(_) => Ok(Some(Mutation::Delete { key: key.into() })),
        None => Ok(None),
    }
}

fn decode_tombstone(bytes: &[u8]) -> Result<Tombstone, Error> {
    if bytes.len() < HEADER_LEN {
        return Err(invalid_tombstone("tombstone envelope is too short"));
    }
    if !bytes.starts_with(TOMBSTONE_MAGIC) {
        return Err(invalid_tombstone("tombstone missing PLTD magic"));
    }

    let wire_version = bytes[4];
    if wire_version != TOMBSTONE_WIRE_VERSION {
        return Err(invalid_tombstone(format!(
            "unsupported tombstone wire version {wire_version}"
        )));
    }

    let timestamp_millis = read_u64(bytes, TIMESTAMP_OFFSET)?;
    let actor_len = read_u32(bytes, ACTOR_LEN_OFFSET)? as usize;
    let metadata_count = read_u32(bytes, METADATA_COUNT_OFFSET)? as usize;
    let actor_end = checked_add(HEADER_LEN, actor_len, "actor")?;
    if actor_end > bytes.len() {
        return Err(invalid_tombstone("actor length exceeds envelope length"));
    }

    let actor = bytes[HEADER_LEN..actor_end].to_vec();
    let mut offset = actor_end;
    let mut causal_metadata = BTreeMap::new();

    for _ in 0..metadata_count {
        let entry_header_end = checked_add(offset, METADATA_ENTRY_HEADER_LEN, "metadata header")?;
        if entry_header_end > bytes.len() {
            return Err(invalid_tombstone("metadata entry header is truncated"));
        }

        let key_len = read_u32(bytes, offset)? as usize;
        let value_len = usize::try_from(read_u64(bytes, offset + 4)?)
            .map_err(|_| invalid_tombstone("metadata value length does not fit in usize"))?;
        offset = entry_header_end;

        let key_end = checked_add(offset, key_len, "metadata key")?;
        let value_end = checked_add(key_end, value_len, "metadata value")?;
        if value_end > bytes.len() {
            return Err(invalid_tombstone(
                "metadata key/value length exceeds envelope length",
            ));
        }

        let key = decode_utf8(&bytes[offset..key_end], "metadata key")?;
        let value = bytes[key_end..value_end].to_vec();
        if causal_metadata.insert(key, value).is_some() {
            return Err(invalid_tombstone("duplicate metadata key"));
        }
        offset = value_end;
    }

    if offset != bytes.len() {
        return Err(invalid_tombstone(format!(
            "tombstone has {} trailing bytes",
            bytes.len() - offset
        )));
    }

    Ok(Tombstone {
        actor,
        timestamp_millis,
        causal_metadata,
    })
}

fn metadata_encoded_len(metadata: &BTreeMap<String, Vec<u8>>) -> usize {
    metadata.iter().fold(0usize, |len, (key, value)| {
        len.saturating_add(METADATA_ENTRY_HEADER_LEN)
            .saturating_add(key.len())
            .saturating_add(value.len())
    })
}

fn checked_u32_len(len: usize, field: &str) -> Result<u32, Error> {
    u32::try_from(len).map_err(|_| invalid_tombstone(format!("{field} is too large")))
}

fn checked_u64_len(len: usize, field: &str) -> Result<u64, Error> {
    u64::try_from(len).map_err(|_| invalid_tombstone(format!("{field} is too large")))
}

fn checked_add(base: usize, len: usize, field: &str) -> Result<usize, Error> {
    base.checked_add(len)
        .ok_or_else(|| invalid_tombstone(format!("{field} length overflows usize")))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, Error> {
    let value = bytes
        .get(offset..offset + 4)
        .ok_or_else(|| invalid_tombstone("tombstone header is truncated"))?;
    Ok(u32::from_be_bytes(
        value.try_into().expect("fixed slice length"),
    ))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, Error> {
    let value = bytes
        .get(offset..offset + 8)
        .ok_or_else(|| invalid_tombstone("tombstone header is truncated"))?;
    Ok(u64::from_be_bytes(
        value.try_into().expect("fixed slice length"),
    ))
}

fn decode_utf8(bytes: &[u8], field: &str) -> Result<String, Error> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|err| invalid_tombstone(format!("{field} is not valid UTF-8: {err}")))
}

fn invalid_tombstone(message: impl Into<String>) -> Error {
    Error::Deserialize(format!("invalid tombstone: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tombstone_round_trips_and_encodes_metadata_deterministically() {
        let left = Tombstone::new(b"agent-a".to_vec(), 1_700_000)
            .with_causal_metadata("right-root", b"r1".to_vec())
            .with_causal_metadata("left-root", b"l1".to_vec());
        let right = Tombstone::new(b"agent-a".to_vec(), 1_700_000)
            .with_causal_metadata("left-root", b"l1".to_vec())
            .with_causal_metadata("right-root", b"r1".to_vec());

        assert_eq!(left.to_bytes().unwrap(), right.to_bytes().unwrap());

        let decoded = Tombstone::from_bytes(&left.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, left);
        assert_eq!(decoded.causal_metadata("left-root"), Some(b"l1".as_slice()));
    }

    #[test]
    fn stored_bytes_distinguish_non_tombstones_from_invalid_tombstones() {
        assert_eq!(Tombstone::from_stored_bytes(b"value").unwrap(), None);
        assert!(matches!(
            Tombstone::from_stored_bytes(b"PLTD"),
            Err(Error::Deserialize(_))
        ));
    }

    #[test]
    fn compaction_returns_delete_only_for_tombstones() {
        let tombstone = Tombstone::new(b"actor".to_vec(), 42);
        let mutation = tombstone_compaction(b"k".to_vec(), &tombstone.to_bytes().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(mutation, Mutation::Delete { key: b"k".to_vec() });

        assert_eq!(tombstone_compaction(b"k".to_vec(), b"value").unwrap(), None);
    }
}
